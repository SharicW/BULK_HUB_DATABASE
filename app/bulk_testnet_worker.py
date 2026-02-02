import os
import time
import signal
import logging
import shutil

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from app.stats import insert_bulk_testnet_rows
from app.stats import _make_driver, _clean_spaces


BULK_TESTNET_URL = "https://early.bulk.trade/"
BULK_MARKETS = ["BTC-USD", "ETH-USD", "SOL-USD"]

# NOTE:
# Funding/Countdown block is a nested structure:
#   <h4>Funding/Countdown</h4>
#   <div class="flex items-center ..."><span>0.0008%</span><span>00:51:12</span></div>
# Old xpath "//h4[contains(., 'Funding')]/ancestor::div[1]//span[1]" could pick header text
# or the wrong wrapper depending on render timing.
BULK_XPATHS = {
    "oracle_price": "//h4[contains(., 'Oracle Price')]/parent::div//span",
    "volume_24h": "//h4[contains(., '24h Volume')]/parent::div//span",
    "open_interest": "//h4[contains(., 'Open Interest')]/parent::div//span",
    "funding": (
        "//h4[contains(normalize-space(.), 'Funding/Countdown') or contains(normalize-space(.), 'Funding')]"
        "/ancestor::div[contains(@class,'flex-col')][1]"
        "//div[contains(@class,'items-center')]//span[1]"
    ),
    "countdown": (
        "//h4[contains(normalize-space(.), 'Funding/Countdown') or contains(normalize-space(.), 'Funding')]"
        "/ancestor::div[contains(@class,'flex-col')][1]"
        "//div[contains(@class,'items-center')]//span[2]"
    ),
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("bulk_testnet_worker")

STOP = False


def _handle_stop(signum, frame):
    global STOP
    STOP = True
    logger.info("stop signal received (%s)", signum)


signal.signal(signal.SIGTERM, _handle_stop)
signal.signal(signal.SIGINT, _handle_stop)


def _js_text(driver, el) -> str:
    value = driver.execute_script(
        "return arguments[0].innerText || arguments[0].textContent;", el
    )
    return _clean_spaces(value)


def _find(driver, xpath: str, timeout: int = 15, invalid: set[str] | None = None) -> str:
    """Find text by xpath and wait until it becomes non-empty and not in invalid list."""
    invalid = invalid or set()
    wait = WebDriverWait(driver, timeout)

    def _cond(d):
        try:
            el = d.find_element(By.XPATH, xpath)
        except Exception:
            return False
        txt = _js_text(d, el)
        if not txt:
            return False
        if txt in invalid:
            return False
        return txt

    try:
        return wait.until(_cond)
    except Exception:
        return ""


def _switch_market(driver, market: str):
    btn = driver.find_element(By.XPATH, f"//*[normalize-space(text())='{market}']")
    btn.click()
    # give UI time to switch & update numbers
    time.sleep(3)


def parse_bulk_testnet() -> dict:
    driver, tmp_dir = _make_driver()
    rows = []

    try:
        driver.get(BULK_TESTNET_URL)
        time.sleep(6)

        for market in BULK_MARKETS:
            _switch_market(driver, market)

            oracle_price = _find(driver, BULK_XPATHS["oracle_price"])
            volume_24h = _find(driver, BULK_XPATHS["volume_24h"])
            open_interest = _find(driver, BULK_XPATHS["open_interest"])

            # Funding sometimes was stored as the header "Funding/Countdown" due to xpath/race.
            funding = _find(
                driver,
                BULK_XPATHS["funding"],
                invalid={"Funding/Countdown", "Funding"},
            )
            countdown = _find(
                driver,
                BULK_XPATHS["countdown"],
                invalid={"Funding/Countdown", "Funding"},
            )

            # extra safety: if somehow we still got the header, drop it
            if "Funding" in (funding or ""):
                funding = ""
            if "Funding" in (countdown or ""):
                countdown = ""

            rows.append((market, oracle_price, volume_24h, open_interest, funding, countdown))

    finally:
        try:
            driver.quit()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return insert_bulk_testnet_rows(rows)


def main():
    interval = int(os.getenv("BULK_TESTNET_INTERVAL_SEC", "300"))
    logger.info("bulk testnet worker started interval=%ss", interval)

    while not STOP:
        try:
            result = parse_bulk_testnet()
            logger.info("bulk testnet parsed %s rows", result.get("inserted"))
        except Exception:
            logger.exception("bulk testnet parse error")

        for _ in range(interval):
            if STOP:
                break
            time.sleep(1)

    logger.info("bulk testnet worker stopped")


if __name__ == "__main__":
    main()
