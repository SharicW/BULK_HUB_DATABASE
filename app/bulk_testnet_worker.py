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

BULK_XPATHS = {
    "oracle_price": "//h4[contains(., 'Oracle Price')]/parent::div//span",
    "volume_24h": "//h4[contains(., '24h Volume')]/parent::div//span",
    "open_interest": "//h4[contains(., 'Open Interest')]/parent::div//span",
    "funding": "//h4[contains(., 'Funding')]/ancestor::div[1]//span[1]",
    "countdown": "//h4[contains(., 'Funding')]/ancestor::div[1]//span[2]",
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


def _find(driver, xpath: str, timeout: int = 15) -> str:
    try:
        wait = WebDriverWait(driver, timeout)
        el = wait.until(lambda d: d.find_element(By.XPATH, xpath))
        return _js_text(driver, el)
    except Exception:
        return ""


def _switch_market(driver, market: str):
    btn = driver.find_element(
        By.XPATH,
        f"//*[normalize-space(text())='{market}']"
    )
    btn.click()
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
            funding = _find(driver, BULK_XPATHS["funding"])
            countdown = _find(driver, BULK_XPATHS["countdown"])

            rows.append(
                (market, oracle_price, volume_24h, open_interest, funding, countdown)
            )

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
