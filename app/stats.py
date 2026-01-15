# app/stats.py
import os
import re
import json
import shutil
import tempfile
import logging
import threading
import concurrent.futures
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List, Tuple

import requests
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


logger = logging.getLogger(__name__)

# --------------------
# DB CONFIG / POOL
# --------------------
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "discord_stats"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}

_pool: Optional[ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _get_dsn() -> str:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    if not DB_CONFIG.get("password"):
        raise ValueError(
            "DATABASE_URL not found and local PGPASSWORD is empty. Set DATABASE_URL or PGPASSWORD."
        )

    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def init_pool(minconn: int = 1, maxconn: int = 10) -> None:
    global _pool
    if _pool is not None:
        return

    with _pool_lock:
        if _pool is not None:
            return
        dsn = _get_dsn()
        _pool = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, dsn=dsn)
        logger.info("Postgres pool initialized (min=%s, max=%s)", minconn, maxconn)


def close_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.closeall()
                logger.info("Postgres pool closed")
            finally:
                _pool = None


def _get_conn():
    init_pool()
    assert _pool is not None
    return _pool.getconn()


def _put_conn(conn) -> None:
    assert _pool is not None
    _pool.putconn(conn)


# --------------------
# THREAD POOL (bot writes)
# --------------------
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)


def shutdown_workers() -> None:
    _thread_pool.shutdown(wait=False, cancel_futures=False)


def _submit_background(fn, *args, **kwargs) -> None:
    fut = _thread_pool.submit(fn, *args, **kwargs)

    def _log_exc(f: concurrent.futures.Future):
        exc = f.exception()
        if exc:
            logger.exception("Background DB task failed: %s", exc)

    fut.add_done_callback(_log_exc)


# --------------------
# SCHEMA ENSURE
# --------------------
_schema_ready = False
_schema_lock = threading.Lock()


def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return

    with _schema_lock:
        if _schema_ready:
            return

        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS discord_users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        message_count BIGINT DEFAULT 0,
                        last_active TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS telegram_users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        message_count BIGINT DEFAULT 0,
                        last_active TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sanctum_bulk_metrics (
                        id BIGSERIAL PRIMARY KEY,
                        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        total_staked TEXT,
                        bulk_to_sol TEXT,
                        total_holders TEXT
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS sanctum_bulk_metrics_fetched_at_idx
                    ON sanctum_bulk_metrics (fetched_at DESC);
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS solscan_transactions (
                        signature     TEXT PRIMARY KEY,
                        time          TEXT,
                        action        TEXT,
                        from_address  TEXT,
                        to_address    TEXT,
                        amount        NUMERIC,
                        value         NUMERIC,
                        token         TEXT
                    );
                    """
                )

            conn.commit()
            _schema_ready = True
            logger.info("Schema ensured")
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)


# --------------------
# HELPERS
# --------------------
def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _to_decimal(text: Any) -> Optional[Decimal]:
    if text is None:
        return None
    s = str(text).strip()
    if not s or s in {"-", "—"}:
        return None
    s = s.replace(",", "").replace(" ", "")
    s = re.sub(r"[^0-9\.\-]", "", s)
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _unix_to_iso(ts: Any) -> str:
    try:
        t = int(ts)
        return datetime.fromtimestamp(t, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ts) if ts is not None else ""


def _normalize_amount(raw_amount: Any, token_decimals: Any) -> Optional[Decimal]:
    amt = _to_decimal(raw_amount)
    if amt is None:
        return None
    try:
        dec = int(token_decimals) if token_decimals is not None else None
    except Exception:
        dec = None

    if dec is not None:
        try:
            return amt / (Decimal(10) ** Decimal(dec))
        except Exception:
            return amt
    return amt


# --------------------
# SELENIUM (ТОЛЬКО ДЛЯ SANCTUM)
# --------------------
def _make_driver() -> Tuple[webdriver.Chrome, str]:
    chrome_bin = (
        os.getenv("CHROME_BIN")
        or shutil.which("chromium")
        or shutil.which("google-chrome")
        or shutil.which("chrome")
    )
    chromedriver_path = os.getenv("CHROMEDRIVER_PATH") or shutil.which("chromedriver")

    if not chromedriver_path:
        raise RuntimeError("chromedriver not found in PATH. Install chromium + chromedriver in the image.")

    tmp_dir = tempfile.mkdtemp(prefix="chrome-data-")

    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument(f"--user-data-dir={tmp_dir}")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    if chrome_bin:
        opts.binary_location = chrome_bin

    service = ChromeService(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    return driver, tmp_dir


# --------------------
# SANCTUM PARSER (selenium)
# --------------------
SANCTUM_URL = "https://app.sanctum.so/explore/BulkSOL"


def _find_value_near_label(driver, label: str) -> str:
    xpaths = [
        f"//*[contains(normalize-space(.), '{label}')]/following::*[1]",
        f"//*[contains(normalize-space(.), '{label}')]/following-sibling::*[1]",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            txt = _clean_spaces(el.text)
            if txt:
                return txt
        except Exception:
            continue
    return ""


def parse_sanctum() -> Dict[str, Any]:
    ensure_schema()

    driver, tmp_dir = _make_driver()
    try:
        driver.get(SANCTUM_URL)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Total staked')]")))

        total_staked = _find_value_near_label(driver, "Total staked")
        bulk_to_sol = _find_value_near_label(driver, "1 BulkSOL") or _find_value_near_label(driver, "BulkSOL =")
        total_holders = _find_value_near_label(driver, "Total holders")
    finally:
        try:
            driver.quit()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sanctum_bulk_metrics (total_staked, bulk_to_sol, total_holders)
                VALUES (%s, %s, %s)
                """,
                (total_staked, bulk_to_sol, total_holders),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)

    return {"total_staked": total_staked, "bulk_to_sol": bulk_to_sol, "total_holders": total_holders}


def get_latest_sanctum() -> Dict[str, Any]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT fetched_at, total_staked, bulk_to_sol, total_holders
                FROM sanctum_bulk_metrics
                ORDER BY fetched_at DESC
                LIMIT 1
                """
            )
            return cur.fetchone() or {
                "fetched_at": None,
                "total_staked": None,
                "bulk_to_sol": None,
                "total_holders": None,
            }
    finally:
        _put_conn(conn)


# --------------------
# SOLSCAN PARSER (FREE: selenium scrape, no API key)
# --------------------
SOLSCAN_TOKEN_MINT = os.getenv("SOLSCAN_TOKEN_MINT", "BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn")
SOLSCAN_TOKEN_SYMBOL = os.getenv("SOLSCAN_TOKEN_SYMBOL", "BULK")


def _solscan_token_url() -> str:
    return f"https://solscan.io/token/{SOLSCAN_TOKEN_MINT}"


def _try_click_transfers_tab(driver) -> None:
    # Solscan UI может быть разной: кнопка/вкладка "Transfers"
    xps = [
        "//button[contains(., 'Transfers')]",
        "//a[contains(., 'Transfers')]",
        "//*[contains(@role,'tab') and contains(., 'Transfers')]",
    ]
    for xp in xps:
        try:
            el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            return
        except Exception:
            continue


def _extract_rows(driver) -> List[webdriver.remote.webelement.WebElement]:
    # 1) таблица
    trs = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    if trs:
        return trs

    # 2) data-grid (часто у них именно так)
    rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row']")
    # обычно первая строка — header
    if len(rows) > 1:
        return rows[1:]
    return []


def _parse_row(row) -> Optional[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]]:
    # Пытаемся вытащить signature из ссылки /tx/...
    signature = ""
    try:
        a = row.find_element(By.CSS_SELECTOR, "a[href^='/tx/']")
        href = a.get_attribute("href") or ""
        signature = href.split("/tx/")[-1].split("?")[0].strip()
    except Exception:
        signature = ""

    if not signature:
        return None

    # Собираем ячейки
    cells_text: List[str] = []
    cells = row.find_elements(By.CSS_SELECTOR, "td")
    if not cells:
        cells = row.find_elements(By.CSS_SELECTOR, "div[role='cell']")

    for c in cells:
        t = _clean_spaces(c.text)
        if t:
            cells_text.append(t)

    # Очень приблизительная разметка (у Solscan может меняться),
    # поэтому берём “что есть” и стараемся не падать.
    # Обычно: Time | Action | From | To | Amount ...
    time_txt = cells_text[0] if len(cells_text) > 0 else ""
    action = cells_text[1] if len(cells_text) > 1 else "TRANSFER"
    from_addr = cells_text[2] if len(cells_text) > 2 else ""
    to_addr = cells_text[3] if len(cells_text) > 3 else ""

    # Amount (если есть) — пробуем найти первое число в конце списка
    amount = None
    for t in reversed(cells_text):
        d = _to_decimal(t)
        if d is not None:
            amount = d
            break

    # value на сайте может быть в $, но часто отсутствует — оставим None
    value = None
    token = SOLSCAN_TOKEN_SYMBOL

    return (signature, time_txt, action, from_addr, to_addr, amount, value, token)


def parse_solscan(limit_rows: int = 10) -> Dict[str, Any]:
    """
    FREE: забираем последние transfer'ы токена со страницы Solscan (через Selenium)
    """
    ensure_schema()

    driver, tmp_dir = _make_driver()
    try:
        driver.get(_solscan_token_url())

        # иногда нужно кликнуть вкладку Transfers
        _try_click_transfers_tab(driver)

        wait = WebDriverWait(driver, 30)

        def _has_any_rows(d):
            return len(_extract_rows(d)) > 0

        wait.until(_has_any_rows)

        rows = _extract_rows(driver)[: max(1, int(limit_rows))]
        parsed: List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]] = []
        for r in rows:
            item = _parse_row(r)
            if item:
                parsed.append(item)

    finally:
        try:
            driver.quit()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if not parsed:
        return {"inserted_or_updated": 0, "note": "No rows parsed from Solscan page (UI may have changed or blocked)."}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO solscan_transactions
              (signature, time, action, from_address, to_address, amount, value, token)
            VALUES %s
            ON CONFLICT (signature) DO UPDATE SET
              time         = EXCLUDED.time,
              action       = EXCLUDED.action,
              from_address = EXCLUDED.from_address,
              to_address   = EXCLUDED.to_address,
              amount       = EXCLUDED.amount,
              value        = EXCLUDED.value,
              token        = EXCLUDED.token
            """
            execute_values(cur, sql, parsed, page_size=100)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)

    return {"inserted_or_updated": len(parsed)}

# --------------------
# BOT WRITE FUNCTIONS
# --------------------
def add_discord_message(user_id: int, username: str) -> None:
    def _add():
        ensure_schema()
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO discord_users (user_id, username, message_count, last_active)
                    VALUES (%s, %s, 1, now())
                    ON CONFLICT (user_id) DO UPDATE SET
                        message_count = discord_users.message_count + 1,
                        username = EXCLUDED.username,
                        last_active = now()
                    """,
                    (user_id, username),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)

    _submit_background(_add)


def add_telegram_message(user_id: int, username: Optional[str], first_name: Optional[str]) -> None:
    def _add():
        ensure_schema()
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO telegram_users (user_id, username, first_name, message_count, last_active)
                    VALUES (%s, %s, %s, 1, now())
                    ON CONFLICT (user_id) DO UPDATE SET
                        message_count = telegram_users.message_count + 1,
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_active = now()
                    """,
                    (user_id, username, first_name),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)

    _submit_background(_add)


# --------------------
# USER LOOKUP
# --------------------
def get_tg_user(username: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COALESCE(username, first_name, 'ID' || user_id) AS username, message_count
                FROM telegram_users
                WHERE COALESCE(username, first_name, '') ILIKE %s
                ORDER BY message_count DESC NULLS LAST
                LIMIT 1
                """,
                (f"%{username}%",),
            )
            row = cur.fetchone()
            return {"platform": "TG", "username": row["username"], "messages": row["message_count"]} if row else None
    finally:
        _put_conn(conn)


def get_dc_user(username: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT username, message_count
                FROM discord_users
                WHERE COALESCE(username, '') ILIKE %s
                ORDER BY message_count DESC NULLS LAST
                LIMIT 1
                """,
                (f"%{username}%",),
            )
            row = cur.fetchone()
            return {"platform": "DC", "username": row["username"], "messages": row["message_count"]} if row else None
    finally:
        _put_conn(conn)


# --------------------
# TOP/STATS
# --------------------
def get_discord_top(limit: int = 15) -> List[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT user_id, username, message_count
                FROM discord_users
                ORDER BY message_count DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [
                {"place": i + 1, "username": r["username"], "messages": r["message_count"]}
                for i, r in enumerate(rows)
            ] or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)


def get_telegram_top(limit: int = 15) -> List[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT user_id, COALESCE(username, first_name, 'ID' || user_id) AS username, message_count
                FROM telegram_users
                ORDER BY message_count DESC NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [
                {"place": i + 1, "username": r["username"], "messages": r["message_count"]}
                for i, r in enumerate(rows)
            ] or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)


def get_discord_stats() -> Dict[str, int]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS total_users, COALESCE(SUM(message_count), 0) AS messages_total
                FROM discord_users
                """
            )
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)


def get_telegram_stats() -> Dict[str, int]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS total_users, COALESCE(SUM(message_count), 0) AS messages_total
                FROM telegram_users
                """
            )
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)


def get_community_stats() -> Dict[str, int]:
    dc = get_discord_stats()
    tg = get_telegram_stats()
    x_users = 0
    return {
        "discord_users": dc["total_users"],
        "telegram_users": tg["total_users"],
        "x_users": x_users,
        "total_users": dc["total_users"] + tg["total_users"] + x_users,
    }

