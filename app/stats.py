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
from datetime import datetime, timezone, timedelta
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
        raise ValueError("DATABASE_URL not found and local PGPASSWORD is empty. Set DATABASE_URL or PGPASSWORD.")

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

                # --- solscan_transactions ---
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS solscan_transactions (
                        id BIGSERIAL PRIMARY KEY,
                        signature     TEXT NOT NULL,
                        time          TEXT,
                        event_ts      TIMESTAMPTZ,
                        action        TEXT,
                        from_address  TEXT,
                        to_address    TEXT,
                        amount        NUMERIC,
                        value         NUMERIC,
                        token         TEXT
                    );
                    """
                )

                # Если таблица уже была создана раньше — добавим недостающие колонки
                cur.execute("ALTER TABLE solscan_transactions ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ;")
                cur.execute("ALTER TABLE solscan_transactions ADD COLUMN IF NOT EXISTS id BIGSERIAL;")  # безопасно, если уже есть
                # Индексы/уникальность: не даём плодить одно и то же transfer-событие
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS solscan_transactions_uniq_event
                    ON solscan_transactions (signature, from_address, to_address, amount, action, token);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS solscan_transactions_event_ts_idx
                    ON solscan_transactions (event_ts DESC NULLS LAST);
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


def _parse_age_seconds(time_text: str) -> Optional[int]:
    if not time_text:
        return None
    t = str(time_text).lower().strip()

    if "just now" in t:
        return 0

    m = re.search(
        r"(\d+)\s*(sec|secs|second|seconds|min|mins|minute|minutes|hr|hrs|hour|hours|day|days|week|weeks|month|months|year|years)\s*ago",
        t,
    )
    if not m:
        return None

    n = int(m.group(1))
    unit = m.group(2)

    mult = (
        1 if unit.startswith("sec") else
        60 if unit.startswith("min") else
        3600 if unit.startswith("hr") else
        86400 if unit.startswith("day") else
        604800 if unit.startswith("week") else
        2592000 if unit.startswith("month") else
        31536000
    )
    return n * mult


def _guess_event_ts(time_text: str) -> Optional[datetime]:
    # 1) "x mins ago" -> now - delta
    sec = _parse_age_seconds(time_text)
    if sec is not None:
        return datetime.now(timezone.utc) - timedelta(seconds=sec)

    # 2) ISO / parseable date
    try:
        ms = datetime.fromisoformat(str(time_text).replace("Z", "+00:00"))
        if ms.tzinfo is None:
            ms = ms.replace(tzinfo=timezone.utc)
        return ms.astimezone(timezone.utc)
    except Exception:
        pass

    # 3) Date.parse-like (очень грубо)
    try:
        parsed = datetime.fromtimestamp(int(time_text), tz=timezone.utc)  # если вдруг unix
        return parsed
    except Exception:
        return None


# --------------------
# SELENIUM (shared)
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
            return cur.fetchone() or {"fetched_at": None, "total_staked": None, "bulk_to_sol": None, "total_holders": None}
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
    xps = [
        "//button[contains(., 'Transfers')]",
        "//a[contains(., 'Transfers')]",
        "//*[contains(@role,'tab') and contains(., 'Transfers')]",
    ]
    for xp in xps:
        try:
            el = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            return
        except Exception:
            continue


def _collect_headers_and_rows(driver) -> Tuple[List[str], List[Any], str]:
    # 1) table mode
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    for tbl in tables:
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        if not rows:
            continue
        headers = [_clean_spaces(th.text) for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
        # ищем “похожую” таблицу
        if headers and any("time" in h.lower() for h in headers) and any("from" in h.lower() for h in headers):
            return headers, rows, "table"

    # 2) grid mode (role-based)
    headers = [_clean_spaces(h.text) for h in driver.find_elements(By.CSS_SELECTOR, "div[role='columnheader']")]
    rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row']")
    if headers and len(rows) > 1:
        return headers, rows[1:], "grid"

    # 3) fallback
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    return [], rows, "table"


def _cells_for_row(row, mode: str) -> List[Any]:
    if mode == "grid":
        return row.find_elements(By.CSS_SELECTOR, "div[role='cell']")
    return row.find_elements(By.CSS_SELECTOR, "td")


def _map_by_headers(headers: List[str], cells: List[Any]) -> Dict[str, str]:
    if not headers:
        return {}
    out: Dict[str, str] = {}
    n = min(len(headers), len(cells))
    for i in range(n):
        k = headers[i]
        v = _clean_spaces(cells[i].text)
        out[k] = v
    return out


def _pick_field(m: Dict[str, str], aliases: List[str]) -> str:
    if not m:
        return ""
    for a in aliases:
        al = a.lower()
        for k, v in m.items():
            kl = k.lower()
            if kl == al or kl.startswith(al) or al in kl:
                if v:
                    return v
    return ""


def _extract_signature(row) -> str:
    try:
        a = row.find_element(By.CSS_SELECTOR, "a[href*='/tx/']")
        href = a.get_attribute("href") or ""
        sig = href.split("/tx/")[-1].split("?")[0].split("#")[0].strip()
        return sig
    except Exception:
        return ""


def _extract_accounts(row) -> List[str]:
    links = row.find_elements(By.CSS_SELECTOR, "a[href*='/account/'], a[href*='/address/']")
    addrs: List[str] = []
    for a in links:
        href = (a.get_attribute("href") or "").strip()
        for marker in ("/account/", "/address/"):
            if marker in href:
                addr = href.split(marker)[-1].split("?")[0].split("#")[0].strip()
                if addr and addr not in addrs:
                    addrs.append(addr)
    return addrs


def _split_amount_and_token(text: str) -> Tuple[Optional[Decimal], str, str]:
    """
    Возвращает (amount_decimal, token_symbol, raw_amount_string)
    """
    raw = _clean_spaces(text)
    if not raw:
        return None, "", ""

    # примеры: "5.819163 BULK", "+12 BULK", "5819163", "5,819,163 BULK"
    parts = raw.split()
    token = ""
    raw_amount = raw

    if len(parts) >= 2:
        # токен обычно последним словом
        token = parts[-1]
        raw_amount = " ".join(parts[:-1])

    amt = _to_decimal(raw_amount)
    return amt, token, raw_amount


def parse_solscan(limit_rows: int = 10) -> Dict[str, Any]:
    """
    FREE: забираем последние transfer'ы токена со страницы Solscan (через Selenium)
    """
    ensure_schema()

    driver, tmp_dir = _make_driver()
    parsed_rows: List[Tuple[str, str, Optional[datetime], str, str, str, Optional[Decimal], Optional[Decimal], str]] = []

    try:
        driver.get(_solscan_token_url())
        _try_click_transfers_tab(driver)

        wait = WebDriverWait(driver, 30)
        headers, rows, mode = _collect_headers_and_rows(driver)
        wait.until(lambda d: len(rows) > 0)

        # попробуем вытащить decimals токена (если получится)
        token_decimals: Optional[int] = None
        try:
            dec_txt = _find_value_near_label(driver, "Decimals")
            d = _to_decimal(dec_txt)
            if d is not None:
                token_decimals = int(d)
        except Exception:
            token_decimals = None

        # обновим модель (после прогрузки)
        headers, rows, mode = _collect_headers_and_rows(driver)
        rows = rows[: max(1, int(limit_rows))]

        for r in rows:
            sig = _extract_signature(r)
            if not sig:
                continue

            cells = _cells_for_row(r, mode)
            m = _map_by_headers(headers, cells)

            time_txt = _pick_field(m, ["Time", "Age", "Block Time"])
            action = _pick_field(m, ["Action", "Type"]) or "TRANSFER"

            # from/to: лучше из ссылок
            accounts = _extract_accounts(r)
            from_addr = accounts[0] if len(accounts) > 0 else (_pick_field(m, ["From"]) or "")
            to_addr = accounts[1] if len(accounts) > 1 else (_pick_field(m, ["To"]) or "")

            # amount/value/token
            amount_cell = _pick_field(m, ["Amount", "Change", "Change Amount"])
            value_cell = _pick_field(m, ["Value", "USD", "$"])
            token_cell = _pick_field(m, ["Token", "Symbol"])

            amt, tok_from_amt, raw_amt = _split_amount_and_token(amount_cell)
            token = (tok_from_amt or token_cell or SOLSCAN_TOKEN_SYMBOL).strip() or SOLSCAN_TOKEN_SYMBOL

            # если amount пришёл "целым" и decimals известны — нормализуем (5819163 -> 5.819163 при decimals=6)
            if amt is not None and token_decimals is not None:
                raw_digits = re.sub(r"[^0-9]", "", str(raw_amt))
                has_dot = "." in str(raw_amt)
                # масштабируем только если нет точки и число большое (чтобы не ломать "1")
                if (not has_dot) and raw_digits and len(raw_digits) > token_decimals and amt >= (Decimal(10) ** Decimal(token_decimals)):
                    try:
                        amt = amt / (Decimal(10) ** Decimal(token_decimals))
                    except Exception:
                        pass

            # value
            value = _to_decimal(value_cell)

            event_ts = _guess_event_ts(time_txt)

            parsed_rows.append((sig, time_txt, event_ts, str(action), from_addr, to_addr, amt, value, token))

    finally:
        try:
            driver.quit()
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if not parsed_rows:
        return {"inserted_or_updated": 0, "note": "No rows parsed from Solscan Transfers."}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
            INSERT INTO solscan_transactions
              (signature, time, event_ts, action, from_address, to_address, amount, value, token)
            VALUES %s
            ON CONFLICT (signature, from_address, to_address, amount, action, token)
            DO UPDATE SET
              time     = EXCLUDED.time,
              event_ts = EXCLUDED.event_ts,
              value    = EXCLUDED.value
            """
            execute_values(cur, sql, parsed_rows, page_size=100)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)

    return {"inserted_or_updated": len(parsed_rows)}


def get_latest_solscan(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Возвращает последние транзакции из таблицы solscan_transactions (новые -> старые).
    """
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, signature, time, action, from_address, to_address, amount, value, token
                FROM solscan_transactions
                ORDER BY event_ts DESC NULLS LAST, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()
    finally:
        _put_conn(conn)


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
            return [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)] or [{"error": "Нет данных"}]
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
            return [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)] or [{"error": "Нет данных"}]
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
