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
                cur.execute("ALTER TABLE solscan_transactions ADD COLUMN IF NOT EXISTS event_ts TIMESTAMPTZ;")
                cur.execute("ALTER TABLE solscan_transactions ADD COLUMN IF NOT EXISTS id BIGSERIAL;")
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


                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS communities (
                        community_id TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )


                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ingest_state (
                        key TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS community_tweets (
                        tweet_id TEXT PRIMARY KEY,
                        community_id TEXT NOT NULL,
                        created_at TIMESTAMPTZ,
                        url TEXT,
                        text TEXT,
                        author_id TEXT,
                        author_username TEXT,
                        author_name TEXT,
                        raw_json JSONB,
                        inserted_at TIMESTAMPTZ DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ct_community_created
                    ON community_tweets (community_id, created_at DESC);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ct_community_author
                    ON community_tweets (community_id, lower(author_username));
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_ct_created
                    ON community_tweets (created_at DESC);
                    """
                )


                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS raw_json JSONB;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ DEFAULT now();")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS author_id TEXT;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS author_username TEXT;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS author_name TEXT;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS url TEXT;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS text TEXT;")
                cur.execute("ALTER TABLE community_tweets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tweet_metrics_latest (
                        tweet_id TEXT PRIMARY KEY,
                        view_count BIGINT DEFAULT 0,
                        like_count BIGINT DEFAULT 0,
                        retweet_count BIGINT DEFAULT 0,
                        reply_count BIGINT DEFAULT 0,
                        quote_count BIGINT DEFAULT 0,
                        bookmark_count BIGINT DEFAULT 0,
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        raw_json JSONB
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_tml_updated
                    ON tweet_metrics_latest (updated_at DESC);
                    """
                )
                cur.execute("ALTER TABLE tweet_metrics_latest ADD COLUMN IF NOT EXISTS raw_json JSONB;")
                cur.execute("ALTER TABLE tweet_metrics_latest ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT PRIMARY KEY,
                        username TEXT UNIQUE,
                        name TEXT,
                        followers BIGINT DEFAULT 0,
                        following BIGINT DEFAULT 0,
                        profile_picture TEXT,
                        verified_type TEXT,
                        is_blue_verified BOOLEAN,
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        raw_json JSONB
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_users_username_lower
                    ON users (lower(username));
                    """
                )
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS raw_json JSONB;")
                cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();")

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS community_members (
                        community_id TEXT NOT NULL,
                        user_id TEXT,
                        username TEXT,
                        name TEXT,
                        followers BIGINT DEFAULT 0,
                        following BIGINT DEFAULT 0,
                        profile_picture TEXT,
                        is_blue_verified BOOLEAN,
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        raw_json JSONB,
                        PRIMARY KEY (community_id, user_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_members_community_username_lower
                    ON community_members (community_id, lower(username));
                    """
                )
                cur.execute("ALTER TABLE community_members ADD COLUMN IF NOT EXISTS raw_json JSONB;")
                cur.execute("ALTER TABLE community_members ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();")

            conn.commit()
            _schema_ready = True
            logger.info("Schema ensured")
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)



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
    sec = _parse_age_seconds(time_text)
    if sec is not None:
        return datetime.now(timezone.utc) - timedelta(seconds=sec)

    try:
        ms = datetime.fromisoformat(str(time_text).replace("Z", "+00:00"))
        if ms.tzinfo is None:
            ms = ms.replace(tzinfo=timezone.utc)
        return ms.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        parsed = datetime.fromtimestamp(int(time_text), tz=timezone.utc)
        return parsed
    except Exception:
        return None


def _get_x_community_id() -> Optional[str]:
    return (
        os.getenv("COMMUNITY_ID")
        or os.getenv("X_COMMUNITY_ID")
        or os.getenv("TWITTER_COMMUNITY_ID")
    )


def _x_engage_expr_sql() -> str:
    return """
      (COUNT(*) * 5
       + COALESCE(SUM(tm.like_count),0) * 2
       + COALESCE(SUM(tm.retweet_count),0) * 3
       + COALESCE(SUM(tm.reply_count),0) * 4
       + COALESCE(SUM(tm.quote_count),0) * 3
       + COALESCE(SUM(tm.bookmark_count),0) * 2
       + FLOOR(COALESCE(SUM(tm.view_count),0) / 100.0)
      )::bigint
    """



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
    tables = driver.find_elements(By.CSS_SELECTOR, "table")
    for tbl in tables:
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        if not rows:
            continue
        headers = [_clean_spaces(th.text) for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
        if headers and any("time" in h.lower() for h in headers) and any("from" in h.lower() for h in headers):
            return headers, rows, "table"

    headers = [_clean_spaces(h.text) for h in driver.find_elements(By.CSS_SELECTOR, "div[role='columnheader']")]
    rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row']")
    if headers and len(rows) > 1:
        return headers, rows[1:], "grid"

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
    raw = _clean_spaces(text)
    if not raw:
        return None, "", ""

    parts = raw.split()
    token = ""
    raw_amount = raw

    if len(parts) >= 2:
        token = parts[-1]
        raw_amount = " ".join(parts[:-1])

    amt = _to_decimal(raw_amount)
    return amt, token, raw_amount


def parse_solscan(limit_rows: int = 10) -> Dict[str, Any]:
    ensure_schema()

    driver, tmp_dir = _make_driver()
    parsed_rows: List[Tuple[str, str, Optional[datetime], str, str, str, Optional[Decimal], Optional[Decimal], str]] = []

    try:
        driver.get(_solscan_token_url())
        _try_click_transfers_tab(driver)

        wait = WebDriverWait(driver, 30)
        headers, rows, mode = _collect_headers_and_rows(driver)
        wait.until(lambda d: len(rows) > 0)

        token_decimals: Optional[int] = None
        try:
            dec_txt = _find_value_near_label(driver, "Decimals")
            d = _to_decimal(dec_txt)
            if d is not None:
                token_decimals = int(d)
        except Exception:
            token_decimals = None

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

            accounts = _extract_accounts(r)
            from_addr = accounts[0] if len(accounts) > 0 else (_pick_field(m, ["From"]) or "")
            to_addr = accounts[1] if len(accounts) > 1 else (_pick_field(m, ["To"]) or "")

            amount_cell = _pick_field(m, ["Amount", "Change", "Change Amount"])
            value_cell = _pick_field(m, ["Value", "USD", "$"])
            token_cell = _pick_field(m, ["Token", "Symbol"])

            amt, tok_from_amt, raw_amt = _split_amount_and_token(amount_cell)
            token = (tok_from_amt or token_cell or SOLSCAN_TOKEN_SYMBOL).strip() or SOLSCAN_TOKEN_SYMBOL

            if amt is not None and token_decimals is not None:
                raw_digits = re.sub(r"[^0-9]", "", str(raw_amt))
                has_dot = "." in str(raw_amt)
                if (not has_dot) and raw_digits and len(raw_digits) > token_decimals and amt >= (Decimal(10) ** Decimal(token_decimals)):
                    try:
                        amt = amt / (Decimal(10) ** Decimal(token_decimals))
                    except Exception:
                        pass

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


def get_x_user(username: str) -> Optional[Dict[str, Any]]:
    """
    Поиск X пользователя и расчёт Engage Points.
    Возвращаем `messages` = engage_points, чтобы фронт не переделывать.
    """
    ensure_schema()
    community_id = _get_x_community_id()

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            engage_expr = _x_engage_expr_sql()

            if community_id:
                cur.execute(
                    f"""
                    SELECT
                      lower(ct.author_username) AS username,
                      COUNT(*)::bigint AS posts,
                      COALESCE(SUM(tm.view_count),0)::bigint AS views,
                      COALESCE(SUM(tm.like_count),0)::bigint AS likes,
                      COALESCE(SUM(tm.retweet_count),0)::bigint AS retweets,
                      COALESCE(SUM(tm.reply_count),0)::bigint AS replies,
                      COALESCE(SUM(tm.quote_count),0)::bigint AS quotes,
                      COALESCE(SUM(tm.bookmark_count),0)::bigint AS bookmarks,
                      {engage_expr} AS engage_points
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
                    WHERE ct.community_id = %s
                      AND ct.author_username IS NOT NULL
                      AND lower(ct.author_username) ILIKE %s
                    GROUP BY lower(ct.author_username)
                    ORDER BY engage_points DESC NULLS LAST
                    LIMIT 1
                    """,
                    (community_id, f"%{username.lower()}%"),
                )
            else:
                cur.execute(
                    f"""
                    SELECT
                      lower(ct.author_username) AS username,
                      COUNT(*)::bigint AS posts,
                      COALESCE(SUM(tm.view_count),0)::bigint AS views,
                      COALESCE(SUM(tm.like_count),0)::bigint AS likes,
                      COALESCE(SUM(tm.retweet_count),0)::bigint AS retweets,
                      COALESCE(SUM(tm.reply_count),0)::bigint AS replies,
                      COALESCE(SUM(tm.quote_count),0)::bigint AS quotes,
                      COALESCE(SUM(tm.bookmark_count),0)::bigint AS bookmarks,
                      {engage_expr} AS engage_points
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
                    WHERE ct.author_username IS NOT NULL
                      AND lower(ct.author_username) ILIKE %s
                    GROUP BY lower(ct.author_username)
                    ORDER BY engage_points DESC NULLS LAST
                    LIMIT 1
                    """,
                    (f"%{username.lower()}%",),
                )

            row = cur.fetchone()
            if not row:
                return None

            return {
                "platform": "X",
                "username": row["username"],
                "messages": int(row["engage_points"] or 0),

                "engage_points": int(row["engage_points"] or 0),
                "posts": int(row["posts"] or 0),
                "views": int(row["views"] or 0),
                "likes": int(row["likes"] or 0),
                "retweets": int(row["retweets"] or 0),
                "replies": int(row["replies"] or 0),
                "quotes": int(row["quotes"] or 0),
                "bookmarks": int(row["bookmarks"] or 0),
            }
    finally:
        _put_conn(conn)



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


def get_x_top(limit: int = 15) -> List[Dict[str, Any]]:
    """
    Топ X по Engage Points.
    Возвращаем `messages` = engage_points для совместимости с фронтом.
    """
    ensure_schema()
    community_id = _get_x_community_id()

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            engage_expr = _x_engage_expr_sql()

            if community_id:
                cur.execute(
                    f"""
                    SELECT
                      lower(ct.author_username) AS username,
                      {engage_expr} AS engage_points
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
                    WHERE ct.community_id = %s
                      AND ct.author_username IS NOT NULL
                    GROUP BY lower(ct.author_username)
                    ORDER BY engage_points DESC NULLS LAST
                    LIMIT %s
                    """,
                    (community_id, limit),
                )
            else:
                cur.execute(
                    f"""
                    SELECT
                      lower(ct.author_username) AS username,
                      {engage_expr} AS engage_points
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
                    WHERE ct.author_username IS NOT NULL
                    GROUP BY lower(ct.author_username)
                    ORDER BY engage_points DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,),
                )

            rows = cur.fetchall() or []
            return [
                {"place": i + 1, "username": r["username"], "messages": int(r["engage_points"] or 0)}
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


def get_x_stats() -> Dict[str, int]:
    """
    Размер X-комьюнити.
    1) если есть community_members (после sync-members) -> count(*)
    2) fallback: distinct authors в community_tweets
    """
    ensure_schema()
    community_id = _get_x_community_id()

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if community_id:
                cur.execute(
                    """
                    SELECT COUNT(*) AS total_users
                    FROM community_members
                    WHERE community_id = %s
                    """,
                    (community_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) AS total_users
                    FROM community_members
                    """
                )
            row = cur.fetchone() or {"total_users": 0}
            total_users = int(row["total_users"] or 0)

            if total_users == 0:
                if community_id:
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT lower(author_username)) AS total_users
                        FROM community_tweets
                        WHERE community_id = %s AND author_username IS NOT NULL
                        """,
                        (community_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT lower(author_username)) AS total_users
                        FROM community_tweets
                        WHERE author_username IS NOT NULL
                        """
                    )
                row2 = cur.fetchone() or {"total_users": 0}
                total_users = int(row2["total_users"] or 0)

            return {"total_users": total_users}
    finally:
        _put_conn(conn)

def _extract_x_media_urls(raw: Any) -> List[str]:
    """
    Best-effort вытаскиваем картинки из raw_json твита.
    Возвращает уникальные URL'ы картинок.
    """
    if raw is None:
        return []

    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []

    urls: List[str] = []
    seen = set()

    def add(url: Any):
        if not url:
            return
        u = str(url).strip()
        if not u.startswith("http"):
            return
        if ("pbs.twimg.com/media" in u) or any(u.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
            if u not in seen:
                seen.add(u)
                urls.append(u)

    def walk(x: Any):
        if x is None:
            return
        if isinstance(x, dict):
            add(x.get("media_url_https"))
            add(x.get("media_url"))
            add(x.get("url"))
            add(x.get("preview_image_url"))

            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(raw)
    return urls


def get_x_posts(username: str, limit: int = 30, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Возвращает посты из community_tweets по author_username (в рамках COMMUNITY_ID если задан).
    Пагинация: limit/offset.
    + возвращает media: [url, url, ...]
    """
    ensure_schema()
    community_id = _get_x_community_id()

    u = (username or "").strip()
    if u.startswith("@"):
        u = u[1:]
    u = u.lower().strip()

    if not u:
        return []

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if community_id:
                cur.execute(
                    """
                    SELECT
                      ct.tweet_id,
                      ct.author_username,
                      ct.author_name,
                      ct.created_at,
                      ct.url,
                      ct.text,
                      ct.media_urls,
                      ct.raw_json,
                      COALESCE(tm.view_count, 0)      AS views,
                      COALESCE(tm.like_count, 0)      AS likes,
                      COALESCE(tm.retweet_count, 0)   AS retweets,
                      COALESCE(tm.reply_count, 0)     AS replies,
                      COALESCE(tm.quote_count, 0)     AS quotes,
                      COALESCE(tm.bookmark_count, 0)  AS bookmarks
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm
                      ON tm.tweet_id = ct.tweet_id
                    WHERE ct.community_id = %s
                      AND ct.author_username IS NOT NULL
                      AND lower(ct.author_username) = %s
                    ORDER BY ct.created_at DESC NULLS LAST, ct.inserted_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (community_id, u, limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT
                      ct.tweet_id,
                      ct.author_username,
                      ct.author_name,
                      ct.created_at,
                      ct.url,
                      ct.text,
                      ct.media_urls,
                      ct.raw_json,
                      COALESCE(tm.view_count, 0)      AS views,
                      COALESCE(tm.like_count, 0)      AS likes,
                      COALESCE(tm.retweet_count, 0)   AS retweets,
                      COALESCE(tm.reply_count, 0)     AS replies,
                      COALESCE(tm.quote_count, 0)     AS quotes,
                      COALESCE(tm.bookmark_count, 0)  AS bookmarks
                    FROM community_tweets ct
                    LEFT JOIN tweet_metrics_latest tm
                      ON tm.tweet_id = ct.tweet_id
                    WHERE ct.author_username IS NOT NULL
                      AND lower(ct.author_username) = %s
                    ORDER BY ct.created_at DESC NULLS LAST, ct.inserted_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (u, limit, offset),
                )

            rows = cur.fetchall() or []

            out: List[Dict[str, Any]] = []
            for r in rows:
                tid = r.get("tweet_id") or ""
                url = r.get("url") or ""
                if not url and tid:
                    url = f"https://x.com/i/web/status/{tid}"

                media = r.get("media_urls")

                if isinstance(media, str):
                    try:
                        media = json.loads(media)
                    except Exception:
                        media = []

                if not isinstance(media, list):
                    media = []

                if not media:
                    media = _extract_x_media_urls(r.get("raw_json"))

                clean_media: List[str] = []
                seen = set()
                for m in media:
                    if not m:
                        continue
                    s = str(m).strip()
                    if not s.startswith("http"):
                        continue
                    if s not in seen:
                        seen.add(s)
                        clean_media.append(s)

                out.append(
                    {
                        "tweet_id": tid,
                        "username": r.get("author_username") or u,
                        "name": r.get("author_name"),
                        "created_at": (r.get("created_at").isoformat() if r.get("created_at") else None),
                        "url": url,
                        "text": r.get("text") or "",
                        "media": clean_media,
                        "views": int(r.get("views") or 0),
                        "likes": int(r.get("likes") or 0),
                        "retweets": int(r.get("retweets") or 0),
                        "replies": int(r.get("replies") or 0),
                        "quotes": int(r.get("quotes") or 0),
                        "bookmarks": int(r.get("bookmarks") or 0),
                    }
                )

            return out
    finally:
        _put_conn(conn)


def get_community_stats() -> Dict[str, int]:
    dc = get_discord_stats()
    tg = get_telegram_stats()
    x = get_x_stats()
    x_users = int(x.get("total_users", 0))

    return {
        "discord_users": dc["total_users"],
        "telegram_users": tg["total_users"],
        "x_users": x_users,
        "total_users": dc["total_users"] + tg["total_users"] + x_users,
    }



