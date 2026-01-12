# app/stats.py
import os
import re
import time
import logging
import threading
import concurrent.futures
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Dict, List, Tuple

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor, execute_values

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
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
# SCHEMA ENSURE (SANCTUM + SOLSCAN)
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
                # Sanctum history table
                cur.execute("""
                CREATE TABLE IF NOT EXISTS sanctum_bulk_metrics (
                  id BIGSERIAL PRIMARY KEY,
                  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  total_staked TEXT,
                  bulk_to_sol TEXT,
                  total_holders TEXT
                );
                """)
                cur.execute("""
                CREATE INDEX IF NOT EXISTS sanctum_bulk_metrics_fetched_at_idx
                ON sanctum_bulk_metrics (fetched_at DESC);
                """)

                # Solscan transactions table (8 columns)
                cur.execute("""
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
                """)
            conn.commit()
            _schema_ready = True
            logger.info("Schema ensured (sanctum_bulk_metrics, solscan_transactions)")
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)


# --------------------
# API FUNCTIONS (TOP + STATS)
# --------------------
def get_discord_top(limit: int = 15) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT user_id, username, message_count
                FROM discord_users
                ORDER BY message_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            result = [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)]
            return result or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)

def get_telegram_top(limit: int = 15) -> List[Dict[str, Any]]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    user_id,
                    COALESCE(username, first_name, 'ID' || user_id) AS username,
                    message_count
                FROM telegram_users
                ORDER BY message_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            result = [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)]
            return result or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)

def get_discord_stats() -> Dict[str, int]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT COUNT(*) AS total_users,
                       COALESCE(SUM(message_count), 0) AS messages_total
                FROM discord_users
            """)
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)

def get_telegram_stats() -> Dict[str, int]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT COUNT(*) AS total_users,
                       COALESCE(SUM(message_count), 0) AS messages_total
                FROM telegram_users
            """)
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)

def get_community_stats() -> Dict[str, int]:
    dc = get_discord_stats()
    tg = get_telegram_stats()
    x_users = 0
    total = dc["total_users"] + tg["total_users"] + x_users
    return {
        "discord_users": dc["total_users"],
        "telegram_users": tg["total_users"],
        "x_users": x_users,
        "total_users": total,
    }


# --------------------
# SELENIUM HELPERS
# --------------------
def _make_driver() -> webdriver.Chrome:
    """
    Chrome headless driver.
    На Railway нужен установленный chromium/chrome в контейнере.
    """
    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    # Если задашь CHROME_BIN (путь к chromium), можно раскомментить:
    # chrome_bin = os.getenv("CHROME_BIN")
    # if chrome_bin:
    #     opts.binary_location = chrome_bin

    return webdriver.Chrome(options=opts)

def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _to_decimal(text: str) -> Optional[Decimal]:
    if text is None:
        return None
    s = str(text).strip()
    if not s or s in {"-", "—"}:
        return None
    s = s.replace(",", "").replace(" ", "")
    # убираем всё, кроме цифр, точки и минуса
    s = re.sub(r"[^0-9\.\-]", "", s)
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


# --------------------
# SANCTUM PARSER (3 values -> history table)
# --------------------
SANCTUM_URL = "https://app.sanctum.so/explore/BulkSOL"

def _find_value_near_label(driver, label: str) -> str:
    """
    Очень терпимый поиск: находим элемент с текстом label, затем берём ближайший value рядом/ниже.
    """
    # варианты — Sanctum может менять теги/структуру
    xpaths = [
        f"//*[contains(normalize-space(.), '{label}')]/following::*[1]",
        f"//*[contains(normalize-space(.), '{label}')]/following-sibling::*[1]",
        f"//*[self::div or self::span or self::p][contains(., '{label}')]/ancestor::*[1]//*[self::div or self::span][2]",
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
    """
    Парсит 3 показателя Sanctum и сохраняет НОВУЮ строку в sanctum_bulk_metrics.
    """
    ensure_schema()

    driver = _make_driver()
    driver.get(SANCTUM_URL)

    try:
        wait = WebDriverWait(driver, 30)
        # ждём появления ключевой подписи
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Total staked')]")))

        total_staked = _find_value_near_label(driver, "Total staked")
        bulk_to_sol = _find_value_near_label(driver, "1 BulkSOL")
        total_holders = _find_value_near_label(driver, "Total holders")

    finally:
        driver.quit()

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
            cur.execute("""
                SELECT fetched_at, total_staked, bulk_to_sol, total_holders
                FROM sanctum_bulk_metrics
                ORDER BY fetched_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            return row or {"fetched_at": None, "total_staked": None, "bulk_to_sol": None, "total_holders": None}
    finally:
        _put_conn(conn)


# --------------------
# SOLSCAN PARSER (8 columns -> upsert by signature)
# --------------------
SOLSCAN_URL = "https://solscan.io/token/BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn"

def _parse_solscan_rows_table(driver, limit_rows: int) -> List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]]:
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    rows = rows[:limit_rows]

    parsed: List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]] = []
    for r in rows:
        tds = r.find_elements(By.CSS_SELECTOR, "td")
        if len(tds) < 8:
            continue
        signature = _clean_spaces(tds[0].text)
        t_time = _clean_spaces(tds[1].text)
        action = _clean_spaces(tds[2].text)
        from_addr = _clean_spaces(tds[3].text)
        to_addr = _clean_spaces(tds[4].text)
        amount = _to_decimal(tds[5].text)
        value = _to_decimal(tds[6].text)
        token = _clean_spaces(tds[7].text)

        if signature:
            parsed.append((signature, t_time, action, from_addr, to_addr, amount, value, token))
    return parsed

def _parse_solscan_rows_rolegrid(driver, limit_rows: int) -> List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]]:
    # fallback если Solscan рендерит не <table>, а div-grid (role=row/cell)
    rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row']")
    if len(rows) <= 1:
        return []
    rows = rows[1:limit_rows + 1]  # пропускаем заголовок

    parsed: List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]] = []
    for r in rows:
        cells = r.find_elements(By.CSS_SELECTOR, "div[role='cell']")
        if len(cells) < 8:
            continue
        signature = _clean_spaces(cells[0].text)
        t_time = _clean_spaces(cells[1].text)
        action = _clean_spaces(cells[2].text)
        from_addr = _clean_spaces(cells[3].text)
        to_addr = _clean_spaces(cells[4].text)
        amount = _to_decimal(cells[5].text)
        value = _to_decimal(cells[6].text)
        token = _clean_spaces(cells[7].text)

        if signature:
            parsed.append((signature, t_time, action, from_addr, to_addr, amount, value, token))
    return parsed


def parse_solscan(limit_rows: int = 25) -> Dict[str, Any]:
    """
    Забирает последние строки из Solscan и upsert'ит в solscan_transactions (PK signature).
    """
    ensure_schema()

    driver = _make_driver()
    driver.get(SOLSCAN_URL)

    parsed: List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]] = []

    try:
        wait = WebDriverWait(driver, 35)

        # ждём появление строк (либо table, либо rolegrid)
        def _has_any_rows(d):
            return len(d.find_elements(By.CSS_SELECTOR, "table tbody tr")) > 0 or len(d.find_elements(By.CSS_SELECTOR, "div[role='row']")) > 1

        wait.until(_has_any_rows)

        parsed = _parse_solscan_rows_table(driver, limit_rows)
        if not parsed:
            parsed = _parse_solscan_rows_rolegrid(driver, limit_rows)

    finally:
        driver.quit()

    if not parsed:
        return {"inserted_or_updated": 0, "note": "No rows parsed (Solscan DOM changed)."}

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
            execute_values(cur, sql, parsed)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _put_conn(conn)

    return {"inserted_or_updated": len(parsed)}


def get_latest_solscan(limit: int = 25) -> List[Dict[str, Any]]:
    """
    Для фронта/апи: отдаёт последние транзакции из БД.
    """
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT signature, time, action, from_address, to_address, amount, value, token
                FROM solscan_transactions
                ORDER BY time DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        _put_conn(conn)


# --------------------
# BOT WRITE FUNCTIONS
# --------------------
def add_discord_message(user_id: int, username: str) -> None:
    def _add():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO discord_users (user_id, username, message_count)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (user_id) DO UPDATE SET
                        message_count = discord_users.message_count + 1,
                        username = EXCLUDED.username,
                        last_active = CURRENT_TIMESTAMP
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
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO telegram_users (user_id, username, first_name, message_count)
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (user_id) DO UPDATE SET
                        message_count = telegram_users.message_count + 1,
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        last_active = CURRENT_TIMESTAMP
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
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT username, message_count
                FROM telegram_users
                WHERE username ILIKE %s
                """,
                (f"%{username}%",),
            )
            tg = cur.fetchone()
            if tg:
                return {"platform": "TG", "username": tg["username"], "messages": tg["message_count"]}
            return None
    except Exception as e:
        logger.exception("TG lookup error: %s", e)
        return None
    finally:
        _put_conn(conn)

def get_dc_user(username: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT username, message_count
                FROM discord_users
                WHERE username ILIKE %s
                """,
                (f"%{username}%",),
            )
            dc = cur.fetchone()
            if dc:
                return {"platform": "DC", "username": dc["username"], "messages": dc["message_count"]}
            return None
    except Exception as e:
        logger.exception("DC lookup error: %s", e)
        return None
    finally:
        _put_conn(conn)
