# app/stats.py
import os
import re
import shutil
import logging
import threading
import concurrent.futures
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Dict, List, Tuple

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
                # discord_users
                cur.execute("""
                CREATE TABLE IF NOT EXISTS discord_users (
                    user_id BIGINT,
                    username TEXT,
                    message_count BIGINT DEFAULT 0,
                    last_active TIMESTAMPTZ DEFAULT now()
                );
                """)
                cur.execute("ALTER TABLE discord_users ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cur.execute("ALTER TABLE discord_users ADD COLUMN IF NOT EXISTS username TEXT;")
                cur.execute("ALTER TABLE discord_users ADD COLUMN IF NOT EXISTS message_count BIGINT DEFAULT 0;")
                cur.execute("ALTER TABLE discord_users ADD COLUMN IF NOT EXISTS last_active TIMESTAMPTZ DEFAULT now();")

                # telegram_users
                cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_users (
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    message_count BIGINT DEFAULT 0,
                    last_active TIMESTAMPTZ DEFAULT now()
                );
                """)
                cur.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS user_id BIGINT;")
                cur.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS username TEXT;")
                cur.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS first_name TEXT;")
                cur.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS message_count BIGINT DEFAULT 0;")
                cur.execute("ALTER TABLE telegram_users ADD COLUMN IF NOT EXISTS last_active TIMESTAMPTZ DEFAULT now();")

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

                # Solscan transactions
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
            logger.info("Schema ensured")
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)


# --------------------
# API FUNCTIONS (TOP + STATS)
# --------------------
def get_discord_top(limit: int = 15) -> List[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, username, message_count
                FROM discord_users
                ORDER BY message_count DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)] \
                or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)


def get_telegram_top(limit: int = 15) -> List[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id,
                       COALESCE(username, first_name, 'ID' || user_id) AS username,
                       message_count
                FROM telegram_users
                ORDER BY message_count DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{"place": i + 1, "username": r["username"], "messages": r["message_count"]} for i, r in enumerate(rows)] \
                or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)


def get_discord_stats() -> Dict[str, int]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(DISTINCT user_id) AS total_users,
                    COALESCE(SUM(message_count), 0) AS messages_total
                FROM discord_users
            """)
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)


def get_telegram_stats() -> Dict[str, int]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(DISTINCT user_id) AS total_users,
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
    return {
        "discord_users": dc["total_users"],
        "telegram_users": tg["total_users"],
        "x_users": x_users,
        "total_users": dc["total_users"] + tg["total_users"] + x_users,
    }


# --------------------
# USER LOOKUP
# --------------------
def get_tg_user(username: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT COALESCE(username, first_name, 'ID' || user_id) AS username,
                       message_count
                FROM telegram_users
                WHERE COALESCE(username, first_name, '') ILIKE %s
                ORDER BY message_count DESC NULLS LAST
                LIMIT 1
            """, (f"%{username}%",))
            row = cur.fetchone()
            if row:
                return {"platform": "TG", "username": row["username"], "messages": row["message_count"]}
            return None
    finally:
        _put_conn(conn)


def get_dc_user(username: str) -> Optional[Dict[str, Any]]:
    ensure_schema()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT username, message_count
                FROM discord_users
                WHERE COALESCE(username, '') ILIKE %s
                ORDER BY message_count DESC NULLS LAST
                LIMIT 1
            """, (f"%{username}%",))
            row = cur.fetchone()
            if row:
                return {"platform": "DC", "username": row["username"], "messages": row["message_count"]}
            return None
    finally:
        _put_conn(conn)


# --------------------
# SELENIUM HELPERS (Railway)
# --------------------
def _make_driver() -> webdriver.Chrome:
    """
    ВАЖНО: берём chromedriver из PATH (ставится nixpacks'ом).
    Так мы НЕ используем Selenium Manager (который у тебя и падал с code 127).
    """
    # chrome binary
    chrome_bin = os.getenv("CHROME_BIN") or shutil.which("chromium") or shutil.which("google-chrome") or shutil.which("chrome")
    # chromedriver
    chromedriver_path = os.getenv("CHROMEDRIVER_PATH") or shutil.which("chromedriver")

    if not chromedriver_path:
        raise RuntimeError("chromedriver not found in PATH. Add it via nixpacks.toml: chromedriver")

    opts = ChromeOptions()
    opts.add_argument("--headless=new")  # Новый headless режим для Chromium
    opts.add_argument("--no-sandbox")  # Для работы в контейнерах
    opts.add_argument("--disable-dev-shm-usage")  # Убираем ошибку с памятью
    opts.add_argument("--disable-gpu")  # Отключение GPU
    opts.add_argument("--window-size=1920,1080")  # Устанавливаем размер окна
    opts.add_argument("--lang=en-US")  # Устанавливаем язык
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")  # Указываем User-Agent для корректного отображения
    opts.add_argument("--disable-software-rasterizer")  # Добавим этот параметр для устранения некоторых ошибок
    opts.add_argument('--remote-debugging-port=9222')  # Указывает порт для отладки, если проблема с DevTools
    opts.add_argument('--no-sandbox') 

    if chrome_bin:
        opts.binary_location = chrome_bin  # Устанавливаем путь к бинарнику Chromium

    service = ChromeService(executable_path=chromedriver_path)  # Указываем путь к chromedriver
    return webdriver.Chrome(service=service, options=opts)


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _to_decimal(text: str) -> Optional[Decimal]:
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


# --------------------
# SANCTUM PARSER
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
    driver = _make_driver()
    try:
        driver.get(SANCTUM_URL)
        wait = WebDriverWait(driver, 40)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(., 'Total staked')]")))

        total_staked = _find_value_near_label(driver, "Total staked")
        bulk_to_sol = _find_value_near_label(driver, "1 BulkSOL")
        total_holders = _find_value_near_label(driver, "Total holders")
    finally:
        driver.quit()

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sanctum_bulk_metrics (total_staked, bulk_to_sol, total_holders)
                VALUES (%s, %s, %s)
            """, (total_staked, bulk_to_sol, total_holders))
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
            return cur.fetchone() or {"fetched_at": None, "total_staked": None, "bulk_to_sol": None, "total_holders": None}
    finally:
        _put_conn(conn)


# --------------------
# SOLSCAN PARSER
# --------------------
SOLSCAN_URL = "https://solscan.io/token/BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn"


def _parse_solscan_rows_table(driver, limit_rows: int) -> List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]]:
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")[:limit_rows]
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
    rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row']")
    if len(rows) <= 1:
        return []
    rows = rows[1:limit_rows + 1]  # skip header

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
    ensure_schema()
    driver = _make_driver()
    parsed: List[Tuple[str, str, str, str, str, Optional[Decimal], Optional[Decimal], str]] = []

    try:
        driver.get(SOLSCAN_URL)
        wait = WebDriverWait(driver, 45)

        def _has_any_rows(d):
            return (len(d.find_elements(By.CSS_SELECTOR, "table tbody tr")) > 0) or (len(d.find_elements(By.CSS_SELECTOR, "div[role='row']")) > 1)

        wait.until(_has_any_rows)

        parsed = _parse_solscan_rows_table(driver, limit_rows)
        if not parsed:
            parsed = _parse_solscan_rows_rolegrid(driver, limit_rows)
    finally:
        driver.quit()

    if not parsed:
        return {"inserted_or_updated": 0, "note": "No rows parsed (Solscan DOM changed / blocked)."}

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
# BOT WRITE FUNCTIONS (без ON CONFLICT)
# --------------------
def add_discord_message(user_id: int, username: str) -> None:
    def _add():
        ensure_schema()
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE discord_users
                    SET message_count = COALESCE(message_count, 0) + 1,
                        username = %s,
                        last_active = now()
                    WHERE user_id = %s
                """, (username, user_id))
                if cur.rowcount == 0:
                    cur.execute("""
                        INSERT INTO discord_users (user_id, username, message_count, last_active)
                        VALUES (%s, %s, 1, now())
                    """, (user_id, username))
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
                cur.execute("""
                    UPDATE telegram_users
                    SET message_count = COALESCE(message_count, 0) + 1,
                        username = %s,
                        first_name = %s,
                        last_active = now()
                    WHERE user_id = %s
                """, (username, first_name, user_id))
                if cur.rowcount == 0:
                    cur.execute("""
                        INSERT INTO telegram_users (user_id, username, first_name, message_count, last_active)
                        VALUES (%s, %s, %s, 1, now())
                    """, (user_id, username, first_name))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)

    _submit_background(_add)


