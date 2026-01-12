import os
import logging
import threading
import concurrent.futures
from typing import Any, Optional, Dict, List

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

logger = logging.getLogger(__name__)

# --- Optional local fallback (если хочешь локально без DATABASE_URL) ---
# Лучше перенести пароль в env, но оставляю как шаблон.
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "discord_stats"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),  # лучше НЕ хардкодить
}

# --- Connection pool ---
_pool: Optional[ThreadedConnectionPool] = None
_pool_lock = threading.Lock()

def _get_dsn() -> str:
    """
    Возвращает DSN для подключения.
    На Railway должен быть DATABASE_URL.
    Локально можно без него (через DB_CONFIG).
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url

    # fallback локально: собираем DSN из DB_CONFIG
    if not DB_CONFIG.get("password"):
        # чтобы не ловить странные ошибки "password authentication failed"
        raise ValueError("DATABASE_URL not found and local PGPASSWORD is empty. Set DATABASE_URL or PGPASSWORD.")
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def init_pool(minconn: int = 1, maxconn: int = 10) -> None:
    """
    Инициализирует пул подключений один раз.
    maxconn подстрой под лимиты Railway и нагрузку (обычно 5-15 хватает).
    """
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
    """Закрывает пул (вызови на shutdown приложения)."""
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

# --- Thread pool for bot writes ---
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def shutdown_workers() -> None:
    """Закрывает ThreadPool (вызови на shutdown приложения)."""
    _thread_pool.shutdown(wait=False, cancel_futures=False)

def _submit_background(fn, *args, **kwargs) -> None:
    """Submit + лог ошибок, чтобы они не терялись."""
    fut = _thread_pool.submit(fn, *args, **kwargs)

    def _log_exc(f: concurrent.futures.Future):
        exc = f.exception()
        if exc:
            logger.exception("Background DB task failed: %s", exc)

    fut.add_done_callback(_log_exc)

# ====================
# API функции
# ====================

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
            result = [
                {"place": i + 1, "username": r["username"], "messages": r["message_count"]}
                for i, r in enumerate(rows)
            ]
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
            result = [
                {"place": i + 1, "username": r["username"], "messages": r["message_count"]}
                for i, r in enumerate(rows)
            ]
            return result or [{"error": "Нет данных"}]
    finally:
        _put_conn(conn)

def get_discord_stats() -> Dict[str, int]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_users,
                    COALESCE(SUM(message_count), 0) AS messages_total
                FROM discord_users
                """
            )
            row = cur.fetchone() or {"total_users": 0, "messages_total": 0}
            return {"total_users": int(row["total_users"]), "messages_total": int(row["messages_total"])}
    finally:
        _put_conn(conn)


def get_telegram_stats() -> Dict[str, int]:
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_users,
                    COALESCE(SUM(message_count), 0) AS messages_total
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
    x_users = 0  # пока заглушка
    total = dc["total_users"] + tg["total_users"] + x_users

    return {
        "discord_users": dc["total_users"],
        "telegram_users": tg["total_users"],
        "x_users": x_users,
        "total_users": total,
    }

# ====================
# Парсеры
# ====================

def parse_sanctum():
    driver = webdriver.Edge(executable_path='/path/to/msedgedriver')
    driver.get("https://app.sanctum.so/explore/BulkSOL")
    time.sleep(5)

    # Извлекаем данные с сайта
    total_staked = driver.find_element(By.XPATH, "//div[contains(text(), 'Total staked')]/following-sibling::div").text
    bulk_to_sol = driver.find_element(By.XPATH, "//div[contains(text(), '1 BulkSOL =')]/following-sibling::div").text
    total_holders = driver.find_element(By.XPATH, "//div[contains(text(), 'Total holders')]/following-sibling::div").text

    driver.quit()

    # Сохраняем данные в базу данных
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO bulk_data (total_staked, bulk_to_sol, total_holders)
        VALUES (%s, %s, %s)
    """, (total_staked, bulk_to_sol, total_holders))

    conn.commit()
    cursor.close()
    conn.close()

    return {"total_staked": total_staked, "bulk_to_sol": bulk_to_sol, "total_holders": total_holders}

def parse_solscan():
    driver = webdriver.Edge(executable_path='/path/to/msedgedriver')
    driver.get("https://solscan.io/token/BULKoNSGzxtCqzwTvg5hFJg8fx6dqZRScyXe5LYMfxrn")
    time.sleep(5)

    # Извлекаем количество транзакций
    transaction_count = driver.find_element(By.XPATH, "//div[contains(text(), 'Total transfers')]/following-sibling::div").text

    driver.quit()

    # Сохраняем данные в базу данных
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO transactions (transaction_count)
        VALUES (%s)
    """, (transaction_count,))

    conn.commit()
    cursor.close()
    conn.close()

    return {"transaction_count": transaction_count}

# ====================
# Bot write функции (ThreadPool)
# ====================

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

# ====================
# Поиск пользователя
# ====================

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

