# stats.py — Windows + Uvicorn безопасная версия!
import psycopg2
from psycopg2.extras import RealDictCursor
import concurrent.futures
import os
import time

DB_CONFIG = {
    "host": "localhost", 
    "port": 5432, 
    "database": "discord_stats",
    "user": "postgres", 
    "password": "sqwiztashsain8310"
}

def get_connection():
    DATABASE_URL = os.getenv("DATABASE_URL")  # ← Railway переменная!
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found! Check Railway Variables")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# API функции (БЕЗ изменений)
def get_discord_top(limit=15):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, message_count FROM discord_users ORDER BY message_count DESC LIMIT %s", (limit,))
    result = [{"place": i+1, "username": row['username'], "messages": row['message_count']} 
              for i, row in enumerate(cur.fetchall())]
    cur.close(); conn.close()
    return result or [{"error": "Нет данных"}]

def get_telegram_top(limit=15):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, COALESCE(username, first_name, 'ID' || user_id) as username, 
               message_count FROM telegram_users ORDER BY message_count DESC LIMIT %s
    """, (limit,))
    result = [{"place": i+1, "username": row['username'], "messages": row['message_count']} 
              for i, row in enumerate(cur.fetchall())]
    cur.close(); conn.close()
    return result or [{"error": "Нет данных"}]

def get_discord_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as total_users, COALESCE(SUM(message_count), 0) as messages_total FROM discord_users")
    row = cur.fetchone()
    cur.close(); conn.close()
    return {"total_users": int(row['total_users']), "messages_total": int(row['messages_total'])}

# Бот функции (ThreadPool)
_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

def add_discord_message(user_id: int, username: str):
    def _add():
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO discord_users (user_id, username, message_count) 
            VALUES (%s, %s, 1) 
            ON CONFLICT (user_id) DO UPDATE SET 
                message_count = discord_users.message_count + 1,
                username = EXCLUDED.username,
                last_active = CURRENT_TIMESTAMP
        """, (user_id, username))
        conn.commit()
        cur.close(); conn.close()
    _thread_pool.submit(_add)

def add_telegram_message(user_id: int, username: str | None, first_name: str | None):
    def _add():
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO telegram_users (user_id, username, first_name, message_count) 
            VALUES (%s, %s, %s, 1) 
            ON CONFLICT (user_id) DO UPDATE SET 
                message_count = telegram_users.message_count + 1,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_active = CURRENT_TIMESTAMP
        """, (user_id, username, first_name))
        conn.commit()
        cur.close(); conn.close()
    _thread_pool.submit(_add)


def get_tg_user(username: str):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            query = "SELECT username, message_count FROM telegram_users WHERE username ILIKE %s"
            print(f"🔍 SQL: {query} | PARAM: %{username}%")
            cur.execute(query, (f'%{username}%',))
            tg = cur.fetchone()
            print(f"📊 TG RESULT: {tg}")
            
            # ✅ RealDictRow = ДИКТ!
            if tg:
                print(f"✅ TG FOUND: {tg['username']} = {tg['message_count']} msgs")
                return {"platform": "TG", "username": tg['username'], "messages": tg['message_count']}
                
    except Exception as e:
        print(f"❌ TG ERROR: {e}")
        return None
    
    print("👤 TG не найден")
    return None


def get_dc_user(username: str):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            query = "SELECT username, message_count FROM discord_users WHERE username ILIKE %s"
            print(f"🔍 DC SQL: {query} | PARAM: %{username}%")
            cur.execute(query, (f'%{username}%',))
            dc = cur.fetchone()
            print(f"📊 DC RESULT: {dc}")
            
            # ✅ RealDictRow = ДИКТ!
            if dc:
                print(f"✅ DC FOUND: {dc['username']} = {dc['message_count']} msgs")
                return {"platform": "DC", "username": dc['username'], "messages": dc['message_count']}
                
    except Exception as e:
        print(f"❌ DC ERROR: {e}")
        return None
    
    print("👤 DC не найден")
    return None