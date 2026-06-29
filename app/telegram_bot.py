import os
import asyncio
import psycopg2
import psycopg2.pool
from telethon import TelegramClient, events
from telethon.sessions import StringSession

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION = os.getenv("TG_SESSION")
TG_CHANNEL = os.getenv("TG_CHANNEL", "@tradebulk")
TG_SCAN = os.getenv("TG_SCAN", "0") == "1"

if not TG_API_ID or not TG_API_HASH or not TG_SESSION:
    raise RuntimeError("Set TG_API_ID, TG_API_HASH, TG_SESSION env variables")

_pool = psycopg2.pool.ThreadedConnectionPool(1, 5, dsn=os.getenv("DATABASE_URL"))

def _upsert_user(user_id: int, username, first_name):
    conn = _pool.getconn()
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
        _pool.putconn(conn)

client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

async def full_scan_history():
    print(f"Scanning history of {TG_CHANNEL}...")
    entity = await client.get_entity(TG_CHANNEL)
    total = 0
    async for message in client.iter_messages(entity, reverse=True):
        if not message.sender_id:
            continue
        sender = await message.get_sender()
        if not sender or getattr(sender, "bot", False):
            continue
        _upsert_user(sender.id, getattr(sender, "username", None), getattr(sender, "first_name", None))
        total += 1
        if total % 1000 == 0:
            print(f"{total} messages processed")
    print(f"Scan done: {total} messages")

@client.on(events.NewMessage(chats=TG_CHANNEL))
async def handler(event):
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return
    _upsert_user(sender.id, getattr(sender, "username", None), getattr(sender, "first_name", None))

async def main():
    await client.start()
    print("Telegram bot started")
    if TG_SCAN:
        await full_scan_history()
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
