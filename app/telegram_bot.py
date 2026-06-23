import os
import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from app.stats import add_telegram_message

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION = os.getenv("TG_SESSION")
TG_CHANNEL = os.getenv("TG_CHANNEL", "@tradebulk")
TG_SCAN = os.getenv("TG_SCAN", "0") == "1"

if not TG_API_ID or not TG_API_HASH or not TG_SESSION:
    raise RuntimeError("Set TG_API_ID, TG_API_HASH, TG_SESSION in Railway Variables")

client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

async def full_scan_history():
    print(f"🔄 Сканирую историю {TG_CHANNEL}...")
    entity = await client.get_entity(TG_CHANNEL)
    total = 0

    async for message in client.iter_messages(entity, reverse=True):
        if not message.sender_id:
            continue

        sender = await message.get_sender()
        if not sender or getattr(sender, "bot", False):
            continue

        add_telegram_message(
            sender.id,
            getattr(sender, "username", None),
            getattr(sender, "first_name", None),
        )

        total += 1
        if total % 1000 == 0:
            print(f"📤 {total} сообщений отправлено в запись")

    print(f"Скан завершён: {total} сообщений обработано")

@client.on(events.NewMessage(chats=TG_CHANNEL))
async def handler(event):
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return

    add_telegram_message(
        sender.id,
        getattr(sender, "username", None),
        getattr(sender, "first_name", None),
    )

async def main():
    await client.start()
    print("Telegram bot started (listening)")

    if TG_SCAN:
        await full_scan_history()

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())

