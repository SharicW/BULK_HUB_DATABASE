from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.stats import get_discord_top, get_telegram_top, get_tg_user, get_dc_user

app = FastAPI(title="BULK Stats API")


@app.get("/")
async def root():
    return {"status": "🚀 BULK API OK", "discord": "/discord/top/15", "telegram": "/telegram/top/15"}

@app.get("/discord/top/{limit}")
async def discord_top(limit: int = 15):
    return get_discord_top(limit)

@app.get("/telegram/top/{limit}")
async def telegram_top(limit: int = 15):
    return get_telegram_top(limit)

@app.get("/tg/{username}")
async def get_tg_user_endpoint(username: str):
    result = get_tg_user(username)
    return result or {"error": f"👤 TG {username} не найден"}

@app.get("/dc/{username}")
async def get_dc_user_endpoint(username: str):
    result = get_dc_user(username)
    return result or {"error": f"👤 DC {username} не найден"}

