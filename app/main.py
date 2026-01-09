from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool

from app.stats import (
    get_discord_top,
    get_telegram_top,
    get_tg_user,
    get_dc_user,
    close_pool,
    shutdown_workers,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: ничего не обязательно (пул сам инициализируется при первом запросе)
    yield
    # shutdown: аккуратно закрываем ресурсы
    shutdown_workers()
    close_pool()

app = FastAPI(title="BULK Stats API", lifespan=lifespan)

@app.get("/")
def root():
    return {"status": "🚀 BULK API OK", "discord": "/discord/top/15", "telegram": "/telegram/top/15"}

@app.get("/discord/top/{limit}")
async def discord_top(limit: int = 15):
    return await run_in_threadpool(get_discord_top, limit)

@app.get("/telegram/top/{limit}")
async def telegram_top(limit: int = 15):
    return await run_in_threadpool(get_telegram_top, limit)

@app.get("/tg/{username}")
async def get_tg_user_endpoint(username: str):
    result = await run_in_threadpool(get_tg_user, username)
    return result or {"error": f"👤 TG {username} не найден"}

@app.get("/dc/{username}")
async def get_dc_user_endpoint(username: str):
    result = await run_in_threadpool(get_dc_user, username)
    return result or {"error": f"👤 DC {username} не найден"}
