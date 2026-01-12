from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.stats import (
    get_discord_top,
    get_telegram_top,
    get_tg_user,
    get_dc_user,
    get_community_stats,  # <-- убедись, что добавил эту функцию в stats.py
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

# ✅ CORS для фронта
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://bulkhub-production.up.railway.app"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "status": "🚀 BULK API OK",
        "discord": "/discord/top/15",
        "telegram": "/telegram/top/15",
        "community": "/community/stats",
    }

@app.get("/community/stats")
async def community_stats():
    return await run_in_threadpool(get_community_stats)

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
