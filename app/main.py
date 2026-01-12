# app/main.py
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.stats import (
    # top + search
    get_discord_top,
    get_telegram_top,
    get_tg_user,
    get_dc_user,
    get_community_stats,

    # sanctum
    parse_sanctum,
    get_latest_sanctum,

    # solscan
    parse_solscan,
    get_latest_solscan,

    # shutdown
    close_pool,
    shutdown_workers,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup: ничего не обязательно (pool/schema подтянутся при первом вызове)
    yield
    # shutdown
    shutdown_workers()
    close_pool()

app = FastAPI(title="BULK Stats API", lifespan=lifespan)

# ✅ CORS для фронта (и локальной разработки)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bulkhub-production.up.railway.app",
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "status": "🚀 BULK API OK",
        "routes": {
            "community": "/community/stats",
            "discord_top": "/discord/top/15",
            "telegram_top": "/telegram/top/15",
            "find_tg_user": "/tg/{username}",
            "find_dc_user": "/dc/{username}",
            "sanctum_latest": "/sanctum/latest",
            "sanctum_refresh": "/sanctum/refresh",
            "solscan_latest": "/solscan/latest?limit=25",
            "solscan_refresh": "/solscan/refresh?limit=25",
        },
    }

# --------------------
# Community stats (counts)
# --------------------
@app.get("/community/stats")
async def community_stats():
    return await run_in_threadpool(get_community_stats)

# --------------------
# Discord / Telegram leaderboards
# --------------------
@app.get("/discord/top/{limit}")
async def discord_top(limit: int = 15):
    return await run_in_threadpool(get_discord_top, limit)

@app.get("/telegram/top/{limit}")
async def telegram_top(limit: int = 15):
    return await run_in_threadpool(get_telegram_top, limit)

# --------------------
# User search
# --------------------
@app.get("/tg/{username}")
async def tg_user(username: str):
    result = await run_in_threadpool(get_tg_user, username)
    return result or {"error": f"👤 TG {username} не найден"}

@app.get("/dc/{username}")
async def dc_user(username: str):
    result = await run_in_threadpool(get_dc_user, username)
    return result or {"error": f"👤 DC {username} не найден"}

# --------------------
# Sanctum (3 metrics)
# --------------------
@app.get("/sanctum/latest")
async def sanctum_latest():
    return await run_in_threadpool(get_latest_sanctum)

@app.post("/sanctum/refresh")
async def sanctum_refresh():
    """
    Парсит Sanctum и сохраняет новую строку в sanctum_bulk_metrics.
    """
    return await run_in_threadpool(parse_sanctum)

# --------------------
# Solscan (transactions)
# --------------------
@app.get("/solscan/latest")
async def solscan_latest(limit: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(get_latest_solscan, limit)

@app.post("/solscan/refresh")
async def solscan_refresh(limit: int = Query(25, ge=1, le=200)):
    """
    Парсит Solscan и upsert'ит последние limit строк по signature.
    """
    return await run_in_threadpool(parse_solscan, limit)
