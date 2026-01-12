from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.stats import (
    close_pool,
    shutdown_workers,
    # community
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
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    shutdown_workers()
    close_pool()


app = FastAPI(title="BULK Stats API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bulkhub-production.up.railway.app",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "status": "🚀 BULK API OK",
        "community": "/community/stats",
        "discord_top": "/discord/top/15",
        "telegram_top": "/telegram/top/15",
        "sanctum_latest": "/sanctum/latest",
        "sanctum_refresh": "/sanctum/refresh",
        "solscan_latest": "/solscan/latest?limit=25",
        "solscan_refresh": "/solscan/refresh?limit_rows=25",
    }


# -------- COMMUNITY --------
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
async def tg_user(username: str):
    result = await run_in_threadpool(get_tg_user, username)
    return result or {"error": f"👤 TG {username} не найден"}


@app.get("/dc/{username}")
async def dc_user(username: str):
    result = await run_in_threadpool(get_dc_user, username)
    return result or {"error": f"👤 DC {username} не найден"}


# -------- SANCTUM --------
@app.get("/sanctum/latest")
async def sanctum_latest():
    return await run_in_threadpool(get_latest_sanctum)


@app.post("/sanctum/refresh")
async def sanctum_refresh():
    # запускаешь вручную или по cron
    return await run_in_threadpool(parse_sanctum)


# -------- SOLSCAN --------
@app.get("/solscan/latest")
async def solscan_latest(limit: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(get_latest_solscan, limit)


@app.post("/solscan/refresh")
async def solscan_refresh(limit_rows: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(parse_solscan, limit_rows)
