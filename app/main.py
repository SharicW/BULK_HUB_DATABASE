from contextlib import asynccontextmanager

import asyncio
import os
import logging

from fastapi import FastAPI, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.auth_db import init_auth_pool, close_auth_pool
from app.auth_routes import router as auth_router

# ⚠️ ВАЖНО: импортируем весь модуль, а не функции поштучно
import app.stats as stats

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):

    await init_auth_pool()

    auto_refresh = os.getenv("DISCORD_MEMBERS_AUTO_REFRESH", "1").lower() not in ("0", "false", "no", "off")
    interval_seconds = int(os.getenv("DISCORD_MEMBERS_REFRESH_SECONDS", "3600"))

    refresh_task = None

    async def _discord_members_refresher() -> None:
        await asyncio.sleep(2)
        while True:
            try:
                await asyncio.to_thread(stats.parse_discord_guild_members)
            except Exception:
                logger.exception("Discord member count auto-refresh failed")
            await asyncio.sleep(interval_seconds)

    if auto_refresh:
        refresh_task = asyncio.create_task(_discord_members_refresher())

    try:
        yield
    finally:
        if refresh_task is not None:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass

        stats.shutdown_workers()
        stats.close_pool()
        await close_auth_pool()


app = FastAPI(title="BULK Stats API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.bulkhub.online",
        "https://bulkhub.online",
        "https://bulkhub-production.up.railway.app",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)


@app.get("/")
def root():
    return {
        "status": "BULK API OK",
        "community": "/community/stats",
        "discord_top": "/discord/top/15",
        "telegram_top": "/telegram/top/15",
        "x_top": "/x/top/15",
        "x_user": "/x/<username>",
        "x_user_totals": "/x/totals/<username>",
    }


@app.get("/community/stats")
async def community_stats():
    return await run_in_threadpool(stats.get_community_stats)


@app.get("/discord/members/latest")
async def discord_members_latest():
    return await run_in_threadpool(stats.get_latest_discord_guild_members)


@app.post("/discord/members/refresh")
async def discord_members_refresh():
    return await run_in_threadpool(stats.parse_discord_guild_members)


@app.get("/x/totals/{username}")
async def x_user_totals(username: str):
    return await run_in_threadpool(stats.get_x_user_totals, username)


@app.get("/x/posts")
async def x_posts(
    username: str = Query(..., min_length=1),
    limit: int = Query(30, ge=1, le=60),
    offset: int = Query(0, ge=0),
):
    return await run_in_threadpool(stats.get_x_posts, username, limit, offset)


@app.get("/discord/top/{limit}")
async def discord_top(limit: int = 15):
    return await run_in_threadpool(stats.get_discord_top, limit)


@app.get("/telegram/top/{limit}")
async def telegram_top(limit: int = 15):
    return await run_in_threadpool(stats.get_telegram_top, limit)


@app.get("/x/top/{limit}")
async def x_top(limit: int = 15):
    return await run_in_threadpool(stats.get_x_top, limit)


@app.get("/x/{username}")
async def x_user(username: str):
    result = await run_in_threadpool(stats.get_x_user, username)
    return result or {"error": f"X {username} не найден"}


@app.get("/tg/{username}")
async def tg_user(username: str):
    result = await run_in_threadpool(stats.get_tg_user, username)
    return result or {"error": f"TG {username} не найден"}


@app.get("/dc/{username}")
async def dc_user(username: str):
    result = await run_in_threadpool(stats.get_dc_user, username)
    return result or {"error": f"DC {username} не найден"}


@app.get("/sanctum/latest")
async def sanctum_latest():
    return await run_in_threadpool(stats.get_latest_sanctum)


@app.post("/sanctum/refresh")
async def sanctum_refresh():
    return await run_in_threadpool(stats.parse_sanctum)


@app.get("/solscan/latest")
async def solscan_latest(limit: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(stats.get_latest_solscan, limit)


@app.post("/solscan/refresh")
async def solscan_refresh(limit_rows: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(stats.parse_solscan, limit_rows)
