from contextlib import asynccontextmanager

import asyncio
import os
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.auth_db import init_auth_pool, close_auth_pool
from app.auth_routes import router as auth_router
from app.aura_routes import router as aura_router

import app.stats as stats
import app.aura_worker as aura_worker

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

    async def _aura_weekly_syncer():
        await asyncio.sleep(20)
        while True:
            try:
                await asyncio.to_thread(aura_worker.sync_aura)
            except Exception:
                logger.exception("Aura weekly sync failed")
            await asyncio.sleep(7 * 24 * 3600)

    aura_task = asyncio.create_task(_aura_weekly_syncer())

    if auto_refresh:
        refresh_task = asyncio.create_task(_discord_members_refresher())

    try:
        yield
    finally:
        aura_task.cancel()
        try:
            await aura_task
        except asyncio.CancelledError:
            pass

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
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(aura_router)


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
    try:
        return await run_in_threadpool(stats.parse_solscan, limit_rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bulk/testnet/latest")
async def bulk_testnet_latest():
    return await run_in_threadpool(stats.get_latest_bulk_testnet)


@app.post("/bulk/testnet/refresh")
async def bulk_testnet_refresh():
    return await run_in_threadpool(stats.parse_bulk_testnet)
    
@app.get("/bulk/testnet/summary")
async def bulk_testnet_summary():
    return await run_in_threadpool(stats.get_bulk_testnet_summary)
