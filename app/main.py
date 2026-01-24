from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware

from app.auth_db import init_auth_pool, close_auth_pool
from app.auth_routes import router as auth_router

from app.stats import (
    close_pool,
    shutdown_workers,

    get_discord_top,
    get_telegram_top,
    get_tg_user,
    get_dc_user,
    get_community_stats,

    get_x_top,
    get_x_user,

    parse_sanctum,
    get_latest_sanctum,

    parse_solscan,
    get_latest_solscan,
    get_x_posts,
    get_x_user_totals,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_auth_pool()
    try:
        yield
    finally:
        shutdown_workers()
        close_pool()
        await close_auth_pool()


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

app.include_router(auth_router)


@app.get("/")
def root():
    return {
        "status": " BULK API OK",
        "auth_login": "/auth/login",
        "auth_me": "/auth/me",
        "markers_save": "POST /markers",
        "markers_me": "/markers/me",
        "markers_list": "/markers?limit=500",
        "community": "/community/stats",
        "discord_top": "/discord/top/15",
        "telegram_top": "/telegram/top/15",
        "x_top": "/x/top/15",
        "x_user": "/x/<username>",
        "sanctum_latest": "/sanctum/latest",
        "sanctum_refresh": "/sanctum/refresh",
        "solscan_latest": "/solscan/latest?limit=25",
        "solscan_refresh": "/solscan/refresh?limit_rows=25",
    }


@app.get("/community/stats")
async def community_stats():
    return await run_in_threadpool(get_community_stats)

from fastapi import Query

@app.get("/x/posts")
async def x_posts(
    username: str = Query(..., min_length=1),
    limit: int = Query(30, ge=1, le=60),
    offset: int = Query(0, ge=0),
):
    return await run_in_threadpool(get_x_posts, username, limit, offset)

@app.get("/discord/top/{limit}")
async def discord_top(limit: int = 15):
    return await run_in_threadpool(get_discord_top, limit)


@app.get("/telegram/top/{limit}")
async def telegram_top(limit: int = 15):
    return await run_in_threadpool(get_telegram_top, limit)


@app.get("/x/top/{limit}")
async def x_top(limit: int = 15):
    return await run_in_threadpool(get_x_top, limit)


@app.get("/x/{username}")
async def x_user(username: str):
    result = await run_in_threadpool(get_x_user, username)
    return result or {"error": f" X {username} не найден"}


@app.get("/x/user/{username}/totals")
async def x_user_totals(username: str):
    return await run_in_threadpool(get_x_user_totals, username)



@app.get("/tg/{username}")
async def tg_user(username: str):
    result = await run_in_threadpool(get_tg_user, username)
    return result or {"error": f" TG {username} не найден"}


@app.get("/dc/{username}")
async def dc_user(username: str):
    result = await run_in_threadpool(get_dc_user, username)
    return result or {"error": f" DC {username} не найден"}


@app.get("/sanctum/latest")
async def sanctum_latest():
    return await run_in_threadpool(get_latest_sanctum)


@app.post("/sanctum/refresh")
async def sanctum_refresh():
    return await run_in_threadpool(parse_sanctum)


@app.get("/solscan/latest")
async def solscan_latest(limit: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(get_latest_solscan, limit)


@app.post("/solscan/refresh")
async def solscan_refresh(limit_rows: int = Query(25, ge=1, le=200)):
    return await run_in_threadpool(parse_solscan, limit_rows)




