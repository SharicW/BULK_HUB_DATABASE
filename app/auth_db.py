import os
from typing import Optional

import asyncpg

_pool: Optional[asyncpg.Pool] = None


def _normalize_pg_url(url: str) -> str:

    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def get_auth_db_url() -> str:
    url = (
        os.getenv("AUTH_DATABASE_URL")
        or os.getenv("FRONTEND_DATABASE_URL")
        or os.getenv("DATABASE_URL")
    )
    if not url:
        raise RuntimeError("Set AUTH_DATABASE_URL (Railway Postgres URL for frontend auth DB)")
    return _normalize_pg_url(url)


async def init_auth_pool() -> None:
    global _pool
    if _pool is not None:
        return

    min_size = int(os.getenv("AUTH_DB_MIN_POOL", "1"))
    max_size = int(os.getenv("AUTH_DB_MAX_POOL", "5"))

    _pool = await asyncpg.create_pool(
        dsn=get_auth_db_url(),
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
    )


async def close_auth_pool() -> None:
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Auth DB pool is not initialized")
    return _pool
