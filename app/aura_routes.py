from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
import app.aura_worker as aura

router = APIRouter(prefix="/aura", tags=["aura"])


@router.get("/stats")
async def aura_stats():
    try:
        return await run_in_threadpool(aura.get_aura_stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/leaderboard")
async def aura_leaderboard(
    sort_by: str = Query("aura", pattern="^(aura|deposit)$"),
    limit: int = Query(10, ge=1, le=200),
):
    try:
        return await run_in_threadpool(aura.get_aura_leaderboard, sort_by, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/wallet/{address}")
async def aura_wallet(address: str):
    try:
        result = await run_in_threadpool(aura.proxy_wallet, address)
        if result is None:
            raise HTTPException(status_code=404, detail="Wallet not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync")
async def aura_sync():
    try:
        return await run_in_threadpool(aura.sync_aura)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/week")
async def aura_set_week(n: int = Query(..., ge=1, description="Current week number")):
    try:
        return await run_in_threadpool(aura.set_week_index, n)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
