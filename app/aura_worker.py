import json
import time
import logging
import requests
from typing import Optional

import app.stats as stats
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

BULK_LEADERBOARD_URL = "https://early.bulk.trade/api/aura/v1/aura/predeposit/leaderboard"
BULK_WALLET_URL = "https://early.bulk.trade/api/aura/v1/aura/predeposit/wallet"
STORE_TOP_N = 200
PAGE_SIZE = 100
REQ_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://early.bulk.trade/deposit",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://early.bulk.trade",
}


def ensure_aura_schema():
    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aura_leaderboard (
                    wallet TEXT PRIMARY KEY,
                    rank INT,
                    aura_rank INT,
                    aura BIGINT DEFAULT 0,
                    deposited_amount NUMERIC DEFAULT 0,
                    withdrawn_amount NUMERIC DEFAULT 0,
                    current_amount NUMERIC DEFAULT 0,
                    referral_number INT DEFAULT 0,
                    categories JSONB,
                    api_updated_at TIMESTAMPTZ,
                    synced_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS aura_snapshots (
                    id SERIAL PRIMARY KEY,
                    week_index INT,
                    snapshot_at TIMESTAMPTZ DEFAULT now(),
                    total_aura BIGINT DEFAULT 0,
                    total_wallets INT DEFAULT 0,
                    total_deposited NUMERIC DEFAULT 0,
                    total_current NUMERIC DEFAULT 0,
                    total_withdrawn NUMERIC DEFAULT 0
                );
            """)
        conn.commit()
    finally:
        stats._put_conn(conn)


def _fetch_page(page: int, page_size: int = PAGE_SIZE, retries: int = 3):
    params = {"page": page, "page_size": page_size}
    for attempt in range(retries):
        try:
            resp = requests.get(BULK_LEADERBOARD_URL, params=params, headers=REQ_HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                logger.warning("[aura] 429 on page %d, waiting %ds (attempt %d/%d)", page, wait, attempt + 1, retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            if attempt == retries - 1:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch page {page} after {retries} retries")


def sync_aura():
    ensure_aura_schema()
    logger.info("[aura] Starting sync (top %d wallets only)...", STORE_TOP_N)

    try:
        first = _fetch_page(1, PAGE_SIZE)
    except Exception as e:
        logger.error("[aura] Failed to fetch first page: %s", e)
        raise

    totals = first.get("totals", {})
    total_pages = first.get("total_pages", 1)
    week_index = int(totals.get("referral_min_held_week_index", 0))
    # total_pages * PAGE_SIZE gives actual participant count (totals.total_wallets is a filtered subset)
    total_wallets = total_pages * PAGE_SIZE
    total_deposited = float(totals.get("total_deposited_amount", 0))
    total_current = float(totals.get("total_current_amount", 0))
    total_withdrawn = float(totals.get("total_withdrawn_amount", 0))

    all_rows = list(first.get("rows", []))

    for page in range(2, total_pages + 1):
        try:
            data = _fetch_page(page, PAGE_SIZE)
            rows = data.get("rows", [])
            if not rows:
                break
            all_rows.extend(rows)
            time.sleep(0.15)
        except Exception as e:
            logger.error("[aura] Error on page %d: %s", page, e)

    top_n = all_rows
    total_aura = sum(r.get("aura", 0) for r in top_n)

    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            if top_n:
                execute_values(cur, """
                    INSERT INTO aura_leaderboard
                        (wallet, rank, aura_rank, aura, deposited_amount, withdrawn_amount,
                         current_amount, referral_number, categories, api_updated_at)
                    VALUES %s
                    ON CONFLICT (wallet) DO UPDATE SET
                        rank = EXCLUDED.rank,
                        aura_rank = EXCLUDED.aura_rank,
                        aura = EXCLUDED.aura,
                        deposited_amount = EXCLUDED.deposited_amount,
                        withdrawn_amount = EXCLUDED.withdrawn_amount,
                        current_amount = EXCLUDED.current_amount,
                        referral_number = EXCLUDED.referral_number,
                        categories = EXCLUDED.categories,
                        api_updated_at = EXCLUDED.api_updated_at,
                        synced_at = now()
                """, [
                    (
                        r["wallet"], r.get("rank"), r.get("aura_rank"),
                        r.get("aura", 0), float(r.get("deposited_amount", 0)),
                        float(r.get("withdrawn_amount", 0)), float(r.get("current_amount", 0)),
                        r.get("referral_number", 0),
                        json.dumps(r.get("categories", {})),
                        r.get("updated_at"),
                    )
                    for r in top_n
                ])

            cur.execute("""
                INSERT INTO aura_snapshots
                    (week_index, total_aura, total_wallets, total_deposited, total_current, total_withdrawn)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (week_index, total_aura, total_wallets, total_deposited, total_current, total_withdrawn))

        conn.commit()
        logger.info("[aura] Sync done: %d wallets stored, total_aura=%d, week=%d", len(top_n), total_aura, week_index)
        return {"synced": len(top_n), "total_aura": total_aura, "week_index": week_index}
    except Exception:
        conn.rollback()
        raise
    finally:
        stats._put_conn(conn)


def get_aura_stats():
    ensure_aura_schema()
    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT week_index, snapshot_at, total_aura, total_wallets,
                       total_deposited, total_current, total_withdrawn
                FROM aura_snapshots
                ORDER BY snapshot_at DESC
                LIMIT 2
            """)
            rows = cur.fetchall()
    finally:
        stats._put_conn(conn)

    if not rows:
        return {
            "total_aura": None,
            "total_wallets": None,
            "total_deposited": None,
            "total_current": None,
            "week_index": None,
            "weekly_gain": None,
            "last_synced": None,
        }

    latest = rows[0]
    prev = rows[1] if len(rows) > 1 else None
    weekly_gain = None
    if prev:
        time_diff = (latest[1] - prev[1]).total_seconds()
        if time_diff >= 6 * 3600:  # only compare snapshots at least 6 hours apart
            weekly_gain = int(latest[2]) - int(prev[2])

    return {
        "total_aura": latest[2],
        "total_wallets": latest[3],
        "total_deposited": float(latest[4]) if latest[4] else None,
        "total_current": float(latest[5]) if latest[5] else None,
        "week_index": latest[0],
        "weekly_gain": weekly_gain,
        "last_synced": latest[1].isoformat() if latest[1] else None,
    }


def get_aura_leaderboard(sort_by: str = "aura", limit: int = 10):
    ensure_aura_schema()
    limit = min(max(1, limit), 200)
    order_col = "aura" if sort_by == "aura" else "deposited_amount"

    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT wallet, rank, aura_rank, aura, deposited_amount,
                       withdrawn_amount, current_amount, referral_number, categories
                FROM aura_leaderboard
                ORDER BY {order_col} DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    finally:
        stats._put_conn(conn)

    return [
        {
            "wallet": r[0],
            "rank": r[1],
            "aura_rank": r[2],
            "aura": r[3],
            "deposited_amount": float(r[4]) if r[4] else 0,
            "withdrawn_amount": float(r[5]) if r[5] else 0,
            "current_amount": float(r[6]) if r[6] else 0,
            "referral_number": r[7],
            "categories": r[8],
        }
        for r in rows
    ]


def set_week_index(week: int) -> dict:
    ensure_aura_schema()
    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE aura_snapshots
                SET week_index = %s
                WHERE id = (SELECT id FROM aura_snapshots ORDER BY snapshot_at DESC LIMIT 1)
            """, (week,))
            updated = cur.rowcount
        conn.commit()
        logger.info("[aura] week_index manually set to %d", week)
        return {"week_index": week, "updated_rows": updated}
    finally:
        stats._put_conn(conn)


def proxy_wallet(address: str) -> Optional[dict]:
    ensure_aura_schema()

    conn = stats._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT wallet, rank, aura_rank, aura, deposited_amount,
                       withdrawn_amount, current_amount, referral_number, categories
                FROM aura_leaderboard
                WHERE LOWER(wallet) = LOWER(%s)
            """, (address,))
            row = cur.fetchone()
    finally:
        stats._put_conn(conn)

    if row:
        return {
            "wallet": row[0], "rank": row[1], "aura_rank": row[2],
            "aura": row[3], "deposited_amount": float(row[4]) if row[4] else 0,
            "withdrawn_amount": float(row[5]) if row[5] else 0,
            "current_amount": float(row[6]) if row[6] else 0,
            "referral_number": row[7], "categories": row[8],
            "source": "db",
        }

    return None
