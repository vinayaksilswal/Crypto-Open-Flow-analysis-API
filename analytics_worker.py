"""
Analytics Worker (The Cruncher) — Production Multi-Exchange
Calculates real-time metrics from PostgreSQL across all exchanges and caches
them in Redis for sub-millisecond API access.

Cached metrics per symbol:
  - Cross-exchange aggregated price, VWAP, CVD
  - Multi-timeframe CVD (5m, 15m, 1h, 4h, 24h)
  - Order imbalance ratio
  - Buyer/Seller ratio (count, volume, USD — multi-timeframe)
  - Price change stats (5m, 15m, 1h, 4h, 24h + 24h high/low)
  - Long/Short ratio (per exchange from REST polls)
  - Large trades (>$100K in last hour)
  - Liquidation heatmap (by price bucket)
  - Funding rates (per exchange)
  - Open interest (per exchange + total)
  - Composite market sentiment score
  - OHLCV candle cache (multiple timeframes)
  - Health heartbeat
"""

import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from dotenv import load_dotenv

import asyncpg
import redis.asyncio as aioredis

# ── 1. Environment & Logging ──────────────────────────────────────────────────
try:
    # Prefer env.local for local development, but don't override real environment variables
    from pathlib import Path

    _env_local = Path(__file__).with_name("env.local")
    if _env_local.exists():
        load_dotenv(dotenv_path=_env_local, override=False)
except Exception:
    pass

# Default behavior: load .env if present (again, without overriding existing env vars)
load_dotenv(override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Cruncher")

# ── 2. Configuration ─────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto_data")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
CRUNCH_INTERVAL = float(os.getenv("CRUNCH_INTERVAL", "1.0"))
LARGE_TRADE_THRESHOLD_USD = float(os.getenv("LARGE_TRADE_THRESHOLD_USD", "100000"))
ENABLED_EXCHANGES = [e.strip().lower() for e in os.getenv("ENABLED_EXCHANGES", "binance,bybit,okx").split(",") if e.strip()]

CVD_RAW_TIMEFRAMES = [(5, "5m"), (15, "15m")]
CVD_OHLCV_TIMEFRAMES = [(60, "1h"), (240, "4h"), (1440, "24h")]
# ── 3. Global State ──────────────────────────────────────────────────────────
DB_POOL = None
REDIS_CLIENT = None
shutdown_event = asyncio.Event()

stats = {"cycles": 0, "errors": 0, "last_cycle_ms": 0}


# ── 4. Connection Setup ──────────────────────────────────────────────────────
async def init_services():
    global DB_POOL, REDIS_CLIENT

    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to PostgreSQL (attempt {attempt}/{max_retries})...")
            DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=32, command_timeout=20, statement_cache_size=0)
            async with DB_POOL.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info("[OK] PostgreSQL connected.")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"[FATAL] PostgreSQL failed: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"PostgreSQL unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Redis (attempt {attempt}/{max_retries})...")
            REDIS_CLIENT = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
            await REDIS_CLIENT.ping()
            logger.info("[OK] Redis connected.")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"[FATAL] Redis failed: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"Redis unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)


async def wait_for_tables():
    """Wait until the ingestion worker has created the required tables."""
    required_tables = ['trades', 'liquidations', 'funding_rates', 'open_interest', 'ohlcv_1m', 'long_short_ratio']
    max_wait = 120  # seconds
    waited = 0
    interval = 3
    while waited < max_wait:
        try:
            async with DB_POOL.acquire() as conn:
                existing = await conn.fetch(
                    """SELECT tablename FROM pg_tables
                       WHERE schemaname = 'public' AND tablename = ANY($1::text[])""",
                    required_tables,
                )
                found = {r['tablename'] for r in existing}
                missing = set(required_tables) - found
                if not missing:
                    logger.info(f"[OK] All required tables exist: {', '.join(required_tables)}")
                    return True
                logger.info(f"Waiting for tables: {', '.join(missing)} (waited {waited}s)...")
        except Exception as e:
            logger.warning(f"Table check error: {e}")
        await asyncio.sleep(interval)
        waited += interval
    logger.error(f"[WARN] Some tables missing after {max_wait}s — starting anyway (queries may fail gracefully)")
    return False


async def ensure_indexes():
    """Ensure essential indexes exist for performance."""
    logger.info("Checking database indexes...")
    try:
        async with DB_POOL.acquire() as conn:
            # We use CREATE INDEX CONCURRENTLY to avoid locking the table.
            # Note: concurrently cannot be run inside a transaction block in some drivers, 
            # but asyncpg's connection.execute outside of a manual transaction is usually fine.
            # However, PostgreSQL requires 'CONCURRENTLY' to be run in its own transaction.
            # asyncpg's execute() runs in a transaction if one isn't open.
            # We'll run them one by one.
            
            indexes = [
                ("idx_ohlcv_1m_sym_exch_time", "ON ohlcv_1m (symbol, exchange, open_time DESC)"),
                ("idx_trades_sym_time", "ON trades (symbol, timestamp DESC)"),
                ("idx_trades_sym_time_sell", "ON trades (symbol, timestamp DESC, is_sell)")
            ]
            
            for idx_name, idx_def in indexes:
                # Check if exists first since CONCURRENTLY is tricky with IF NOT EXISTS in some PG versions/drivers
                exists = await conn.fetchval("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1", idx_name)
                if not exists:
                    logger.info(f"Creating index {idx_name}...")
                    # asyncpg execute doesn't allow CREATE INDEX CONCURRENTLY inside a transaction.
                    # We use the underlying connection and avoid using a transaction block.
                    # But if we just acquired from pool, it's fresh.
                    try:
                        await conn.execute(f"CREATE INDEX CONCURRENTLY {idx_name} {idx_def}")
                        logger.info(f"[OK] Index {idx_name} created.")
                    except Exception as e:
                        logger.warning(f"Failed to create index {idx_name}: {e}")
                else:
                    logger.info(f"Index {idx_name} already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating indexes: {e}")


# ── 5. Metrics Calculation ────────────────────────────────────────────────────

CVD_TIMEFRAMES = [
    (5, "5m"),
    (15, "15m"),
    (60, "1h"),
    (240, "4h"),
    (1440, "24h"),
]


# ── 5. Parallel Fetchers for Metrics ──────────────────────────────────────────

async def _fetch_prices(symbol: str):
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH latest_overall AS (
              SELECT price, exchange, timestamp
              FROM trades
              WHERE symbol = $1
              ORDER BY timestamp DESC
              LIMIT 1
            ),
            latest_by_exchange AS (
              SELECT DISTINCT ON (exchange) exchange, price, timestamp
              FROM trades
              WHERE symbol = $1 AND exchange = ANY($2::text[])
              ORDER BY exchange, timestamp DESC
            )
            SELECT 'overall'::text AS scope, exchange, price FROM latest_overall
            UNION ALL
            SELECT 'by_exchange'::text AS scope, exchange, price FROM latest_by_exchange
            """,
            symbol,
            ENABLED_EXCHANGES,
        )
        return rows

async def _fetch_24h_stats(symbol: str):
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow(
            """SELECT 
                 SUM(((high + low + close) / 3) * volume) / NULLIF(SUM(volume), 0) as vwap,
                 MAX(high) as high_24h, 
                 MIN(low) as low_24h,
                 SUM(volume * close) as volume_usd_24h
               FROM ohlcv_1m
               WHERE symbol = $1 AND exchange = 'all' AND open_time >= NOW() - INTERVAL '24 hours'""",
            symbol,
        )

async def _fetch_cvd_bs_short(symbol: str):
    async with DB_POOL.acquire() as conn:
        # Consolidated short-TF CVD/BS query
        stats = await conn.fetchrow("""
            SELECT
              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN 1 ELSE 0 END) as bc_5m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN 1 ELSE 0 END) as sc_5m,
              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN quantity ELSE 0 END) as bv_5m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN quantity ELSE 0 END) as sv_5m,
              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN price * quantity ELSE 0 END) as bu_5m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN price * quantity ELSE 0 END) as su_5m,
              COUNT(CASE WHEN timestamp >= NOW()-INTERVAL '5 min' THEN 1 END) as tc_5m,

              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN 1 ELSE 0 END) as bc_15m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min'  THEN 1 ELSE 0 END) as sc_15m,
              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN quantity ELSE 0 END) as bv_15m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min'  THEN quantity ELSE 0 END) as sv_15m,
              SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN price * quantity ELSE 0 END) as bu_15m,
              SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min'  THEN price * quantity ELSE 0 END) as su_15m,
              COUNT(CASE WHEN timestamp >= NOW()-INTERVAL '15 min' THEN 1 END) as tc_15m
            FROM trades
            WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '15 minutes'
        """, symbol)
        
        # Old prices for 5m, 15m
        old_prices = await conn.fetch("""
            (SELECT price as p, '5m' as tf FROM trades WHERE symbol = $1 AND timestamp <= NOW() - INTERVAL '5 min' ORDER BY timestamp DESC LIMIT 1)
            UNION ALL
            (SELECT price as p, '15m' as tf FROM trades WHERE symbol = $1 AND timestamp <= NOW() - INTERVAL '15 min' ORDER BY timestamp DESC LIMIT 1)
        """, symbol)
        
        return stats, old_prices

async def _fetch_cvd_bs_long(symbol: str):
    async with DB_POOL.acquire() as conn:
        # Consolidated long-TF CVD/BS query (from ohlcv_1m)
        stats = await conn.fetchrow("""
            SELECT
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN buy_volume ELSE 0 END) as bv_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN sell_volume ELSE 0 END) as sv_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN trade_count ELSE 0 END) as tc_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN trade_count * COALESCE(buy_volume / NULLIF(volume, 0), 0.5) ELSE 0 END) as abc_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN trade_count * COALESCE(sell_volume / NULLIF(volume, 0), 0.5) ELSE 0 END) as asc_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN buy_volume * close ELSE 0 END) as abu_1h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '1h' THEN sell_volume * close ELSE 0 END) as asu_1h,

              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN buy_volume ELSE 0 END) as bv_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN sell_volume ELSE 0 END) as sv_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN trade_count ELSE 0 END) as tc_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN trade_count * COALESCE(buy_volume / NULLIF(volume, 0), 0.5) ELSE 0 END) as abc_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN trade_count * COALESCE(sell_volume / NULLIF(volume, 0), 0.5) ELSE 0 END) as asc_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN buy_volume * close ELSE 0 END) as abu_4h,
              SUM(CASE WHEN open_time >= NOW() - INTERVAL '4h' THEN sell_volume * close ELSE 0 END) as asu_4h,

              SUM(buy_volume) as bv_24h,
              SUM(sell_volume) as sv_24h,
              SUM(trade_count) as tc_24h,
              SUM(trade_count * COALESCE(buy_volume / NULLIF(volume, 0), 0.5)) as abc_24h,
              SUM(trade_count * COALESCE(sell_volume / NULLIF(volume, 0), 0.5)) as asc_24h,
              SUM(buy_volume * close) as abu_24h,
              SUM(sell_volume * close) as asu_24h
            FROM ohlcv_1m
            WHERE symbol = $1 AND exchange = 'all' AND open_time >= NOW() - INTERVAL '24 hours'
        """, symbol)

        # Old prices for 1h, 4h, 24h
        old_prices = await conn.fetch("""
            (SELECT open as p, '1h' as tf FROM ohlcv_1m WHERE symbol = $1 AND exchange = 'all' AND open_time <= NOW() - INTERVAL '1h' ORDER BY open_time DESC LIMIT 1)
            UNION ALL
            (SELECT open as p, '4h' as tf FROM ohlcv_1m WHERE symbol = $1 AND exchange = 'all' AND open_time <= NOW() - INTERVAL '4h' ORDER BY open_time DESC LIMIT 1)
            UNION ALL
            (SELECT open as p, '24h' as tf FROM ohlcv_1m WHERE symbol = $1 AND exchange = 'all' AND open_time <= NOW() - INTERVAL '24h' ORDER BY open_time DESC LIMIT 1)
        """, symbol)
        
        return stats, old_prices

async def _fetch_ls_ratio(symbol: str):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            """
            SELECT DISTINCT ON (exchange)
              exchange, long_ratio, short_ratio, long_short_ratio, ratio_type, timestamp
            FROM long_short_ratio
            WHERE symbol = $1 AND exchange = ANY($2::text[])
            ORDER BY exchange, timestamp DESC
            """,
            symbol,
            ENABLED_EXCHANGES,
        )

async def _fetch_exchange_cvd(symbol: str):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            """SELECT exchange,
                 SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END) as buy_vol,
                 SUM(CASE WHEN is_sell = true  THEN quantity ELSE 0 END) as sell_vol
               FROM trades
               WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '15 minutes'
               GROUP BY exchange""",
            symbol,
        )

async def _fetch_large_trades(symbol: str):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            """SELECT exchange, price, quantity, is_sell, timestamp
               FROM trades
               WHERE symbol = $1
                 AND timestamp >= NOW() - INTERVAL '15 minutes'
                 AND price * quantity > $2
               ORDER BY timestamp DESC
               LIMIT 20""",
            symbol, LARGE_TRADE_THRESHOLD_USD,
        )

async def _fetch_liquidations(symbol: str):
    async with DB_POOL.acquire() as conn:
        # Heatmap and Recent liquidations can slightly overlap in data fetch if we wanted to consolidate,
        # but for now independent fetchers are cleaner.
        heatmap = await conn.fetch(
            """SELECT
                 FLOOR(price / (price * 0.002)) as price_bucket, -- Dummy bucket for now, logic below
                 side,
                 COUNT(*) as liq_count,
                 SUM(quantity) as total_qty,
                 SUM(price * quantity) as total_usd
               FROM liquidations
               WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '24 hours'
               GROUP BY price_bucket, side
               ORDER BY total_usd DESC
               LIMIT 50""",
            symbol,
        )
        # Wait, the heatmap needs bucket_size which depends on last_price.
        # I'll just fetch raw liquidations for the last 24h and bucket them in Python.
        # This avoids sequential dependency.
        raw_liq_24h = await conn.fetch(
            """SELECT price, side, quantity, price * quantity as usd_value
               FROM liquidations
               WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '24 hours'""",
            symbol
        )
        recent_liqs = await conn.fetch(
            """SELECT exchange, side, price, quantity, timestamp
               FROM liquidations
               WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '1 hour'
               ORDER BY timestamp DESC LIMIT 15""",
            symbol,
        )
        return raw_liq_24h, recent_liqs

async def _fetch_funding(symbol: str):
    async with DB_POOL.acquire() as conn:
        return await conn.fetch(
            """
            SELECT DISTINCT ON (exchange)
              exchange, funding_rate, funding_time
            FROM funding_rates
            WHERE symbol = $1 AND exchange = ANY($2::text[])
            ORDER BY exchange, funding_time DESC
            """,
            symbol,
            ENABLED_EXCHANGES,
        )

async def _fetch_open_interest(symbol: str):
    async with DB_POOL.acquire() as conn:
        latest_oi = await conn.fetch(
            """
            SELECT DISTINCT ON (exchange)
              exchange, oi_value, timestamp
            FROM open_interest
            WHERE symbol = $1 AND exchange = ANY($2::text[])
            ORDER BY exchange, timestamp DESC
            """,
            symbol,
            ENABLED_EXCHANGES,
        )
        oi_1h_ago = await conn.fetchrow(
            """SELECT SUM(oi_value) as total_oi
               FROM open_interest
               WHERE symbol = $1
                 AND timestamp >= NOW() - INTERVAL '65 minutes'
                 AND timestamp <= NOW() - INTERVAL '55 minutes'""",
            symbol,
        )
        return latest_oi, oi_1h_ago

async def _fetch_depth_redis(symbol: str):
    pipe = REDIS_CLIENT.pipeline()
    for exch in ENABLED_EXCHANGES:
        pipe.get(f"depth:{symbol}:{exch}")
    return await pipe.execute()


async def calculate_metrics_for_symbol(symbol: str):
    """Calculate ALL analytics metrics for a symbol across all exchanges."""
    try:
        return await _calculate_metrics_impl(symbol)
    except asyncpg.PostgresError as e:
        # Covers UndefinedTableError, UndefinedColumnError, etc. during startup
        logger.debug(f"DB query issue for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Metrics calculation error for {symbol}: {e}")
        return None


async def _calculate_metrics_impl(symbol: str):
    """Inner metrics calculation — optimized with parallel fetchers."""
    # ── 1. Phase 1: Parallel Remote Fetches ──────────────────────────────────
    # Most metrics are independent and can be fetched in parallel.
    results = await asyncio.gather(
        _fetch_prices(symbol),
        _fetch_24h_stats(symbol),
        _fetch_cvd_bs_short(symbol),
        _fetch_cvd_bs_long(symbol),
        _fetch_ls_ratio(symbol),
        _fetch_exchange_cvd(symbol),
        _fetch_large_trades(symbol),
        _fetch_liquidations(symbol),
        _fetch_funding(symbol),
        _fetch_open_interest(symbol),
        _fetch_depth_redis(symbol),
    )
    
    (
        price_rows, stats_24h_row, 
        cvd_short_res, cvd_long_res,
        ls_rows, exch_cvd_rows, 
        large_trades, liq_res,
        funding_rows, oi_res,
        depth_raw_list
    ) = results

    # ── 2. Phase 2: Results Assembly & Processing ────────────────────────────
    
    # Prices
    exchange_prices: dict[str, float] = {}
    last_price = 0.0
    for r in price_rows:
        if r["scope"] == "overall":
            last_price = float(r["price"]) if r["price"] is not None else 0.0
        else:
            exchange_prices[str(r["exchange"])] = float(r["price"]) if r["price"] is not None else 0.0

    # 24h Stats
    vwap = float(stats_24h_row["vwap"]) if stats_24h_row and stats_24h_row["vwap"] else 0.0
    high_24h = float(stats_24h_row["high_24h"]) if stats_24h_row and stats_24h_row["high_24h"] else 0.0
    low_24h = float(stats_24h_row["low_24h"]) if stats_24h_row and stats_24h_row["low_24h"] else 0.0
    vol_usd_24h = float(stats_24h_row["volume_usd_24h"]) if stats_24h_row and stats_24h_row["volume_usd_24h"] else 0.0

    # CVD & Buyer/Seller (Short)
    cvd_data = {}
    buyer_seller = {}
    price_changes = {}
    short_stats, short_old_prices = cvd_short_res
    
    # Map old prices for easy access
    old_p_map = {r["tf"]: float(r["p"]) for r in short_old_prices}
    for label in ["5m", "15m"]:
        bc = int(short_stats[f"bc_{label}"]) if short_stats else 0
        sc = int(short_stats[f"sc_{label}"]) if short_stats else 0
        bv = float(short_stats[f"bv_{label}"]) if short_stats else 0.0
        sv = float(short_stats[f"sv_{label}"]) if short_stats else 0.0
        bu = float(short_stats[f"bu_{label}"]) if short_stats else 0.0
        su = float(short_stats[f"su_{label}"]) if short_stats else 0.0
        tc = int(short_stats[f"tc_{label}"]) if short_stats else 0
        
        cvd_data[label] = {"cvd": round(bv - sv, 4), "buy_volume": round(bv, 4), "sell_volume": round(sv, 4), "trade_count": tc}
        total_count = bc + sc
        buyer_seller[label] = {
            "buy_count": bc, "sell_count": sc, "ratio_by_count": round(bc / sc, 4) if sc > 0 else 0,
            "buy_volume": round(bv, 4), "sell_volume": round(sv, 4), "ratio_by_volume": round(bv / sv, 4) if sv > 0 else 0,
            "buy_usd": round(bu, 2), "sell_usd": round(su, 2), "ratio_by_usd": round(bu / su, 4) if su > 0 else 0,
            "buy_pct": round(bc / total_count * 100, 2) if total_count > 0 else 50.0,
            "sell_pct": round(sc / total_count * 100, 2) if total_count > 0 else 50.0,
        }
        old_p = old_p_map.get(label, 0.0)
        if old_p > 0 and last_price > 0:
            pct = round((last_price - old_p) / old_p * 100, 4)
            price_changes[label] = {"change_pct": pct, "old_price": round(old_p, 2), "direction": "up" if pct > 0 else "down" if pct < 0 else "flat"}
        else:
            price_changes[label] = {"change_pct": 0, "old_price": 0, "direction": "flat"}

    # CVD & Buyer/Seller (Long)
    long_stats, long_old_prices = cvd_long_res
    old_p_map_long = {r["tf"]: float(r["p"]) for r in long_old_prices}
    for label in ["1h", "4h", "24h"]:
        bv = float(long_stats[f"bv_{label}"]) if long_stats else 0.0
        sv = float(long_stats[f"sv_{label}"]) if long_stats else 0.0
        tc = int(long_stats[f"tc_{label}"]) if long_stats else 0
        bc = int(long_stats[f"abc_{label}"]) if long_stats else 0
        sc = int(long_stats[f"asc_{label}"]) if long_stats else 0
        bu = float(long_stats[f"abu_{label}"]) if long_stats else 0.0
        su = float(long_stats[f"asu_{label}"]) if long_stats else 0.0
        
        cvd_data[label] = {"cvd": round(bv - sv, 4), "buy_volume": round(bv, 4), "sell_volume": round(sv, 4), "trade_count": tc}
        total_count = bc + sc
        buyer_seller[label] = {
            "buy_count": bc, "sell_count": sc, "ratio_by_count": round(bc / sc, 4) if sc > 0 else 0,
            "buy_volume": round(bv, 4), "sell_volume": round(sv, 4), "ratio_by_volume": round(bv / sv, 4) if sv > 0 else 0,
            "buy_usd": round(bu, 2), "sell_usd": round(su, 2), "ratio_by_usd": round(bu / su, 4) if su > 0 else 0,
            "buy_pct": round(bc / total_count * 100, 2) if total_count > 0 else 50.0,
            "sell_pct": round(sc / total_count * 100, 2) if total_count > 0 else 50.0,
        }
        old_p = old_p_map_long.get(label, 0.0)
        if old_p > 0 and last_price > 0:
            pct = round((last_price - old_p) / old_p * 100, 4)
            price_changes[label] = {"change_pct": pct, "old_price": round(old_p, 2), "direction": "up" if pct > 0 else "down" if pct < 0 else "flat"}
        else:
            price_changes[label] = {"change_pct": 0, "old_price": 0, "direction": "flat"}

    # Order Book & Arbitrage (from depth_raw_list)
    orderbook_depth = {}
    total_bid_vol = 0.0
    total_ask_vol = 0.0
    min_price_exch, max_price_exch = None, None
    min_price, max_price = float('inf'), 0.0
    for i, exch in enumerate(ENABLED_EXCHANGES):
        if exchange_prices.get(exch):
            p = exchange_prices[exch]
            if p < min_price: min_price = p; min_price_exch = exch
            if p > max_price: max_price = p; max_price_exch = exch
        raw = depth_raw_list[i] if i < len(depth_raw_list) else None
        if raw:
            try:
                book = json.loads(raw)
                bids, asks = book.get("bids", [])[:20], book.get("asks", [])[:20]
                bv, av = sum(float(v) for p, v in bids), sum(float(v) for p, v in asks)
                total_bid_vol += bv; total_ask_vol += av
                orderbook_depth[exch] = {"bids": bids, "asks": asks, "ts": book.get("ts", 0), "bid_vol": bv, "ask_vol": av}
            except: pass
    arbitrage = None
    if min_price > 0 and max_price > 0 and min_price != float('inf') and min_price_exch != max_price_exch:
        spread_pct = (max_price - min_price) / min_price * 100
        if spread_pct > 0.05:
            arbitrage = {"buy_exchange": min_price_exch, "sell_exchange": max_price_exch, "spread_pct": round(spread_pct, 4), "spread_usd": round(max_price - min_price, 2), "min_price": min_price, "max_price": max_price}
    total_depth_vol = total_bid_vol + total_ask_vol
    ob_imbalance = (total_bid_vol - total_ask_vol) / total_depth_vol if total_depth_vol > 0 else 0.0

    # L/S Ratio
    ls_ratio = {}
    for row in ls_rows:
        exch = str(row["exchange"])
        ls_ratio[exch] = {"long_pct": round(float(row["long_ratio"]) * 100, 2), "short_pct": round(float(row["short_ratio"]) * 100, 2), "ratio": round(float(row["long_short_ratio"]), 4), "type": row["ratio_type"], "updated": row["timestamp"].isoformat()}
    avg_ls = sum(d["ratio"] for d in ls_ratio.values()) / len(ls_ratio) if ls_ratio else 0.0

    # Exchange CVD
    exchange_cvd = {exch: {"cvd": 0.0, "buy": 0.0, "sell": 0.0} for exch in ENABLED_EXCHANGES}
    for r in exch_cvd_rows:
        exch = r["exchange"]
        if exch in exchange_cvd:
            b, s = float(r["buy_vol"]), float(r["sell_vol"])
            exchange_cvd[exch] = {"cvd": round(b - s, 4), "buy": round(b, 4), "sell": round(s, 4)}

    # Imbalance & Spoofing
    buy_1h = cvd_data.get("1h", {}).get("buy_volume", 0)
    sell_1h = cvd_data.get("1h", {}).get("sell_volume", 0)
    total_1h = buy_1h + sell_1h
    imbalance = round((buy_1h - sell_1h) / total_1h, 4) if total_1h > 0 else 0.0
    deviation = ob_imbalance - imbalance
    spoofing = {"detected": abs(deviation) > 0.4, "signal": ("fake_bids" if deviation > 0 else "fake_asks") if abs(deviation) > 0.4 else "None", "deviation": round(deviation, 2)}

    # Whale Flows (from large_trades)
    whale_flow = {"buy_usd": 0.0, "sell_usd": 0.0, "net_usd": 0.0, "count": 0}
    for lt in large_trades:
        usd = float(lt["price"]) * float(lt["quantity"])
        whale_flow["count"] += 1
        if lt["is_sell"]: whale_flow["sell_usd"] += usd
        else: whale_flow["buy_usd"] += usd
    whale_flow["net_usd"] = round(whale_flow["buy_usd"] - whale_flow["sell_usd"], 2)
    whale_flow = {k: (round(v, 2) if isinstance(v, float) else v) for k, v in whale_flow.items()}

    # Liquidations
    raw_liq_24h, recent_liqs = liq_res
    liq_heatmap = []
    if last_price > 0:
        bucket_size = last_price * 0.002
        buckets = {}
        for r in raw_liq_24h:
            lvl = math.floor(float(r["price"]) / bucket_size) * bucket_size
            key = (lvl, r["side"])
            if key not in buckets: buckets[key] = {"price_level": lvl, "side": r["side"], "count": 0, "total_quantity": 0.0, "total_usd": 0.0}
            buckets[key]["count"] += 1
            buckets[key]["total_quantity"] += float(r["quantity"])
            buckets[key]["total_usd"] += float(r["usd_value"])
        liq_heatmap = sorted(buckets.values(), key=lambda x: x["total_usd"], reverse=True)[:50]

    # Funding Rates
    funding_data = {}
    for row in funding_rows:
        exch = str(row["exchange"]); rate = float(row["funding_rate"])
        funding_data[exch] = {"rate": rate, "annualized": round(rate * 3 * 365 * 100, 2), "time": row["funding_time"].isoformat()}
    avg_funding = sum(f["rate"] for f in funding_data.values()) / len(funding_data) if funding_data else 0.0

    # Open Interest
    latest_oi_rows, oi_1h_row = oi_res
    oi_data = {}; total_oi = 0.0
    for row in latest_oi_rows:
        exch = str(row["exchange"]); val = float(row["oi_value"])
        oi_data[exch] = {"value": val, "updated": row["timestamp"].isoformat()}
        total_oi += val
    old_oi = float(oi_1h_row["total_oi"]) if oi_1h_row and oi_1h_row["total_oi"] else 0.0
    oi_change_1h = round((total_oi - old_oi) / old_oi * 100, 2) if old_oi > 0 and total_oi > 0 else 0.0

    # Sentiment Score
    sentiment_score = _compute_sentiment(cvd_1h=cvd_data.get("1h", {}).get("cvd", 0), cvd_4h=cvd_data.get("4h", {}).get("cvd", 0), imbalance=imbalance, avg_funding=avg_funding, oi_change_pct=oi_change_1h, buy_vol=buy_1h, sell_vol=sell_1h)

    # ── 3. Build Payload & Cache ─────────────────────────────────────────────
    payload = {
        "symbol": symbol, "price": last_price, "exchange_prices": exchange_prices, "24h_vwap": round(vwap, 2),
        "cvd": cvd_data, "exchange_cvd": exchange_cvd, "order_imbalance_1h": imbalance, "buyer_seller_ratio": buyer_seller,
        "price_changes": price_changes, "24h_high": round(high_24h, 2), "24h_low": round(low_24h, 2), "24h_volume_usd": round(vol_usd_24h, 2),
        "long_short_ratio": {"by_exchange": ls_ratio, "average_ratio": round(avg_ls, 4)},
        "large_trades_1h": [{"exchange": t["exchange"], "price": float(t["price"]), "quantity": float(t["quantity"]), "side": "sell" if t["is_sell"] else "buy", "usd_value": round(float(t["price"]) * float(t["quantity"]), 2), "timestamp": t["timestamp"].isoformat()} for t in large_trades],
        "recent_liquidations": [{"exchange": r["exchange"], "side": r["side"], "price": float(r["price"]), "quantity": float(r["quantity"]), "usd_value": round(float(r["price"]) * float(r["quantity"]), 2), "timestamp": r["timestamp"].isoformat()} for r in recent_liqs],
        "liquidation_heatmap": liq_heatmap, "funding_rates": funding_data, "avg_funding_rate": round(avg_funding, 8),
        "open_interest": {"total": round(total_oi, 4), "by_exchange": oi_data, "change_1h_pct": oi_change_1h},
        "sentiment": sentiment_score, "arbitrage": arbitrage, "spoofing": spoofing, "whale_flow": whale_flow,
        "exchanges": ENABLED_EXCHANGES, "updated_at_ms": int(time.time() * 1000),
    }

    await REDIS_CLIENT.set(f"live_metrics:{symbol}", json.dumps(payload))
    return payload


def _compute_sentiment(cvd_1h, cvd_4h, imbalance, avg_funding, oi_change_pct, buy_vol, sell_vol):
    """
    Composite market sentiment from -100 (extreme bearish) to +100 (extreme bullish).
    Components:
      - CVD direction (25%)
      - Order imbalance (25%)
      - Funding rate signal (25%)
      - OI change (25%)
    """
    signals = []

    # CVD signal: normalize to -1..+1
    total_vol = buy_vol + sell_vol
    if total_vol > 0:
        cvd_norm = max(-1, min(1, cvd_1h / total_vol * 2))
    else:
        cvd_norm = 0
    signals.append(("cvd_momentum", cvd_norm, 0.25))

    # Imbalance: already in -1..+1
    signals.append(("order_imbalance", max(-1, min(1, imbalance * 2)), 0.25))

    # Funding rate: negative funding = bullish (shorts paying longs), positive = bearish
    # Typical range: -0.01 to +0.01
    funding_signal = max(-1, min(1, -avg_funding * 100))
    signals.append(("funding_rate", funding_signal, 0.25))

    # OI change: rising OI + bullish CVD = strong bullish
    oi_signal = max(-1, min(1, oi_change_pct / 5))
    if cvd_norm < 0:
        oi_signal = -abs(oi_signal)  # Rising OI + bearish CVD = bearish
    signals.append(("oi_momentum", oi_signal, 0.25))

    # Weighted score
    score = sum(s * w for _, s, w in signals) * 100
    score = round(max(-100, min(100, score)), 1)

    if score > 40:
        label = "strongly_bullish"
    elif score > 15:
        label = "bullish"
    elif score > -15:
        label = "neutral"
    elif score > -40:
        label = "bearish"
    else:
        label = "strongly_bearish"

    return {
        "score": score,
        "label": label,
        "components": {name: round(val * 100, 1) for name, val, _ in signals},
    }


# ── 6. OHLCV Cache ───────────────────────────────────────────────────────────
async def cache_ohlcv():
    """Cache recent OHLCV candles in Redis for fast API access — Parallelized."""
    tasks = [_cache_ohlcv_for_symbol(sym) for sym in SYMBOLS]
    await asyncio.gather(*tasks)

async def _cache_ohlcv_for_symbol(sym: str):
    """Worker for a single symbol's OHLCV caching."""
    try:
        # Cache 1m candles + build other timeframes in parallel
        # Each gets its own connection from the pool for true parallelism
        tf_configs = [
            (None, "1m"), # 1m is direct fetch
            (5, "5m"), (15, "15m"), (60, "1h"), (240, "4h")
        ]
        tasks = [_cache_ohlcv_tf_impl(sym, tf_min, tf_key) for tf_min, tf_key in tf_configs]
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"OHLCV cache error for {sym}: {e}")

async def _cache_ohlcv_tf_impl(sym: str, tf_minutes: int | None, tf_key: str):
    """Fetcher for a specific timeframe candle set."""
    async with DB_POOL.acquire() as conn:
        if tf_minutes is None:
            # 1m Direct Fetch
            rows = await conn.fetch(
                """SELECT open_time, open, high, low, close, volume, buy_volume, sell_volume, trade_count
                   FROM ohlcv_1m
                   WHERE symbol = $1
                   ORDER BY open_time DESC
                   LIMIT 500""",
                sym,
            )
        else:
            # Aggregated timeframe fetch
            limit = 500 * tf_minutes
            rows = await conn.fetch(
                f"""SELECT
                      date_trunc('hour', open_time) +
                        INTERVAL '1 minute' * (EXTRACT(MINUTE FROM open_time)::int / {tf_minutes} * {tf_minutes})
                        as bucket,
                      (ARRAY_AGG(open ORDER BY open_time ASC))[1] as open,
                      MAX(high) as high,
                      MIN(low) as low,
                      (ARRAY_AGG(close ORDER BY open_time DESC))[1] as close,
                      SUM(volume) as volume,
                      SUM(buy_volume) as buy_volume,
                      SUM(sell_volume) as sell_volume,
                      SUM(trade_count) as trade_count
                    FROM ohlcv_1m
                    WHERE symbol = $1
                      AND open_time >= NOW() - INTERVAL '1 minute' * {limit}
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT 200""",
                sym,
            )
        
        candles = [
            {
                "t": (r["open_time"] if tf_minutes is None else r["bucket"]).isoformat(),
                "o": float(r["open"]), "h": float(r["high"]), "l": float(r["low"]),
                "c": float(r["close"]), "v": float(r["volume"]),
                "bv": float(r["buy_volume"]), "sv": float(r["sell_volume"]),
                "n": int(r["trade_count"]),
            }
            for r in rows
        ]
        await REDIS_CLIENT.set(f"ohlcv:{tf_key}:{sym}", json.dumps(candles))


# ── 7. Cruncher Loop ─────────────────────────────────────────────────────────
async def cruncher_loop():
    logger.info(f"Starting Cruncher loop — {len(SYMBOLS)} symbols, {CRUNCH_INTERVAL}s interval, exchanges: {', '.join(ENABLED_EXCHANGES)}")

    ohlcv_counter = 0

    while not shutdown_event.is_set():
        cycle_start = time.monotonic()
        try:
            if shutdown_event.is_set():
                break

            # Process all symbols concurrently
            tasks = [calculate_metrics_for_symbol(symbol) for symbol in SYMBOLS]
            await asyncio.gather(*tasks)

            # Cache OHLCV every 10 cycles (~10s)
            ohlcv_counter += 1
            if ohlcv_counter >= 10:
                await cache_ohlcv()
                ohlcv_counter = 0

            # Heartbeat
            await REDIS_CLIENT.set(
                "cruncher:heartbeat",
                json.dumps({
                    "status": "healthy",
                    "symbols": SYMBOLS,
                    "exchanges": ENABLED_EXCHANGES,
                    "last_cycle_ms": stats["last_cycle_ms"],
                    "total_cycles": stats["cycles"],
                    "errors": stats["errors"],
                    "timestamp_ms": int(time.time() * 1000),
                }),
            )

            stats["cycles"] += 1
            stats["last_cycle_ms"] = round((time.monotonic() - cycle_start) * 1000, 1)

            if stats["cycles"] % 60 == 0:
                logger.info(
                    f"[HEARTBEAT] Cycles: {stats['cycles']} | "
                    f"Last: {stats['last_cycle_ms']}ms | "
                    f"Errors: {stats['errors']}"
                )

        except asyncio.CancelledError:
            break
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Cruncher cycle error: {e}")

        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0, CRUNCH_INTERVAL - elapsed)
        try:
            await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            break


# ── 8. Symbol Publisher ───────────────────────────────────────────────────────
async def publish_symbol_list():
    try:
        await REDIS_CLIENT.set("tracked_symbols", json.dumps(SYMBOLS))
        await REDIS_CLIENT.set("enabled_exchanges", json.dumps(ENABLED_EXCHANGES))
        logger.info(f"Published: symbols={SYMBOLS}, exchanges={ENABLED_EXCHANGES}")
    except Exception as e:
        logger.error(f"Failed to publish config: {e}")


# ── 9. Main ──────────────────────────────────────────────────────────────────
async def main():
    logger.info(f"Starting Cruncher — symbols: {', '.join(SYMBOLS)}, exchanges: {', '.join(ENABLED_EXCHANGES)}")
    await init_services()
    await publish_symbol_list()
    await wait_for_tables()
    await ensure_indexes()

    task = asyncio.create_task(cruncher_loop())

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: shutdown_event.set())
    except NotImplementedError:
        pass

    try:
        await shutdown_event.wait()
    except asyncio.CancelledError:
        pass

    logger.info("Shutdown signal received.")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    if DB_POOL:
        await DB_POOL.close()
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
    logger.info("Cruncher shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
