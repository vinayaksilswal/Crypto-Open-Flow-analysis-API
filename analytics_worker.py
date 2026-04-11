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
    from pathlib import Path
    _env_local = Path(__file__).with_name("env.local")
    if _env_local.exists():
        load_dotenv(dotenv_path=_env_local, override=True)
except Exception:
    pass

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Cruncher")

# ── 2. Configuration ─────────────────────────────────────────────────────────
DATABASE_URL        = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto_data")
REDIS_URL           = os.getenv("REDIS_URL", "redis://localhost:6379")
SYMBOLS             = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
CRUNCH_INTERVAL     = float(os.getenv("CRUNCH_INTERVAL", "1.0"))
LARGE_TRADE_USD     = float(os.getenv("LARGE_TRADE_THRESHOLD_USD", "100000"))
ENABLED_EXCHANGES   = [e.strip().lower() for e in os.getenv("ENABLED_EXCHANGES", "binance,bybit,okx").split(",") if e.strip()]

# Connection pool sizing:
# Cruncher uses max 2 connections per symbol in flight (trades_agg + ls/funding/oi).
# Semaphore below ensures we never exceed this pool.
# Leave the rest for Harvester (max=8) and API Gateway (max=10).
DB_POOL_MAX_SIZE = int(os.getenv("CRUNCHER_DB_POOL_MAX", "15"))

# Initialised in main() after the event loop exists
_SYMBOL_SEM: asyncio.Semaphore = None   # type: ignore[assignment]
_MAX_CONCURRENCY = int(os.getenv("CRUNCHER_MAX_CONCURRENCY", "3"))

# ── 3. Global State ──────────────────────────────────────────────────────────
DB_POOL      = None
REDIS_CLIENT = None
shutdown_event = asyncio.Event()

import uuid

stats = {"cycles": 0, "errors": 0, "last_cycle_ms": 0}
WORKER_ID = str(uuid.uuid4())[:8]


# ── 4. Connection Setup ──────────────────────────────────────────────────────
async def init_services():
    global DB_POOL, REDIS_CLIENT

    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            from urllib.parse import urlparse
            db_parts = urlparse(DATABASE_URL)
            logger.info(f"Connecting to PostgreSQL at {db_parts.hostname} (attempt {attempt}/{max_retries})...")
            DB_POOL = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=DB_POOL_MAX_SIZE,
                command_timeout=15,
                statement_cache_size=0,
            )
            async with DB_POOL.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info(f"[OK] PostgreSQL connected to {db_parts.hostname} (pool max={DB_POOL_MAX_SIZE}).")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"[FATAL] PostgreSQL failed at {urlparse(DATABASE_URL).hostname}: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"PostgreSQL unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)

    for attempt in range(1, max_retries + 1):
        try:
            from urllib.parse import urlparse
            red_parts = urlparse(REDIS_URL)
            logger.info(f"Connecting to Redis at {red_parts.hostname}:{red_parts.port} (attempt {attempt}/{max_retries})...")
            REDIS_CLIENT = aioredis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=10,
                max_connections=4,
            )
            await REDIS_CLIENT.ping()
            logger.info(f"[OK] Redis connected to {red_parts.hostname}.")
            break
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"[FATAL] Redis failed at {urlparse(REDIS_URL).hostname}: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"Redis unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)


async def wait_for_tables():
    required_tables = ['trades', 'liquidations', 'funding_rates', 'open_interest', 'ohlcv_1m', 'long_short_ratio']
    max_wait = 120
    waited   = 0
    interval = 3
    while waited < max_wait:
        try:
            async with DB_POOL.acquire() as conn:
                existing = await conn.fetch(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename=ANY($1::text[])",
                    required_tables,
                )
                missing = set(required_tables) - {r['tablename'] for r in existing}
                if not missing:
                    logger.info("[OK] All required tables exist.")
                    return True
                logger.info(f"Waiting for tables: {', '.join(missing)} ({waited}s)...")
        except Exception as e:
            logger.warning(f"Table check error: {e}")
        await asyncio.sleep(interval)
        waited += interval
    logger.error(f"[WARN] Some tables missing after {max_wait}s — starting anyway.")
    return False


async def ensure_indexes():
    logger.info("Checking database indexes...")
    try:
        async with DB_POOL.acquire() as conn:
            indexes = [
                ("idx_ohlcv_1m_sym_exch_time", "ON ohlcv_1m (symbol, exchange, open_time DESC)"),
                ("idx_trades_sym_time",          "ON trades (symbol, timestamp DESC)"),
                ("idx_trades_sym_time_sell",     "ON trades (symbol, timestamp DESC, is_sell)"),
                ("idx_liq_sym_time",             "ON liquidations (symbol, timestamp DESC)"),
                ("idx_trades_exch_sym_time",     "ON trades (exchange, symbol, timestamp DESC)"),
                ("idx_funding_sym_exch_time",    "ON funding_rates (symbol, exchange, funding_time DESC)"),
                ("idx_oi_sym_exch_time",         "ON open_interest (symbol, exchange, timestamp DESC)"),
                ("idx_ls_ratio_sym_exch_time",   "ON long_short_ratio (symbol, exchange, timestamp DESC)"),
            ]
            for idx_name, idx_def in indexes:
                exists = await conn.fetchval(
                    "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname=$1",
                    idx_name,
                )
                if not exists:
                    logger.info(f"Creating index {idx_name}...")
                    await conn.execute(f"CREATE INDEX CONCURRENTLY {idx_name} {idx_def}")
                    logger.info(f"[OK] Index {idx_name} created.")
                else:
                    logger.info(f"Index {idx_name} already exists.")
    except Exception as e:
        logger.error(f"Index check error: {e}")


# ── 5a. Fetchers ─────────────────────────────────────────────────────────────

async def _fetch_trades_agg(symbol: str):
    """
    Single connection, one big CTE query.
    Row-type tags: 'old_p_short' (5m/15m) vs 'old_p_long' (1h/4h/24h) — no collision.
    """
    async with DB_POOL.acquire() as conn:
        rows = await conn.fetch("""
        WITH latest_overall AS (
            SELECT price, exchange FROM trades
            WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1
        ),
        latest_by_exchange AS (
            SELECT DISTINCT ON (exchange) exchange, price
            FROM trades
            WHERE symbol = $1 AND exchange = ANY($2::text[])
            ORDER BY exchange, timestamp DESC
        ),
        stats_short AS (
            SELECT
                SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN quantity       ELSE 0 END) as bv_5m,
                SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN quantity       ELSE 0 END) as sv_5m,
                SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN price*quantity ELSE 0 END) as bu_5m,
                SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN price*quantity ELSE 0 END) as su_5m,
                COUNT(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '5 min'  THEN 1 END) as bc_5m,
                COUNT(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '5 min'  THEN 1 END) as sc_5m,
                COUNT(CASE WHEN              timestamp >= NOW()-INTERVAL '5 min'       THEN 1 END) as tc_5m,

                SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN quantity       ELSE 0 END) as bv_15m,
                SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min' THEN quantity       ELSE 0 END) as sv_15m,
                SUM(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN price*quantity ELSE 0 END) as bu_15m,
                SUM(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min' THEN price*quantity ELSE 0 END) as su_15m,
                COUNT(CASE WHEN is_sell=false AND timestamp >= NOW()-INTERVAL '15 min' THEN 1 END) as bc_15m,
                COUNT(CASE WHEN is_sell=true  AND timestamp >= NOW()-INTERVAL '15 min' THEN 1 END) as sc_15m,
                COUNT(CASE WHEN              timestamp >= NOW()-INTERVAL '15 min'      THEN 1 END) as tc_15m
            FROM trades WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '15 minutes'
        ),
        old_short AS (
            SELECT * FROM (SELECT price as p, '5m'  as tf FROM trades WHERE symbol=$1 AND timestamp <= NOW()-INTERVAL '5 min'  ORDER BY timestamp DESC LIMIT 1) t5m
            UNION ALL
            SELECT * FROM (SELECT price as p, '15m' as tf FROM trades WHERE symbol=$1 AND timestamp <= NOW()-INTERVAL '15 min' ORDER BY timestamp DESC LIMIT 1) t15m
        ),
        stats_long AS (
            SELECT
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN buy_volume  ELSE 0 END) as bv_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN sell_volume ELSE 0 END) as sv_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN trade_count ELSE 0 END) as tc_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN trade_count*COALESCE(buy_volume/NULLIF(volume,0),0.5)  ELSE 0 END) as abc_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN trade_count*COALESCE(sell_volume/NULLIF(volume,0),0.5) ELSE 0 END) as asc_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN buy_volume*close  ELSE 0 END) as abu_1h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '1h' THEN sell_volume*close ELSE 0 END) as asu_1h,

                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN buy_volume  ELSE 0 END) as bv_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN sell_volume ELSE 0 END) as sv_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN trade_count ELSE 0 END) as tc_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN trade_count*COALESCE(buy_volume/NULLIF(volume,0),0.5)  ELSE 0 END) as abc_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN trade_count*COALESCE(sell_volume/NULLIF(volume,0),0.5) ELSE 0 END) as asc_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN buy_volume*close  ELSE 0 END) as abu_4h,
                SUM(CASE WHEN open_time >= NOW()-INTERVAL '4h' THEN sell_volume*close ELSE 0 END) as asu_4h,

                SUM(buy_volume)  as bv_24h,
                SUM(sell_volume) as sv_24h,
                SUM(trade_count) as tc_24h,
                SUM(trade_count*COALESCE(buy_volume/NULLIF(volume,0),0.5))  as abc_24h,
                SUM(trade_count*COALESCE(sell_volume/NULLIF(volume,0),0.5)) as asc_24h,
                SUM(buy_volume*close)  as abu_24h,
                SUM(sell_volume*close) as asu_24h,
                -- merged stats_24h fields (avoids duplicate 24h ohlcv scan)
                SUM(((high+low+close)/3)*volume)/NULLIF(SUM(volume),0) as vwap,
                MAX(high)         as high_24h,
                MIN(low)          as low_24h,
                SUM(volume*close) as volume_usd_24h
            FROM ohlcv_1m WHERE symbol=$1 AND exchange='all' AND open_time >= NOW()-INTERVAL '24 hours'
        ),
        exch_cvd AS (
            SELECT exchange,
                SUM(CASE WHEN is_sell=false THEN quantity ELSE 0 END) as buy_vol,
                SUM(CASE WHEN is_sell=true  THEN quantity ELSE 0 END) as sell_vol
            FROM trades
            WHERE symbol=$1 AND exchange=ANY($2::text[]) AND timestamp >= NOW()-INTERVAL '15 minutes'
            GROUP BY exchange
        ),
        large_trades AS (
            SELECT exchange, price, quantity, is_sell, timestamp
            FROM trades
            WHERE symbol=$1 AND timestamp >= NOW()-INTERVAL '15 minutes' AND price*quantity > $3
            ORDER BY timestamp DESC LIMIT 20
        ),
        recent_liq AS (
            SELECT exchange, side, price, quantity, timestamp
            FROM liquidations
            WHERE symbol=$1 AND timestamp >= NOW()-INTERVAL '1 hour'
            ORDER BY timestamp DESC LIMIT 15
        ),
        raw_liq_24h AS (
            SELECT price, side, quantity, price*quantity as usd_value
            FROM liquidations
            WHERE symbol=$1 AND timestamp >= NOW()-INTERVAL '24 hours'
            LIMIT 5000
        ),
        old_long AS (
            SELECT * FROM (SELECT open as p, '1h'  as tf FROM ohlcv_1m WHERE symbol=$1 AND exchange='all' AND open_time <= NOW()-INTERVAL '1h'  ORDER BY open_time DESC LIMIT 1) t1h
            UNION ALL
            SELECT * FROM (SELECT open as p, '4h'  as tf FROM ohlcv_1m WHERE symbol=$1 AND exchange='all' AND open_time <= NOW()-INTERVAL '4h'  ORDER BY open_time DESC LIMIT 1) t4h
            UNION ALL
            SELECT * FROM (SELECT open as p, '24h' as tf FROM ohlcv_1m WHERE symbol=$1 AND exchange='all' AND open_time <= NOW()-INTERVAL '24h' ORDER BY open_time DESC LIMIT 1) t24h
        )

        SELECT 'price_overall' as k, exchange::text as e, price::text as v FROM latest_overall
        UNION ALL SELECT 'price_exchange', exchange::text, price::text FROM latest_by_exchange
        UNION ALL SELECT 'short_stats', 'bv_5m',  bv_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'sv_5m',  sv_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'bu_5m',  bu_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'su_5m',  su_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'bc_5m',  bc_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'sc_5m',  sc_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'tc_5m',  tc_5m::text  FROM stats_short
        UNION ALL SELECT 'short_stats', 'bv_15m', bv_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'sv_15m', sv_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'bu_15m', bu_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'su_15m', su_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'bc_15m', bc_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'sc_15m', sc_15m::text FROM stats_short
        UNION ALL SELECT 'short_stats', 'tc_15m', tc_15m::text FROM stats_short
        UNION ALL SELECT 'old_p_short', tf, p::text FROM old_short
        UNION ALL SELECT 'long_stats', 'bv_1h',   bv_1h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'sv_1h',   sv_1h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'tc_1h',   tc_1h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'abc_1h',  abc_1h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'asc_1h',  asc_1h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'abu_1h',  abu_1h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'asu_1h',  asu_1h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'bv_4h',   bv_4h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'sv_4h',   sv_4h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'tc_4h',   tc_4h::text   FROM stats_long
        UNION ALL SELECT 'long_stats', 'abc_4h',  abc_4h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'asc_4h',  asc_4h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'abu_4h',  abu_4h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'asu_4h',  asu_4h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'bv_24h',  bv_24h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'sv_24h',  sv_24h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'tc_24h',  tc_24h::text  FROM stats_long
        UNION ALL SELECT 'long_stats', 'abc_24h', abc_24h::text FROM stats_long
        UNION ALL SELECT 'long_stats', 'asc_24h', asc_24h::text FROM stats_long
        UNION ALL SELECT 'long_stats', 'abu_24h', abu_24h::text FROM stats_long
        UNION ALL SELECT 'long_stats', 'asu_24h', asu_24h::text FROM stats_long
        UNION ALL SELECT 'old_p_long', tf, p::text FROM old_long
        UNION ALL SELECT '24h', 'vwap',    COALESCE(vwap::text,'0')           FROM stats_long
        UNION ALL SELECT '24h', 'high',    COALESCE(high_24h::text,'0')       FROM stats_long
        UNION ALL SELECT '24h', 'low',     COALESCE(low_24h::text,'0')        FROM stats_long
        UNION ALL SELECT '24h', 'vol_usd', COALESCE(volume_usd_24h::text,'0') FROM stats_long
        UNION ALL SELECT 'exch_cvd', exchange::text, buy_vol::text            FROM exch_cvd
        UNION ALL SELECT 'exch_cvd', exchange::text, 'SELL_'||sell_vol::text  FROM exch_cvd
        UNION ALL SELECT 'large', COALESCE(exchange::text,''),
            price::text||'|'||quantity::text||'|'||CASE WHEN is_sell THEN '1' ELSE '0' END||'|'||timestamp::text
            FROM large_trades
        UNION ALL SELECT 'recent_liq', COALESCE(exchange::text,''),
            price::text||'|'||side::text||'|'||quantity::text||'|'||timestamp::text
            FROM recent_liq
        UNION ALL SELECT 'raw_liq', '',
            price::text||'|'||COALESCE(side,'')||'|'||quantity::text||'|'||COALESCE(usd_value::text,'0')
            FROM raw_liq_24h
        """, symbol, ENABLED_EXCHANGES, LARGE_TRADE_USD)
        return rows


async def _fetch_ls_ratio_funding_oi(symbol: str):
    """4 sequential queries on a single acquired connection (safe for asyncpg)."""
    async with DB_POOL.acquire() as conn:
        ls_rows = await conn.fetch("""
            SELECT exchange::text as exchange,
                   long_ratio, short_ratio, long_short_ratio, ratio_type, timestamp
            FROM long_short_ratio
            WHERE symbol=$1 AND exchange=ANY($2::text[])
              AND timestamp >= NOW()-INTERVAL '25 hours'
            ORDER BY exchange, timestamp DESC
            LIMIT 10
        """, symbol, ENABLED_EXCHANGES)

        funding_rows = await conn.fetch("""
            SELECT DISTINCT ON (exchange) exchange::text as exchange, funding_rate, funding_time
            FROM funding_rates
            WHERE symbol=$1 AND exchange=ANY($2::text[])
            ORDER BY exchange, funding_time DESC
        """, symbol, ENABLED_EXCHANGES)

        oi_latest = await conn.fetch("""
            SELECT DISTINCT ON (exchange) exchange::text as exchange, oi_value, timestamp
            FROM open_interest
            WHERE symbol=$1 AND exchange=ANY($2::text[])
            ORDER BY exchange, timestamp DESC
        """, symbol, ENABLED_EXCHANGES)

        oi_1h_ago = await conn.fetchrow("""
            SELECT SUM(oi_value) as total_oi
            FROM open_interest
            WHERE symbol=$1
              AND timestamp >= NOW()-INTERVAL '65 minutes'
              AND timestamp <= NOW()-INTERVAL '55 minutes'
        """, symbol)

        return ls_rows, funding_rows, oi_latest, oi_1h_ago


async def _fetch_depth_redis(symbol: str):
    if not ENABLED_EXCHANGES:
        return []
    pipe = REDIS_CLIENT.pipeline()
    for exch in ENABLED_EXCHANGES:
        pipe.get(f"depth:{symbol}:{exch}")
    return await pipe.execute()


# ── 5b. Symbol metrics (with semaphore guard) ─────────────────────────────────

async def calculate_metrics_for_symbol(symbol: str):
    async with _SYMBOL_SEM:
        try:
            return await _calculate_metrics_impl(symbol)
        except asyncpg.TooManyConnectionsError as e:
            logger.warning(f"[{symbol}] DB pool exhausted — skipping cycle: {e}")
            return None
        except asyncpg.PostgresError as e:
            logger.debug(f"[{symbol}] DB query issue: {e}")
            return None
        except Exception as e:
            logger.error(f"[{symbol}] Metrics error: {e}")
            return None


async def _calculate_metrics_impl(symbol: str):
    # ── 2 parallel fetches, each uses exactly 1 DB connection ───────────────
    trades_agg_rows, (ls_rows, funding_rows, oi_latest, oi_1h_row) = await asyncio.gather(
        _fetch_trades_agg(symbol),
        _fetch_ls_ratio_funding_oi(symbol),
    )
    # Redis fetch — no DB connection needed
    depth_raw_list = await _fetch_depth_redis(symbol)

    # ── Parse ────────────────────────────────────────────────────────────────
    short_stats     = {}
    long_stats      = {}
    old_p_short_map = {}    # '5m', '15m'        ← from trades table
    old_p_long_map  = {}    # '1h', '4h', '24h'  ← from ohlcv_1m
    stats_24h       = {}
    exchange_prices = {}
    last_price      = 0.0
    exch_cvd_data   = {}
    large_trades    = []
    recent_liqs     = []
    raw_liq_24h     = []

    for r in trades_agg_rows:
        k, e, v = r["k"], str(r["e"]), str(r["v"])
        try:
            fv = float(v) if v else 0.0
        except (ValueError, TypeError):
            fv = 0.0

        if k == "price_overall":
            last_price = fv
        elif k == "price_exchange":
            exchange_prices[e] = fv
        elif k == "short_stats":
            short_stats[e] = fv
        elif k == "long_stats":
            long_stats[e] = fv
        elif k == "old_p_short":
            old_p_short_map[e] = fv          # e is '5m' or '15m'
        elif k == "old_p_long":
            old_p_long_map[e] = fv           # e is '1h', '4h', '24h'
        elif k == "24h":
            stats_24h[e] = fv
        elif k == "exch_cvd":
            if v.startswith("SELL_"):
                try:
                    exch_cvd_data[e + "_sel"] = float(v[5:])
                except ValueError:
                    pass
            else:
                exch_cvd_data[e] = fv
        elif k == "large":
            parts = v.split("|")
            if len(parts) >= 4:
                try:
                    large_trades.append({
                        "exchange":  e,
                        "price":     float(parts[0]),
                        "quantity":  float(parts[1]),
                        "is_sell":   parts[2] == "1",
                        "timestamp": parts[3].strip(),
                    })
                except (ValueError, IndexError):
                    pass
        elif k == "recent_liq":
            parts = v.split("|")
            if len(parts) >= 4:
                try:
                    recent_liqs.append({
                        "exchange":  e,
                        "price":     float(parts[0]),
                        "side":      parts[1],
                        "quantity":  float(parts[2]),
                        "timestamp": parts[3],
                    })
                except (ValueError, IndexError):
                    pass
        elif k == "raw_liq":
            parts = v.split("|")
            if len(parts) >= 4:
                try:
                    raw_liq_24h.append({
                        "price":     float(parts[0]),
                        "side":      parts[1],
                        "quantity":  float(parts[2]),
                        "usd_value": float(parts[3]),
                    })
                except (ValueError, IndexError):
                    pass

    # 24h aggregates
    vwap        = stats_24h.get("vwap",    0.0)
    high_24h    = stats_24h.get("high",    0.0)
    low_24h     = stats_24h.get("low",     0.0)
    vol_usd_24h = stats_24h.get("vol_usd", 0.0)

    def _f(d, key): return float(d.get(key) or 0.0)
    def _i(d, key): return int(d.get(key)   or 0)

    # ── CVD & BS — short timeframes (trades table) ───────────────────────────
    cvd_data      = {}
    buyer_seller  = {}
    price_changes = {}

    for label in ("5m", "15m"):
        bv = _f(short_stats, f"bv_{label}")
        sv = _f(short_stats, f"sv_{label}")
        bu = _f(short_stats, f"bu_{label}")
        su = _f(short_stats, f"su_{label}")
        bc = _i(short_stats, f"bc_{label}")
        sc = _i(short_stats, f"sc_{label}")
        tc = _i(short_stats, f"tc_{label}")

        cvd_data[label] = {"cvd": round(bv-sv,4), "buy_volume": round(bv,4),
                           "sell_volume": round(sv,4), "trade_count": tc}
        tot = bc + sc
        buyer_seller[label] = {
            "buy_count": bc, "sell_count": sc,
            "ratio_by_count":  round(bc/sc, 4)  if sc  > 0 else 0,
            "buy_volume": round(bv,4), "sell_volume": round(sv,4),
            "ratio_by_volume": round(bv/sv, 4) if sv  > 0 else 0,
            "buy_usd": round(bu,2), "sell_usd": round(su,2),
            "ratio_by_usd":    round(bu/su, 4) if su  > 0 else 0,
            "buy_pct":  round(bc/tot*100, 2) if tot > 0 else 50.0,
            "sell_pct": round(sc/tot*100, 2) if tot > 0 else 50.0,
        }
        old_p = old_p_short_map.get(label, 0.0)
        if old_p > 0 and last_price > 0:
            pct = round((last_price - old_p) / old_p * 100, 4)
            price_changes[label] = {"change_pct": pct, "old_price": round(old_p,2),
                                    "direction": "up" if pct > 0 else "down" if pct < 0 else "flat"}
        else:
            price_changes[label] = {"change_pct": 0, "old_price": 0, "direction": "flat"}

    # ── CVD & BS — long timeframes (ohlcv_1m) ────────────────────────────────
    for label in ("1h", "4h", "24h"):
        bv   = _f(long_stats, f"bv_{label}")
        sv   = _f(long_stats, f"sv_{label}")
        tc   = _i(long_stats, f"tc_{label}")
        abc  = _i(long_stats, f"abc_{label}")
        asc_ = _i(long_stats, f"asc_{label}")
        abu  = _f(long_stats, f"abu_{label}")
        asu  = _f(long_stats, f"asu_{label}")

        cvd_data[label] = {"cvd": round(bv-sv,4), "buy_volume": round(bv,4),
                           "sell_volume": round(sv,4), "trade_count": tc}
        tot = abc + asc_
        buyer_seller[label] = {
            "buy_count": abc, "sell_count": asc_,
            "ratio_by_count":  round(abc/asc_, 4) if asc_ > 0 else 0,
            "buy_volume": round(bv,4), "sell_volume": round(sv,4),
            "ratio_by_volume": round(bv/sv,   4) if sv   > 0 else 0,
            "buy_usd": round(abu,2), "sell_usd": round(asu,2),
            "ratio_by_usd":    round(abu/asu, 4) if asu  > 0 else 0,
            "buy_pct":  round(abc/tot*100,  2) if tot  > 0 else 50.0,
            "sell_pct": round(asc_/tot*100, 2) if tot  > 0 else 50.0,
        }
        old_p = old_p_long_map.get(label, 0.0)
        if old_p > 0 and last_price > 0:
            pct = round((last_price - old_p) / old_p * 100, 4)
            price_changes[label] = {"change_pct": pct, "old_price": round(old_p,2),
                                    "direction": "up" if pct > 0 else "down" if pct < 0 else "flat"}
        else:
            price_changes[label] = {"change_pct": 0, "old_price": 0, "direction": "flat"}

    # ── Order Book & Arbitrage ───────────────────────────────────────────────
    orderbook_depth = {}
    total_bid_vol   = 0.0
    total_ask_vol   = 0.0
    min_price_exch = max_price_exch = None
    min_price = float('inf')
    max_price = 0.0

    for i, exch in enumerate(ENABLED_EXCHANGES):
        p = exchange_prices.get(exch, 0.0)
        if p > 0:
            if p < min_price: min_price, min_price_exch = p, exch
            if p > max_price: max_price, max_price_exch = p, exch
        raw = depth_raw_list[i] if i < len(depth_raw_list) else None
        if raw:
            try:
                book = json.loads(raw)
                bids = book.get("bids", [])[:20]
                asks = book.get("asks", [])[:20]
                bv   = sum(float(x[1]) for x in bids)
                av   = sum(float(x[1]) for x in asks)
                total_bid_vol += bv
                total_ask_vol += av
                orderbook_depth[exch] = {"bids": bids, "asks": asks,
                                         "ts": book.get("ts", 0), "bid_vol": bv, "ask_vol": av}
            except Exception:
                pass

    arbitrage = None
    if (min_price > 0 and max_price > 0 and min_price != float('inf')
            and min_price_exch != max_price_exch):
        spread_pct = (max_price - min_price) / min_price * 100
        if spread_pct > 0.05:
            arbitrage = {"buy_exchange": min_price_exch, "sell_exchange": max_price_exch,
                         "spread_pct": round(spread_pct,4), "spread_usd": round(max_price-min_price,2),
                         "min_price": min_price, "max_price": max_price}

    total_depth_vol = total_bid_vol + total_ask_vol
    ob_imbalance    = (total_bid_vol - total_ask_vol) / total_depth_vol if total_depth_vol > 0 else 0.0

    # ── L/S Ratio ────────────────────────────────────────────────────────────
    seen_exch = set()
    ls_ratio  = {}
    avg_ls_sum, avg_ls_count = 0.0, 0
    for row in ls_rows:
        exch = str(row["exchange"])
        if exch in seen_exch: continue
        seen_exch.add(exch)
        ls_ratio[exch] = {
            "long_pct":  round(float(row["long_ratio"])       * 100, 2),
            "short_pct": round(float(row["short_ratio"])      * 100, 2),
            "ratio":     round(float(row["long_short_ratio"]),        4),
            "type":      row["ratio_type"],
            "updated":   (row["timestamp"].isoformat()
                          if hasattr(row["timestamp"], 'isoformat') else str(row["timestamp"])),
        }
        avg_ls_sum   += float(row["long_short_ratio"])
        avg_ls_count += 1
    avg_ls = avg_ls_sum / avg_ls_count if avg_ls_count > 0 else 0.0

    # ── Exchange CVD ─────────────────────────────────────────────────────────
    exchange_cvd = {}
    for exch in ENABLED_EXCHANGES:
        buy_v  = exch_cvd_data.get(exch,          0.0)
        sell_v = exch_cvd_data.get(exch + "_sel", 0.0)
        exchange_cvd[exch] = {"cvd": round(buy_v-sell_v,4), "buy": round(buy_v,4), "sell": round(sell_v,4)}

    # ── Imbalance & Spoofing ─────────────────────────────────────────────────
    buy_1h   = cvd_data.get("1h", {}).get("buy_volume",  0)
    sell_1h  = cvd_data.get("1h", {}).get("sell_volume", 0)
    total_1h = buy_1h + sell_1h
    imbalance = round((buy_1h-sell_1h)/total_1h, 4) if total_1h > 0 else 0.0
    deviation = ob_imbalance - imbalance
    spoofing  = {
        "detected":  abs(deviation) > 0.4,
        "signal":    ("fake_bids" if deviation > 0 else "fake_asks") if abs(deviation) > 0.4 else "None",
        "deviation": round(deviation, 2),
    }

    # ── Whale Flows ──────────────────────────────────────────────────────────
    whale_flow = {"buy_usd": 0.0, "sell_usd": 0.0, "net_usd": 0.0, "count": 0}
    for lt in large_trades:
        usd = lt["price"] * lt["quantity"]
        whale_flow["count"] += 1
        if lt["is_sell"]: whale_flow["sell_usd"] += usd
        else:             whale_flow["buy_usd"]  += usd
    whale_flow["net_usd"] = whale_flow["buy_usd"] - whale_flow["sell_usd"]
    whale_flow = {k: (round(v,2) if isinstance(v,float) else v) for k,v in whale_flow.items()}

    # ── Liquidation Heatmap ──────────────────────────────────────────────────
    liq_heatmap = []
    if last_price > 0 and raw_liq_24h:
        bucket_size = last_price * 0.002
        buckets: dict = {}
        for r in raw_liq_24h:
            lvl = math.floor(float(r["price"]) / bucket_size) * bucket_size
            key = (lvl, r["side"])
            if key not in buckets:
                buckets[key] = {"price_level": lvl, "side": r["side"], "count": 0,
                                "total_quantity": 0.0, "total_usd": 0.0}
            buckets[key]["count"]          += 1
            buckets[key]["total_quantity"] += float(r["quantity"])
            buckets[key]["total_usd"]      += float(r["usd_value"])
        liq_heatmap = sorted(buckets.values(), key=lambda x: x["total_usd"], reverse=True)[:50]

    # ── Funding Rates ────────────────────────────────────────────────────────
    seen_fund = set()
    funding_data = {}
    avg_funding_sum, avg_funding_count = 0.0, 0
    for row in funding_rows:
        exch = str(row["exchange"])
        if exch in seen_fund: continue
        seen_fund.add(exch)
        rate = float(row["funding_rate"])
        funding_data[exch] = {
            "rate":       rate,
            "annualized": round(rate * 3 * 365 * 100, 2),
            "time":       (row["funding_time"].isoformat()
                           if hasattr(row["funding_time"], 'isoformat') else str(row["funding_time"])),
        }
        avg_funding_sum   += rate
        avg_funding_count += 1
    avg_funding = avg_funding_sum / avg_funding_count if avg_funding_count > 0 else 0.0

    # ── Open Interest ────────────────────────────────────────────────────────
    oidata   = {}
    total_oi = 0.0
    seen_oi  = set()
    for row in oi_latest:
        exch = str(row["exchange"])
        if exch in seen_oi: continue
        seen_oi.add(exch)
        val = float(row["oi_value"])
        oidata[exch] = {
            "value":   val,
            "updated": (row["timestamp"].isoformat()
                        if hasattr(row["timestamp"], 'isoformat') else str(row["timestamp"])),
        }
        total_oi += val
    old_oi       = float(oi_1h_row["total_oi"]) if oi_1h_row and oi_1h_row["total_oi"] else 0.0
    oi_change_1h = round((total_oi-old_oi)/old_oi*100, 2) if old_oi > 0 and total_oi > 0 else 0.0

    # ── Sentiment ────────────────────────────────────────────────────────────
    sentiment_score = _compute_sentiment(
        cvd_1h=cvd_data.get("1h",{}).get("cvd",0),
        cvd_4h=cvd_data.get("4h",{}).get("cvd",0),
        imbalance=imbalance, avg_funding=avg_funding,
        oi_change_pct=oi_change_1h, buy_vol=buy_1h, sell_vol=sell_1h,
    )

    # ── Build & Cache Payload ────────────────────────────────────────────────
    payload = {
        "symbol":             symbol,
        "price":              last_price,
        "exchange_prices":    exchange_prices,
        "24h_vwap":           round(vwap, 2),
        "cvd":                cvd_data,
        "exchange_cvd":       exchange_cvd,
        "order_imbalance_1h": imbalance,
        "buyer_seller_ratio": buyer_seller,
        "price_changes":      price_changes,
        "24h_high":           round(high_24h, 2),
        "24h_low":            round(low_24h,  2),
        "24h_volume_usd":     round(vol_usd_24h, 2),
        "long_short_ratio":   {"by_exchange": ls_ratio, "average_ratio": round(avg_ls, 4)},
        "large_trades_1h": [
            {"exchange": t["exchange"], "price": t["price"], "quantity": t["quantity"],
             "side": "sell" if t["is_sell"] else "buy",
             "usd_value": round(t["price"]*t["quantity"], 2), "timestamp": t["timestamp"]}
            for t in large_trades
        ],
        "recent_liquidations": [
            {"exchange": r["exchange"], "side": r["side"], "price": r["price"],
             "quantity": r["quantity"], "usd_value": round(r["price"]*r["quantity"], 2),
             "timestamp": r["timestamp"]}
            for r in recent_liqs
        ],
        "liquidation_heatmap": liq_heatmap,
        "funding_rates":       funding_data,
        "avg_funding_rate":    round(avg_funding, 8),
        "open_interest":       {"total": round(total_oi,4), "by_exchange": oidata, "change_1h_pct": oi_change_1h},
        "sentiment":           sentiment_score,
        "arbitrage":           arbitrage,
        "spoofing":            spoofing,
        "whale_flow":          whale_flow,
        "exchanges":           ENABLED_EXCHANGES,
        "updated_at_ms":       int(time.time() * 1000),
    }

    await REDIS_CLIENT.set(f"live_metrics:{symbol}", json.dumps(payload))
    return payload


def _compute_sentiment(cvd_1h, cvd_4h, imbalance, avg_funding, oi_change_pct, buy_vol, sell_vol):
    signals = []
    total_vol = buy_vol + sell_vol
    cvd_norm  = max(-1, min(1, cvd_1h/total_vol*2)) if total_vol > 0 else 0
    signals.append(("cvd_momentum",    cvd_norm,                             0.25))
    signals.append(("order_imbalance", max(-1, min(1, imbalance*2)),         0.25))
    signals.append(("funding_rate",    max(-1, min(1, -avg_funding*100)),    0.25))
    oi_signal = max(-1, min(1, oi_change_pct/5))
    if cvd_norm < 0: oi_signal = -abs(oi_signal)
    signals.append(("oi_momentum", oi_signal, 0.25))
    score = round(max(-100, min(100, sum(s*w for _,s,w in signals)*100)), 1)
    label = ("strongly_bullish" if score>40 else "bullish"  if score>15 else
             "neutral"          if score>-15 else "bearish" if score>-40 else "strongly_bearish")
    return {"score": score, "label": label,
            "components": {name: round(val*100,1) for name,val,_ in signals}}


# ── 6. OHLCV Cache ───────────────────────────────────────────────────────────

async def cache_ohlcv():
    """Symbols processed one at a time; each uses 1 connection for all 5 timeframes."""
    for sym in SYMBOLS:
        try:
            await _cache_ohlcv_for_symbol(sym)
        except Exception as e:
            logger.error(f"OHLCV cache error for {sym}: {e}")


async def _cache_ohlcv_for_symbol(sym: str):
    """All 5 timeframes run sequentially on a single connection — 1 conn total."""
    async with DB_POOL.acquire() as conn:
        tf_configs = [(None,"1m"),(5,"5m"),(15,"15m"),(60,"1h"),(240,"4h")]
        for tf_minutes, tf_key in tf_configs:
            try:
                if tf_minutes is None:
                    rows = await conn.fetch(
                        """SELECT open_time, open, high, low, close, volume,
                                  buy_volume, sell_volume, trade_count
                           FROM ohlcv_1m WHERE symbol=$1
                           ORDER BY open_time DESC LIMIT 500""",
                        sym,
                    )
                else:
                    limit = 500 * tf_minutes
                    rows = await conn.fetch(
                        f"""SELECT
                              date_trunc('hour', open_time) +
                                INTERVAL '1 minute' * (EXTRACT(MINUTE FROM open_time)::int
                                  / {tf_minutes} * {tf_minutes}) as bucket,
                              (ARRAY_AGG(open  ORDER BY open_time ASC))[1]  as open,
                              MAX(high)  as high, MIN(low) as low,
                              (ARRAY_AGG(close ORDER BY open_time DESC))[1] as close,
                              SUM(volume) as volume, SUM(buy_volume) as buy_volume,
                              SUM(sell_volume) as sell_volume, SUM(trade_count) as trade_count
                           FROM ohlcv_1m
                           WHERE symbol=$1
                             AND open_time >= NOW() - INTERVAL '1 minute' * {limit}
                           GROUP BY bucket ORDER BY bucket DESC LIMIT 200""",
                        sym,
                    )
                candles = [
                    {"t": (r["open_time"] if tf_minutes is None else r["bucket"]).isoformat(),
                     "o": float(r["open"]), "h": float(r["high"]),
                     "l": float(r["low"]),  "c": float(r["close"]),
                     "v": float(r["volume"]), "bv": float(r["buy_volume"]),
                     "sv": float(r["sell_volume"]), "n": int(r["trade_count"])}
                    for r in rows
                ]
                await REDIS_CLIENT.set(f"ohlcv:{tf_key}:{sym}", json.dumps(candles))
            except Exception as e:
                logger.error(f"OHLCV {tf_key} for {sym}: {e}")


# ── 7. Cruncher Loop ─────────────────────────────────────────────────────────

async def cruncher_loop():
    logger.info(
        f"Cruncher loop — {len(SYMBOLS)} symbols, {CRUNCH_INTERVAL}s interval, "
        f"exchanges: {', '.join(ENABLED_EXCHANGES)}, "
        f"DB pool={DB_POOL_MAX_SIZE}, concurrency={_SYMBOL_SEM._value}"
    )
    ohlcv_counter = 0

    while not shutdown_event.is_set():
        cycle_start = time.monotonic()
        try:
            await asyncio.gather(
                *[calculate_metrics_for_symbol(sym) for sym in SYMBOLS],
                return_exceptions=True,
            )

            ohlcv_counter += 1
            if ohlcv_counter >= 10:
                # Run the heavy OHLCV caching in a background task so it doesn't block the loop and spike the ping
                bg_task = asyncio.create_task(cache_ohlcv())
                bg_task.add_done_callback(lambda t: t.exception() if not t.cancelled() and t.exception() else None)
                ohlcv_counter = 0

            await REDIS_CLIENT.set("cruncher:heartbeat", json.dumps({
                "status":                  "healthy",
                "symbols":                 SYMBOLS,
                "exchanges":               ENABLED_EXCHANGES,
                "last_cycle_ms":           stats["last_cycle_ms"],
                "total_cycles":            stats["cycles"],
                "errors":                  stats["errors"],
                "worker_id":               WORKER_ID,
                "total_symbols_processed": stats["cycles"] * len(SYMBOLS),
                "timestamp_ms":            int(time.time() * 1000),
            }))

            stats["cycles"]       += 1
            stats["last_cycle_ms"] = round((time.monotonic() - cycle_start) * 1000, 1)

            if stats["cycles"] % 60 == 0:
                logger.info(
                    f"[HEARTBEAT] Cycles={stats['cycles']} | "
                    f"Last={stats['last_cycle_ms']}ms | Errors={stats['errors']}"
                )

        except asyncio.CancelledError:
            break
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Cruncher cycle error: {e}")

        elapsed    = time.monotonic() - cycle_start
        sleep_time = max(0, CRUNCH_INTERVAL - elapsed)
        try:
            await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            break


# ── 8. Symbol Publisher ───────────────────────────────────────────────────────

async def publish_symbol_list():
    try:
        await REDIS_CLIENT.set("tracked_symbols",   json.dumps(SYMBOLS))
        await REDIS_CLIENT.set("enabled_exchanges", json.dumps(ENABLED_EXCHANGES))
        logger.info(f"Published: symbols={SYMBOLS}, exchanges={ENABLED_EXCHANGES}")
    except Exception as e:
        logger.error(f"Failed to publish config: {e}")


# ── 9. Main ──────────────────────────────────────────────────────────────────

async def main():
    global _SYMBOL_SEM

    # Each symbol consumes exactly 2 DB connections (trades_agg + ls/funding/oi).
    # For local operation with remote DBs, keep this low (1-3) to avoid connection timeouts.
    concurrency = _MAX_CONCURRENCY
    _SYMBOL_SEM = asyncio.Semaphore(concurrency)

    logger.info(
        f"Starting Cruncher — symbols: {', '.join(SYMBOLS)}, "
        f"exchanges: {', '.join(ENABLED_EXCHANGES)}, "
        f"DB pool max: {DB_POOL_MAX_SIZE}, symbol concurrency: {concurrency}"
    )

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
        pass  # Windows

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

    if DB_POOL:      await DB_POOL.close()
    if REDIS_CLIENT: await REDIS_CLIENT.close()
    logger.info("Cruncher shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")