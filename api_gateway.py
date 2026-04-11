"""
API Gateway — Production Multi-Exchange RapidAPI Service
Serves authenticated, rate-limited REST endpoints for RapidAPI subscribers
and a real-time testing dashboard.

Architecture:
  /           → Dashboard (no auth)
  /ws/live    → WebSocket dashboard stream (no auth)
  /v1/*       → RapidAPI-authenticated, rate-limited, tier-gated endpoints
  /health     → Render health check (no auth)

Exchanges: Binance, Bybit, OKX
"""

from fastapi import FastAPI, HTTPException, Depends, Request, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.routing import APIRouter
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import asyncio
import json
import os
import sys
import logging
import time
import uuid

import asyncpg
import redis.asyncio as aioredis

# ── 1. Environment & Logging ──────────────────────────────────────────────────
try:
    # Prefer env.local for local development, but don't override real environment variables
    from pathlib import Path

    _env_local = Path(__file__).with_name("env.local")
    if _env_local.exists():
        load_dotenv(dotenv_path=_env_local, override=True)
except Exception:
    pass

# Default behavior: load .env if present (again, without overriding existing env vars)
load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("APIGateway")

# ── 2. Configuration ─────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto_data")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
RAPIDAPI_PROXY_SECRET = os.getenv("RAPIDAPI_PROXY_SECRET", "")
PORT = int(os.getenv("PORT", "8000"))
HOST = os.getenv("HOST", "0.0.0.0")
SYMBOLS_DEFAULT = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")

# Rate limits / tier gating handled entirely by RapidAPI.
# All endpoints are open — RapidAPI manages authentication, subscriptions, and rate limits.


# ── 3. Pydantic Response Models ──────────────────────────────────────────────

class ErrorResponse(BaseModel):
    error: str
    message: str
    correlation_id: str = ""
    hint: str = ""


class LiveMetricsResponse(BaseModel):
    symbol: str
    price: float
    exchange_prices: Dict[str, float] = {}
    vwap_24h: float = Field(alias="24h_vwap", default=0)
    order_imbalance_1h: float = 0
    cvd: Dict[str, Any] = {}
    exchanges: List[str] = []
    updated_at_ms: int = 0

    class Config:
        populate_by_name = True


class OHLCVCandle(BaseModel):
    timestamp: str = Field(alias="t")
    open: float = Field(alias="o")
    high: float = Field(alias="h")
    low: float = Field(alias="l")
    close: float = Field(alias="c")
    volume: float = Field(alias="v")
    buy_volume: float = Field(alias="bv")
    sell_volume: float = Field(alias="sv")
    trade_count: int = Field(alias="n")

    class Config:
        populate_by_name = True


class FundingRateEntry(BaseModel):
    exchange: str
    rate: float
    annualized_pct: float
    time: str


class SentimentResponse(BaseModel):
    symbol: str
    score: float = Field(description="Composite score from -100 (extreme bearish) to +100 (extreme bullish)")
    label: str
    components: Dict[str, float]


# ── 4. Global State ──────────────────────────────────────────────────────────
DB_POOL = None
REDIS_CLIENT = None
WS_CLIENTS: set = set()
SERVER_START_TIME = time.time()


# ── 5. Service Initializers ──────────────────────────────────────────────────
async def init_postgres():
    global DB_POOL
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, command_timeout=15, statement_cache_size=0)
            async with DB_POOL.acquire() as conn:
                await conn.fetchval("SELECT 1")
            logger.info("[OK] PostgreSQL connected.")
            return
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"[ERROR] PostgreSQL unavailable: {e}")
                return
            wait = min(2 ** attempt, 30)
            logger.warning(f"PostgreSQL attempt {attempt} failed: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)


async def init_redis():
    global REDIS_CLIENT
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            REDIS_CLIENT = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5, max_connections=2)
            await REDIS_CLIENT.ping()
            logger.info("[OK] Redis connected.")
            return
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"[ERROR] Redis unavailable: {e}")
                return
            wait = min(2 ** attempt, 30)
            logger.warning(f"Redis attempt {attempt} failed: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)


# ── 6. Dashboard WebSocket Broadcaster ───────────────────────────────────────
async def dashboard_broadcaster():
    while True:
        try:
            await asyncio.sleep(1)
            if not WS_CLIENTS or not REDIS_CLIENT:
                continue

            symbols_raw = await REDIS_CLIENT.get("tracked_symbols")
            symbols = json.loads(symbols_raw) if symbols_raw else []

            exchanges_raw = await REDIS_CLIENT.get("enabled_exchanges")
            exchanges = json.loads(exchanges_raw) if exchanges_raw else []

            all_metrics = {}
            for sym in symbols:
                data = await REDIS_CLIENT.get(f"live_metrics:{sym}")
                if data:
                    all_metrics[sym] = json.loads(data)

            heartbeat = await REDIS_CLIENT.get("cruncher:heartbeat")
            cruncher_health = json.loads(heartbeat) if heartbeat else None

            payload = json.dumps({
                "type": "snapshot",
                "symbols": symbols,
                "exchanges": exchanges,
                "metrics": all_metrics,
                "cruncher": cruncher_health,
                "server_time_ms": int(time.time() * 1000),
                "server_time_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "server_uptime_s": round(time.time() - SERVER_START_TIME),
            })

            stale = set()
            for ws in WS_CLIENTS:
                try:
                    await ws.send_text(payload)
                except Exception:
                    stale.add(ws)
            WS_CLIENTS.difference_update(stale)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Dashboard broadcaster error: {e}")


# ── 7. Lifespan ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_postgres()
    await init_redis()
    broadcaster_task = asyncio.create_task(dashboard_broadcaster())
    logger.info(f"[READY] API Gateway on {HOST}:{PORT}")
    yield
    broadcaster_task.cancel()
    try:
        await broadcaster_task
    except asyncio.CancelledError:
        pass
    if DB_POOL:
        await DB_POOL.close()
    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
    logger.info("API Gateway shutdown complete.")


# ── 8. App ───────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CryptoFlow — Multi-Exchange Order Flow API",
    description=(
        "Real-time crypto derivatives analytics across Binance, Bybit, and OKX. "
        "VWAP, CVD, liquidation heatmaps, funding rates, open interest, whale detection, "
        "market sentiment scoring, and OHLCV candlestick data. "
        "Built for algorithmic traders, trading bots, and institutional analytics."
    ),
    version="7.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 9. Middleware ─────────────────────────────────────────────────────────────

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("x-correlation-id", str(uuid.uuid4())[:8])
        request.state.correlation_id = correlation_id
        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = round((time.monotonic() - start) * 1000, 2)
        response.headers["X-Response-Time"] = f"{elapsed_ms}ms"
        response.headers["X-Correlation-ID"] = correlation_id
        return response


app.add_middleware(TimingMiddleware)


# Auth, tier gating, and rate limiting handled by RapidAPI.
# No server-side middleware needed — RapidAPI proxy manages everything.


# ── 11. API Router ───────────────────────────────────────────────────────────
api_v1 = APIRouter(
    prefix="/v1",
    responses={
        503: {"description": "Service unavailable"},
    },
)


def _headers(request: Request):
    """Build rate limit + cache headers."""
    h = {"Cache-Control": "public, max-age=1"}
    if hasattr(request.state, "rate_limit"):
        h["X-RateLimit-Limit"] = str(request.state.rate_limit)
        h["X-RateLimit-Remaining"] = str(request.state.rate_remaining)
        h["X-RateLimit-Tier"] = str(getattr(request.state, "rate_tier", ""))
    return h


def _err(status: int, msg: str, request: Request, hint: str = ""):
    cid = getattr(request.state, "correlation_id", "")
    detail = {"error": "Error", "message": msg, "correlation_id": cid}
    if hint:
        detail["hint"] = hint
    raise HTTPException(status_code=status, detail=detail)


# ═══════════════════════════════════════════════════════════════════════════════
# FREE TIER ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Live Metrics (sub-ms from Redis) ──
@api_v1.get("/metrics/live", tags=["Real-Time Metrics"], summary="Live cached metrics (sub-ms)")
async def get_live_metrics(
    request: Request,
    symbol: str = Query(default="BTCUSDT", description="Trading pair (e.g., BTCUSDT, ETHUSDT, SOLUSDT)"),
):
    """
    **[FREE]** Sub-millisecond cached metrics including price across exchanges,
    VWAP, multi-timeframe CVD, and order imbalance. Updated every second.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache backend offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "status": "pending", "message": "Data not yet available."},
            headers=_headers(request),
        )
    return JSONResponse(content=json.loads(data), headers=_headers(request))


# ── Historical CVD ──
@api_v1.get("/metrics/historical-cvd", tags=["Real-Time Metrics"], summary="Historical CVD")
async def get_historical_cvd(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    minutes: int = Query(default=60, ge=1, le=1440, description="Lookback window (max 24h)"),
    exchange: Optional[str] = Query(default=None, description="Filter by exchange (binance, bybit, okx)"),
):
    """
    **[FREE]** Cumulative Volume Delta over a custom timeframe. Shows net buying vs selling pressure.
    Optionally filter by exchange to compare order flow across venues.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                row = await conn.fetchrow(
                    """SELECT
                         COALESCE(SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END), 0) as buy_vol,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN quantity ELSE 0 END), 0) as sell_vol,
                         COUNT(*) as trade_count
                       FROM trades
                       WHERE symbol = $1 AND exchange = $3 AND timestamp >= NOW() - INTERVAL '1 minute' * $2""",
                    symbol, minutes, exchange.lower(),
                )
            else:
                row = await conn.fetchrow(
                    """SELECT
                         COALESCE(SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END), 0) as buy_vol,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN quantity ELSE 0 END), 0) as sell_vol,
                         COUNT(*) as trade_count
                       FROM trades
                       WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '1 minute' * $2""",
                    symbol, minutes,
                )
        buy = float(row["buy_vol"]) if row else 0.0
        sell = float(row["sell_vol"]) if row else 0.0

        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "timeframe_minutes": minutes,
                "cvd": round(buy - sell, 4),
                "buy_volume": round(buy, 4),
                "sell_volume": round(sell, 4),
                "trade_count": int(row["trade_count"]) if row else 0,
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Historical CVD error: {e}")
        _err(503, str(e), request)


# ── VWAP ──
@api_v1.get("/metrics/vwap", tags=["Real-Time Metrics"], summary="24h VWAP")
async def get_vwap(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    exchange: Optional[str] = Query(default=None, description="Filter by exchange"),
):
    """
    **[FREE]** 24-hour Volume Weighted Average Price from raw trade data across all exchanges.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                row = await conn.fetchrow(
                    """SELECT SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
                       FROM trades
                       WHERE symbol = $1 AND exchange = $2 AND timestamp >= NOW() - INTERVAL '24 hours'""",
                    symbol, exchange.lower(),
                )
            else:
                row = await conn.fetchrow(
                    """SELECT SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap
                       FROM trades
                       WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '24 hours'""",
                    symbol,
                )
        if not row or row["vwap"] is None:
            return JSONResponse(
                content={"symbol": symbol, "exchange": exchange or "all", "24h_vwap": None, "note": "Insufficient data"},
                headers=_headers(request),
            )
        return JSONResponse(
            content={"symbol": symbol, "exchange": exchange or "all", "24h_vwap": round(float(row["vwap"]), 2)},
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"VWAP error: {e}")
        _err(503, str(e), request)


# ── Liquidations ──
@api_v1.get("/data/liquidations", tags=["Market Data"], summary="Recent liquidations")
async def get_liquidations(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    limit: int = Query(default=50, ge=1, le=500),
    exchange: Optional[str] = Query(default=None, description="Filter by exchange"),
):
    """
    **[FREE]** Most recent liquidation events from all connected exchanges.
    Shows forced closures of leveraged positions.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                rows = await conn.fetch(
                    """SELECT exchange, side, price, quantity, timestamp
                       FROM liquidations
                       WHERE symbol = $1 AND exchange = $3
                       ORDER BY timestamp DESC LIMIT $2""",
                    symbol, limit, exchange.lower(),
                )
            else:
                rows = await conn.fetch(
                    """SELECT exchange, side, price, quantity, timestamp
                       FROM liquidations
                       WHERE symbol = $1
                       ORDER BY timestamp DESC LIMIT $2""",
                    symbol, limit,
                )
        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "count": len(rows),
                "liquidations": [
                    {
                        "exchange": r["exchange"],
                        "side": r["side"],
                        "price": float(r["price"]),
                        "quantity": float(r["quantity"]),
                        "usd_value": round(float(r["price"]) * float(r["quantity"]), 2),
                        "timestamp": r["timestamp"].isoformat(),
                    }
                    for r in rows
                ],
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Liquidations error: {e}")
        _err(503, str(e), request)


# ── Symbols ──
@api_v1.get("/symbols", tags=["Info"], summary="Tracked trading pairs")
async def get_symbols(request: Request):
    """**[FREE]** Returns all tracked trading pairs and connected exchanges."""
    symbols = []
    exchanges = []
    if REDIS_CLIENT:
        try:
            raw = await REDIS_CLIENT.get("tracked_symbols")
            if raw:
                symbols = json.loads(raw)
            raw = await REDIS_CLIENT.get("enabled_exchanges")
            if raw:
                exchanges = json.loads(raw)
        except Exception:
            pass

    if not symbols:
        symbols = [s.strip().upper() for s in SYMBOLS_DEFAULT.split(",") if s.strip()]

    return JSONResponse(
        content={"symbols": symbols, "count": len(symbols), "exchanges": exchanges},
        headers=_headers(request),
    )


# ── Exchanges ──
@api_v1.get("/exchanges", tags=["Info"], summary="Connected exchanges")
async def get_exchanges(request: Request):
    """**[FREE]** Returns connected exchanges and their current status."""
    exchanges = []
    if REDIS_CLIENT:
        try:
            raw = await REDIS_CLIENT.get("enabled_exchanges")
            if raw:
                exchanges = json.loads(raw)
        except Exception:
            pass

    exchange_info = {}
    for exch in exchanges:
        exchange_info[exch] = {
            "status": "connected",
            "data_types": ["trades", "liquidations", "funding_rates", "open_interest"],
        }

    return JSONResponse(
        content={"exchanges": exchange_info, "count": len(exchanges)},
        headers=_headers(request),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PRO TIER ENDPOINTS ($14.99/mo)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Large Trades ──
@api_v1.get("/data/large-trades", tags=["Premium Data"], summary="Whale trade detector")
async def get_large_trades(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    min_usd: float = Query(default=100000, ge=10000, description="Minimum trade value in USD"),
    hours: int = Query(default=1, ge=1, le=24),
    limit: int = Query(default=50, ge=1, le=200),
    exchange: Optional[str] = Query(default=None),
):
    """
    **[PRO]** Whale trade detector — individual trades above a USD threshold
    across all exchanges. Identify institutional order flow in real-time.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                rows = await conn.fetch(
                    """SELECT exchange, price, quantity, is_sell, timestamp
                       FROM trades
                       WHERE symbol = $1 AND exchange = $5
                         AND timestamp >= NOW() - INTERVAL '1 hour' * $2
                         AND price * quantity > $3
                       ORDER BY price * quantity DESC LIMIT $4""",
                    symbol, hours, min_usd, limit, exchange.lower(),
                )
            else:
                rows = await conn.fetch(
                    """SELECT exchange, price, quantity, is_sell, timestamp
                       FROM trades
                       WHERE symbol = $1
                         AND timestamp >= NOW() - INTERVAL '1 hour' * $2
                         AND price * quantity > $3
                       ORDER BY price * quantity DESC LIMIT $4""",
                    symbol, hours, min_usd, limit,
                )
        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "min_usd_threshold": min_usd,
                "lookback_hours": hours,
                "count": len(rows),
                "large_trades": [
                    {
                        "exchange": r["exchange"],
                        "price": float(r["price"]),
                        "quantity": float(r["quantity"]),
                        "side": "sell" if r["is_sell"] else "buy",
                        "usd_value": round(float(r["price"]) * float(r["quantity"]), 2),
                        "timestamp": r["timestamp"].isoformat(),
                    }
                    for r in rows
                ],
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Large trades error: {e}")
        _err(503, str(e), request)


# ── Order Imbalance ──
@api_v1.get("/analysis/order-imbalance", tags=["Premium Analytics"], summary="Buy/sell pressure analysis")
async def get_order_imbalance(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    minutes: int = Query(default=60, ge=1, le=1440),
    exchange: Optional[str] = Query(default=None),
):
    """
    **[PRO]** Buy/sell pressure ratio. Values > 0 = net buying, < 0 = net selling.
    Range: -1.0 (100% sells) to +1.0 (100% buys).
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                row = await conn.fetchrow(
                    """SELECT
                         COALESCE(SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END), 0) as buy_vol,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN quantity ELSE 0 END), 0) as sell_vol,
                         COALESCE(SUM(CASE WHEN is_sell = false THEN price * quantity ELSE 0 END), 0) as buy_usd,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN price * quantity ELSE 0 END), 0) as sell_usd,
                         COUNT(*) as trade_count
                       FROM trades
                       WHERE symbol = $1 AND exchange = $3 AND timestamp >= NOW() - INTERVAL '1 minute' * $2""",
                    symbol, minutes, exchange.lower(),
                )
            else:
                row = await conn.fetchrow(
                    """SELECT
                         COALESCE(SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END), 0) as buy_vol,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN quantity ELSE 0 END), 0) as sell_vol,
                         COALESCE(SUM(CASE WHEN is_sell = false THEN price * quantity ELSE 0 END), 0) as buy_usd,
                         COALESCE(SUM(CASE WHEN is_sell = true  THEN price * quantity ELSE 0 END), 0) as sell_usd,
                         COUNT(*) as trade_count
                       FROM trades
                       WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '1 minute' * $2""",
                    symbol, minutes,
                )
        buy = float(row["buy_vol"]) if row else 0
        sell = float(row["sell_vol"]) if row else 0
        buy_usd = float(row["buy_usd"]) if row else 0
        sell_usd = float(row["sell_usd"]) if row else 0
        total = buy + sell
        total_usd = buy_usd + sell_usd

        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "timeframe_minutes": minutes,
                "imbalance_ratio": round((buy - sell) / total, 4) if total > 0 else 0,
                "imbalance_usd_ratio": round((buy_usd - sell_usd) / total_usd, 4) if total_usd > 0 else 0,
                "buy_volume": round(buy, 4),
                "sell_volume": round(sell, 4),
                "buy_usd": round(buy_usd, 2),
                "sell_usd": round(sell_usd, 2),
                "trade_count": int(row["trade_count"]) if row else 0,
                "signal": "bullish" if buy > sell else "bearish" if sell > buy else "neutral",
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Order imbalance error: {e}")
        _err(503, str(e), request)


# ── Buyer/Seller Ratio ──
@api_v1.get("/analysis/buyer-seller-ratio", tags=["Premium Analytics"], summary="Taker buy/sell ratio")
async def get_buyer_seller_ratio(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    timeframe: str = Query(default="1h", description="Timeframe: 5m, 15m, 1h, 4h, 24h"),
):
    """
    **[PRO]** Taker Buy/Sell ratio by count, volume, and USD value.
    Ratio > 1 = more buyers (bullish). Ratio < 1 = more sellers (bearish).
    Includes percentage breakdowns for easy visualization.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    valid_tfs = ["5m", "15m", "1h", "4h", "24h"]
    if timeframe not in valid_tfs:
        _err(400, f"Invalid timeframe. Use: {', '.join(valid_tfs)}", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "status": "pending", "message": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    bs_all = metrics.get("buyer_seller_ratio", {})
    bs = bs_all.get(timeframe, {})

    return JSONResponse(
        content={
            "symbol": symbol,
            "timeframe": timeframe,
            "ratio_by_count": bs.get("ratio_by_count", 0),
            "ratio_by_volume": bs.get("ratio_by_volume", 0),
            "ratio_by_usd": bs.get("ratio_by_usd", 0),
            "buy_count": bs.get("buy_count", 0),
            "sell_count": bs.get("sell_count", 0),
            "buy_volume": bs.get("buy_volume", 0),
            "sell_volume": bs.get("sell_volume", 0),
            "buy_usd": bs.get("buy_usd", 0),
            "sell_usd": bs.get("sell_usd", 0),
            "buy_pct": bs.get("buy_pct", 50),
            "sell_pct": bs.get("sell_pct", 50),
            "signal": "bullish" if bs.get("ratio_by_volume", 1) > 1.05 else "bearish" if bs.get("ratio_by_volume", 1) < 0.95 else "neutral",
            "all_timeframes": {tf: {"ratio_by_count": d.get("ratio_by_count", 0), "ratio_by_volume": d.get("ratio_by_volume", 0), "buy_pct": d.get("buy_pct", 50)} for tf, d in bs_all.items()},
        },
        headers=_headers(request),
    )


# ── Price Changes ──
@api_v1.get("/metrics/price-changes", tags=["Real-Time Metrics"], summary="Price change statistics")
async def get_price_changes(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[FREE]** Price change percentages across multiple timeframes (5m, 15m, 1h, 4h, 24h)
    plus 24-hour high, low, and total volume in USD. Essential market overview data.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "status": "pending", "message": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    pc = metrics.get("price_changes", {})

    return JSONResponse(
        content={
            "symbol": symbol,
            "price": metrics.get("price", 0),
            "changes": pc,
            "24h_high": metrics.get("24h_high", 0),
            "24h_low": metrics.get("24h_low", 0),
            "24h_volume_usd": metrics.get("24h_volume_usd", 0),
            "24h_range_pct": round(
                (metrics.get("24h_high", 0) - metrics.get("24h_low", 0)) / metrics.get("24h_low", 1) * 100, 2
            ) if metrics.get("24h_low", 0) > 0 else 0,
        },
        headers=_headers(request),
    )


# ── OHLCV Candlestick Data ──
@api_v1.get("/data/ohlcv", tags=["Premium Data"], summary="OHLCV candlestick data")
async def get_ohlcv(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    interval: str = Query(default="1m", description="Candle interval: 1m, 5m, 15m, 1h, 4h"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    **[PRO]** OHLCV candlestick data aggregated from real trades across all exchanges.
    Includes buy/sell volume breakdown per candle.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    valid_intervals = ["1m", "5m", "15m", "1h", "4h"]
    if interval not in valid_intervals:
        _err(400, f"Invalid interval. Use: {', '.join(valid_intervals)}", request)

    symbol = symbol.upper()
    cache_key = f"ohlcv:{interval}:{symbol}"
    data = await REDIS_CLIENT.get(cache_key)
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "interval": interval, "candles": [], "note": "Data building up. Try again shortly."},
            headers=_headers(request),
        )

    candles = json.loads(data)[:limit]
    return JSONResponse(
        content={
            "symbol": symbol,
            "interval": interval,
            "count": len(candles),
            "candles": candles,
        },
        headers={**_headers(request), "Cache-Control": "public, max-age=5"},
    )


# ── Funding Rates ──
@api_v1.get("/data/funding-rates", tags=["Premium Data"], summary="Current funding rates")
async def get_funding_rates(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[PRO]** Current funding rates from all connected exchanges.
    Negative = shorts paying longs (bullish), Positive = longs paying shorts (bearish).
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "funding_rates": {}, "note": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    funding = metrics.get("funding_rates", {})
    avg = metrics.get("avg_funding_rate", 0)

    return JSONResponse(
        content={
            "symbol": symbol,
            "funding_rates": funding,
            "average_rate": avg,
            "average_annualized_pct": round(avg * 3 * 365 * 100, 2),
            "signal": "bullish" if avg < 0 else "bearish" if avg > 0 else "neutral",
        },
        headers=_headers(request),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ULTRA TIER ENDPOINTS ($49.99/mo)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Open Interest ──
@api_v1.get("/data/open-interest", tags=["Ultra Data"], summary="Open interest across exchanges")
async def get_open_interest(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[ULTRA]** Current open interest from all exchanges with 1-hour change percentage.
    Rising OI with bullish CVD = strong bullish signal.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "open_interest": {}, "note": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    oi = metrics.get("open_interest", {})

    return JSONResponse(
        content={
            "symbol": symbol,
            "total_oi": oi.get("total", 0),
            "by_exchange": oi.get("by_exchange", {}),
            "change_1h_pct": oi.get("change_1h_pct", 0),
        },
        headers=_headers(request),
    )


# ── Open Interest History ──
@api_v1.get("/data/oi-history", tags=["Ultra Data"], summary="Open interest history")
async def get_oi_history(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    hours: int = Query(default=24, ge=1, le=168, description="Lookback hours (max 7 days)"),
    exchange: Optional[str] = Query(default=None),
):
    """
    **[ULTRA]** Historical open interest data points. Track how OI evolves over time
    to identify position building or unwinding.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                rows = await conn.fetch(
                    """SELECT exchange, oi_value, timestamp
                       FROM open_interest
                       WHERE symbol = $1 AND exchange = $3
                         AND timestamp >= NOW() - INTERVAL '1 hour' * $2
                       ORDER BY timestamp DESC LIMIT 500""",
                    symbol, hours, exchange.lower(),
                )
            else:
                rows = await conn.fetch(
                    """SELECT exchange, oi_value, timestamp
                       FROM open_interest
                       WHERE symbol = $1
                         AND timestamp >= NOW() - INTERVAL '1 hour' * $2
                       ORDER BY timestamp DESC LIMIT 500""",
                    symbol, hours,
                )
        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "lookback_hours": hours,
                "count": len(rows),
                "data": [
                    {
                        "exchange": r["exchange"],
                        "oi_value": float(r["oi_value"]),
                        "timestamp": r["timestamp"].isoformat(),
                    }
                    for r in rows
                ],
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"OI history error: {e}")
        _err(503, str(e), request)


# ── Funding Rate History ──
@api_v1.get("/data/funding-history", tags=["Ultra Data"], summary="Funding rate history")
async def get_funding_history(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    hours: int = Query(default=24, ge=1, le=168),
    exchange: Optional[str] = Query(default=None),
):
    """
    **[ULTRA]** Historical funding rate data. Track funding rate trends
    to identify market sentiment shifts over time.
    """
    if not DB_POOL:
        _err(503, "Database offline", request)

    symbol = symbol.upper()
    try:
        async with DB_POOL.acquire() as conn:
            if exchange:
                rows = await conn.fetch(
                    """SELECT exchange, funding_rate, funding_time
                       FROM funding_rates
                       WHERE symbol = $1 AND exchange = $3
                         AND funding_time >= NOW() - INTERVAL '1 hour' * $2
                       ORDER BY funding_time DESC LIMIT 500""",
                    symbol, hours, exchange.lower(),
                )
            else:
                rows = await conn.fetch(
                    """SELECT exchange, funding_rate, funding_time
                       FROM funding_rates
                       WHERE symbol = $1
                         AND funding_time >= NOW() - INTERVAL '1 hour' * $2
                       ORDER BY funding_time DESC LIMIT 500""",
                    symbol, hours,
                )
        return JSONResponse(
            content={
                "symbol": symbol,
                "exchange": exchange or "all",
                "lookback_hours": hours,
                "count": len(rows),
                "data": [
                    {
                        "exchange": r["exchange"],
                        "funding_rate": float(r["funding_rate"]),
                        "annualized_pct": round(float(r["funding_rate"]) * 3 * 365 * 100, 2),
                        "time": r["funding_time"].isoformat(),
                    }
                    for r in rows
                ],
            },
            headers=_headers(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Funding history error: {e}")
        _err(503, str(e), request)


# ── Liquidation Heatmap ──
@api_v1.get("/analysis/liquidation-heatmap", tags=["Ultra Analytics"], summary="Liquidation heatmap by price level")
async def get_liquidation_heatmap(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    hours: int = Query(default=24, ge=1, le=168),
):
    """
    **[ULTRA]** Aggregated liquidation data by price bucket. Shows where leveraged
    positions were liquidated, indicating key support/resistance zones.
    Similar to CoinGlass liquidation heatmap.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "heatmap": [], "note": "Data building up."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    heatmap = metrics.get("liquidation_heatmap", [])

    # Also compute summary stats
    total_long_liq = sum(h["total_usd"] for h in heatmap if h["side"] in ("BUY", "LONG", "Buy"))
    total_short_liq = sum(h["total_usd"] for h in heatmap if h["side"] in ("SELL", "SHORT", "Sell"))

    return JSONResponse(
        content={
            "symbol": symbol,
            "lookback_hours": 24,
            "bucket_count": len(heatmap),
            "total_long_liquidation_usd": round(total_long_liq, 2),
            "total_short_liquidation_usd": round(total_short_liq, 2),
            "heatmap": heatmap,
        },
        headers={**_headers(request), "Cache-Control": "public, max-age=5"},
    )


# ── Market Sentiment ──
@api_v1.get("/analysis/sentiment", tags=["Ultra Analytics"], summary="Composite market sentiment")
async def get_sentiment(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[ULTRA]** Composite market sentiment score from -100 (extreme bearish) to +100 (extreme bullish).
    Combines CVD momentum, order imbalance, funding rates, and OI changes.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "sentiment": None, "note": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    sentiment = metrics.get("sentiment", {})

    return JSONResponse(
        content={
            "symbol": symbol,
            "score": sentiment.get("score", 0),
            "label": sentiment.get("label", "neutral"),
            "components": sentiment.get("components", {}),
            "price": metrics.get("price", 0),
            "vwap_24h": metrics.get("24h_vwap", 0),
        },
        headers=_headers(request),
    )


# ── Cross-Exchange Analysis ──
@api_v1.get("/analysis/cross-exchange", tags=["Ultra Analytics"], summary="Cross-exchange flow comparison")
async def get_cross_exchange(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[ULTRA]** Compare order flow metrics across Binance, Bybit, and OKX.
    Detect exchange-specific buying/selling divergences.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "exchanges": {}, "note": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)

    return JSONResponse(
        content={
            "symbol": symbol,
            "price_by_exchange": metrics.get("exchange_prices", {}),
            "cvd_by_exchange": metrics.get("exchange_cvd", {}),
            "funding_by_exchange": metrics.get("funding_rates", {}),
            "oi_by_exchange": metrics.get("open_interest", {}).get("by_exchange", {}),
            "exchanges": metrics.get("exchanges", []),
        },
        headers=_headers(request),
    )


# ── Long/Short Ratio ──
@api_v1.get("/analysis/long-short-ratio", tags=["Ultra Analytics"], summary="Top trader long/short ratio")
async def get_long_short_ratio(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
):
    """
    **[ULTRA]** Top trader long/short position ratio across all exchanges.
    Shows what percentage of top trader accounts are long vs short.
    Ratio > 1 = majority long, < 1 = majority short. Similar to CoinGlass L/S data.
    """
    if not REDIS_CLIENT:
        _err(503, "Cache offline", request)

    symbol = symbol.upper()
    data = await REDIS_CLIENT.get(f"live_metrics:{symbol}")
    if not data:
        return JSONResponse(
            content={"symbol": symbol, "status": "pending", "message": "Data not yet available."},
            headers=_headers(request),
        )

    metrics = json.loads(data)
    ls = metrics.get("long_short_ratio", {})
    by_exchange = ls.get("by_exchange", {})
    avg_ratio = ls.get("average_ratio", 0)

    # Determine bias
    if avg_ratio > 1.1:
        signal = "strongly_long"
    elif avg_ratio > 1.02:
        signal = "slightly_long"
    elif avg_ratio < 0.9:
        signal = "strongly_short"
    elif avg_ratio < 0.98:
        signal = "slightly_short"
    else:
        signal = "balanced"

    return JSONResponse(
        content={
            "symbol": symbol,
            "average_ratio": avg_ratio,
            "signal": signal,
            "by_exchange": by_exchange,
            "exchange_count": len(by_exchange),
        },
        headers=_headers(request),
    )


# ── Mount API Router ──
app.include_router(api_v1)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC ENDPOINTS (no auth)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
async def health_check():
    """Render health check. No authentication required."""
    db_ok = False
    if DB_POOL:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_ok = True
        except Exception:
            pass

    redis_ok = False
    if REDIS_CLIENT:
        try:
            await REDIS_CLIENT.ping()
            redis_ok = True
        except Exception:
            pass

    cruncher_ok = False
    if REDIS_CLIENT:
        try:
            hb = await REDIS_CLIENT.get("cruncher:heartbeat")
            if hb:
                hb_data = json.loads(hb)
                age_ms = int(time.time() * 1000) - hb_data.get("timestamp_ms", 0)
                cruncher_ok = age_ms < 10000
        except Exception:
            pass

    overall = "healthy" if (db_ok and redis_ok) else "degraded"

    return {
        "status": overall,
        "uptime_seconds": round(time.time() - SERVER_START_TIME),
        "version": "7.0.0",
        "services": {
            "postgresql": "connected" if db_ok else "unavailable",
            "redis": "connected" if redis_ok else "unavailable",
            "cruncher": "healthy" if cruncher_ok else "unknown",
        },
    }


@app.get("/v1/status", tags=["System"])
async def api_status():
    """Public status showing API capabilities, data freshness, and exchange connectivity."""
    symbols = []
    exchanges = []
    freshness = {}

    if REDIS_CLIENT:
        try:
            raw = await REDIS_CLIENT.get("tracked_symbols")
            if raw:
                symbols = json.loads(raw)
            raw = await REDIS_CLIENT.get("enabled_exchanges")
            if raw:
                exchanges = json.loads(raw)
            for sym in symbols:
                data = await REDIS_CLIENT.get(f"live_metrics:{sym}")
                if data:
                    metrics = json.loads(data)
                    age_ms = int(time.time() * 1000) - metrics.get("updated_at_ms", 0)
                    freshness[sym] = {
                        "last_update_ms_ago": age_ms,
                        "price": metrics.get("price", 0),
                        "exchanges_with_data": list(metrics.get("exchange_prices", {}).keys()),
                    }
        except Exception:
            pass

    return {
        "api": "CryptoFlow — Multi-Exchange Order Flow API",
        "version": "7.0.0",
        "exchanges": exchanges,
        "symbols_tracked": symbols,
        "data_freshness": freshness,
        "endpoints": {
            "free": {
                "/v1/metrics/live": "Real-time cached metrics (sub-ms)",
                "/v1/metrics/historical-cvd": "Historical CVD analysis",
                "/v1/metrics/vwap": "24h VWAP",
                "/v1/metrics/price-changes": "Price change stats (5m-24h) + 24h high/low",
                "/v1/data/liquidations": "Liquidation feed",
                "/v1/symbols": "Tracked symbols & exchanges",
                "/v1/exchanges": "Exchange connectivity info",
            },
            "pro ($14.99/mo)": {
                "/v1/data/large-trades": "Whale trade detector",
                "/v1/analysis/order-imbalance": "Buy/sell pressure ratio",
                "/v1/analysis/buyer-seller-ratio": "Taker buy/sell ratio (count + volume + USD)",
                "/v1/data/ohlcv": "OHLCV candlestick data (1m-4h)",
                "/v1/data/funding-rates": "Funding rates across exchanges",
            },
            "ultra ($49.99/mo)": {
                "/v1/data/open-interest": "Open interest across exchanges",
                "/v1/data/oi-history": "OI history (up to 7 days)",
                "/v1/data/funding-history": "Funding rate history",
                "/v1/analysis/liquidation-heatmap": "Liquidation heatmap by price",
                "/v1/analysis/sentiment": "Composite market sentiment score",
                "/v1/analysis/cross-exchange": "Cross-exchange flow comparison",
                "/v1/analysis/long-short-ratio": "Top trader long/short ratio",
            },
        },
        "pricing_tiers": {
            "BASIC (Free)": "50 requests/minute — try before you buy",
            "PRO ($14.99/mo)": "500 requests/minute — individual traders",
            "ULTRA ($49.99/mo)": "2,000 requests/minute — trading bots",
            "MEGA ($199.99/mo)": "10,000 requests/minute — institutions",
        },
    }


# ── Dashboard ──
@app.get("/", include_in_schema=False)
async def serve_dashboard():
    return FileResponse("static/index.html")


@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await ws.accept()
    WS_CLIENTS.add(ws)
    logger.info(f"Dashboard client connected ({len(WS_CLIENTS)} total)")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        WS_CLIENTS.discard(ws)
        logger.info(f"Dashboard client disconnected ({len(WS_CLIENTS)} total)")


# Mount static after routes
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Entry Point ──
if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting API Gateway on {HOST}:{PORT}")
    uvicorn.run(
        "api_gateway:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
        access_log=True,
    )
