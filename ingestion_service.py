"""
Ingestion Service (The Harvester) — Production Multi-Exchange
Streams trades & liquidations from Binance, Bybit, and OKX Futures
into PostgreSQL using batched inserts. Also polls funding rates,
open interest, and long/short ratios via REST APIs.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

import asyncpg
import httpx
import websockets
import redis.asyncio as aioredis

# ── 1. Environment & Logging ──────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Harvester")

# ── 2. Configuration ─────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/crypto_data")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
BATCH_FLUSH_INTERVAL = float(os.getenv("BATCH_FLUSH_INTERVAL", "0.5"))
BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", "200"))
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))
ENABLED_EXCHANGES = [e.strip().lower() for e in os.getenv("ENABLED_EXCHANGES", "binance,bybit,okx").split(",") if e.strip()]

# ── 3. Symbol Mapping ────────────────────────────────────────────────────────
# Each exchange has different symbol formats
def _symbol_map(symbol: str, exchange: str) -> str:
    """Convert a canonical symbol (BTCUSDT) to exchange-specific format."""
    if exchange == "binance":
        return symbol  # BTCUSDT
    elif exchange == "bybit":
        return symbol  # BTCUSDT
    elif exchange == "okx":
        # OKX uses BTC-USDT-SWAP for perps
        base = symbol.replace("USDT", "")
        return f"{base}-USDT-SWAP"
    return symbol


def _canonical_symbol(symbol: str, exchange: str) -> str:
    """Convert exchange-specific symbol back to canonical (BTCUSDT)."""
    if exchange == "okx":
        # BTC-USDT-SWAP → BTCUSDT
        return symbol.replace("-SWAP", "").replace("-", "")
    return symbol.upper()


# ── 4. Global State ──────────────────────────────────────────────────────────
DB_POOL = None
REDIS_CLIENT = None
shutdown_event = asyncio.Event()

# Local orderbook snapshots before flush
local_orderbooks = {
    "binance": {sym: {"bids": [], "asks": [], "ts": 0} for sym in SYMBOLS},
    "bybit": {sym: {"bids": [], "asks": [], "ts": 0} for sym in SYMBOLS},
    "okx": {sym: {"bids": [], "asks": [], "ts": 0} for sym in SYMBOLS},
}

# Batched insert buffers
trade_buffer: list = []
liq_buffer: list = []
funding_buffer: list = []
oi_buffer: list = []
ls_ratio_buffer: list = []
buffer_lock = asyncio.Lock()

# Runtime stats
stats = {
    "trades_ingested": 0,
    "liqs_ingested": 0,
    "trades_flushed": 0,
    "liqs_flushed": 0,
    "funding_flushed": 0,
    "oi_flushed": 0,
    "ls_ratio_flushed": 0,
    "flush_errors": 0,
    "ws_reconnects": 0,
}


# ── 5. Database Setup ────────────────────────────────────────────────────────
async def init_postgres():
    """Connect to PostgreSQL with exponential backoff. Create all tables."""
    global DB_POOL
    max_retries = 10

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to PostgreSQL (attempt {attempt}/{max_retries})...")
            DB_POOL = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=15, command_timeout=30
            )
            async with DB_POOL.acquire() as conn:
                await conn.execute("""
                    -- Core trades table
                    CREATE TABLE IF NOT EXISTS trades (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL DEFAULT 'binance',
                        symbol VARCHAR(20) NOT NULL,
                        price NUMERIC NOT NULL,
                        quantity NUMERIC NOT NULL,
                        is_sell BOOLEAN NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_trades_exch_sym_ts
                        ON trades (exchange, symbol, timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_trades_sym_ts
                        ON trades (symbol, timestamp DESC);
                    CREATE INDEX IF NOT EXISTS idx_trades_whale
                        ON trades (symbol, timestamp DESC, price, quantity);

                    -- Liquidations
                    CREATE TABLE IF NOT EXISTS liquidations (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL DEFAULT 'binance',
                        symbol VARCHAR(20) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        price NUMERIC NOT NULL,
                        quantity NUMERIC NOT NULL,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_liq_exch_sym_ts
                        ON liquidations (exchange, symbol, timestamp DESC);

                    -- Funding Rates
                    CREATE TABLE IF NOT EXISTS funding_rates (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        funding_rate NUMERIC NOT NULL,
                        funding_time TIMESTAMP NOT NULL,
                        UNIQUE(exchange, symbol, funding_time)
                    );
                    CREATE INDEX IF NOT EXISTS idx_funding_sym_ts
                        ON funding_rates (symbol, funding_time DESC);

                    -- Open Interest
                    CREATE TABLE IF NOT EXISTS open_interest (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        oi_value NUMERIC NOT NULL,
                        oi_quantity NUMERIC,
                        timestamp TIMESTAMP NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_oi_sym_ts
                        ON open_interest (symbol, timestamp DESC);

                    -- OHLCV 1-minute candles (aggregated from trades)
                    CREATE TABLE IF NOT EXISTS ohlcv_1m (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL DEFAULT 'all',
                        symbol VARCHAR(20) NOT NULL,
                        open_time TIMESTAMP NOT NULL,
                        open NUMERIC NOT NULL,
                        high NUMERIC NOT NULL,
                        low NUMERIC NOT NULL,
                        close NUMERIC NOT NULL,
                        volume NUMERIC NOT NULL,
                        buy_volume NUMERIC NOT NULL DEFAULT 0,
                        sell_volume NUMERIC NOT NULL DEFAULT 0,
                        trade_count INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(exchange, symbol, open_time)
                    );
                    CREATE INDEX IF NOT EXISTS idx_ohlcv_sym_ts
                        ON ohlcv_1m (symbol, open_time DESC);

                    -- Long/Short Ratio (top trader positions)
                    CREATE TABLE IF NOT EXISTS long_short_ratio (
                        id SERIAL PRIMARY KEY,
                        exchange VARCHAR(10) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        long_ratio NUMERIC NOT NULL,
                        short_ratio NUMERIC NOT NULL,
                        long_short_ratio NUMERIC NOT NULL,
                        ratio_type VARCHAR(30) NOT NULL DEFAULT 'topTraderPositions',
                        timestamp TIMESTAMP NOT NULL,
                        UNIQUE(exchange, symbol, ratio_type, timestamp)
                    );
                    CREATE INDEX IF NOT EXISTS idx_ls_ratio_sym_ts
                        ON long_short_ratio (symbol, timestamp DESC);
                """)

            logger.info(f"[OK] PostgreSQL connected. Tracking: {', '.join(SYMBOLS)} on {', '.join(ENABLED_EXCHANGES)}")
            return
        except Exception as e:
            if attempt == max_retries:
                logger.critical(f"[FATAL] PostgreSQL failed after {max_retries} attempts: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"PostgreSQL unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)



async def init_redis():
    global REDIS_CLIENT
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Connecting to Redis (attempt {attempt}/{max_retries})...")
            REDIS_CLIENT = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
            await REDIS_CLIENT.ping()
            logger.info("[OK] Redis connected.")
            return
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"[ERROR] Redis unavailable: {e}")
                sys.exit(1)
            wait = min(2 ** attempt, 30)
            logger.warning(f"Redis unavailable: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)


# ── 6. Batch Flush Logic ─────────────────────────────────────────────────────
async def flush_buffers():
    """Background task that flushes all buffers to DB periodically."""
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            await _do_flush()
        except asyncio.CancelledError:
            logger.info("Flush task cancelled — performing final flush...")
            await _do_flush()
            break
        except Exception as e:
            logger.error(f"Flush loop error: {e}")
            stats["flush_errors"] += 1


async def _do_flush():
    """Atomically drains all buffers and batch-inserts into PostgreSQL."""
    async with buffer_lock:
        trades = list(trade_buffer);  trade_buffer.clear()
        liqs = list(liq_buffer);      liq_buffer.clear()
        fundings = list(funding_buffer); funding_buffer.clear()
        ois = list(oi_buffer);        oi_buffer.clear()
        ls_ratios = list(ls_ratio_buffer); ls_ratio_buffer.clear()

    if trades:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO trades (exchange, symbol, price, quantity, is_sell, timestamp)
                       VALUES ($1, $2, $3, $4, $5, to_timestamp($6 / 1000.0))""",
                    trades,
                )
            stats["trades_flushed"] += len(trades)
        except Exception as e:
            logger.error(f"Trade batch insert failed ({len(trades)} records): {e}")
            stats["flush_errors"] += 1

    if liqs:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO liquidations (exchange, symbol, side, price, quantity, timestamp)
                       VALUES ($1, $2, $3, $4, $5, to_timestamp($6 / 1000.0))""",
                    liqs,
                )
            stats["liqs_flushed"] += len(liqs)
        except Exception as e:
            logger.error(f"Liquidation batch insert failed ({len(liqs)} records): {e}")
            stats["flush_errors"] += 1

    if fundings:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO funding_rates (exchange, symbol, funding_rate, funding_time)
                       VALUES ($1, $2, $3, to_timestamp($4 / 1000.0))
                       ON CONFLICT (exchange, symbol, funding_time) DO NOTHING""",
                    fundings,
                )
            stats["funding_flushed"] += len(fundings)
        except Exception as e:
            logger.error(f"Funding rate insert failed ({len(fundings)} records): {e}")
            stats["flush_errors"] += 1

    if ois:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO open_interest (exchange, symbol, oi_value, oi_quantity, timestamp)
                       VALUES ($1, $2, $3, $4, NOW())""",
                    ois,
                )
            stats["oi_flushed"] += len(ois)
        except Exception as e:
            logger.error(f"Open interest insert failed ({len(ois)} records): {e}")
            stats["flush_errors"] += 1

    if ls_ratios:
        try:
            async with DB_POOL.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO long_short_ratio (exchange, symbol, long_ratio, short_ratio, long_short_ratio, ratio_type, timestamp)
                       VALUES ($1, $2, $3, $4, $5, $6, NOW())
                       ON CONFLICT (exchange, symbol, ratio_type, timestamp) DO NOTHING""",
                    ls_ratios,
                )
            stats["ls_ratio_flushed"] += len(ls_ratios)
        except Exception as e:
            logger.error(f"Long/Short ratio insert failed ({len(ls_ratios)} records): {e}")
            stats["flush_errors"] += 1



async def depth_flusher():
    """Flushes orderbook depth snapshots directly to Redis every 500ms."""
    await asyncio.sleep(5)  # Wait for WS connect
    logger.info("[Flusher] Orderbook depth flusher started (every 500ms)")
    while not shutdown_event.is_set():
        try:
            if REDIS_CLIENT:
                pipe = REDIS_CLIENT.pipeline()
                for exch in ENABLED_EXCHANGES:
                    for sym in SYMBOLS:
                        book = local_orderbooks[exch][sym]
                        if book["ts"] > 0:
                            pipe.set(f"depth:{sym}:{exch}", json.dumps(book))
                await pipe.execute()
        except Exception as e:
            logger.error(f"Depth flusher error: {e}")
        
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break


# ── 7. WebSocket Processors ──────────────────────────────────────────────────

# ─── 7a. Binance ─────────────────────────────────────────────────────────────
async def binance_ingestor():
    """Connects to Binance Futures combined stream."""
    if "binance" not in ENABLED_EXCHANGES:
        return

    streams = []
    for sym in SYMBOLS:
        s = sym.lower()
        streams.append(f"{s}@aggTrade")
        streams.append(f"{s}@forceOrder")
        streams.append(f"{s}@depth20@100ms")

    ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
    logger.info(f"[Binance] Connecting ({len(streams)} streams)...")

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                logger.info(f"[Binance] Connected — harvesting {', '.join(SYMBOLS)}")
                async for message in ws:
                    if shutdown_event.is_set():
                        break
                    data = json.loads(message)
                    await _process_binance(data)
        except asyncio.CancelledError:
            logger.info("[Binance] Ingestor cancelled.")
            break
        except Exception as e:
            stats["ws_reconnects"] += 1
            logger.error(f"[Binance] Disconnected: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


async def _process_binance(data: dict):
    stream_data = data.get("data", data)
    event = stream_data.get("e")

    if event == "aggTrade":
        symbol = stream_data.get("s", "UNKNOWN").upper()
        record = (
            "binance", symbol,
            float(stream_data["p"]), float(stream_data["q"]),
            stream_data["m"],  # is_sell
            stream_data["T"],
        )
        async with buffer_lock:
            trade_buffer.append(record)
        stats["trades_ingested"] += 1

    elif event == "forceOrder":
        order = stream_data["o"]
        symbol = order.get("s", "UNKNOWN").upper()
        record = (
            "binance", symbol,
            order["S"],  # side
            float(order["p"]), float(order["q"]),
            order["T"],
        )
        async with buffer_lock:
            liq_buffer.append(record)
        stats["liqs_ingested"] += 1
        usd = float(order["p"]) * float(order["q"])
        if usd > 50000:
            logger.info(f"[Binance LIQ] {order['S']} {float(order['q']):.4f} {symbol} @ ${float(order['p']):,.2f} (${usd:,.0f})")

    elif stream_data.get("bids") and stream_data.get("asks"):
        # Partial book depth stream (snapshot)
        symbol = str(data.get("stream", "")).split("@")[0].upper()
        if symbol in SYMBOLS:
            local_orderbooks["binance"][symbol]["bids"] = stream_data["bids"]
            local_orderbooks["binance"][symbol]["asks"] = stream_data["asks"]
            local_orderbooks["binance"][symbol]["ts"] = int(time.time() * 1000)


# ─── 7b. Bybit ───────────────────────────────────────────────────────────────
async def bybit_ingestor():
    """Connects to Bybit V5 linear perpetual WebSocket."""
    if "bybit" not in ENABLED_EXCHANGES:
        return

    ws_url = "wss://stream.bybit.com/v5/public/linear"
    logger.info(f"[Bybit] Connecting...")

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                # Subscribe to trades and liquidations
                trade_topics = [f"publicTrade.{sym}" for sym in SYMBOLS]
                liq_topics = [f"liquidation.{sym}" for sym in SYMBOLS]
                depth_topics = [f"orderbook.50.{sym}" for sym in SYMBOLS]

                subscribe_msg = json.dumps({
                    "op": "subscribe",
                    "args": trade_topics + liq_topics + depth_topics,
                })
                await ws.send(subscribe_msg)
                logger.info(f"[Bybit] Connected — subscribed to {len(trade_topics)} trade + {len(liq_topics)} liq + {len(depth_topics)} depth streams")

                async for message in ws:
                    if shutdown_event.is_set():
                        break
                    data = json.loads(message)
                    await _process_bybit(data)

        except asyncio.CancelledError:
            logger.info("[Bybit] Ingestor cancelled.")
            break
        except Exception as e:
            stats["ws_reconnects"] += 1
            logger.error(f"[Bybit] Disconnected: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


async def _process_bybit(data: dict):
    topic = data.get("topic", "")

    if topic.startswith("publicTrade."):
        for trade in data.get("data", []):
            symbol = trade.get("s", "UNKNOWN").upper()
            is_sell = trade.get("S", "Buy") == "Sell"
            record = (
                "bybit", symbol,
                float(trade["p"]), float(trade["v"]),
                is_sell,
                trade["T"],  # timestamp ms
            )
            async with buffer_lock:
                trade_buffer.append(record)
            stats["trades_ingested"] += 1

    elif topic.startswith("liquidation."):
        liq = data.get("data", {})
        symbol = liq.get("symbol", "UNKNOWN").upper()
        side = liq.get("side", "Buy")
        record = (
            "bybit", symbol,
            side,
            float(liq.get("price", 0)), float(liq.get("size", 0)),
            liq.get("updatedTime", int(time.time() * 1000)),
        )
        async with buffer_lock:
            liq_buffer.append(record)
        stats["liqs_ingested"] += 1
        usd = float(liq.get("price", 0)) * float(liq.get("size", 0))
        if usd > 50000:
            logger.info(f"[Bybit LIQ] {side} {float(liq.get('size', 0)):.4f} {symbol} @ ${float(liq.get('price', 0)):,.2f} (${usd:,.0f})")

    elif topic.startswith("orderbook."):
        obj = data.get("data", {})
        symbol = obj.get("s", "UNKNOWN").upper()
        if symbol in SYMBOLS:
            msg_type = data.get("type", "snapshot")
            
            if msg_type == "snapshot":
                local_orderbooks["bybit"][symbol]["bids"] = obj.get("b", [])
                local_orderbooks["bybit"][symbol]["asks"] = obj.get("a", [])
            else:
                # Basic delta apply:
                bids_dict = {float(p): q for p, q in local_orderbooks["bybit"][symbol]["bids"]}
                asks_dict = {float(p): q for p, q in local_orderbooks["bybit"][symbol]["asks"]}
                
                for p, q in obj.get("b", []):
                    if float(q) == 0: bids_dict.pop(float(p), None)
                    else: bids_dict[float(p)] = q
                for p, q in obj.get("a", []):
                    if float(q) == 0: asks_dict.pop(float(p), None)
                    else: asks_dict[float(p)] = q
                
                # Sort and store list of arrays [p, q] top 50
                sorted_bids = sorted(bids_dict.items(), key=lambda x: -x[0])[:50]
                sorted_asks = sorted(asks_dict.items(), key=lambda x: x[0])[:50]
                
                local_orderbooks["bybit"][symbol]["bids"] = [[str(k), str(v)] for k, v in sorted_bids]
                local_orderbooks["bybit"][symbol]["asks"] = [[str(k), str(v)] for k, v in sorted_asks]
                
            local_orderbooks["bybit"][symbol]["ts"] = obj.get("ts", int(time.time() * 1000))


# ─── 7c. OKX ─────────────────────────────────────────────────────────────────
async def okx_ingestor():
    """Connects to OKX public WebSocket for swaps."""
    if "okx" not in ENABLED_EXCHANGES:
        return

    ws_url = "wss://ws.okx.com:8443/ws/v5/public"
    logger.info(f"[OKX] Connecting...")

    while not shutdown_event.is_set():
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                # Subscribe to trades and liquidations
                args = []
                for sym in SYMBOLS:
                    okx_sym = _symbol_map(sym, "okx")
                    args.append({"channel": "trades", "instId": okx_sym})
                    args.append({"channel": "liquidation-orders", "instType": "SWAP"})
                    args.append({"channel": "books5", "instId": okx_sym})

                subscribe_msg = json.dumps({"op": "subscribe", "args": args})
                await ws.send(subscribe_msg)
                logger.info(f"[OKX] Connected — subscribed to {len(SYMBOLS)} symbols")

                async for message in ws:
                    if shutdown_event.is_set():
                        break
                    data = json.loads(message)
                    await _process_okx(data)

        except asyncio.CancelledError:
            logger.info("[OKX] Ingestor cancelled.")
            break
        except Exception as e:
            stats["ws_reconnects"] += 1
            logger.error(f"[OKX] Disconnected: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


async def _process_okx(data: dict):
    arg = data.get("arg", {})
    channel = arg.get("channel", "")

    if channel == "trades":
        for trade in data.get("data", []):
            inst_id = trade.get("instId", "")
            canonical = _canonical_symbol(inst_id, "okx")
            if canonical not in SYMBOLS:
                return
            is_sell = trade.get("side", "buy") == "sell"
            record = (
                "okx", canonical,
                float(trade["px"]), float(trade["sz"]),
                is_sell,
                int(trade["ts"]),
            )
            async with buffer_lock:
                trade_buffer.append(record)
            stats["trades_ingested"] += 1

    elif channel == "liquidation-orders":
        for liq in data.get("data", []):
            inst_id = liq.get("instId", "")
            canonical = _canonical_symbol(inst_id, "okx")
            if canonical not in SYMBOLS:
                return
            details = liq.get("details", [{}])
            for d in details:
                side = d.get("side", "buy").upper()
                record = (
                    "okx", canonical,
                    side.upper(),
                    float(d.get("bkPx", 0)), float(d.get("sz", 0)),
                    int(d.get("ts", int(time.time() * 1000))),
                )
                async with buffer_lock:
                    liq_buffer.append(record)
                stats["liqs_ingested"] += 1

    elif channel == "books5":
        for book in data.get("data", []):
            arg = data.get("arg", {})
            inst_id = arg.get("instId", "")
            canonical = _canonical_symbol(inst_id, "okx")
            if canonical in SYMBOLS:
                # books5 sends 5 levels of bids and asks directly, so just snapshot
                bids_clean = [[b[0], b[1]] for b in book.get("bids", [])] # prune orders count etc
                asks_clean = [[a[0], a[1]] for a in book.get("asks", [])]
                local_orderbooks["okx"][canonical]["bids"] = bids_clean
                local_orderbooks["okx"][canonical]["asks"] = asks_clean
                local_orderbooks["okx"][canonical]["ts"] = int(book.get("ts", int(time.time() * 1000)))


# ── 8. REST API Pollers ──────────────────────────────────────────────────────

async def funding_rate_poller():
    """Polls funding rates from all exchanges every 5 minutes."""
    await asyncio.sleep(10)  # Let WS connections establish first
    logger.info("[Poller] Funding rate poller started (every 5 min)")

    while not shutdown_event.is_set():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for sym in SYMBOLS:
                    # ── Binance ──
                    if "binance" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://fapi.binance.com/fapi/v1/fundingRate",
                                params={"symbol": sym, "limit": 1}
                            )
                            if resp.status_code == 200:
                                for item in resp.json():
                                    record = (
                                        "binance", sym,
                                        float(item["fundingRate"]),
                                        int(item["fundingTime"]),
                                    )
                                    async with buffer_lock:
                                        funding_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Binance] Funding rate fetch failed for {sym}: {e}")

                    # ── Bybit ──
                    if "bybit" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://api.bybit.com/v5/market/tickers",
                                params={"category": "linear", "symbol": sym}
                            )
                            if resp.status_code == 200:
                                result = resp.json().get("result", {})
                                for item in result.get("list", []):
                                    fr = item.get("fundingRate")
                                    if fr:
                                        record = (
                                            "bybit", sym,
                                            float(fr),
                                            int(time.time() * 1000),
                                        )
                                        async with buffer_lock:
                                            funding_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Bybit] Funding rate fetch failed for {sym}: {e}")

                    # ── OKX ──
                    if "okx" in ENABLED_EXCHANGES:
                        try:
                            okx_sym = _symbol_map(sym, "okx")
                            resp = await client.get(
                                "https://www.okx.com/api/v5/public/funding-rate",
                                params={"instId": okx_sym},
                                headers={"Accept": "application/json"},
                            )
                            if resp.status_code == 200:
                                resp_data = resp.json()
                                if resp_data.get("code") == "0":
                                    for item in resp_data.get("data", []):
                                        fr = item.get("fundingRate")
                                        if fr:
                                            record = (
                                                "okx", sym,
                                                float(fr),
                                                int(item.get("fundingTime", int(time.time() * 1000))),
                                            )
                                            async with buffer_lock:
                                                funding_buffer.append(record)
                                else:
                                    logger.debug(f"[OKX] Funding rate API code {resp_data.get('code')} for {sym}")
                        except Exception as e:
                            logger.debug(f"[OKX] Funding rate fetch skipped for {sym}: {e}")

        except Exception as e:
            logger.error(f"Funding rate poller error: {e}")

        try:
            await asyncio.sleep(300)  # 5 minutes
        except asyncio.CancelledError:
            break


async def open_interest_poller():
    """Polls open interest from all exchanges every 5 minutes."""
    await asyncio.sleep(15)
    logger.info("[Poller] Open interest poller started (every 5 min)")

    while not shutdown_event.is_set():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for sym in SYMBOLS:
                    # ── Binance ──
                    if "binance" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://fapi.binance.com/fapi/v1/openInterest",
                                params={"symbol": sym}
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                record = (
                                    "binance", sym,
                                    float(data.get("openInterest", 0)),
                                    None,  # oi_quantity — same as value for Binance
                                )
                                async with buffer_lock:
                                    oi_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Binance] OI fetch failed for {sym}: {e}")

                    # ── Bybit ──
                    if "bybit" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://api.bybit.com/v5/market/open-interest",
                                params={"category": "linear", "symbol": sym, "intervalTime": "5min", "limit": 1}
                            )
                            if resp.status_code == 200:
                                result = resp.json().get("result", {})
                                for item in result.get("list", []):
                                    record = (
                                        "bybit", sym,
                                        float(item.get("openInterest", 0)),
                                        None,
                                    )
                                    async with buffer_lock:
                                        oi_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Bybit] OI fetch failed for {sym}: {e}")

                    # ── OKX ──
                    if "okx" in ENABLED_EXCHANGES:
                        try:
                            okx_sym = _symbol_map(sym, "okx")
                            resp = await client.get(
                                "https://www.okx.com/api/v5/public/open-interest",
                                params={"instType": "SWAP", "instId": okx_sym},
                                headers={"Accept": "application/json"},
                            )
                            if resp.status_code == 200:
                                resp_data = resp.json()
                                if resp_data.get("code") == "0":
                                    for item in resp_data.get("data", []):
                                        record = (
                                            "okx", sym,
                                            float(item.get("oiCcy", 0)),
                                            float(item.get("oi", 0)),
                                        )
                                        async with buffer_lock:
                                            oi_buffer.append(record)
                                else:
                                    logger.debug(f"[OKX] OI API code {resp_data.get('code')} for {sym}")
                        except Exception as e:
                            logger.debug(f"[OKX] OI fetch skipped for {sym}: {e}")

        except Exception as e:
            logger.error(f"Open interest poller error: {e}")

        try:
            await asyncio.sleep(300)  # 5 minutes
        except asyncio.CancelledError:
            break


async def long_short_ratio_poller():
    """Polls long/short ratio from all exchanges every 5 minutes."""
    await asyncio.sleep(20)
    logger.info("[Poller] Long/Short ratio poller started (every 5 min)")

    while not shutdown_event.is_set():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                for sym in SYMBOLS:
                    # ── Binance: Top Trader Long/Short Position Ratio ──
                    if "binance" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://fapi.binance.com/futures/data/topLongShortPositionRatio",
                                params={"symbol": sym, "period": "5m", "limit": 1}
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                if data and len(data) > 0:
                                    item = data[0]
                                    long_r = float(item.get("longAccount", 0.5))
                                    short_r = float(item.get("shortAccount", 0.5))
                                    ls_r = float(item.get("longShortRatio", 1.0))
                                    record = (
                                        "binance", sym,
                                        long_r, short_r, ls_r,
                                        "topTraderPositions",
                                    )
                                    async with buffer_lock:
                                        ls_ratio_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Binance] L/S ratio fetch failed for {sym}: {e}")

                    # ── Bybit: Account Ratio ──
                    if "bybit" in ENABLED_EXCHANGES:
                        try:
                            resp = await client.get(
                                "https://api.bybit.com/v5/market/account-ratio",
                                params={"category": "linear", "symbol": sym, "period": "1d", "limit": 1}
                            )
                            if resp.status_code == 200:
                                result = resp.json().get("result", {})
                                items = result.get("list", [])
                                if items:
                                    item = items[0]
                                    buy_r = float(item.get("buyRatio", 0.5))
                                    sell_r = float(item.get("sellRatio", 0.5))
                                    ls_r = round(buy_r / sell_r, 4) if sell_r > 0 else 1.0
                                    record = (
                                        "bybit", sym,
                                        buy_r, sell_r, ls_r,
                                        "topTraderPositions",
                                    )
                                    async with buffer_lock:
                                        ls_ratio_buffer.append(record)
                        except Exception as e:
                            logger.warning(f"[Bybit] L/S ratio fetch failed for {sym}: {e}")

                    # ── OKX: Long/Short Account Ratio ──
                    if "okx" in ENABLED_EXCHANGES:
                        try:
                            base = sym.replace("USDT", "")
                            resp = await client.get(
                                "https://www.okx.com/api/v5/rubik/stat/contracts-long-short-account-ratio",
                                params={"ccy": base, "period": "5m"},
                                headers={"Accept": "application/json"},
                            )
                            if resp.status_code == 200:
                                resp_data = resp.json()
                                if resp_data.get("code") == "0":
                                    items = resp_data.get("data", [])
                                    if items:
                                        # OKX returns [timestamp, long_short_ratio] format
                                        # the ratio is long/short
                                        item = items[0]
                                        if isinstance(item, list) and len(item) >= 2:
                                            ls_r = float(item[1])
                                            long_r = ls_r / (1 + ls_r)  # derive from ratio
                                            short_r = 1 / (1 + ls_r)
                                        else:
                                            ls_r = 1.0
                                            long_r = 0.5
                                            short_r = 0.5
                                        record = (
                                            "okx", sym,
                                            round(long_r, 4), round(short_r, 4), round(ls_r, 4),
                                            "topTraderPositions",
                                        )
                                        async with buffer_lock:
                                            ls_ratio_buffer.append(record)
                        except Exception as e:
                            logger.debug(f"[OKX] L/S ratio fetch skipped for {sym}: {e}")

        except Exception as e:
            logger.error(f"Long/Short ratio poller error: {e}")

        try:
            await asyncio.sleep(300)  # 5 minutes
        except asyncio.CancelledError:
            break


# ── 9. OHLCV Aggregator ──────────────────────────────────────────────────────
async def ohlcv_aggregator():
    """Aggregates trades into 1-minute OHLCV candles every 60 seconds."""
    await asyncio.sleep(60)  # Wait for initial data
    logger.info("[OHLCV] Candle aggregator started (every 60s)")

    while not shutdown_event.is_set():
        try:
            async with DB_POOL.acquire() as conn:
                for sym in SYMBOLS:
                    # Aggregate last 2 minutes of trades into 1m candles
                    await conn.execute("""
                        INSERT INTO ohlcv_1m (exchange, symbol, open_time, open, high, low, close, volume, buy_volume, sell_volume, trade_count)
                        SELECT
                            'all' as exchange,
                            symbol,
                            date_trunc('minute', timestamp) as open_time,
                            (ARRAY_AGG(price ORDER BY timestamp ASC))[1] as open,
                            MAX(price) as high,
                            MIN(price) as low,
                            (ARRAY_AGG(price ORDER BY timestamp DESC))[1] as close,
                            SUM(quantity) as volume,
                            SUM(CASE WHEN is_sell = false THEN quantity ELSE 0 END) as buy_volume,
                            SUM(CASE WHEN is_sell = true THEN quantity ELSE 0 END) as sell_volume,
                            COUNT(*) as trade_count
                        FROM trades
                        WHERE symbol = $1
                          AND timestamp >= NOW() - INTERVAL '3 minutes'
                          AND timestamp < date_trunc('minute', NOW())
                        GROUP BY symbol, date_trunc('minute', timestamp)
                        ON CONFLICT (exchange, symbol, open_time) DO UPDATE SET
                            high = GREATEST(ohlcv_1m.high, EXCLUDED.high),
                            low = LEAST(ohlcv_1m.low, EXCLUDED.low),
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            buy_volume = EXCLUDED.buy_volume,
                            sell_volume = EXCLUDED.sell_volume,
                            trade_count = EXCLUDED.trade_count
                    """, sym)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"OHLCV aggregation error: {e}")

        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            break


# ── 10. Heartbeat Logger ─────────────────────────────────────────────────────
async def heartbeat():
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(30)
            logger.info(
                f"[HEARTBEAT] Ingested: {stats['trades_ingested']} trades, {stats['liqs_ingested']} liqs | "
                f"Flushed: {stats['trades_flushed']}t {stats['liqs_flushed']}l {stats['funding_flushed']}f {stats['oi_flushed']}oi {stats['ls_ratio_flushed']}ls | "
                f"Buf: {len(trade_buffer)}t/{len(liq_buffer)}l | "
                f"Errors: {stats['flush_errors']} | Reconnects: {stats['ws_reconnects']}"
            )
        except asyncio.CancelledError:
            break


# ── 11. Data Retention Cleanup ────────────────────────────────────────────────
async def data_retention_cleanup():
    await asyncio.sleep(300)
    while not shutdown_event.is_set():
        try:
            logger.info(f"[CLEANUP] Running retention cleanup (>{DATA_RETENTION_DAYS} days)...")
            async with DB_POOL.acquire() as conn:
                del_trades = await conn.execute(
                    "DELETE FROM trades WHERE timestamp < NOW() - make_interval(days => $1)", DATA_RETENTION_DAYS)
                del_liqs = await conn.execute(
                    "DELETE FROM liquidations WHERE timestamp < NOW() - make_interval(days => $1)", DATA_RETENTION_DAYS)
                del_ohlcv = await conn.execute(
                    "DELETE FROM ohlcv_1m WHERE open_time < NOW() - make_interval(days => $1)", DATA_RETENTION_DAYS)
                del_funding = await conn.execute(
                    "DELETE FROM funding_rates WHERE funding_time < NOW() - make_interval(days => $1)", DATA_RETENTION_DAYS)
                del_oi = await conn.execute(
                    "DELETE FROM open_interest WHERE timestamp < NOW() - make_interval(days => $1)", DATA_RETENTION_DAYS)
            logger.info(f"[CLEANUP] Done: trades={del_trades}, liqs={del_liqs}, ohlcv={del_ohlcv}, funding={del_funding}, oi={del_oi}")
            await asyncio.sleep(6 * 3600)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Retention cleanup error: {e}")
            await asyncio.sleep(3600)


# ── 12. Main ──────────────────────────────────────────────────────────────────
async def main():
    logger.info(f"Starting Harvester — symbols: {', '.join(SYMBOLS)} | exchanges: {', '.join(ENABLED_EXCHANGES)}")
    await init_postgres()
    await init_redis()

    tasks = [
        asyncio.create_task(binance_ingestor()),
        asyncio.create_task(bybit_ingestor()),
        asyncio.create_task(okx_ingestor()),
        asyncio.create_task(flush_buffers()),
        asyncio.create_task(depth_flusher()),
        asyncio.create_task(heartbeat()),
        asyncio.create_task(funding_rate_poller()),
        asyncio.create_task(open_interest_poller()),
        asyncio.create_task(long_short_ratio_poller()),
        asyncio.create_task(ohlcv_aggregator()),
        asyncio.create_task(data_retention_cleanup()),
    ]

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

    logger.info("Shutdown signal received. Cancelling tasks...")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if REDIS_CLIENT:
        await REDIS_CLIENT.close()
    if DB_POOL:
        await DB_POOL.close()
    logger.info("Harvester shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
