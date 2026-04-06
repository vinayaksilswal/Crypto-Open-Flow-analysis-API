# CryptoFlow — Multi-Exchange Order Flow API

Real-time crypto derivatives analytics across **Binance**, **Bybit**, and **OKX**. VWAP, CVD, liquidation heatmaps, funding rates, open interest, whale detection, market sentiment scoring, and OHLCV candlestick data. Built for RapidAPI monetization.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │              API Gateway (FastAPI)              │
                    │  REST API + Dashboard + Auth + Rate Limiting    │
                    └───────────────┬─────────────┬──────────────────┘
                                    │ reads       │ reads
                              PostgreSQL        Redis (cache)
                                    ↑               ↑
                    ┌───────────────┘               └──────────────────┐
                    │                                                   │
          ┌─────────────────────┐                         ┌──────────────────────┐
          │  Ingestion Service  │                         │  Analytics Worker    │
          │  (The Harvester)    │                         │  (The Cruncher)      │
          │                     │                         │                      │
          │  WebSocket streams: │                         │  Calculates:         │
          │  • Binance Futures  │  ────── PostgreSQL ───→ │  • Cross-exchange    │
          │  • Bybit Linear     │                         │    aggregated CVD    │
          │  • OKX Swaps        │                         │  • Sentiment score   │
          │                     │                         │  • Liq heatmap       │
          │  REST pollers:      │                         │  • OHLCV rollups     │
          │  • Funding rates    │                         │  → Caches to Redis   │
          │  • Open interest    │                         └──────────────────────┘
          └─────────────────────┘
```

| Service | Type | Role |
|---------|------|------|
| `api_gateway.py` | Web Service | REST API + Dashboard + RapidAPI auth + tier gating |
| `ingestion_service.py` | Background Worker | Multi-exchange WebSocket → PostgreSQL |
| `analytics_worker.py` | Background Worker | PostgreSQL → Redis (pre-computed metrics) |

## Exchanges

| Exchange | Data Feeds | Symbol Format |
|----------|-----------|---------------|
| **Binance** | Trades, Liquidations, Funding, OI | `BTCUSDT` |
| **Bybit** | Trades, Liquidations, Funding, OI | `BTCUSDT` |
| **OKX** | Trades, Liquidations, Funding, OI | `BTC-USDT-SWAP` (auto-mapped) |

## Tracked Symbols

Configurable via `SYMBOLS` env var: `BTCUSDT,ETHUSDT,SOLUSDT` (add any futures pair).

---

## API Endpoints (16 total)

### Free Tier — 50 req/min

| Endpoint | Description |
|----------|-------------|
| `GET /v1/metrics/live?symbol=BTCUSDT` | Sub-ms cached price, VWAP, CVD, imbalance (all exchanges) |
| `GET /v1/metrics/historical-cvd?symbol=BTCUSDT&minutes=60&exchange=binance` | Historical CVD, optional exchange filter |
| `GET /v1/metrics/vwap?symbol=BTCUSDT` | 24h VWAP across all exchanges |
| `GET /v1/data/liquidations?symbol=BTCUSDT&limit=50` | Recent liquidation events |
| `GET /v1/symbols` | Tracked symbols + connected exchanges |
| `GET /v1/exchanges` | Exchange connectivity status |

### Pro Tier — $14.99/mo, 500 req/min

| Endpoint | Description |
|----------|-------------|
| `GET /v1/data/large-trades?symbol=BTCUSDT&min_usd=100000` | Whale trade detector (cross-exchange) |
| `GET /v1/analysis/order-imbalance?symbol=BTCUSDT&minutes=60` | Buy/sell pressure ratio |
| `GET /v1/data/ohlcv?symbol=BTCUSDT&interval=1m&limit=100` | OHLCV candles (1m, 5m, 15m, 1h, 4h) |
| `GET /v1/data/funding-rates?symbol=BTCUSDT` | Current funding rates per exchange |

### Ultra Tier — $49.99/mo, 2000 req/min

| Endpoint | Description |
|----------|-------------|
| `GET /v1/data/open-interest?symbol=BTCUSDT` | OI per exchange + 1h change |
| `GET /v1/data/oi-history?symbol=BTCUSDT&hours=24` | Historical OI data points |
| `GET /v1/data/funding-history?symbol=BTCUSDT&hours=24` | Historical funding rates |
| `GET /v1/analysis/liquidation-heatmap?symbol=BTCUSDT` | Liq heatmap by price level |
| `GET /v1/analysis/sentiment?symbol=BTCUSDT` | Composite sentiment score (-100 to +100) |
| `GET /v1/analysis/cross-exchange?symbol=BTCUSDT` | Cross-exchange flow comparison |

### Public (No Auth)

| Endpoint | Description |
|----------|-------------|
| `GET /` | Real-time dashboard |
| `GET /health` | Service health check |
| `GET /v1/status` | API capabilities + data freshness |
| `GET /docs` | Interactive Swagger documentation |

---

## Deploy to Render

### One-Click Blueprint

1. Push this repo to GitHub
2. Go to [render.com/deploy](https://render.com/deploy)
3. Paste your repo URL → Render reads `render.yaml` and creates all 5 services
4. Set `RAPIDAPI_PROXY_SECRET` in the API Gateway environment variables

**Monthly cost: ~$28** (4 × $7 starter services)

### Manual Setup

1. **PostgreSQL**: Create database → note connection string
2. **Redis**: Create Redis → note connection string
3. **Web Service** (`api_gateway.py`):
   - Build: `pip install -r requirements.txt`
   - Start: `gunicorn api_gateway:app --worker-class uvicorn.workers.UvicornWorker --workers 2 --bind 0.0.0.0:$PORT`
   - Health: `/health`
   - Env: `DATABASE_URL`, `REDIS_URL`, `RAPIDAPI_PROXY_SECRET`, `SYMBOLS`, `ENABLED_EXCHANGES`
4. **Worker** (`ingestion_service.py`):
   - Start: `python ingestion_service.py`
   - Env: `DATABASE_URL`, `SYMBOLS`, `ENABLED_EXCHANGES`
5. **Worker** (`analytics_worker.py`):
   - Start: `python analytics_worker.py`
   - Env: `DATABASE_URL`, `REDIS_URL`, `SYMBOLS`, `ENABLED_EXCHANGES`

---

## List on RapidAPI

1. Go to [rapidapi.com/provider](https://rapidapi.com/provider) → "Add New API"
2. Set base URL: `https://cryptoflow-api.onrender.com`
3. **Security** tab → copy Proxy Secret → set as `RAPIDAPI_PROXY_SECRET` on Render
4. **Endpoints** tab → import from OpenAPI/Swagger at `/docs`
5. **Pricing** tab:

| Plan | Price | Rate Limit | Access |
|------|-------|------------|--------|
| Basic | Free | 50 req/min | 6 free endpoints |
| Pro | $14.99/mo | 500 req/min | + OHLCV, whale trades, imbalance, funding |
| Ultra | $49.99/mo | 2,000 req/min | + OI, heatmap, sentiment, cross-exchange |
| Mega | $199.99/mo | 10,000 req/min | All endpoints, priority |

---

## Local Development

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start all services (separate terminals)
python ingestion_service.py
python analytics_worker.py
python api_gateway.py

# 4. Open dashboard: http://localhost:8000
# 5. Swagger docs: http://localhost:8000/docs
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `RAPIDAPI_PROXY_SECRET` | _(empty)_ | RapidAPI proxy secret |
| `SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Comma-separated futures pairs |
| `ENABLED_EXCHANGES` | `binance,bybit,okx` | Comma-separated exchanges |
| `PORT` | `8000` | API Gateway port |
| `BATCH_FLUSH_INTERVAL` | `0.5` | Seconds between DB flushes |
| `DATA_RETENTION_DAYS` | `30` | Auto-delete data > N days |
| `LARGE_TRADE_THRESHOLD_USD` | `100000` | Whale trade threshold |

---

## License

Proprietary. All rights reserved.
#   C r y p t o - O p e n - F l o w - a n a l y s i s - A P I 
 
 