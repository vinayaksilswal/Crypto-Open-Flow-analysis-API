import time
import urllib.request
from urllib.error import HTTPError, URLError


BASE = "http://127.0.0.1:8000"


def request(method: str, path: str, headers: dict) -> tuple[object, int, str]:
    url = BASE + path
    req = urllib.request.Request(url, method=method, headers=headers)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            ms = int((time.time() - t0) * 1000)
            return resp.status, ms, body[:400]
    except HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        return e.code, ms, body[:400]
    except URLError as e:
        ms = int((time.time() - t0) * 1000)
        return "URLERR", ms, str(e)


def run(name: str, items: list[tuple[str, str, dict]]):
    print(f"\n== {name} ==")
    for method, path, headers in items:
        status, ms, preview = request(method, path, headers)
        ok = status == 200
        print(f"{method} {path:60} -> {status} {ms:5}ms {'OK' if ok else 'FAIL'}")
        if not ok:
            print("  preview:", preview.replace("\n", " ")[:200])


def main():
    basic = [
        ("GET", "/health", {}),
        ("GET", "/v1/status", {}),
        ("GET", "/v1/symbols", {}),
        ("GET", "/v1/exchanges", {}),
        ("GET", "/v1/metrics/live?symbol=BTCUSDT", {}),
        ("GET", "/v1/metrics/vwap?symbol=BTCUSDT", {}),
        ("GET", "/v1/metrics/historical-cvd?symbol=BTCUSDT&minutes=5", {}),
        ("GET", "/v1/metrics/price-changes?symbol=BTCUSDT", {}),
        ("GET", "/v1/data/liquidations?symbol=BTCUSDT&limit=5", {}),
    ]

    ultra_hdr = {
        "X-RapidAPI-Subscription": "ULTRA",
        "X-RapidAPI-User": "local-smoke",
    }

    premium = [
        ("GET", "/v1/data/large-trades?symbol=BTCUSDT&hours=1&limit=5", ultra_hdr),
        ("GET", "/v1/analysis/order-imbalance?symbol=BTCUSDT&minutes=5", ultra_hdr),
        ("GET", "/v1/analysis/buyer-seller-ratio?symbol=BTCUSDT&timeframe=1h", ultra_hdr),
        ("GET", "/v1/data/ohlcv?symbol=BTCUSDT&interval=1m&limit=10", ultra_hdr),
        ("GET", "/v1/data/funding-rates?symbol=BTCUSDT", ultra_hdr),
        ("GET", "/v1/data/open-interest?symbol=BTCUSDT", ultra_hdr),
        ("GET", "/v1/data/oi-history?symbol=BTCUSDT&hours=1", ultra_hdr),
        ("GET", "/v1/data/funding-history?symbol=BTCUSDT&hours=1", ultra_hdr),
        ("GET", "/v1/analysis/liquidation-heatmap?symbol=BTCUSDT&hours=1", ultra_hdr),
        ("GET", "/v1/analysis/sentiment?symbol=BTCUSDT", ultra_hdr),
        ("GET", "/v1/analysis/cross-exchange?symbol=BTCUSDT", ultra_hdr),
        ("GET", "/v1/analysis/long-short-ratio?symbol=BTCUSDT", ultra_hdr),
    ]

    run("BASIC", basic)
    run("ULTRA", premium)


if __name__ == "__main__":
    main()

