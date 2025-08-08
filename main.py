from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from collections import defaultdict, deque
from typing import List
import csv
import io
import httpx
from datetime import datetime, timedelta
API_KEY = "de74f23bf31d486c909fb20babfd3d9c"

app = FastAPI()

# Store max 100 candles per symbol
candles_store = defaultdict(lambda: deque(maxlen=100))

@app.post("/webhook")
async def receive_webhook(request: Request):
    body = await request.body()
    text = body.decode("utf-8").strip()

    try:
        # Parse CSV: TICKER,TIME,OPEN,HIGH,LOW,CLOSE,VOLUME
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if len(row) != 7:
                return JSONResponse(content={"error": "Invalid CSV format"}, status_code=400)
            symbol, time, open_, high, low, close, volume = row
            candles_store[symbol].append({
                "time": time,
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume)
            })
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
        
@app.get("/candles")
def get_candles(symbol: str):
    if symbol not in candles_store:
        return {"symbol": symbol, "values": []}
    # Sort by time DESC (newest first)
    sorted_data = sorted(
        candles_store[symbol],
        key=lambda x: datetime.strptime(x["time"], "%Y-%m-%dT%H:%M:%SZ"),
        reverse=True
    )
    return {
        "symbol": symbol,
        "values": sorted_data
    }

@app.delete("/candles/clear")
def clear_candles(symbol: str):
    if symbol in candles_store:
        candles_store[symbol].clear()
        return {"status": "cleared", "symbol": symbol}
    return {"status": "not_found", "symbol": symbol}

@app.get("/candles/sync")
async def sync_candles(symbol: str):
    # Convert symbol: EURUSD → EUR/USD
    if len(symbol) != 6:
        return {"error": "Invalid symbol format. Use like EURUSD"}

    formatted_symbol = f"{symbol[:3]}/{symbol[3:]}"
    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": formatted_symbol,
        "interval": "5min",
        "outputsize": 100,
        "apikey": API_KEY,
        "timezone": "UTC"
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)

    if response.status_code != 200:
        return {"error": "Failed to fetch data", "status": response.status_code}

    data = response.json()

    if data.get("status") != "ok" or "values" not in data:
        return {"error": data.get("message", "Unexpected response from TwelveData")}

    # Clear and store latest 100 candles
    candles_store[symbol] = deque(maxlen=100)

    for item in reversed(data["values"]):  # oldest to newest
        try:
            candles_store[symbol].append({
                "time": item["datetime"].replace(" ", "T") + "Z",
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": 0.0  # no volume provided
            })
        except:
            continue  # skip malformed candles
        
    return {
        "status": "synced",
        "symbol": symbol,
        "stored": len(candles_store[symbol])
    }

@app.get("/candles/sync_all")
async def sync_all_candles():
    pairs = [
        "AUDCAD", "AUDJPY", "AUDUSD", "CADJPY", "EURGBP",
        "EURJPY", "EURUSD", "GBPAUD", "GBPCAD", "GBPJPY",
        "GBPUSD", "USDCAD", "USDCHF", "USDJPY"
    ]

    url = "https://api.twelvedata.com/time_series"
    results = {}

    async with httpx.AsyncClient() as client:
        for symbol in pairs:
            formatted_symbol = f"{symbol[:3]}/{symbol[3:]}"
            params = {
                "symbol": formatted_symbol,
                "interval": "5min",
                "outputsize": 100,
                "apikey": API_KEY,
                "timezone": "UTC"
            }

            try:
                response = await client.get(url, params=params)
                data = response.json()

                if response.status_code != 200 or data.get("status") != "ok" or "values" not in data:
                    results[symbol] = {"status": "error", "message": data.get("message", "No data")}
                    continue

                candles_store[symbol] = deque(maxlen=100)

                for item in reversed(data["values"]):
                    try:
                        candles_store[symbol].append({
                            "time": item["datetime"].replace(" ", "T") + "Z",
                            "open": float(item["open"]),
                            "high": float(item["high"]),
                            "low": float(item["low"]),
                            "close": float(item["close"]),
                            "volume": 0.0
                        })
                    except:
                        continue

                results[symbol] = {"status": "synced", "stored": len(candles_store[symbol])}

            except Exception as e:
                results[symbol] = {"status": "error", "message": str(e)}

    return results
    
@app.get("/candles/check_gaps")
def check_gaps():
    results = {}

    for symbol, candles in candles_store.items():
        if len(candles) < 2:
            results[symbol] = {
                "status": "ok",
                "stored": len(candles)
            }
            continue

        try:
            # Sort by time ascending
            sorted_candles = sorted(
                candles,
                key=lambda x: datetime.strptime(x["time"], "%Y-%m-%dT%H:%M:%SZ")
            )

            has_gap = False
            previous_time = datetime.strptime(sorted_candles[0]["time"], "%Y-%m-%dT%H:%M:%SZ")

            for i in range(1, len(sorted_candles)):
                current_time = datetime.strptime(sorted_candles[i]["time"], "%Y-%m-%dT%H:%M:%SZ")
                if (current_time - previous_time) != timedelta(minutes=5):
                    has_gap = True
                    break
                previous_time = current_time

            results[symbol] = {
                "status": "gap_detected" if has_gap else "ok",
                "stored": len(candles)
            }

        except Exception as e:
            results[symbol] = {
                "status": "error",
                "message": str(e),
                "stored": len(candles)
            }

    return results

@app.get("/candles/last_update")
def get_last_updates():
    result = {}

    for symbol, candles in candles_store.items():
        if candles:
            # Find the newest candle by time
            latest_candle = max(
                candles,
                key=lambda x: datetime.strptime(x["time"], "%Y-%m-%dT%H:%M:%SZ")
            )
            result[symbol] = {
                "last_update": latest_candle["time"],
                "stored": len(candles)
            }
        else:
            result[symbol] = {
                "last_update": None,
                "stored": 0
            }

    return result

