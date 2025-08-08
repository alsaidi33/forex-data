from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from collections import defaultdict, deque
from typing import List
import csv
import io
import httpx

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
    data = list(candles_store.get(symbol, []))
    return {"symbol": symbol, "candles": data}

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

