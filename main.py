from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from collections import defaultdict, deque
from typing import List
import csv
import io

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
