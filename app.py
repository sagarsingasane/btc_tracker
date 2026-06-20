import asyncio
import json
import time
import requests
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="templates")

app_state = {
    "live_price": 0.0,
    "atr_1m": 0.0,
    "price_to_beat": 68250.50,
    "closing_second": 59,
    "plus_one_sec": False,
    "tracking": False
}
active_connections = []

def get_initial_atr():
    try:
        url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=30"
        res = requests.get(url, timeout=5).json()
        tr_list = []
        prev_close = None
        for k in res:
            high, low, close = float(k[2]), float(k[3]), float(k[4])
            if prev_close is not None:
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                tr_list.append(tr)
            prev_close = close
        if tr_list:
            return round(sum(tr_list[-14:]) / 14, 2)
    except Exception as e:
        print("ATR Error:", e)
    return 0.0

# --- दर १० सेकंदाला ATR अपडेट करणारे लूप ---
async def update_atr_loop():
    global app_state
    while True:
        try:
            # बॅकग्राऊंडमध्ये ATR फेच करतो जेणेकरून लाईव्ह प्राईसला अडथळा येणार नाही
            new_atr = await asyncio.to_thread(get_initial_atr)
            if new_atr > 0:
                app_state["atr_1m"] = new_atr
        except Exception as e:
            pass
        await asyncio.sleep(10) # १० सेकंदाचा इंटरव्हल सेट केला आहे

async def binance_stream():
    global app_state
    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                while True:
                    data = await ws.recv()
                    msg = json.loads(data)
                    app_state["live_price"] = float(msg["c"])
                    payload = {
                        "live_price": app_state["live_price"],
                        "atr_1m": app_state["atr_1m"],
                        "server_time": time.strftime("%H:%M:%S")
                    }
                    for conn in active_connections:
                        try:
                            await conn.send_text(json.dumps(payload))
                        except:
                            pass
                    await asyncio.sleep(0.1)
        except Exception as e:
            print("Binance Disconnected, reconnecting...", e)
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    app_state["atr_1m"] = get_initial_atr()
    asyncio.create_task(binance_stream())
    asyncio.create_task(update_atr_loop()) # लूप सुरू केले

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.remove(websocket)
