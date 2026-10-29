from fastapi import FastAPI, Query, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import time

from .models import HealthResponse, Device, Policy, NetworkInfo
from .adapters.devices import get_all_devices, get_device
from .adapters.policies import get_all_policies
from .adapters.network import get_networks
from .websocket import live_stream
from .redis_client import get_redis

app = FastAPI(
    title="Firewalla Purple Local API",
    description="Read-only local REST + WebSocket API for Firewalla Purple",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health():
    r = get_redis()
    try:
        r.ping()
        redis_status = "ok"
        device_count = len(list(r.scan_iter(match="host:mac:*", count=500)))
    except Exception:
        redis_status = "error"
        device_count = 0

    return HealthResponse(
        status="ok" if redis_status == "ok" else "degraded",
        redis=redis_status,
        device_count=device_count,
        timestamp=time.time(),
    )


@app.get("/devices", response_model=list[Device])
def list_devices(online: Optional[bool] = Query(None)):
    return get_all_devices(online_only=bool(online))


@app.get("/devices/{mac}", response_model=Device)
def device_detail(mac: str):
    dev = get_device(mac)
    if not dev:
        raise HTTPException(status_code=404, detail="Device not found")
    return dev


@app.get("/policies", response_model=list[Policy])
def list_policies():
    return get_all_policies()


@app.get("/networks", response_model=list[NetworkInfo])
def list_networks():
    return get_networks()


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await live_stream(websocket)
