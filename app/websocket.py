import asyncio
import time
from fastapi import WebSocket, WebSocketDisconnect
from .adapters.devices import get_all_devices


async def live_stream(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Snapshot every 5 seconds (adjust as needed)
            devices = get_all_devices()
            online_count = sum(1 for d in devices if d.online)

            payload = {
                "ts": time.time(),
                "online_devices": online_count,
                "total_devices": len(devices),
                "devices": [d.model_dump() for d in devices if d.online],  # only online by default
                # Uncomment if you want full list:
                # "all_devices": [d.model_dump() for d in devices],
            }

            await websocket.send_json(payload)
            await asyncio.sleep(5)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"error": str(e)})
        except Exception:
            pass
