# Firewalla Purple Local API

Lightweight read-only REST + WebSocket API that runs directly on the Firewalla Purple.

Built against the real Redis schema on original Firewalla Purple (Ubuntu 22.04, Redis 6.0.16).

## Features

- Read-only (never writes to Redis or modifies Firewalla)
- Low resource usage (single worker, minimal dependencies)
- Devices with derived online/offline status
- Policies / rules
- Network info
- Live WebSocket stream of online devices

## Install

```bash
cd /home/pi/firewalla-api          # or any directory you prefer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python run.py
```

The API listens on **http://0.0.0.0:5140**

## Endpoints

| Method | Path              | Description                          |
|--------|-------------------|--------------------------------------|
| GET    | `/health`         | Health + Redis status + device count |
| GET    | `/devices`        | All devices (`?online=true` filter)  |
| GET    | `/devices/{mac}`  | Single device by MAC                 |
| GET    | `/policies`       | Firewall policies / rules            |
| GET    | `/networks`       | Network UUID records                 |
| WS     | `/ws/live`        | Live stream of online devices (5s)   |

Interactive docs (Swagger UI):  
http://\<firewalla-ip\>:5140/docs

## WebSocket example

```javascript
const ws = new WebSocket("ws://192.168.x.x:5140/ws/live");
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`Online: ${data.online_devices}/${data.total_devices}`);
  console.log(data.devices);
};
```

## Configuration

Environment variables (optional):

- `FWAPI_HOST` (default `0.0.0.0`)
- `FWAPI_PORT` (default `5140`)
- `FWAPI_REDIS_HOST` (default `127.0.0.1`)
- `FWAPI_ONLINE_THRESHOLD_SECONDS` (default `300`)

## Notes

- Online status is derived from `lastActiveTimestamp` (device seen within last 5 minutes by default).
- Only `policy:*` keys are returned (device-specific `policy:mac:*` tags are skipped for now).
- Designed to be extended later with traffic (`sumflow`) endpoints if needed.
