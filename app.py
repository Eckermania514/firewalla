#!/usr/bin/env python3
"""
Firewalla Control Panel (asyncio, functional UI)
- Device list + detail
- Pause / Unpause monitoring
- Emergency Access on/off
- Block / Unblock internet
- Rename device
- Fast Redis access via SCAN + short cache
"""

import asyncio
import json
import os
import time
from datetime import datetime
from urllib.parse import quote

import redis.asyncio as redis
from aiohttp import web

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PORT = int(os.environ.get("PORT", "8080"))
CACHE_TTL = float(os.environ.get("CACHE_TTL", "2.5"))

# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

async def scan_keys(r: redis.Redis, match: str, count: int = 300):
    keys = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=match, count=count)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys

async def get_host_key(r: redis.Redis, mac: str):
    for m in (mac, mac.upper(), mac.lower()):
        key = f"host:mac:{m}"
        if await r.exists(key):
            return key
    return None

def parse_policy(data: dict):
    raw = data.get("policy") or ""
    if not raw:
        return False, False, {}
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return False, False, {}
    monitor = obj.get("monitor")
    paused = monitor is False or str(monitor).lower() == "false"
    emergency = (
        obj.get("emergencyAccess") is True
        or str(obj.get("emergencyAccess")).lower() == "true"
        or obj.get("acl") is False
        or str(obj.get("acl")).lower() == "false"
    )
    return paused, emergency, obj

def parse_tags(data: dict):
    tags = data.get("tags") or data.get("tag") or ""
    if not tags:
        return []
    try:
        if isinstance(tags, str) and tags.startswith("["):
            return [str(t) for t in json.loads(tags)]
        if isinstance(tags, str):
            return [t.strip() for t in tags.split(",") if t.strip()]
        if isinstance(tags, list):
            return [str(t) for t in tags]
    except Exception:
        pass
    return [str(tags)]

def human_bytes(n, is_rate=False):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0 B/s" if is_rate else "0 B"
    units = ["B/s", "KB/s", "MB/s", "GB/s"] if is_rate else ["B", "KB", "MB", "GB", "TB"]
    for u in units:
        if abs(n) < 1024:
            return f"{n:.1f} {u}" if abs(n) < 10 else f"{n:.0f} {u}"
        n /= 1024
    return f"{n:.1f} {units[-1]}"

async def build_timed_index(r: redis.Redis):
    """One SCAN for timedTraffic:*:1minute:* ; newest per mac/direction.

    Key format: timedTraffic:{direction}:{mac}:1minute:{ts}
    MAC contains colons, so do not split the whole key on ':'.
    """
    best = {"download": {}, "upload": {}}  # mac -> (ts, key)
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match="timedTraffic:*:1minute:*", count=500)
        for k in batch:
            if not k.startswith("timedTraffic:") or ":1minute:" not in k:
                continue
            try:
                rest = k[len("timedTraffic:"):]       # direction:mac:1minute:ts
                direction, rest2 = rest.split(":", 1)  # direction, mac:1minute:ts
                mac_part, ts_s = rest2.rsplit(":1minute:", 1)
                mac = mac_part.upper()
                ts = int(ts_s)
            except Exception:
                continue
            if direction not in best:
                continue
            prev = best[direction].get(mac)
            if prev is None or ts > prev[0]:
                best[direction][mac] = (ts, k)
        if cursor == 0:
            break

    out = {"download": {}, "upload": {}}
    pipe = r.pipeline()
    jobs = []
    for direction in ("download", "upload"):
        for mac, (ts, key) in best[direction].items():
            jobs.append((direction, mac))
            pipe.get(key)
    vals = await pipe.execute() if jobs else []
    for (direction, mac), val in zip(jobs, vals):
        try:
            out[direction][mac] = int(float(val or 0))
        except Exception:
            out[direction][mac] = 0
    return out

async def flow_fallback(r: redis.Redis, mac: str, direction: str) -> int:
    for m in (mac, mac.upper(), mac.lower()):
        ptr = await r.get(f"lastsumflow:{m}:{direction}")
        if not ptr:
            continue
        try:
            items = await r.zrange(ptr, 0, -1, withscores=True)
            if items:
                return int(sum(float(score) for _, score in items))
        except Exception:
            continue
    return 0

class DeviceStore:
    def __init__(self, r: redis.Redis):
        self.r = r
        self._devices = None
        self._ts = 0.0
        self._lock = asyncio.Lock()
        self._prev = {}  # mac -> (ts, down_total, up_total)

    async def load(self, force: bool = False):
        now = time.time()
        if not force and self._devices is not None and (now - self._ts) < CACHE_TTL:
            return self._devices

        async with self._lock:
            now = time.time()
            if not force and self._devices is not None and (now - self._ts) < CACHE_TTL:
                return self._devices

            host_keys = await scan_keys(self.r, "host:mac:*")
            timed = await build_timed_index(self.r)

            devices = []
            for key in host_keys:
                data = await self.r.hgetall(key)
                if not data:
                    continue

                mac_raw = key.replace("host:mac:", "", 1)
                mac = mac_raw.upper()
                name = data.get("name") or data.get("bname") or data.get("dhcpName") or mac
                ip = data.get("ipv4Addr") or data.get("ipv4") or data.get("ip") or ""
                vendor = data.get("macVendor") or data.get("vendor") or ""
                last = float(data.get("lastActiveTimestamp") or data.get("lastActive") or 0)
                online = (now - last) < 600 if last else False
                last_human = (
                    datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S") if last else "—"
                )
                paused, emergency, policy_obj = parse_policy(data)
                tags = parse_tags(data)
                group = (
                    ", ".join(tags)
                    if tags
                    else (data.get("groupName") or data.get("group") or "Ungrouped")
                )

                down_total = timed["download"].get(mac, 0)
                up_total = timed["upload"].get(mac, 0)
                down_is_timed = down_total > 0
                up_is_timed = up_total > 0

                if not down_is_timed:
                    down_total = await flow_fallback(self.r, mac_raw, "download")
                if not up_is_timed:
                    up_total = await flow_fallback(self.r, mac_raw, "upload")

                down_rate = down_total / 60.0 if down_is_timed else 0.0
                up_rate = up_total / 60.0 if up_is_timed else 0.0

                prev = self._prev.get(mac)
                if prev and not (down_is_timed or up_is_timed):
                    dts = max(now - prev[0], 0.001)
                    if down_total >= prev[1]:
                        down_rate = (down_total - prev[1]) / dts
                    if up_total >= prev[2]:
                        up_rate = (up_total - prev[2]) / dts
                self._prev[mac] = (now, down_total, up_total)

                is_rate = down_is_timed or up_is_timed or (prev is not None)
                down_show = down_rate if is_rate else float(down_total)
                up_show = up_rate if is_rate else float(up_total)

                if not online:
                    status = "Offline"
                elif paused:
                    status = "Paused"
                elif emergency:
                    status = "Emergency"
                else:
                    status = "Online"

                devices.append({
                    "mac": mac,
                    "name": name,
                    "ip": ip,
                    "vendor": vendor,
                    "online": online,
                    "paused": paused,
                    "emergency": emergency,
                    "group": group,
                    "status": status,
                    "last_active_human": last_human,
                    "down_show": down_show,
                    "up_show": up_show,
                    "is_rate": is_rate,
                    "down_total": down_total,
                    "up_total": up_total,
                    "down_h": human_bytes(down_show, is_rate=is_rate),
                    "up_h": human_bytes(up_show, is_rate=is_rate),
                    "down_total_h": human_bytes(down_total, is_rate=False),
                    "up_total_h": human_bytes(up_total, is_rate=False),
                    "raw_fields": sorted(data.items()),
                    "policy_obj": policy_obj,
                })

            devices.sort(key=lambda d: (not d["online"], d["name"].lower()))
            self._devices = devices
            self._ts = time.time()
            return devices

    async def get(self, mac: str):
        mac = mac.upper()
        for d in await self.load():
            if d["mac"] == mac:
                return d
        return None

# ---------------------------------------------------------------------------
# Control actions
# ---------------------------------------------------------------------------

async def set_monitor(r: redis.Redis, mac: str, enabled: bool) -> bool:
    key = await get_host_key(r, mac)
    if not key:
        return False
    data = await r.hgetall(key)
    try:
        policy = json.loads(data.get("policy") or "{}")
    except Exception:
        policy = {}
    policy["monitor"] = bool(enabled)
    await r.hset(key, "policy", json.dumps(policy))
    try:
        await r.publish("TO.FirewallaAgent", json.dumps({
            "type": "Policy:Changed", "mac": mac.upper(), "policy": policy
        }))
        await r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass
    return True

async def set_emergency(r: redis.Redis, mac: str, enabled: bool) -> bool:
    key = await get_host_key(r, mac)
    if not key:
        return False
    data = await r.hgetall(key)
    try:
        policy = json.loads(data.get("policy") or "{}")
    except Exception:
        policy = {}
    if enabled:
        policy["emergencyAccess"] = True
        policy["acl"] = False
    else:
        policy["emergencyAccess"] = False
        policy["acl"] = True
    await r.hset(key, "policy", json.dumps(policy))
    try:
        await r.publish("TO.FirewallaAgent", json.dumps({
            "type": "Policy:Changed", "mac": mac.upper(), "policy": policy
        }))
        await r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass
    return True

async def rename_device(r: redis.Redis, mac: str, name: str) -> bool:
    key = await get_host_key(r, mac)
    if not key or not name.strip():
        return False
    name = name.strip()[:32]
    await r.hset(key, "name", name)
    try:
        await r.publish("Host:Updated", json.dumps({"mac": mac.upper(), "name": name}))
    except Exception:
        pass
    return True

async def block_internet(r: redis.Redis, mac: str) -> bool:
    try:
        pid = int(await r.get("policy:id") or 0) + 1
    except Exception:
        pid = int(time.time())
    policy = {
        "pid": str(pid),
        "type": "mac",
        "target": mac.upper(),
        "action": "block",
        "direction": "outbound",
        "enabled": "1",
        "timestamp": str(int(time.time())),
        "expire": "",
        "cronTime": "",
        "duration": "0",
        "tag": "[]",
        "scope": "[]",
        "notes": f"Blocked via Control Panel {datetime.now().isoformat()}",
    }
    mapping = {
        k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v))
        for k, v in policy.items()
    }
    await r.hset(f"policy:{pid}", mapping=mapping)
    await r.zadd("policy_active", {str(pid): time.time()})
    await r.set("policy:id", str(pid))
    try:
        await r.publish("Policy:PolicyAdded", json.dumps(policy))
        await r.publish("TO.FirewallaAgent", json.dumps({"type": "Policy:Changed"}))
    except Exception:
        pass
    return True

async def unblock_internet(r: redis.Redis, mac: str) -> int:
    removed = 0
    keys = await scan_keys(r, "policy:*")
    for key in keys:
        if key in ("policy:id", "policy_active"):
            continue
        try:
            data = await r.hgetall(key)
            if (
                data.get("type") == "mac"
                and data.get("target", "").upper() == mac.upper()
                and data.get("action") == "block"
            ):
                pid = data.get("pid") or key.split(":")[-1]
                await r.delete(key)
                await r.zrem("policy_active", pid)
                removed += 1
                try:
                    await r.publish("Policy:PolicyRemoved", json.dumps({"pid": pid}))
                except Exception:
                    pass
        except Exception:
            continue
    if removed:
        try:
            await r.publish("TO.FirewallaAgent", json.dumps({"type": "Policy:Changed"}))
        except Exception:
            pass
    return removed

# ---------------------------------------------------------------------------
# HTML (functional, not fancy)
# ---------------------------------------------------------------------------

CSS = """
body { font-family: system-ui, sans-serif; margin: 16px; background: #111; color: #eee; }
a { color: #8ab4ff; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 14px; }
th, td { border: 1px solid #333; padding: 6px 8px; text-align: left; vertical-align: top; }
th { background: #1c1c1c; }
tr:nth-child(even) { background: #181818; }
.offline { color: #888; }
.paused, .emergency { color: #f88; }
.online { color: #8d8; }
.msg { background: #1a3; color: #cfc; padding: 8px 10px; margin-bottom: 12px; }
.err { background: #411; color: #fcc; padding: 8px 10px; margin-bottom: 12px; }
form.inline { display: inline; margin-right: 4px; }
button, input[type=submit] {
  background: #222; color: #eee; border: 1px solid #555; padding: 4px 8px; cursor: pointer;
}
button:hover, input[type=submit]:hover { background: #333; }
input[type=text] { background: #222; color: #eee; border: 1px solid #555; padding: 4px 6px; }
.toolbar { margin: 10px 0; }
.meta { color: #888; font-size: 13px; }
h1, h2 { margin: 0 0 8px 0; }
pre { white-space: pre-wrap; word-break: break-all; font-size: 12px; }
"""

def page(title: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{CSS}</style>
</head><body>
{body}
</body></html>"""

def flash(request: web.Request) -> str:
    msg = request.rel_url.query.get("msg")
    if not msg:
        return ""
    return f'<div class="msg">{msg}</div>'

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

async def index(request: web.Request):
    store: DeviceStore = request.app["store"]
    devices = await store.load()
    rows = []
    for d in devices:
        cls = d["status"].lower()
        pause_form = (
            f"<form class='inline' method='post' action='/action'>"
            f"<input type='hidden' name='mac' value='{d['mac']}'>"
            f"<input type='hidden' name='action' value='unpause'>"
            f"<input type='hidden' name='next' value='/'>"
            f"<button type='submit'>Unpause</button></form>"
            if d["paused"]
            else
            f"<form class='inline' method='post' action='/action'>"
            f"<input type='hidden' name='mac' value='{d['mac']}'>"
            f"<input type='hidden' name='action' value='pause'>"
            f"<input type='hidden' name='next' value='/'>"
            f"<button type='submit'>Pause</button></form>"
        )
        rows.append(
            f"<tr class='{cls}'>"
            f"<td><a href='/device/{d['mac']}'>{d['name']}</a></td>"
            f"<td>{d['ip'] or '—'}</td>"
            f"<td>{d['mac']}</td>"
            f"<td>{d['status']}</td>"
            f"<td>{d['group']}</td>"
            f"<td>{d['down_h']}</td>"
            f"<td>{d['up_h']}</td>"
            f"<td>"
            f"{pause_form}"
            f"<form class='inline' method='post' action='/action'>"
            f"<input type='hidden' name='mac' value='{d['mac']}'>"
            f"<input type='hidden' name='action' value='block'>"
            f"<input type='hidden' name='next' value='/'>"
            f"<button type='submit'>Block</button></form>"
            f"<form class='inline' method='post' action='/action'>"
            f"<input type='hidden' name='mac' value='{d['mac']}'>"
            f"<input type='hidden' name='action' value='unblock'>"
            f"<input type='hidden' name='next' value='/'>"
            f"<button type='submit'>Unblock</button></form>"
            f"</td></tr>"
        )

    body = f"""
    <h1>Firewalla Control</h1>
    <div class="meta">{len(devices)} devices · cache ≤ {CACHE_TTL}s · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    {flash(request)}
    <div class="toolbar">
      <form class="inline" method="post" action="/action">
        <input type="hidden" name="action" value="refresh">
        <input type="hidden" name="next" value="/">
        <button type="submit">Force refresh</button>
      </form>
      <a href="/api/devices">JSON API</a>
    </div>
    <table>
      <tr>
        <th>Name</th><th>IP</th><th>MAC</th><th>Status</th><th>Group</th>
        <th>Down</th><th>Up</th><th>Actions</th>
      </tr>
      {''.join(rows)}
    </table>
    """
    return web.Response(text=page("Firewalla Control", body), content_type="text/html")

async def device_page(request: web.Request):
    store: DeviceStore = request.app["store"]
    mac = request.match_info["mac"].upper()
    d = await store.get(mac)
    if not d:
        await store.load(force=True)
        d = await store.get(mac)
    if not d:
        return web.Response(text="Device not found", status=404)

    raw_rows = "".join(
        f"<tr><th>{k}</th><td><pre>{v}</pre></td></tr>" for k, v in d["raw_fields"]
    )

    pause_btn = (
        f"<form class='inline' method='post' action='/action'>"
        f"<input type='hidden' name='mac' value='{d['mac']}'>"
        f"<input type='hidden' name='action' value='unpause'>"
        f"<input type='hidden' name='next' value='/device/{d['mac']}'>"
        f"<button type='submit'>Unpause monitoring</button></form>"
        if d["paused"]
        else
        f"<form class='inline' method='post' action='/action'>"
        f"<input type='hidden' name='mac' value='{d['mac']}'>"
        f"<input type='hidden' name='action' value='pause'>"
        f"<input type='hidden' name='next' value='/device/{d['mac']}'>"
        f"<button type='submit'>Pause monitoring</button></form>"
    )

    emerg_btn = (
        f"<form class='inline' method='post' action='/action'>"
        f"<input type='hidden' name='mac' value='{d['mac']}'>"
        f"<input type='hidden' name='action' value='emergency_off'>"
        f"<input type='hidden' name='next' value='/device/{d['mac']}'>"
        f"<button type='submit'>Disable emergency access</button></form>"
        if d["emergency"]
        else
        f"<form class='inline' method='post' action='/action'>"
        f"<input type='hidden' name='mac' value='{d['mac']}'>"
        f"<input type='hidden' name='action' value='emergency_on'>"
        f"<input type='hidden' name='next' value='/device/{d['mac']}'>"
        f"<button type='submit'>Enable emergency access</button></form>"
    )

    body = f"""
    <p><a href="/">← All devices</a></p>
    <h1>{d['name']}</h1>
    <div class="meta">{d['mac']} · {d['status']}</div>
    {flash(request)}

    <h2>Controls</h2>
    <div class="toolbar">
      {pause_btn}
      {emerg_btn}
      <form class="inline" method="post" action="/action">
        <input type="hidden" name="mac" value="{d['mac']}">
        <input type="hidden" name="action" value="block">
        <input type="hidden" name="next" value="/device/{d['mac']}">
        <button type="submit">Block internet</button>
      </form>
      <form class="inline" method="post" action="/action">
        <input type="hidden" name="mac" value="{d['mac']}">
        <input type="hidden" name="action" value="unblock">
        <input type="hidden" name="next" value="/device/{d['mac']}">
        <button type="submit">Unblock internet</button>
      </form>
    </div>

    <h2>Rename</h2>
    <form method="post" action="/action">
      <input type="hidden" name="mac" value="{d['mac']}">
      <input type="hidden" name="action" value="rename">
      <input type="hidden" name="next" value="/device/{d['mac']}">
      <input type="text" name="name" value="{d['name']}" maxlength="32">
      <button type="submit">Save</button>
    </form>

    <h2>Summary</h2>
    <table>
      <tr><th>IP</th><td>{d['ip'] or '—'}</td></tr>
      <tr><th>MAC</th><td>{d['mac']}</td></tr>
      <tr><th>Vendor</th><td>{d['vendor'] or '—'}</td></tr>
      <tr><th>Group</th><td>{d['group'] or '—'}</td></tr>
      <tr><th>Online</th><td>{'Yes' if d['online'] else 'No'}</td></tr>
      <tr><th>Monitoring</th><td>{'Paused' if d['paused'] else 'Active'}</td></tr>
      <tr><th>Emergency</th><td>{'On' if d['emergency'] else 'Off'}</td></tr>
      <tr><th>Last active</th><td>{d['last_active_human']}</td></tr>
      <tr><th>Download rate</th><td>{d['down_h']}</td></tr>
      <tr><th>Upload rate</th><td>{d['up_h']}</td></tr>
      <tr><th>Download total</th><td>{d['down_total_h']}</td></tr>
      <tr><th>Upload total</th><td>{d['up_total_h']}</td></tr>
    </table>

    <h2>Raw Redis fields</h2>
    <table>{raw_rows}</table>
    """
    return web.Response(text=page(d["name"], body), content_type="text/html")

async def action(request: web.Request):
    r: redis.Redis = request.app["redis"]
    store: DeviceStore = request.app["store"]
    form = await request.post()
    mac = (form.get("mac") or "").strip().upper()
    act = (form.get("action") or "").strip().lower()
    next_url = form.get("next") or "/"
    msg = "OK"

    if act == "refresh":
        await store.load(force=True)
        msg = "Refreshed"
    elif act == "pause":
        msg = "Paused" if await set_monitor(r, mac, False) else "Pause failed"
        await store.load(force=True)
    elif act == "unpause":
        msg = "Unpaused" if await set_monitor(r, mac, True) else "Unpause failed"
        await store.load(force=True)
    elif act == "emergency_on":
        msg = "Emergency on" if await set_emergency(r, mac, True) else "Failed"
        await store.load(force=True)
    elif act == "emergency_off":
        msg = "Emergency off" if await set_emergency(r, mac, False) else "Failed"
        await store.load(force=True)
    elif act == "block":
        msg = "Blocked" if await block_internet(r, mac) else "Block failed"
        await store.load(force=True)
    elif act == "unblock":
        n = await unblock_internet(r, mac)
        msg = f"Removed {n} block rule(s)"
        await store.load(force=True)
    elif act == "rename":
        name = form.get("name") or ""
        msg = "Renamed" if await rename_device(r, mac, name) else "Rename failed"
        await store.load(force=True)
    else:
        msg = f"Unknown action: {act}"

    sep = "&" if "?" in next_url else "?"
    raise web.HTTPFound(f"{next_url}{sep}msg={quote(msg)}")

async def api_devices(request: web.Request):
    store: DeviceStore = request.app["store"]
    devices = await store.load()
    slim = [{
        "mac": d["mac"],
        "name": d["name"],
        "ip": d["ip"],
        "status": d["status"],
        "paused": d["paused"],
        "emergency": d["emergency"],
        "group": d["group"],
        "down": d["down_h"],
        "up": d["up_h"],
    } for d in devices]
    return web.json_response({
        "now": datetime.now().isoformat(timespec="seconds"),
        "total": len(slim),
        "devices": slim,
    })

# ---------------------------------------------------------------------------
# App bootstrap
# ---------------------------------------------------------------------------

async def on_startup(app: web.Application):
    app["redis"] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    app["store"] = DeviceStore(app["redis"])
    try:
        await app["store"].load(force=True)
    except Exception as e:
        print(f"startup cache warm failed: {e}")

async def on_cleanup(app: web.Application):
    r = app.get("redis")
    if r is not None:
        await r.aclose()

def main():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/", index)
    app.router.add_get("/device/{mac}", device_page)
    app.router.add_post("/action", action)
    app.router.add_get("/api/devices", api_devices)
    print(f"Firewalla Control listening on 0.0.0.0:{PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)

if __name__ == "__main__":
    main()
