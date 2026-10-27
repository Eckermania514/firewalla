#!/usr/bin/env python3
"""
Firewalla Full Control Dashboard (single file, asyncio, tabbed)

Tabs: Devices | Rules | Alarms | Groups | Network | VPN | DNS/Protect | System | Flows | Settings

Implements local Redis + best-effort file/HTTP controls available on-box.
Not a byte-for-byte clone of the official app for FireRouter/MSP/AP7 cloud paths.
Set FIREWALLA_UI_PASSWORD to require login.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

import redis.asyncio as redis
from aiohttp import web

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
PORT = int(os.environ.get("PORT", "8080"))
CACHE_TTL = float(os.environ.get("CACHE_TTL", "2.5"))
UI_PASSWORD = os.environ.get("FIREWALLA_UI_PASSWORD", "admin")  # empty = no auth
SESSION_TTL = int(os.environ.get("SESSION_TTL", "86400"))
SECRET = os.environ.get("FIREWALLA_UI_SECRET") or secrets.token_hex(16)

KNOWN_HOST_BOOLS = [
    "monitor", "acl", "emergencyAccess", "adblock", "family", "dox",
    "vpnClient", "device_service_scan", "deviceOffline", "devicePresence",
    "forcePrivateIP",
]

TABS = [
    ("/", "Devices"),
    ("/rules", "Rules"),
    ("/alarms", "Alarms"),
    ("/groups", "Groups"),
    ("/network", "Network"),
    ("/vpn", "VPN"),
    ("/protect", "DNS/Protect"),
    ("/system", "System"),
    ("/flows", "Flows"),
    ("/settings", "Settings"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_ts() -> float:
    return time.time()

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

def human_ts(ts):
    try:
        ts = float(ts)
        if ts > 1e12:
            ts /= 1000.0
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "—"

def _as_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return None

def parse_json(raw, default=None):
    if default is None:
        default = {}
    if raw is None or raw == "":
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default

async def scan_keys(r: redis.Redis, match: str, count: int = 400):
    out = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=match, count=count)
        out.extend(batch)
        if cursor == 0:
            break
    return out

async def get_host_key(r: redis.Redis, mac: str):
    for m in (mac, mac.upper(), mac.lower()):
        key = f"host:mac:{m}"
        if await r.exists(key):
            return key
    return None

def parse_policy_obj(data: dict) -> dict:
    return parse_json(data.get("policy"), {})

def parse_tags(data: dict, policy: dict):
    tags = data.get("tags") or data.get("tag") or policy.get("tags") or policy.get("tag") or []
    tags = parse_json(tags, tags)
    if isinstance(tags, list):
        return [str(t) for t in tags]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    return []

async def publish_host_policy(r, mac, policy):
    try:
        await r.publish("TO.FirewallaAgent", json.dumps({
            "type": "Policy:Changed", "mac": mac.upper(), "policy": policy
        }))
        await r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass

async def publish_policy_event(r, event, payload):
    try:
        await r.publish(event, json.dumps(payload))
        await r.publish("TO.FirewallaAgent", json.dumps({"type": "Policy:Changed"}))
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Auth (optional)
# ---------------------------------------------------------------------------

def sign_session(exp: int) -> str:
    msg = str(exp).encode()
    sig = hmac.new(SECRET.encode(), msg, hashlib.sha256).hexdigest()[:32]
    return f"{exp}.{sig}"

def verify_session(token: str) -> bool:
    if not UI_PASSWORD:
        return True
    if not token or "." not in token:
        return False
    exp_s, sig = token.split(".", 1)
    try:
        exp = int(exp_s)
    except ValueError:
        return False
    if exp < int(time.time()):
        return False
    expect = hmac.new(SECRET.encode(), exp_s.encode(), hashlib.sha256).hexdigest()[:32]
    return hmac.compare_digest(expect, sig)

def check_auth(request: web.Request) -> bool:
    if not UI_PASSWORD:
        return True
    tok = request.cookies.get("fw_session", "")
    return verify_session(tok)

# ---------------------------------------------------------------------------
# Traffic index
# ---------------------------------------------------------------------------

async def build_timed_index(r: redis.Redis):
    best = {"download": {}, "upload": {}}
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match="timedTraffic:*:1minute:*", count=500)
        for k in batch:
            if not k.startswith("timedTraffic:") or ":1minute:" not in k:
                continue
            try:
                rest = k[len("timedTraffic:"):]
                direction, rest2 = rest.split(":", 1)
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
        for mac, (_ts, key) in best[direction].items():
            jobs.append((direction, mac))
            pipe.get(key)
    vals = await pipe.execute() if jobs else []
    for (direction, mac), val in zip(jobs, vals):
        try:
            out[direction][mac] = int(float(val or 0))
        except Exception:
            out[direction][mac] = 0
    return out

async def flow_fallback(r, mac, direction):
    for m in (mac, mac.upper(), mac.lower()):
        ptr = await r.get(f"lastsumflow:{m}:{direction}")
        if not ptr:
            continue
        try:
            items = await r.zrange(ptr, 0, -1, withscores=True)
            if items:
                return int(sum(float(s) for _, s in items))
        except Exception:
            continue
    return 0

# ---------------------------------------------------------------------------
# Device store
# ---------------------------------------------------------------------------

class DeviceStore:
    def __init__(self, r):
        self.r = r
        self._devices = None
        self._ts = 0.0
        self._lock = asyncio.Lock()
        self._prev = {}

    async def load(self, force=False):
        t = now_ts()
        if not force and self._devices is not None and (t - self._ts) < CACHE_TTL:
            return self._devices
        async with self._lock:
            t = now_ts()
            if not force and self._devices is not None and (t - self._ts) < CACHE_TTL:
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
                policy = parse_policy_obj(data)
                name = data.get("name") or data.get("bname") or data.get("dhcpName") or mac
                ip = data.get("ipv4Addr") or data.get("ipv4") or data.get("ip") or ""
                vendor = data.get("macVendor") or data.get("vendor") or ""
                last = float(data.get("lastActiveTimestamp") or data.get("lastActive") or 0)
                online = (t - last) < 600 if last else False
                tags = parse_tags(data, policy)
                group = ", ".join(tags) if tags else (data.get("groupName") or "Ungrouped")
                monitor = _as_bool(policy.get("monitor"))
                paused = monitor is False
                emergency = _as_bool(policy.get("emergencyAccess")) is True or _as_bool(policy.get("acl")) is False

                down_total = timed["download"].get(mac, 0)
                up_total = timed["upload"].get(mac, 0)
                down_is = down_total > 0
                up_is = up_total > 0
                if not down_is:
                    down_total = await flow_fallback(self.r, mac_raw, "download")
                if not up_is:
                    up_total = await flow_fallback(self.r, mac_raw, "upload")
                down_rate = down_total / 60.0 if down_is else 0.0
                up_rate = up_total / 60.0 if up_is else 0.0
                prev = self._prev.get(mac)
                if prev and not (down_is or up_is):
                    dts = max(t - prev[0], 0.001)
                    if down_total >= prev[1]:
                        down_rate = (down_total - prev[1]) / dts
                    if up_total >= prev[2]:
                        up_rate = (up_total - prev[2]) / dts
                self._prev[mac] = (t, down_total, up_total)
                is_rate = down_is or up_is or prev is not None
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

                bool_flags = {}
                for k in KNOWN_HOST_BOOLS:
                    if k in policy:
                        bool_flags[k] = _as_bool(policy.get(k))
                for k, v in policy.items():
                    if k in bool_flags:
                        continue
                    b = _as_bool(v)
                    if b is not None and not isinstance(v, (dict, list)):
                        bool_flags[k] = b

                devices.append({
                    "mac": mac, "name": name, "ip": ip, "vendor": vendor,
                    "online": online, "paused": paused, "emergency": emergency,
                    "group": group, "tags": tags, "status": status,
                    "last_active_human": human_ts(last) if last else "—",
                    "down_h": human_bytes(down_show, is_rate=is_rate),
                    "up_h": human_bytes(up_show, is_rate=is_rate),
                    "down_total_h": human_bytes(down_total),
                    "up_total_h": human_bytes(up_total),
                    "policy": policy, "bool_flags": bool_flags,
                    "raw_fields": sorted(data.items()),
                    "dhcp_name": data.get("dhcpName") or "",
                    "static_ip": data.get("staticIP") or data.get("ipReservation") or "",
                    "local_domain": data.get("localDomain") or "",
                    "type": data.get("type") or data.get("detect") or "",
                })
            devices.sort(key=lambda d: (not d["online"], d["name"].lower()))
            self._devices = devices
            self._ts = now_ts()
            return devices

    async def get(self, mac: str):
        mac = mac.upper()
        for d in await self.load():
            if d["mac"] == mac:
                return d
        return None

# ---------------------------------------------------------------------------
# Rules / alarms / groups loaders
# ---------------------------------------------------------------------------

async def load_rules(r):
    rules = []
    for key in await scan_keys(r, "policy:*"):
        if key in ("policy:id", "policy_active"):
            continue
        try:
            data = await r.hgetall(key)
        except Exception:
            continue
        if not data:
            continue
        pid = str(data.get("pid") or key.split(":", 1)[-1])
        enabled = str(data.get("enabled", "1")) in ("1", "true", "True")
        disabled = str(data.get("disabled", "0")) in ("1", "true", "True")
        is_enabled = enabled and not disabled
        rules.append({
            "key": key, "pid": pid,
            "type": data.get("type") or "",
            "target": data.get("target") or "",
            "action": data.get("action") or "",
            "direction": data.get("direction") or "",
            "notes": data.get("notes") or data.get("note") or "",
            "timestamp": data.get("timestamp") or "",
            "expire": data.get("expire") or "",
            "cronTime": data.get("cronTime") or "",
            "duration": data.get("duration") or "",
            "tag": data.get("tag") or "",
            "scope": data.get("scope") or "",
            "enabled": is_enabled,
            "raw": data,
        })
    rules.sort(key=lambda x: (not x["enabled"], x["type"], x["target"]))
    return rules

async def load_alarms(r, limit=200):
    alarms = []
    # Common patterns: alarm:{id} hash, or active alarm zsets
    keys = await scan_keys(r, "alarm:*")
    for key in keys:
        if key in ("alarm:id",):
            continue
        try:
            t = await r.type(key)
        except Exception:
            continue
        data = {}
        if t == "hash":
            data = await r.hgetall(key)
        elif t == "string":
            data = parse_json(await r.get(key), {})
            if not isinstance(data, dict):
                data = {"raw": data}
        else:
            continue
        if not data:
            continue
        aid = str(data.get("aid") or data.get("id") or key.split(":", 1)[-1])
        alarms.append({
            "id": aid,
            "type": data.get("type") or data.get("alarmType") or "",
            "device": data.get("device") or data.get("p.device.mac") or data.get("mac") or "",
            "message": data.get("message") or data.get("msg") or data.get("p.dest.name") or json.dumps(data)[:120],
            "timestamp": data.get("timestamp") or data.get("ts") or data.get("alarmTimestamp") or "",
            "state": data.get("state") or data.get("alarmState") or "",
            "raw": data,
            "key": key,
        })
    alarms.sort(key=lambda a: float(a["timestamp"] or 0), reverse=True)
    return alarms[:limit]

async def load_groups(r):
    groups = []
    # tag:UID or group-like keys
    for key in await scan_keys(r, "tag:*"):
        try:
            t = await r.type(key)
            if t == "hash":
                data = await r.hgetall(key)
            elif t == "string":
                data = parse_json(await r.get(key), {"value": await r.get(key)})
            else:
                continue
        except Exception:
            continue
        gid = key.split(":", 1)[-1]
        name = data.get("name") or data.get("uid") or gid
        groups.append({"id": gid, "key": key, "name": name, "raw": data})
    # Also collect unique tags from hosts
    return groups

async def load_feature_flags(r):
    flags = {}
    # common feature keys
    for pattern in ("feature:*", "sys:features", "features"):
        if pattern.endswith("*"):
            for key in await scan_keys(r, pattern):
                try:
                    t = await r.type(key)
                    if t == "string":
                        flags[key] = await r.get(key)
                    elif t == "hash":
                        flags[key] = await r.hgetall(key)
                except Exception:
                    pass
        else:
            if await r.exists(pattern):
                t = await r.type(pattern)
                if t == "string":
                    flags[pattern] = await r.get(pattern)
                elif t == "hash":
                    flags[pattern] = await r.hgetall(pattern)
    return flags

def read_text_file(path: str, max_bytes=200_000):
    p = Path(path)
    if not p.is_file():
        return None
    try:
        return p.read_text(errors="replace")[:max_bytes]
    except Exception as e:
        return f"<error reading {path}: {e}>"

def list_dir(path: str, limit=80):
    p = Path(path)
    if not p.is_dir():
        return []
    try:
        return sorted([x.name for x in p.iterdir()])[:limit]
    except Exception:
        return []

# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------

async def set_host_policy_bool(r, mac, field, enabled):
    key = await get_host_key(r, mac)
    if not key:
        return False
    data = await r.hgetall(key)
    policy = parse_policy_obj(data)
    policy[field] = bool(enabled)
    if field == "emergencyAccess":
        policy["acl"] = not bool(enabled)
    await r.hset(key, "policy", json.dumps(policy))
    await publish_host_policy(r, mac, policy)
    return True

async def set_host_tags(r, mac, tags_csv):
    key = await get_host_key(r, mac)
    if not key:
        return False
    tags = [t.strip() for t in tags_csv.split(",") if t.strip()]
    data = await r.hgetall(key)
    policy = parse_policy_obj(data)
    policy["tags"] = tags
    await r.hset(key, mapping={"policy": json.dumps(policy), "tags": json.dumps(tags)})
    await publish_host_policy(r, mac, policy)
    return True

async def rename_device(r, mac, name):
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

async def set_static_ip(r, mac, ip):
    key = await get_host_key(r, mac)
    if not key:
        return False
    mapping = {}
    if ip.strip():
        mapping["staticIP"] = ip.strip()
    else:
        # clear best-effort
        await r.hdel(key, "staticIP")
        return True
    await r.hset(key, mapping=mapping)
    try:
        await r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass
    return True

async def set_rule_enabled(r, pid, enabled):
    key = f"policy:{pid}"
    if not await r.exists(key):
        for k in await scan_keys(r, "policy:*"):
            data = await r.hgetall(k)
            if str(data.get("pid")) == str(pid):
                key = k
                break
        else:
            return False
    await r.hset(key, mapping={"enabled": "1" if enabled else "0", "disabled": "0" if enabled else "1"})
    if enabled:
        await r.zadd("policy_active", {str(pid): now_ts()})
    else:
        await r.zrem("policy_active", str(pid))
    await publish_policy_event(r, "Policy:PolicyUpdated", {"pid": str(pid), "enabled": enabled})
    return True

async def delete_rule(r, pid):
    key = f"policy:{pid}"
    if not await r.exists(key):
        for k in await scan_keys(r, "policy:*"):
            data = await r.hgetall(k)
            if str(data.get("pid")) == str(pid):
                key = k
                break
        else:
            return False
    await r.delete(key)
    await r.zrem("policy_active", str(pid))
    await publish_policy_event(r, "Policy:PolicyRemoved", {"pid": str(pid)})
    return True

async def create_rule(r, **fields):
    try:
        pid = int(await r.get("policy:id") or 0) + 1
    except Exception:
        pid = int(time.time())
    policy = {
        "pid": str(pid),
        "type": fields.get("type") or "mac",
        "target": fields.get("target") or "",
        "action": fields.get("action") or "block",
        "direction": fields.get("direction") or "outbound",
        "enabled": "1",
        "disabled": "0",
        "timestamp": str(int(time.time())),
        "expire": fields.get("expire") or "",
        "cronTime": fields.get("cronTime") or "",
        "duration": fields.get("duration") or "0",
        "tag": fields.get("tag") or "[]",
        "scope": fields.get("scope") or "[]",
        "notes": fields.get("notes") or f"Created via dashboard {datetime.now().isoformat()}",
    }
    await r.hset(f"policy:{pid}", mapping={k: str(v) for k, v in policy.items()})
    await r.zadd("policy_active", {str(pid): now_ts()})
    await r.set("policy:id", str(pid))
    await publish_policy_event(r, "Policy:PolicyAdded", policy)
    return str(pid)

async def block_mac(r, mac):
    return await create_rule(type="mac", target=mac.upper(), action="block", direction="outbound",
                             notes=f"Block MAC {mac.upper()}")

async def unblock_mac(r, mac):
    n = 0
    for rule in await load_rules(r):
        if rule["type"] == "mac" and rule["target"].upper() == mac.upper() and rule["action"] == "block":
            if await delete_rule(r, rule["pid"]):
                n += 1
    return n

async def delete_alarm(r, key_or_id):
    key = key_or_id if str(key_or_id).startswith("alarm:") else f"alarm:{key_or_id}"
    if await r.exists(key):
        await r.delete(key)
        try:
            await r.publish("Alarm:AlarmRemoved", json.dumps({"id": key_or_id}))
        except Exception:
            pass
        return True
    # try by scanning aid
    for k in await scan_keys(r, "alarm:*"):
        data = await r.hgetall(k)
        if str(data.get("aid") or data.get("id")) == str(key_or_id):
            await r.delete(k)
            return True
    return False

async def set_feature_flag(r, key, value):
    # only allow feature:* keys
    if not key.startswith("feature:"):
        return False
    await r.set(key, value)
    try:
        await r.publish("Config:Updated", json.dumps({"key": key, "value": value}))
    except Exception:
        pass
    return True

# ---------------------------------------------------------------------------
# HTML shell
# ---------------------------------------------------------------------------

CSS = """
:root { --bg:#0d0f14; --card:#161a22; --line:#2a3140; --text:#e8eef9; --muted:#8b9bb8; --acc:#6aa6ff; --ok:#34d399; --bad:#f87171; }
* { box-sizing:border-box; }
body { margin:0; font-family:system-ui,sans-serif; background:var(--bg); color:var(--text); }
a { color:var(--acc); text-decoration:none; }
.top { position:sticky; top:0; z-index:10; background:#0b0d12ee; border-bottom:1px solid var(--line); backdrop-filter: blur(6px); }
.top-inner { max-width:1400px; margin:0 auto; padding:10px 14px; }
.tabs { display:flex; flex-wrap:wrap; gap:6px; }
.tabs a { padding:6px 10px; border:1px solid var(--line); border-radius:8px; color:var(--muted); background:#12151c; }
.tabs a.active { color:var(--text); border-color:#3b4a66; background:#1a2230; }
.wrap { max-width:1400px; margin:0 auto; padding:16px 14px 48px; }
h1 { margin:0 0 8px; font-size:22px; }
h2 { margin:18px 0 8px; font-size:16px; }
.meta { color:var(--muted); font-size:13px; margin-bottom:10px; }
.msg { background:#143; color:#cfc; padding:8px 10px; border-radius:8px; margin:10px 0; }
.err { background:#411; color:#fcc; padding:8px 10px; border-radius:8px; margin:10px 0; }
table { border-collapse:collapse; width:100%; font-size:13px; margin:10px 0; }
th, td { border:1px solid var(--line); padding:6px 8px; text-align:left; vertical-align:top; }
th { background:#141821; }
tr:nth-child(even) { background:#12161e; }
.offline { color:#888; } .paused,.emergency { color:var(--bad); } .online { color:var(--ok); }
form.inline { display:inline; margin-right:4px; }
button, input[type=submit] { background:#1c2330; color:var(--text); border:1px solid #3a455c; padding:5px 9px; border-radius:6px; cursor:pointer; }
button:hover { background:#263142; }
input[type=text], input[type=password], input[type=number], select, textarea {
  background:#12161e; color:var(--text); border:1px solid #3a455c; padding:5px 8px; border-radius:6px;
}
.toolbar { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:10px 0; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(220px,1fr)); gap:8px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:10px; }
.flag b { display:block; margin-bottom:4px; }
pre { white-space:pre-wrap; word-break:break-all; font-size:12px; background:#0b0e14; padding:10px; border-radius:8px; border:1px solid var(--line); }
.note { color:var(--muted); font-size:12px; }
.warn { color:#fbbf24; }
"""

def flash(request):
    msg = request.rel_url.query.get("msg")
    err = request.rel_url.query.get("err")
    html = ""
    if msg:
        html += f'<div class="msg">{escape(msg)}</div>'
    if err:
        html += f'<div class="err">{escape(err)}</div>'
    return html

def page(request, title, body, active_path="/"):
    tabs = []
    for href, label in TABS:
        cls = "active" if href == active_path else ""
        tabs.append(f'<a class="{cls}" href="{href}">{label}</a>')
    auth = "open" if not UI_PASSWORD else "password protected"
    return f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} · Firewalla</title>
<style>{CSS}</style>
</head><body>
<div class="top"><div class="top-inner">
  <div class="tabs">{''.join(tabs)}</div>
</div></div>
<div class="wrap">
{body}
<p class="note">Local Redis dashboard · auth: {auth} · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</div>
</body></html>"""

def redirect_msg(url, msg=None, err=None):
    parts = []
    if msg:
        parts.append("msg=" + quote(msg))
    if err:
        parts.append("err=" + quote(err))
    if not parts:
        raise web.HTTPFound(url)
    sep = "&" if "?" in url else "?"
    raise web.HTTPFound(url + sep + "&".join(parts))

@web.middleware
async def auth_middleware(request, handler):
    if request.path in ("/login", "/action/login") or request.path.startswith("/static"):
        return await handler(request)
    if not check_auth(request):
        raise web.HTTPFound("/login")
    return await handler(request)

# ---------------------------------------------------------------------------
# Routes: auth
# ---------------------------------------------------------------------------

async def login_page(request):
    if not UI_PASSWORD:
        raise web.HTTPFound("/")
    body = f"""
    <h1>Login</h1>
    {flash(request)}
    <form method="post" action="/action/login">
      <p><input type="password" name="password" placeholder="Password" autofocus></p>
      <button type="submit">Sign in</button>
    </form>
    """
    # minimal page without tabs requiring auth
    return web.Response(text=f"<!DOCTYPE html><html><head><meta charset=utf-8><title>Login</title><style>{CSS}</style></head><body><div class=wrap>{body}</div></body></html>", content_type="text/html")

async def login_action(request):
    form = await request.post()
    if (form.get("password") or "") == UI_PASSWORD:
        exp = int(time.time()) + SESSION_TTL
        tok = sign_session(exp)
        resp = web.HTTPFound("/")
        resp.set_cookie("fw_session", tok, max_age=SESSION_TTL, httponly=True, samesite="Lax")
        raise resp
    redirect_msg("/login", err="Bad password")

async def logout_action(request):
    resp = web.HTTPFound("/login" if UI_PASSWORD else "/")
    resp.del_cookie("fw_session")
    raise resp

# ---------------------------------------------------------------------------
# Devices
# ---------------------------------------------------------------------------

async def index(request):
    store: DeviceStore = request.app["store"]
    devices = await store.load()
    q = (request.rel_url.query.get("q") or "").strip().lower()
    if q:
        devices = [d for d in devices if q in d["name"].lower() or q in d["mac"].lower() or q in (d["ip"] or "").lower()]
    rows = []
    for d in devices:
        cls = d["status"].lower()
        pause = "unpause" if d["paused"] else "pause"
        plab = "Unpause" if d["paused"] else "Pause"
        rows.append(
            f"<tr class='{cls}'>"
            f"<td><a href='/device/{d['mac']}'>{escape(d['name'])}</a></td>"
            f"<td>{escape(d['ip'] or '—')}</td><td>{d['mac']}</td><td>{d['status']}</td>"
            f"<td>{escape(d['group'])}</td><td>{d['down_h']}</td><td>{d['up_h']}</td><td>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=mac value='{d['mac']}'>"
            f"<input type=hidden name=action value='{pause}'><input type=hidden name=next value='/'>"
            f"<button>{plab}</button></form>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=mac value='{d['mac']}'>"
            f"<input type=hidden name=action value='block_mac'><input type=hidden name=next value='/'>"
            f"<button>Block</button></form>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=mac value='{d['mac']}'>"
            f"<input type=hidden name=action value='unblock_mac'><input type=hidden name=next value='/'>"
            f"<button>Unblock</button></form>"
            f"</td></tr>"
        )
    body = f"""
    <h1>Devices</h1>
    <div class="meta">{len(devices)} shown · cache ≤ {CACHE_TTL}s</div>
    {flash(request)}
    <div class="toolbar">
      <form method="get" action="/"><input type="text" name="q" value="{escape(q)}" placeholder="Filter name/ip/mac"> <button>Filter</button></form>
      <form class="inline" method="post" action="/action"><input type=hidden name=action value=refresh><input type=hidden name=next value="/"><button>Force refresh</button></form>
    </div>
    <table>
      <tr><th>Name</th><th>IP</th><th>MAC</th><th>Status</th><th>Group/Tags</th><th>Down</th><th>Up</th><th>Actions</th></tr>
      {''.join(rows)}
    </table>
    """
    return web.Response(text=page(request, "Devices", body, "/"), content_type="text/html")

async def device_page(request):
    store: DeviceStore = request.app["store"]
    mac = request.match_info["mac"].upper()
    d = await store.get(mac)
    if not d:
        await store.load(force=True)
        d = await store.get(mac)
    if not d:
        return web.Response(text="Not found", status=404)

    flags = []
    seen = set()
    for field, val in sorted(d["bool_flags"].items()):
        seen.add(field)
        target = "false" if val else "true"
        label = "OFF" if val else "ON"
        flags.append(
            f"<div class='card flag'><b>{escape(field)}</b>{val}"
            f"<div style='margin-top:6px'><form class='inline' method='post' action='/action'>"
            f"<input type=hidden name=mac value='{d['mac']}'><input type=hidden name=action value=set_flag>"
            f"<input type=hidden name=field value='{escape(field)}'><input type=hidden name=value value='{target}'>"
            f"<input type=hidden name=next value='/device/{d['mac']}'><button>Set {label}</button></form></div></div>"
        )
    for field in KNOWN_HOST_BOOLS:
        if field in seen:
            continue
        flags.append(
            f"<div class='card flag'><b>{escape(field)}</b><i>unset</i><div style='margin-top:6px'>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=mac value='{d['mac']}'>"
            f"<input type=hidden name=action value=set_flag><input type=hidden name=field value='{escape(field)}'>"
            f"<input type=hidden name=value value=true><input type=hidden name=next value='/device/{d['mac']}'>"
            f"<button>ON</button></form> "
            f"<form class='inline' method='post' action='/action'><input type=hidden name=mac value='{d['mac']}'>"
            f"<input type=hidden name=action value=set_flag><input type=hidden name=field value='{escape(field)}'>"
            f"<input type=hidden name=value value=false><input type=hidden name=next value='/device/{d['mac']}'>"
            f"<button>OFF</button></form></div></div>"
        )

    raw_rows = "".join(f"<tr><th>{escape(str(k))}</th><td><pre>{escape(str(v))}</pre></td></tr>" for k, v in d["raw_fields"])
    body = f"""
    <p><a href="/">← Devices</a></p>
    <h1>{escape(d['name'])}</h1>
    <div class="meta">{d['mac']} · {d['status']}</div>
    {flash(request)}
    <h2>Quick actions</h2>
    <div class="toolbar">
      <form class="inline" method="post" action="/action">
        <input type=hidden name=mac value="{d['mac']}">
        <input type=hidden name=action value="{'unpause' if d['paused'] else 'pause'}">
        <input type=hidden name=next value="/device/{d['mac']}">
        <button>{"Unpause" if d['paused'] else "Pause"} monitoring</button>
      </form>
      <form class="inline" method="post" action="/action">
        <input type=hidden name=mac value="{d['mac']}"><input type=hidden name=action value=block_mac>
        <input type=hidden name=next value="/device/{d['mac']}"><button>Block internet</button>
      </form>
      <form class="inline" method="post" action="/action">
        <input type=hidden name=mac value="{d['mac']}"><input type=hidden name=action value=unblock_mac>
        <input type=hidden name=next value="/device/{d['mac']}"><button>Unblock</button>
      </form>
    </div>
    <h2>Rename</h2>
    <form method="post" action="/action">
      <input type=hidden name=mac value="{d['mac']}"><input type=hidden name=action value=rename>
      <input type=hidden name=next value="/device/{d['mac']}">
      <input type=text name=name value="{escape(d['name'])}" maxlength=32> <button>Save</button>
    </form>
    <h2>Tags / groups (string tags)</h2>
    <form method="post" action="/action">
      <input type=hidden name=mac value="{d['mac']}"><input type=hidden name=action value=set_tags>
      <input type=hidden name=next value="/device/{d['mac']}">
      <input type=text name=tags value="{escape(', '.join(d['tags']))}" style="width:min(480px,100%)"> <button>Save tags</button>
    </form>
    <h2>Static IP reservation (best-effort host field)</h2>
    <form method="post" action="/action">
      <input type=hidden name=mac value="{d['mac']}"><input type=hidden name=action value=set_static_ip>
      <input type=hidden name=next value="/device/{d['mac']}">
      <input type=text name=ip value="{escape(str(d.get('static_ip') or ''))}" placeholder="192.168.x.x"> <button>Save</button>
    </form>
    <h2>Host policy flags</h2>
    <div class="grid">{''.join(flags)}</div>
    <h2>Summary</h2>
    <table>
      <tr><th>IP</th><td>{escape(d['ip'] or '—')}</td></tr>
      <tr><th>Vendor</th><td>{escape(d['vendor'] or '—')}</td></tr>
      <tr><th>Type</th><td>{escape(str(d.get('type') or '—'))}</td></tr>
      <tr><th>Last active</th><td>{d['last_active_human']}</td></tr>
      <tr><th>Down / Up</th><td>{d['down_h']} / {d['up_h']}</td></tr>
      <tr><th>Totals</th><td>{d['down_total_h']} / {d['up_total_h']}</td></tr>
    </table>
    <h2>Policy JSON</h2>
    <pre>{escape(json.dumps(d['policy'], indent=2, sort_keys=True))}</pre>
    <h2>Raw Redis fields</h2>
    <table>{raw_rows}</table>
    """
    return web.Response(text=page(request, d["name"], body, "/"), content_type="text/html")

# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

async def rules_page(request):
    r = request.app["redis"]
    rules = await load_rules(r)
    q = (request.rel_url.query.get("q") or "").strip().lower()
    if q:
        rules = [x for x in rules if q in json.dumps(x).lower()]
    rows = []
    for rule in rules:
        toggle = "disable_rule" if rule["enabled"] else "enable_rule"
        tlab = "Disable" if rule["enabled"] else "Enable"
        rows.append(
            f"<tr><td>{escape(rule['pid'])}</td><td>{escape(rule['type'])}</td><td>{escape(rule['action'])}</td>"
            f"<td>{escape(rule['target'])}</td><td>{escape(rule['direction'])}</td>"
            f"<td>{'yes' if rule['enabled'] else 'no'}</td>"
            f"<td>{escape(rule['cronTime'] or '—')}</td><td>{escape(rule['expire'] or '—')}</td>"
            f"<td>{escape((rule['scope'] or '')[:40])}</td><td>{escape(rule['notes'][:60])}</td><td>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=pid value='{escape(rule['pid'])}'>"
            f"<input type=hidden name=action value='{toggle}'><input type=hidden name=next value='/rules'><button>{tlab}</button></form>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=pid value='{escape(rule['pid'])}'>"
            f"<input type=hidden name=action value=delete_rule><input type=hidden name=next value='/rules'><button>Delete</button></form>"
            f"</td></tr>"
        )
    body = f"""
    <h1>Rules</h1>
    <div class="meta">{len(rules)} policies</div>
    {flash(request)}
    <div class="toolbar">
      <a href="/rules/new"><button type="button">New rule</button></a>
      <form method="get" action="/rules"><input name=q value="{escape(q)}" placeholder="Filter"><button>Filter</button></form>
    </div>
    <table>
      <tr><th>PID</th><th>Type</th><th>Action</th><th>Target</th><th>Dir</th><th>On</th>
      <th>Schedule</th><th>Expire</th><th>Scope</th><th>Notes</th><th>Actions</th></tr>
      {''.join(rows)}
    </table>
    <p class="note">Schedule/scope fields are stored on the policy object. Complex category engines may still require the official app.</p>
    """
    return web.Response(text=page(request, "Rules", body, "/rules"), content_type="text/html")

async def rules_new_page(request):
    body = f"""
    <h1>New rule</h1>
    {flash(request)}
    <form method="post" action="/action">
      <input type=hidden name=action value=create_rule>
      <input type=hidden name=next value="/rules">
      <table>
        <tr><th>Type</th><td><select name=type>
          <option>mac</option><option>ip</option><option>dns</option><option>domain</option>
          <option>category</option><option>net</option><option>devicePort</option><option>remotePort</option>
        </select></td></tr>
        <tr><th>Target</th><td><input name=target required style="width:min(420px,100%)"></td></tr>
        <tr><th>Action</th><td><select name=rule_action><option>block</option><option>allow</option><option>alarm</option></select></td></tr>
        <tr><th>Direction</th><td><select name=direction><option>outbound</option><option>inbound</option><option>bidirection</option></select></td></tr>
        <tr><th>Schedule cronTime</th><td><input name=cronTime placeholder='optional'></td></tr>
        <tr><th>Expire (epoch/sec)</th><td><input name=expire placeholder='optional'></td></tr>
        <tr><th>Duration</th><td><input name=duration value="0"></td></tr>
        <tr><th>Scope JSON</th><td><input name=scope value="[]" style="width:min(420px,100%)"></td></tr>
        <tr><th>Tag JSON</th><td><input name=tag value="[]" style="width:min(420px,100%)"></td></tr>
        <tr><th>Notes</th><td><input name=notes style="width:min(420px,100%)"></td></tr>
      </table>
      <button>Create</button>
    </form>
    """
    return web.Response(text=page(request, "New rule", body, "/rules"), content_type="text/html")

# ---------------------------------------------------------------------------
# Alarms
# ---------------------------------------------------------------------------

async def alarms_page(request):
    r = request.app["redis"]
    alarms = await load_alarms(r)
    rows = []
    for a in alarms:
        rows.append(
            f"<tr><td>{escape(a['id'])}</td><td>{escape(a['type'])}</td><td>{escape(a['device'])}</td>"
            f"<td>{escape(str(a['message'])[:160])}</td><td>{human_ts(a['timestamp'])}</td><td>{escape(str(a['state']))}</td><td>"
            f"<form class='inline' method='post' action='/action'><input type=hidden name=alarm_key value='{escape(a['key'])}'>"
            f"<input type=hidden name=action value=delete_alarm><input type=hidden name=next value='/alarms'>"
            f"<button>Delete</button></form></td></tr>"
        )
    body = f"""
    <h1>Alarms</h1>
    <div class="meta">{len(alarms)} loaded from Redis alarm:* keys</div>
    {flash(request)}
    <table>
      <tr><th>ID</th><th>Type</th><th>Device</th><th>Message</th><th>Time</th><th>State</th><th></th></tr>
      {''.join(rows) if rows else '<tr><td colspan=7>No alarm keys found (firmware may store alarms differently).</td></tr>'}
    </table>
    <p class="note">Mute/exception flows vary by version; delete removes the Redis alarm object when present.</p>
    """
    return web.Response(text=page(request, "Alarms", body, "/alarms"), content_type="text/html")

# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------

async def groups_page(request):
    r = request.app["redis"]
    store: DeviceStore = request.app["store"]
    groups = await load_groups(r)
    devices = await store.load()
    tag_map = {}
    for d in devices:
        for t in d["tags"]:
            tag_map.setdefault(t, []).append(d["name"])

    grow = "".join(
        f"<tr><td>{escape(g['id'])}</td><td>{escape(g['name'])}</td><td><pre>{escape(json.dumps(g['raw'], indent=2)[:500])}</pre></td></tr>"
        for g in groups
    )
    trows = "".join(
        f"<tr><td>{escape(t)}</td><td>{escape(', '.join(names[:20]))}</td></tr>" for t, names in sorted(tag_map.items())
    )
    body = f"""
    <h1>Groups / Tags</h1>
    {flash(request)}
    <h2>Redis tag:* objects</h2>
    <table><tr><th>ID</th><th>Name</th><th>Raw</th></tr>
    {grow if grow else '<tr><td colspan=3>No tag:* keys found.</td></tr>'}</table>
    <h2>Tags seen on devices</h2>
    <table><tr><th>Tag</th><th>Devices</th></tr>
    {trows if trows else '<tr><td colspan=2>No tags on hosts.</td></tr>'}</table>
    <p class="note">Edit membership from device page Tags field. Full Firewalla group UX may use additional keys.</p>
    """
    return web.Response(text=page(request, "Groups", body, "/groups"), content_type="text/html")

# ---------------------------------------------------------------------------
# Network / VPN / Protect / System / Flows
# ---------------------------------------------------------------------------

async def network_page(request):
    snippets = []
    candidates = [
        "/home/pi/.firewalla/config",
        "/home/pi/.router/config",
        "/home/pi/.firewalla/run",
        "/etc/firewalla",
    ]
    for c in candidates:
        files = list_dir(c)
        snippets.append(f"<div class='card'><b>{escape(c)}</b><pre>{escape(chr(10).join(files) if files else '(missing or unreadable)')}</pre></div>")

    # redis network-ish keys
    r = request.app["redis"]
    net_keys = []
    for pat in ("sys:network*", "network*", "mode", "sys:mode", "firerouter*"):
        net_keys.extend(await scan_keys(r, pat))
    net_keys = sorted(set(net_keys))[:80]
    kv = []
    for k in net_keys:
        try:
            t = await r.type(k)
            if t == "string":
                v = await r.get(k)
            elif t == "hash":
                v = json.dumps(await r.hgetall(k))[:300]
            else:
                v = f"<{t}>"
        except Exception as e:
            v = str(e)
        kv.append(f"<tr><th>{escape(k)}</th><td><pre>{escape(str(v)[:500])}</pre></td></tr>")

    body = f"""
    <h1>Network</h1>
    {flash(request)}
    <p class="warn">WAN/LAN/VLAN/DHCP/port-forward live edits usually go through FireRouter. This tab is inventory + Redis discovery.</p>
    <h2>Config directories</h2>
    <div class="grid">{''.join(snippets)}</div>
    <h2>Redis network-related keys</h2>
    <table>{''.join(kv) if kv else '<tr><td>None matched.</td></tr>'}</table>
    """
    return web.Response(text=page(request, "Network", body, "/network"), content_type="text/html")

async def vpn_page(request):
    r = request.app["redis"]
    keys = []
    for pat in ("vpn*", "ocvpn*", "wireguard*", "wg*", "sys:vpn*"):
        keys.extend(await scan_keys(r, pat))
    keys = sorted(set(keys))[:100]
    rows = []
    for k in keys:
        try:
            t = await r.type(k)
            if t == "string":
                v = await r.get(k)
            elif t == "hash":
                v = json.dumps(await r.hgetall(k))[:400]
            else:
                v = f"<{t}>"
        except Exception as e:
            v = str(e)
        rows.append(f"<tr><th>{escape(k)}</th><td><pre>{escape(str(v)[:600])}</pre></td></tr>")
    conf_dirs = []
    for c in ["/home/pi/.firewalla/run/ovpn", "/home/pi/.firewalla/run/wireguard", "/etc/openvpn"]:
        conf_dirs.append(f"<div class='card'><b>{escape(c)}</b><pre>{escape(chr(10).join(list_dir(c)) or '(none)')}</pre></div>")
    body = f"""
    <h1>VPN</h1>
    {flash(request)}
    <p class="warn">Full VPN create/connect wizards are not reimplemented. This surfaces Redis + config presence.</p>
    <h2>Config dirs</h2><div class="grid">{''.join(conf_dirs)}</div>
    <h2>Redis VPN-related keys</h2>
    <table>{''.join(rows) if rows else '<tr><td>No VPN keys found.</td></tr>'}</table>
    """
    return web.Response(text=page(request, "VPN", body, "/vpn"), content_type="text/html")

async def protect_page(request):
    r = request.app["redis"]
    flags = await load_feature_flags(r)
    # host aggregate feature counts
    store: DeviceStore = request.app["store"]
    devices = await store.load()
    counts = {"adblock_on": 0, "family_on": 0, "dox_on": 0, "monitor_off": 0}
    for d in devices:
        bf = d["bool_flags"]
        if bf.get("adblock") is True:
            counts["adblock_on"] += 1
        if bf.get("family") is True:
            counts["family_on"] += 1
        if bf.get("dox") is True:
            counts["dox_on"] += 1
        if d["paused"]:
            counts["monitor_off"] += 1

    flag_rows = []
    for k, v in sorted(flags.items(), key=lambda x: x[0]):
        if isinstance(v, dict):
            val = json.dumps(v)[:200]
        else:
            val = str(v)
        if k.startswith("feature:"):
            flag_rows.append(
                f"<tr><td>{escape(k)}</td><td><pre>{escape(val)}</pre></td><td>"
                f"<form class='inline' method='post' action='/action'>"
                f"<input type=hidden name=action value=set_feature><input type=hidden name=key value='{escape(k)}'>"
                f"<input type=hidden name=value value='1'><input type=hidden name=next value='/protect'><button>1</button></form>"
                f"<form class='inline' method='post' action='/action'>"
                f"<input type=hidden name=action value=set_feature><input type=hidden name=key value='{escape(k)}'>"
                f"<input type=hidden name=value value='0'><input type=hidden name=next value='/protect'><button>0</button></form>"
                f"</td></tr>"
            )
        else:
            flag_rows.append(f"<tr><td>{escape(k)}</td><td><pre>{escape(val)}</pre></td><td></td></tr>")

    body = f"""
    <h1>DNS / Protect</h1>
    {flash(request)}
    <div class="grid">
      <div class="card"><b>Devices adblock on</b><div>{counts['adblock_on']}</div></div>
      <div class="card"><b>Family on</b><div>{counts['family_on']}</div></div>
      <div class="card"><b>DoX/DoH flag on</b><div>{counts['dox_on']}</div></div>
      <div class="card"><b>Monitoring paused</b><div>{counts['monitor_off']}</div></div>
    </div>
    <h2>Feature flags (Redis)</h2>
    <table><tr><th>Key</th><th>Value</th><th>Set</th></tr>
    {''.join(flag_rows) if flag_rows else '<tr><td colspan=3>No feature keys found.</td></tr>'}
    </table>
    <p class="note">Per-device adblock/family/dox toggles are on each device page. Global DNS mode may live in FireRouter.</p>
    """
    return web.Response(text=page(request, "DNS/Protect", body, "/protect"), content_type="text/html")

async def system_page(request):
    r = request.app["redis"]
    info = {}
    for k in ("sys:status", "sys:mode", "mode", "sys:bone:url", "sys:version", "version"):
        if await r.exists(k):
            t = await r.type(k)
            info[k] = await r.get(k) if t == "string" else await r.hgetall(k)

    # process uptime rough
    uptime = read_text_file("/proc/uptime") or ""
    loadavg = read_text_file("/proc/loadavg") or ""
    uname = ""
    try:
        import platform
        uname = platform.platform()
    except Exception:
        pass

    rows = "".join(
        f"<tr><th>{escape(k)}</th><td><pre>{escape(str(v)[:800])}</pre></td></tr>" for k, v in info.items()
    )
    body = f"""
    <h1>System</h1>
    {flash(request)}
    <div class="grid">
      <div class="card"><b>Platform</b><pre>{escape(uname)}</pre></div>
      <div class="card"><b>/proc/uptime</b><pre>{escape(uptime.strip())}</pre></div>
      <div class="card"><b>loadavg</b><pre>{escape(loadavg.strip())}</pre></div>
    </div>
    <h2>Redis sys keys</h2>
    <table>{rows if rows else '<tr><td>None of the common sys keys present.</td></tr>'}</table>
    <h2>Dangerous actions</h2>
    <p class="warn">Reboot/shutdown execute host commands. Only available if process user can do it.</p>
    <div class="toolbar">
      <form method="post" action="/action" onsubmit="return confirm('Reboot the Firewalla box?');">
        <input type=hidden name=action value=reboot><input type=hidden name=next value="/system">
        <button>Reboot box</button>
      </form>
      <form method="post" action="/action" onsubmit="return confirm('Sync redis publish Config:Updated only');">
        <input type=hidden name=action value=config_ping><input type=hidden name=next value="/system">
        <button>Publish Config:Updated</button>
      </form>
    </div>
    <p class="note">Firmware upgrade channels / MSP link are not fully automated here.</p>
    """
    return web.Response(text=page(request, "System", body, "/system"), content_type="text/html")

async def flows_page(request):
    r = request.app["redis"]
    # show recent timedTraffic keys sample + lastsumflow pointers
    samples = (await scan_keys(r, "timedTraffic:*"))[:40]
    flows = (await scan_keys(r, "lastsumflow:*"))[:40]
    body = f"""
    <h1>Flows</h1>
    {flash(request)}
    <p class="note">Full flow browser needs large history datasets; this is a live index snapshot.</p>
    <h2>timedTraffic sample ({len(samples)} shown)</h2>
    <pre>{escape(chr(10).join(samples))}</pre>
    <h2>lastsumflow pointers ({len(flows)} shown)</h2>
    <pre>{escape(chr(10).join(flows))}</pre>
    """
    return web.Response(text=page(request, "Flows", body, "/flows"), content_type="text/html")

async def settings_page(request):
    body = f"""
    <h1>Settings</h1>
    {flash(request)}
    <div class="card">
      <p><b>Auth:</b> {'enabled' if UI_PASSWORD else 'disabled (set FIREWALLA_UI_PASSWORD)'}</p>
      <p><b>Redis:</b> {REDIS_HOST}:{REDIS_PORT}</p>
      <p><b>Cache TTL:</b> {CACHE_TTL}s</p>
      <p><b>Bind port:</b> {PORT}</p>
    </div>
    <div class="toolbar">
      <form method="post" action="/action"><input type=hidden name=action value=logout><button>Logout</button></form>
    </div>
    <h2>Environment</h2>
    <pre>FIREWALLA_UI_PASSWORD, FIREWALLA_UI_SECRET, REDIS_HOST, REDIS_PORT, PORT, CACHE_TTL, SESSION_TTL</pre>
    <h2>Coverage honesty</h2>
    <p class="note">This dashboard exposes on-box Redis controls + best-effort files. FireRouter schema writes, official VPN wizards, MSP cloud, and AP7 radio controllers are not fully reimplemented.</p>
    """
    return web.Response(text=page(request, "Settings", body, "/settings"), content_type="text/html")

# ---------------------------------------------------------------------------
# Action router
# ---------------------------------------------------------------------------

async def action(request):
    r = request.app["redis"]
    store: DeviceStore = request.app["store"]
    form = await request.post()
    act = (form.get("action") or "").strip().lower()
    next_url = form.get("next") or "/"
    mac = (form.get("mac") or "").strip().upper()
    pid = (form.get("pid") or "").strip()

    if act == "logout":
        return await logout_action(request)

    if act == "refresh":
        await store.load(force=True)
        redirect_msg(next_url, "Refreshed")

    if act == "pause":
        ok = await set_host_policy_bool(r, mac, "monitor", False)
        await store.load(force=True)
        redirect_msg(next_url, "Paused" if ok else None, None if ok else "Pause failed")

    if act == "unpause":
        ok = await set_host_policy_bool(r, mac, "monitor", True)
        await store.load(force=True)
        redirect_msg(next_url, "Unpaused" if ok else None, None if ok else "Unpause failed")

    if act == "set_flag":
        field = (form.get("field") or "").strip()
        value = (form.get("value") or "").strip().lower() == "true"
        if not field or re.search(r"\s", field):
            redirect_msg(next_url, err="Bad field")
        ok = await set_host_policy_bool(r, mac, field, value)
        await store.load(force=True)
        redirect_msg(next_url, f"{field}={value}" if ok else None, None if ok else "Failed")

    if act == "set_tags":
        ok = await set_host_tags(r, mac, form.get("tags") or "")
        await store.load(force=True)
        redirect_msg(next_url, "Tags saved" if ok else None, None if ok else "Tags failed")

    if act == "rename":
        ok = await rename_device(r, mac, form.get("name") or "")
        await store.load(force=True)
        redirect_msg(next_url, "Renamed" if ok else None, None if ok else "Rename failed")

    if act == "set_static_ip":
        ok = await set_static_ip(r, mac, form.get("ip") or "")
        await store.load(force=True)
        redirect_msg(next_url, "Static IP saved" if ok else None, None if ok else "Static IP failed")

    if act == "block_mac":
        new_pid = await block_mac(r, mac)
        await store.load(force=True)
        redirect_msg(next_url, f"Blocked pid {new_pid}")

    if act == "unblock_mac":
        n = await unblock_mac(r, mac)
        await store.load(force=True)
        redirect_msg(next_url, f"Removed {n} rules")

    if act == "enable_rule":
        ok = await set_rule_enabled(r, pid, True)
        redirect_msg(next_url, f"Enabled {pid}" if ok else None, None if ok else "Enable failed")

    if act == "disable_rule":
        ok = await set_rule_enabled(r, pid, False)
        redirect_msg(next_url, f"Disabled {pid}" if ok else None, None if ok else "Disable failed")

    if act == "delete_rule":
        ok = await delete_rule(r, pid)
        redirect_msg(next_url, f"Deleted {pid}" if ok else None, None if ok else "Delete failed")

    if act == "create_rule":
        new_pid = await create_rule(
            r,
            type=(form.get("type") or "mac").strip(),
            target=(form.get("target") or "").strip(),
            action=(form.get("rule_action") or "block").strip(),
            direction=(form.get("direction") or "outbound").strip(),
            cronTime=(form.get("cronTime") or "").strip(),
            expire=(form.get("expire") or "").strip(),
            duration=(form.get("duration") or "0").strip(),
            scope=(form.get("scope") or "[]").strip(),
            tag=(form.get("tag") or "[]").strip(),
            notes=(form.get("notes") or "").strip(),
        )
        redirect_msg("/rules", f"Created {new_pid}")

    if act == "delete_alarm":
        ok = await delete_alarm(r, form.get("alarm_key") or "")
        redirect_msg(next_url, "Alarm deleted" if ok else None, None if ok else "Alarm delete failed")

    if act == "set_feature":
        key = (form.get("key") or "").strip()
        value = (form.get("value") or "").strip()
        ok = await set_feature_flag(r, key, value)
        redirect_msg(next_url, f"Set {key}" if ok else None, None if ok else "Feature set denied/failed")

    if act == "config_ping":
        try:
            await r.publish("Config:Updated", json.dumps({"ts": int(time.time())}))
            redirect_msg(next_url, "Published Config:Updated")
        except Exception as e:
            redirect_msg(next_url, err=str(e))

    if act == "reboot":
        # best-effort; may fail without permission
        try:
            proc = await asyncio.create_subprocess_shell(
                "sudo /sbin/reboot",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await proc.communicate()
            redirect_msg(next_url, "Reboot requested")
        except Exception as e:
            redirect_msg(next_url, err=f"Reboot failed: {e}")

    redirect_msg(next_url, err=f"Unknown action {act}")

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

async def api_devices(request):
    devices = await request.app["store"].load()
    return web.json_response({"total": len(devices), "devices": [
        {k: d[k] for k in ("mac", "name", "ip", "status", "paused", "group", "down_h", "up_h", "bool_flags")}
        for d in devices
    ]})

async def api_rules(request):
    rules = await load_rules(request.app["redis"])
    return web.json_response({"total": len(rules), "rules": [
        {k: x[k] for k in ("pid", "type", "action", "target", "direction", "enabled", "cronTime", "expire", "notes")}
        for x in rules
    ]})

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

async def on_startup(app):
    app["redis"] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    app["store"] = DeviceStore(app["redis"])
    try:
        await app["store"].load(force=True)
    except Exception as e:
        print("cache warm failed:", e)

async def on_cleanup(app):
    r = app.get("redis")
    if r is not None:
        await r.aclose()

def main():
    app = web.Application(middlewares=[auth_middleware])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/login", login_page)
    app.router.add_post("/action/login", login_action)
    app.router.add_get("/", index)
    app.router.add_get("/device/{mac}", device_page)
    app.router.add_get("/rules", rules_page)
    app.router.add_get("/rules/new", rules_new_page)
    app.router.add_get("/alarms", alarms_page)
    app.router.add_get("/groups", groups_page)
    app.router.add_get("/network", network_page)
    app.router.add_get("/vpn", vpn_page)
    app.router.add_get("/protect", protect_page)
    app.router.add_get("/system", system_page)
    app.router.add_get("/flows", flows_page)
    app.router.add_get("/settings", settings_page)
    app.router.add_post("/action", action)
    app.router.add_get("/api/devices", api_devices)
    app.router.add_get("/api/rules", api_rules)
    print(f"Firewalla Full Dashboard on 0.0.0.0:{PORT} auth={'on' if UI_PASSWORD else 'off'}")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)

if __name__ == "__main__":
    main()
