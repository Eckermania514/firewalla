#!/usr/bin/env python3
"""
Firewalla Control Dashboard (single file)
- Live device grid with rates
- Pause / Unpause monitoring
- Emergency Access toggle (best-effort)
- Rename device
- Block / Unblock internet (creates/removes a simple block rule)
- Group view + device detail
- WebSocket live updates
- Designed to run on the Firewalla box itself (local Redis)
"""

from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from flask_sock import Sock
import redis
import json
import time
import threading
import os
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)
sock = Sock(app)

# Local Redis on the Firewalla box
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

ARC_LEN = 125.66
_prev = {}
_prev_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def split_human(n, is_rate=False):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0", "B/s" if is_rate else "B"
    units = ["B/s", "KB/s", "MB/s", "GB/s"] if is_rate else ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if abs(n) < 1024:
            return (f"{n:.1f}" if abs(n) < 10 else f"{n:.0f}"), unit
        n /= 1024
    return f"{n:.1f}", units[-1]

def meter_pct(bps, scale=10_000_000):
    try:
        return max(0.0, min(100.0, (float(bps) / scale) * 100))
    except (TypeError, ValueError):
        return 0.0

def parse_policy(data):
    raw = data.get("policy") or ""
    if not raw:
        return False, False, {}
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return False, False, {}
    monitor = obj.get("monitor")
    paused = monitor is False or str(monitor).lower() == "false"
    # Emergency access is often stored as acl=false or emergencyAccess=true depending on version
    emergency = (
        obj.get("emergencyAccess") is True
        or str(obj.get("emergencyAccess")).lower() == "true"
        or obj.get("acl") is False
        or str(obj.get("acl")).lower() == "false"
    )
    return paused, emergency, obj

def parse_tags(data):
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

def get_host_key(mac: str):
    for m in (mac, mac.upper(), mac.lower()):
        key = f"host:mac:{m}"
        if r.exists(key):
            return key
    return None

def latest_timed_bytes(mac, direction):
    for m in (mac, mac.upper(), mac.lower()):
        pattern = f"timedTraffic:{direction}:{m}:1minute:*"
        keys = r.keys(pattern)
        if not keys:
            continue
        best_key = max(keys, key=lambda k: int(k.rsplit(":", 1)[-1]))
        try:
            val = r.get(best_key)
            if val is not None:
                ts = int(best_key.rsplit(":", 1)[-1])
                return int(float(val)), ts
        except Exception:
            continue
    return 0, 0

def flow_total_bytes(mac, direction):
    timed, _ = latest_timed_bytes(mac, direction)
    if timed > 0:
        return timed, True
    for m in (mac, mac.upper(), mac.lower()):
        ptr = r.get(f"lastsumflow:{m}:{direction}")
        if not ptr:
            continue
        try:
            items = r.zrange(ptr, 0, -1, withscores=True)
            if items:
                return int(sum(float(score) for _, score in items)), False
        except Exception:
            continue
    return 0, False

def set_device_monitor(mac: str, enabled: bool) -> bool:
    """True = monitoring on (unpaused), False = monitoring off (paused)."""
    key = get_host_key(mac)
    if not key:
        return False
    data = r.hgetall(key)
    try:
        policy = json.loads(data.get("policy") or "{}")
    except Exception:
        policy = {}
    policy["monitor"] = bool(enabled)
    r.hset(key, "policy", json.dumps(policy))
    try:
        r.publish("TO.FirewallaAgent", json.dumps({
            "type": "Policy:Changed",
            "mac": mac.upper(),
            "policy": policy,
        }))
        r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass
    return True

def set_emergency_access(mac: str, enabled: bool) -> bool:
    """Best-effort emergency access toggle via policy.acl / emergencyAccess."""
    key = get_host_key(mac)
    if not key:
        return False
    data = r.hgetall(key)
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
    r.hset(key, "policy", json.dumps(policy))
    try:
        r.publish("TO.FirewallaAgent", json.dumps({
            "type": "Policy:Changed",
            "mac": mac.upper(),
            "policy": policy,
        }))
        r.publish("Host:Updated", json.dumps({"mac": mac.upper()}))
    except Exception:
        pass
    return True

def rename_device(mac: str, name: str) -> bool:
    key = get_host_key(mac)
    if not key or not name.strip():
        return False
    name = name.strip()[:32]
    r.hset(key, "name", name)
    try:
        r.publish("Host:Updated", json.dumps({"mac": mac.upper(), "name": name}))
    except Exception:
        pass
    return True

def block_internet(mac: str) -> bool:
    """Create a simple permanent block rule targeting this MAC (outbound)."""
    # Find next policy id
    try:
        pid = int(r.get("policy:id") or 0) + 1
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
        "tag": [],
        "scope": [],
        "notes": f"Blocked via Control Dashboard {datetime.now().isoformat()}",
    }
    r.hset(f"policy:{pid}", mapping={k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in policy.items()})
    r.zadd("policy_active", {str(pid): time.time()})
    r.set("policy:id", str(pid))
    try:
        r.publish("Policy:PolicyAdded", json.dumps(policy))
        r.publish("TO.FirewallaAgent", json.dumps({"type": "Policy:Changed"}))
    except Exception:
        pass
    return True

def unblock_internet(mac: str) -> int:
    """Remove block rules that target this exact MAC. Returns count removed."""
    removed = 0
    for key in r.keys("policy:*"):
        if key in ("policy:id", "policy_active"):
            continue
        try:
            data = r.hgetall(key)
            if data.get("type") == "mac" and data.get("target", "").upper() == mac.upper() and data.get("action") == "block":
                pid = data.get("pid") or key.split(":")[-1]
                r.delete(key)
                r.zrem("policy_active", pid)
                removed += 1
                try:
                    r.publish("Policy:PolicyRemoved", json.dumps({"pid": pid}))
                except Exception:
                    pass
        except Exception:
            continue
    if removed:
        try:
            r.publish("TO.FirewallaAgent", json.dumps({"type": "Policy:Changed"}))
        except Exception:
            pass
    return removed

def load_devices():
    devices = []
    now = time.time()
    total_rate = 0.0
    for key in r.keys("host:mac:*"):
        data = r.hgetall(key)
        if not data:
            continue
        mac_raw = key.replace("host:mac:", "", 1)
        mac = mac_raw.upper()
        name = data.get("name") or data.get("bname") or data.get("dhcpName") or mac
        ip = data.get("ipv4Addr") or data.get("ipv4") or data.get("ip") or ""
        vendor = data.get("macVendor") or data.get("vendor") or ""
        last = float(data.get("lastActiveTimestamp") or data.get("lastActive") or 0)
        online = (now - last) < 600 if last else False
        last_human = datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S") if last else "—"
        paused, emergency, policy_obj = parse_policy(data)
        tags = parse_tags(data)
        group = ", ".join(tags) if tags else (data.get("groupName") or data.get("group") or "Ungrouped")

        down_total, down_is_timed = flow_total_bytes(mac_raw, "download")
        up_total, up_is_timed = flow_total_bytes(mac_raw, "upload")
        down_rate = down_total / 60.0 if down_is_timed else 0.0
        up_rate = up_total / 60.0 if up_is_timed else 0.0

        with _prev_lock:
            prev = _prev.get(mac)
            if prev and not (down_is_timed or up_is_timed):
                dts = max(now - prev[0], 0.001)
                if down_total >= prev[1]:
                    down_rate = (down_total - prev[1]) / dts
                if up_total >= prev[2]:
                    up_rate = (up_total - prev[2]) / dts
            _prev[mac] = (now, down_total, up_total)

        is_rate = down_is_timed or up_is_timed or (prev is not None)
        down_show = down_rate if is_rate else float(down_total)
        up_show = up_rate if is_rate else float(up_total)

        if not online:
            glow, badge_class, status_label = "grey", "off", "Offline"
        elif paused:
            glow, badge_class, status_label = "red", "paused", "Paused"
        elif emergency:
            glow, badge_class, status_label = "red", "paused", "Emergency"
        else:
            glow, badge_class, status_label = "green", "on", "Online"

        dv, du = split_human(down_show, is_rate=is_rate)
        uv, uu = split_human(up_show, is_rate=is_rate)
        dth, _ = split_human(down_total, is_rate=False)
        uth, _ = split_human(up_total, is_rate=False)

        devices.append({
            "mac": mac,
            "name": name,
            "ip": ip,
            "vendor": vendor,
            "online": online,
            "paused": paused,
            "emergency": emergency,
            "group": group,
            "glow": glow,
            "badge_class": badge_class,
            "status_label": status_label,
            "last_active_human": last_human,
            "down_show": down_show,
            "up_show": up_show,
            "is_rate": is_rate,
            "down_total_h": dth,
            "up_total_h": uth,
            "download_val": dv,
            "download_unit": du,
            "upload_val": uv,
            "upload_unit": uu,
            "raw_fields": sorted(data.items()),
            "policy_obj": policy_obj,
        })
        total_rate += down_show + up_show

    for d in devices:
        t = d["down_show"] + d["up_show"]
        d["pct"] = round((t / total_rate * 100) if total_rate else 0, 1)
        scale = 5_000_000 if d["is_rate"] else 50_000_000
        d["down_pct"] = meter_pct(d["down_show"], scale)
        d["up_pct"] = meter_pct(d["up_show"], scale)
        d["down_arc"] = f"{d['down_pct'] / 100 * ARC_LEN:.2f}"
        d["up_arc"] = f"{d['up_pct'] / 100 * ARC_LEN:.2f}"

    devices.sort(key=lambda x: (not x["online"], x["name"].lower()))
    return devices

def group_devices(devices):
    groups = defaultdict(list)
    for d in devices:
        groups[d["group"] or "Ungrouped"].append(d)
    return dict(sorted(groups.items(), key=lambda kv: (kv[0] == "Ungrouped", kv[0].lower())))

def snapshot_payload():
    devices = load_devices()
    slim = [{
        "mac": d["mac"],
        "ip": d["ip"],
        "glow": d["glow"],
        "badge_class": d["badge_class"],
        "status_label": d["status_label"],
        "download_val": d["download_val"],
        "download_unit": d["download_unit"],
        "upload_val": d["upload_val"],
        "upload_unit": d["upload_unit"],
        "down_arc": d["down_arc"],
        "up_arc": d["up_arc"],
        "pct": d["pct"],
        "name": d["name"],
    } for d in devices]
    return {
        "type": "snapshot",
        "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total": len(devices),
        "devices": slim,
    }

# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

INDEX_TPL = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Firewalla Control</title>
  <style>
    :root {
      --bg: #070b14; --text: #e8eef9; --muted: #8b9bb8;
      --green: #34d399; --red: #f87171; --grey: #64748b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #1a2744 0%, transparent 50%),
                  radial-gradient(900px 500px at 90% 10%, #1e1a3a 0%, transparent 45%), var(--bg);
      color: var(--text); min-height: 100vh; padding: 28px 20px 60px;
    }
    header { max-width: 1400px; margin: 0 auto 24px; display: flex; justify-content: space-between; align-items: flex-end; gap: 12px; flex-wrap: wrap; }
    h1 {
      font-size: 1.85rem; font-weight: 650; letter-spacing: -0.03em;
      background: linear-gradient(135deg, #fff, #a8c0ff);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .meta { color: var(--muted); font-size: 0.9rem; margin-top: 4px; }
    .ws-status { font-size: 0.75rem; padding: 4px 10px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.15); }
    .ws-status.live { color: var(--green); border-color: rgba(52,211,153,0.4); }
    .ws-status.dead { color: var(--red); border-color: rgba(248,113,113,0.4); }
    .group { max-width: 1400px; margin: 0 auto 36px; }
    .group-title {
      font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em;
      color: var(--muted); margin-bottom: 14px; padding-left: 4px;
    }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 18px; }
    a.card-link { text-decoration: none; color: inherit; display: block; }
    .card-shell {
      border-radius: 22px; padding: 2px;
      transition: transform 0.15s ease, filter 0.15s ease;
    }
    .card-shell:hover { transform: translateY(-3px); filter: brightness(1.05); }
    .card-shell.glow-green {
      background: linear-gradient(135deg, #34d399 0%, #22d3ee 50%, #34d399 100%);
      box-shadow: 0 0 24px rgba(52,211,153,0.25);
    }
    .card-shell.glow-red {
      background: linear-gradient(135deg, #f87171 0%, #fb923c 50%, #f87171 100%);
      box-shadow: 0 0 24px rgba(248,113,113,0.28);
    }
    .card-shell.glow-grey {
      background: linear-gradient(135deg, #64748b 0%, #94a3b8 50%, #64748b 100%);
      box-shadow: 0 0 16px rgba(100,116,139,0.2); opacity: 0.9;
    }
    .card { background: #0c1220; border-radius: 20px; padding: 20px; height: 100%; }
    .card-top { display: flex; justify-content: space-between; gap: 10px; margin-bottom: 14px; }
    .name { font-size: 1.1rem; font-weight: 600; }
    .vendor { color: var(--muted); font-size: 0.78rem; margin-top: 3px; }
    .badge {
      font-size: 0.7rem; font-weight: 600; padding: 4px 9px; border-radius: 999px; height: fit-content;
    }
    .badge.on { background: rgba(52,211,153,0.15); color: var(--green); border: 1px solid rgba(52,211,153,0.3); }
    .badge.off { background: rgba(100,116,139,0.15); color: var(--muted); border: 1px solid rgba(100,116,139,0.3); }
    .badge.paused { background: rgba(248,113,113,0.15); color: var(--red); border: 1px solid rgba(248,113,113,0.35); }
    .info-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 14px; font-size: 0.82rem; }
    .info-item { background: rgba(0,0,0,0.35); border-radius: 11px; padding: 9px 11px; }
    .info-label { color: var(--muted); font-size: 0.68rem; text-transform: uppercase; margin-bottom: 2px; }
    .mac { font-size: 0.68rem; word-break: break-all; font-family: ui-monospace, monospace; }
    .meters { display: flex; justify-content: space-around; margin-bottom: 12px; }
    .meter { text-align: center; flex: 1; }
    .speedo { width: 120px; height: 72px; margin: 0 auto 4px; position: relative; }
    .speedo svg { width: 100%; height: 100%; }
    .speedo-bg { fill: none; stroke: rgba(255,255,255,0.1); stroke-width: 10; stroke-linecap: round; }
    .speedo-fill { fill: none; stroke-width: 10; stroke-linecap: round; }
    .speedo-fill.down { stroke: url(#gDown); }
    .speedo-fill.up { stroke: url(#gUp); }
    .speedo-center { position: absolute; left: 0; right: 0; bottom: 0; text-align: center; }
    .speedo-value { font-size: 1.05rem; font-weight: 700; }
    .speedo-unit { font-size: 0.58rem; color: var(--muted); text-transform: uppercase; }
    .meter-label { font-size: 0.72rem; color: var(--muted); }
    .pct-bar { height: 5px; background: rgba(255,255,255,0.08); border-radius: 99px; overflow: hidden; margin-bottom: 5px; }
    .pct-fill { height: 100%; background: linear-gradient(90deg, #5b8cff, #a78bfa); transition: width 0.4s ease; }
    .pct-text { font-size: 0.75rem; color: var(--muted); }
    .card-actions { display: flex; gap: 6px; margin-top: 10px; flex-wrap: wrap; }
    .mini-btn {
      font-size: 0.68rem; font-weight: 600; padding: 5px 9px; border-radius: 8px;
      border: 1px solid rgba(255,255,255,0.15); background: rgba(255,255,255,0.06);
      color: #fff; cursor: pointer;
    }
    .mini-btn.pause { background: rgba(251,191,36,0.22); }
    .mini-btn.unpause { background: rgba(52,211,153,0.2); }
  </style>
</head>
<body>
  <svg width="0" height="0" style="position:absolute">
    <defs>
      <linearGradient id="gDown" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#6366f1"/><stop offset="100%" stop-color="#22d3ee"/>
      </linearGradient>
      <linearGradient id="gUp" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#a78bfa"/><stop offset="100%" stop-color="#f472b6"/>
      </linearGradient>
    </defs>
  </svg>
  <header>
    <div>
      <h1>Firewalla Control</h1>
      <div class="meta"><span id="device-count">{{ total }}</span> devices · <span id="clock">{{ now }}</span></div>
    </div>
    <div id="ws-status" class="ws-status dead">Connecting…</div>
  </header>
  <div id="groups-root">
  {% for group_name, devices in groups.items() %}
  <section class="group" data-group="{{ group_name }}">
    <div class="group-title">{{ group_name }} · <span class="gcount">{{ devices|length }}</span></div>
    <div class="grid">
      {% for d in devices %}
      <div class="card-shell glow-{{ d.glow }}" id="shell-{{ d.mac }}">
        <article class="card">
          <a class="card-link" href="/device/{{ d.mac }}" data-mac="{{ d.mac }}">
            <div class="card-top">
              <div>
                <div class="name" id="name-{{ d.mac }}">{{ d.name }}</div>
                <div class="vendor">{{ d.vendor or "Unknown" }}</div>
              </div>
              <span class="badge {{ d.badge_class }}" id="badge-{{ d.mac }}">{{ d.status_label }}</span>
            </div>
            <div class="info-row">
              <div class="info-item"><div class="info-label">IP</div><span id="ip-{{ d.mac }}">{{ d.ip or "—" }}</span></div>
              <div class="info-item"><div class="info-label">MAC</div><div class="mac">{{ d.mac }}</div></div>
            </div>
            <div class="meters">
              <div class="meter">
                <div class="speedo">
                  <svg viewBox="0 0 100 60">
                    <path class="speedo-bg" d="M 10 55 A 40 40 0 0 1 90 55"/>
                    <path class="speedo-fill down" id="arc-down-{{ d.mac }}" d="M 10 55 A 40 40 0 0 1 90 55"
                          stroke-dasharray="{{ d.down_arc }} 200"/>
                  </svg>
                  <div class="speedo-center">
                    <div class="speedo-value" id="val-down-{{ d.mac }}">{{ d.download_val }}</div>
                    <div class="speedo-unit" id="unit-down-{{ d.mac }}">{{ d.download_unit }}</div>
                  </div>
                </div>
                <div class="meter-label">Download</div>
              </div>
              <div class="meter">
                <div class="speedo">
                  <svg viewBox="0 0 100 60">
                    <path class="speedo-bg" d="M 10 55 A 40 40 0 0 1 90 55"/>
                    <path class="speedo-fill up" id="arc-up-{{ d.mac }}" d="M 10 55 A 40 40 0 0 1 90 55"
                          stroke-dasharray="{{ d.up_arc }} 200"/>
                  </svg>
                  <div class="speedo-center">
                    <div class="speedo-value" id="val-up-{{ d.mac }}">{{ d.upload_val }}</div>
                    <div class="speedo-unit" id="unit-up-{{ d.mac }}">{{ d.upload_unit }}</div>
                  </div>
                </div>
                <div class="meter-label">Upload</div>
              </div>
            </div>
            <div class="pct-bar"><div class="pct-fill" id="pct-{{ d.mac }}" style="width:{{ d.pct }}%"></div></div>
            <div class="pct-text"><span id="pct-txt-{{ d.mac }}">{{ d.pct }}</span>% of visible traffic</div>
          </a>
          <div class="card-actions" onclick="event.stopPropagation()">
            {% if d.paused %}
            <form method="POST" action="/action" style="display:inline">
              <input type="hidden" name="mac" value="{{ d.mac }}">
              <input type="hidden" name="action" value="unpause">
              <input type="hidden" name="next" value="/">
              <button type="submit" class="mini-btn unpause">Unpause</button>
            </form>
            {% else %}
            <form method="POST" action="/action" style="display:inline">
              <input type="hidden" name="mac" value="{{ d.mac }}">
              <input type="hidden" name="action" value="pause">
              <input type="hidden" name="next" value="/">
              <button type="submit" class="mini-btn pause">Pause</button>
            </form>
            {% endif %}
          </div>
        </article>
      </div>
      {% endfor %}
    </div>
  </section>
  {% endfor %}
  </div>
<script>
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(proto + '://' + location.host + '/ws');
  const statusEl = document.getElementById('ws-status');
  ws.onopen = () => { statusEl.textContent = 'Live'; statusEl.className = 'ws-status live'; };
  ws.onclose = () => { statusEl.textContent = 'Reconnecting…'; statusEl.className = 'ws-status dead'; setTimeout(connect, 2000); };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    let msg; try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg.type !== 'snapshot') return;
    document.getElementById('clock').textContent = msg.now;
    document.getElementById('device-count').textContent = msg.total;
    for (const d of msg.devices) {
      const mac = d.mac;
      const shell = document.getElementById('shell-' + mac);
      if (!shell) continue;
      shell.className = 'card-shell glow-' + d.glow;
      const badge = document.getElementById('badge-' + mac);
      if (badge) { badge.className = 'badge ' + d.badge_class; badge.textContent = d.status_label; }
      const ip = document.getElementById('ip-' + mac);
      if (ip) ip.textContent = d.ip || '—';
      const nameEl = document.getElementById('name-' + mac);
      if (nameEl && d.name) nameEl.textContent = d.name;
      const vd = document.getElementById('val-down-' + mac);
      const ud = document.getElementById('unit-down-' + mac);
      const vu = document.getElementById('val-up-' + mac);
      const uu = document.getElementById('unit-up-' + mac);
      if (vd) vd.textContent = d.download_val;
      if (ud) ud.textContent = d.download_unit;
      if (vu) vu.textContent = d.upload_val;
      if (uu) uu.textContent = d.upload_unit;
      const ad = document.getElementById('arc-down-' + mac);
      const au = document.getElementById('arc-up-' + mac);
      if (ad) ad.setAttribute('stroke-dasharray', d.down_arc + ' 200');
      if (au) au.setAttribute('stroke-dasharray', d.up_arc + ' 200');
      const pf = document.getElementById('pct-' + mac);
      const pt = document.getElementById('pct-txt-' + mac);
      if (pf) pf.style.width = d.pct + '%';
      if (pt) pt.textContent = d.pct;
    }
  };
}
connect();
</script>
</body>
</html>
"""

DETAIL_TPL = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ d.name }} · Firewalla Control</title>
  <style>
    :root { --bg:#070b14; --text:#e8eef9; --muted:#8b9bb8; --green:#34d399; --red:#f87171; }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
           background: var(--bg); color: var(--text); min-height: 100vh; padding: 24px 18px 50px; }
    a.back { color: #93c5fd; text-decoration: none; font-size: 0.9rem; }
    .wrap { max-width: 900px; margin: 16px auto 0; }
    .shell { border-radius: 22px; padding: 2px; margin-bottom: 20px; }
    .shell.glow-green { background: linear-gradient(135deg, #34d399, #22d3ee, #34d399); box-shadow: 0 0 28px rgba(52,211,153,0.25); }
    .shell.glow-red { background: linear-gradient(135deg, #f87171, #fb923c, #f87171); box-shadow: 0 0 28px rgba(248,113,113,0.28); }
    .shell.glow-grey { background: linear-gradient(135deg, #64748b, #94a3b8); box-shadow: 0 0 16px rgba(100,116,139,0.2); }
    .hero { background: #0c1220; border-radius: 20px; padding: 22px; }
    h1 { font-size: 1.6rem; margin-bottom: 6px; }
    .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 16px; }
    .badges { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
    .badge { font-size: 0.75rem; font-weight: 600; padding: 5px 11px; border-radius: 999px; }
    .badge.on { background: rgba(52,211,153,0.15); color: var(--green); }
    .badge.off { background: rgba(100,116,139,0.15); color: var(--muted); }
    .badge.paused { background: rgba(248,113,113,0.15); color: var(--red); }
    .actions { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 8px; }
    .btn {
      border: 1px solid rgba(255,255,255,0.18); border-radius: 12px; padding: 11px 16px;
      font-size: 0.85rem; font-weight: 600; cursor: pointer; color: #fff;
      background: rgba(255,255,255,0.08);
    }
    .btn.pause { background: rgba(251,191,36,0.28); }
    .btn.unpause { background: rgba(52,211,153,0.25); }
    .btn.danger { background: rgba(248,113,113,0.25); }
    .btn.ok { background: rgba(52,211,153,0.22); }
    .section { background: #0c1220; border-radius: 16px; padding: 18px; margin-bottom: 18px; }
    h2 { font-size: 1.05rem; margin-bottom: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.06); }
    th { color: var(--muted); width: 160px; font-weight: 500; }
    .flash { background: rgba(52,211,153,0.12); border: 1px solid rgba(52,211,153,0.3);
             color: var(--green); padding: 10px 14px; border-radius: 10px; margin-bottom: 16px; font-size: 0.9rem; }
    input[type=text] {
      background: rgba(0,0,0,0.35); border: 1px solid rgba(255,255,255,0.15);
      color: #fff; border-radius: 10px; padding: 10px 12px; width: 100%; max-width: 320px;
    }
  </style>
</head>
<body>
  <a class="back" href="/">← Back to dashboard</a>
  <div class="wrap">
    {% if request.args.get('msg') %}
    <div class="flash">{{ request.args.get('msg') }}</div>
    {% endif %}

    <div class="shell glow-{{ d.glow }}">
      <div class="hero">
        <h1>{{ d.name }}</h1>
        <div class="sub">{{ d.vendor or "Unknown vendor" }}</div>
        <div class="badges">
          <span class="badge {{ d.badge_class }}">{{ d.status_label }}</span>
        </div>

        <div class="actions">
          {% if d.paused %}
          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="unpause">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn unpause" type="submit">▶ Unpause Monitoring</button>
          </form>
          {% else %}
          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="pause">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn pause" type="submit">⏸ Pause Monitoring</button>
          </form>
          {% endif %}

          {% if d.emergency %}
          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="emergency_off">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn ok" type="submit">Disable Emergency Access</button>
          </form>
          {% else %}
          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="emergency_on">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn danger" type="submit">Enable Emergency Access</button>
          </form>
          {% endif %}

          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="block">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn danger" type="submit">Block Internet</button>
          </form>
          <form method="POST" action="/action">
            <input type="hidden" name="mac" value="{{ d.mac }}">
            <input type="hidden" name="action" value="unblock">
            <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
            <button class="btn ok" type="submit">Unblock Internet</button>
          </form>
        </div>
      </div>
    </div>

    <h2>Rename</h2>
    <div class="section">
      <form method="POST" action="/action" style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
        <input type="hidden" name="mac" value="{{ d.mac }}">
        <input type="hidden" name="action" value="rename">
        <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
        <input type="text" name="name" value="{{ d.name }}" maxlength="32" placeholder="New name">
        <button class="btn" type="submit">Save name</button>
      </form>
    </div>

    <h2>Summary</h2>
    <div class="section">
      <table>
        <tr><th>IP</th><td>{{ d.ip or "—" }}</td></tr>
        <tr><th>MAC</th><td>{{ d.mac }}</td></tr>
        <tr><th>Vendor</th><td>{{ d.vendor or "—" }}</td></tr>
        <tr><th>Group / tags</th><td>{{ d.group or "—" }}</td></tr>
        <tr><th>Online</th><td>{{ "Yes" if d.online else "No" }}</td></tr>
        <tr><th>Monitoring</th><td>{{ "Paused" if d.paused else "Active" }}</td></tr>
        <tr><th>Emergency Access</th><td>{{ "On" if d.emergency else "Off" }}</td></tr>
        <tr><th>Last active</th><td>{{ d.last_active_human }}</td></tr>
        <tr><th>Download total</th><td>{{ d.down_total_h }}</td></tr>
        <tr><th>Upload total</th><td>{{ d.up_total_h }}</td></tr>
        <tr><th>Download rate</th><td>{{ d.download_val }} {{ d.download_unit }}</td></tr>
        <tr><th>Upload rate</th><td>{{ d.upload_val }} {{ d.upload_unit }}</td></tr>
      </table>
    </div>

    <h2>All Redis fields</h2>
    <div class="section">
      <table>
        {% for k, v in d.raw_fields %}
        <tr><th>{{ k }}</th><td style="word-break:break-all">{{ v }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    devices = load_devices()
    return render_template_string(
        INDEX_TPL,
        groups=group_devices(devices),
        total=len(devices),
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

@app.route("/device/<mac>")
def device(mac):
    d = next((x for x in load_devices() if x["mac"] == mac.upper()), None)
    if not d:
        return "Device not found", 404
    return render_template_string(DETAIL_TPL, d=d, request=request)

@app.route("/action", methods=["POST"])
def action():
    mac = (request.form.get("mac") or "").strip().upper()
    act = (request.form.get("action") or "").strip().lower()
    next_url = request.form.get("next") or url_for("index")
    msg = None

    if not mac and act != "noop":
        return redirect(next_url)

    if act == "pause":
        ok = set_device_monitor(mac, False)
        msg = "Monitoring paused" if ok else "Failed to pause"
    elif act == "unpause":
        ok = set_device_monitor(mac, True)
        msg = "Monitoring resumed" if ok else "Failed to unpause"
    elif act == "emergency_on":
        ok = set_emergency_access(mac, True)
        msg = "Emergency Access enabled" if ok else "Failed"
    elif act == "emergency_off":
        ok = set_emergency_access(mac, False)
        msg = "Emergency Access disabled" if ok else "Failed"
    elif act == "block":
        ok = block_internet(mac)
        msg = "Internet blocked (rule created)" if ok else "Failed to block"
    elif act == "unblock":
        n = unblock_internet(mac)
        msg = f"Removed {n} block rule(s)" if n else "No matching block rules found"
    elif act == "rename":
        name = request.form.get("name") or ""
        ok = rename_device(mac, name)
        msg = "Renamed" if ok else "Rename failed"
    else:
        msg = f"Unknown action: {act}"

    sep = "&" if "?" in next_url else "?"
    return redirect(f"{next_url}{sep}msg={msg}" if msg else next_url)

@app.route("/api/devices")
def api_devices():
    return jsonify(snapshot_payload())

@sock.route("/ws")
def ws_traffic(ws):
    while True:
        try:
            ws.send(json.dumps(snapshot_payload()))
        except Exception:
            break
        time.sleep(2)

if __name__ == "__main__":
    # Bind all interfaces so you can reach it from LAN
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
