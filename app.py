#!/usr/bin/env python3
from flask import Flask, render_template_string, request, redirect, url_for
import redis, json
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
ARC_LEN = 125.66

INDEX_TPL = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Firewalla Dashboard</title>
  <meta http-equiv="refresh" content="30">
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
    header { max-width: 1400px; margin: 0 auto 24px; }
    h1 {
      font-size: 1.85rem; font-weight: 650; letter-spacing: -0.03em;
      background: linear-gradient(135deg, #fff, #a8c0ff);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .meta { color: var(--muted); font-size: 0.9rem; margin-top: 4px; }
    .group { max-width: 1400px; margin: 0 auto 36px; }
    .group-title {
      font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.08em;
      color: var(--muted); margin-bottom: 14px; padding-left: 4px;
    }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 18px; }
    a.card-link { text-decoration: none; color: inherit; display: block; }

    /* Gradient border shell */
    .card-shell {
      border-radius: 22px;
      padding: 2px;
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
      box-shadow: 0 0 16px rgba(100,116,139,0.2);
      opacity: 0.9;
    }

    .card {
      background: #0c1220;
      border-radius: 20px;
      padding: 20px;
      height: 100%;
    }
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
    .mac { font-size: 0.7rem; word-break: break-all; font-family: ui-monospace, monospace; }
    .meters { display: flex; justify-content: space-around; margin-bottom: 12px; }
    .meter { text-align: center; flex: 1; }
    .speedo { width: 120px; height: 72px; margin: 0 auto 4px; position: relative; }
    .speedo svg { width: 100%; height: 100%; }
    .speedo-bg { fill: none; stroke: rgba(255,255,255,0.1); stroke-width: 10; stroke-linecap: round; }
    .speedo-fill { fill: none; stroke-width: 10; stroke-linecap: round; }
    .speedo-fill.down { stroke: url(#gDown); }
    .speedo-fill.up { stroke: url(#gUp); }
    .speedo-center { position: absolute; left: 0; right: 0; bottom: 0; text-align: center; }
    .speedo-value { font-size: 1.15rem; font-weight: 700; }
    .speedo-unit { font-size: 0.6rem; color: var(--muted); text-transform: uppercase; }
    .meter-label { font-size: 0.72rem; color: var(--muted); }
    .pct-bar { height: 5px; background: rgba(255,255,255,0.08); border-radius: 99px; overflow: hidden; margin-bottom: 5px; }
    .pct-fill { height: 100%; background: linear-gradient(90deg, #5b8cff, #a78bfa); }
    .pct-text { font-size: 0.75rem; color: var(--muted); }
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
    <h1>Firewalla Dashboard</h1>
    <div class="meta">{{ total }} devices · updated {{ now }}</div>
  </header>

  {% for group_name, devices in groups.items() %}
  <section class="group">
    <div class="group-title">{{ group_name }} · {{ devices|length }}</div>
    <div class="grid">
      {% for d in devices %}
      <a class="card-link" href="{{ url_for('device', mac=d.mac) }}">
        <div class="card-shell glow-{{ d.glow }}">
          <article class="card">
            <div class="card-top">
              <div>
                <div class="name">{{ d.name }}</div>
                <div class="vendor">{{ d.vendor or "Unknown" }}</div>
              </div>
              <span class="badge {{ d.badge_class }}">{{ d.status_label }}</span>
            </div>
            <div class="info-row">
              <div class="info-item"><div class="info-label">IP</div>{{ d.ip or "—" }}</div>
              <div class="info-item"><div class="info-label">MAC</div><div class="mac">{{ d.mac }}</div></div>
            </div>
            <div class="meters">
              <div class="meter">
                <div class="speedo">
                  <svg viewBox="0 0 100 60">
                    <path class="speedo-bg" d="M 10 55 A 40 40 0 0 1 90 55"/>
                    {% if d.down_pct > 0 %}
                    <path class="speedo-fill down" d="M 10 55 A 40 40 0 0 1 90 55"
                          stroke-dasharray="{{ d.down_arc }} 200"/>
                    {% endif %}
                  </svg>
                  <div class="speedo-center">
                    <div class="speedo-value">{{ d.download_val }}</div>
                    <div class="speedo-unit">{{ d.download_unit }}</div>
                  </div>
                </div>
                <div class="meter-label">Download</div>
              </div>
              <div class="meter">
                <div class="speedo">
                  <svg viewBox="0 0 100 60">
                    <path class="speedo-bg" d="M 10 55 A 40 40 0 0 1 90 55"/>
                    {% if d.up_pct > 0 %}
                    <path class="speedo-fill up" d="M 10 55 A 40 40 0 0 1 90 55"
                          stroke-dasharray="{{ d.up_arc }} 200"/>
                    {% endif %}
                  </svg>
                  <div class="speedo-center">
                    <div class="speedo-value">{{ d.upload_val }}</div>
                    <div class="speedo-unit">{{ d.upload_unit }}</div>
                  </div>
                </div>
                <div class="meter-label">Upload</div>
              </div>
            </div>
            <div class="pct-bar"><div class="pct-fill" style="width:{{ d.pct }}%"></div></div>
            <div class="pct-text">{{ d.pct }}% of visible traffic</div>
          </article>
        </div>
      </a>
      {% endfor %}
    </div>
  </section>
  {% endfor %}
</body>
</html>
"""

DETAIL_TPL = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ d.name }} · Firewalla</title>
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
    .actions { display: flex; flex-wrap: wrap; gap: 10px; }
    .btn { border: 1px solid rgba(255,255,255,0.18); border-radius: 12px; padding: 11px 16px;
           font-size: 0.85rem; font-weight: 600; cursor: pointer; color: #fff; background: rgba(255,255,255,0.08); }
    .btn.pause { background: rgba(251,191,36,0.25); }
    .btn.unpause { background: rgba(52,211,153,0.25); }
    .btn.quarantine { background: rgba(248,113,113,0.25); }
    .btn.unquarantine { background: rgba(91,140,255,0.25); }
    h2 { font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); margin: 22px 0 10px; }
    .section { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1); border-radius: 16px; overflow: hidden; }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); vertical-align: top; }
    th { color: var(--muted); font-weight: 500; width: 34%; }
    td { word-break: break-word; font-family: ui-monospace, monospace; font-size: 0.82rem; }
  </style>
</head>
<body>
  <div class="wrap">
    <a class="back" href="{{ url_for('index') }}">← All devices</a>
    <div class="shell glow-{{ d.glow }}">
      <div class="hero">
        <h1>{{ d.name }}</h1>
        <div class="sub">{{ d.vendor or "Unknown vendor" }} · {{ d.mac }}</div>
        <div class="badges">
          <span class="badge {{ d.badge_class }}">{{ d.status_label }}</span>
          {% if d.group %}<span class="badge off">{{ d.group }}</span>{% endif %}
        </div>
        <form class="actions" method="post" action="{{ url_for('action') }}">
          <input type="hidden" name="mac" value="{{ d.mac }}">
          <input type="hidden" name="next" value="{{ url_for('device', mac=d.mac) }}">
          <button class="btn pause" name="action" value="pause">Pause</button>
          <button class="btn unpause" name="action" value="unpause">Unpause</button>
          <button class="btn quarantine" name="action" value="quarantine">Quarantine</button>
          <button class="btn unquarantine" name="action" value="unquarantine">Release</button>
        </form>
      </div>
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
        <tr><th>Last active</th><td>{{ d.last_active_human }}</td></tr>
        <tr><th>Download</th><td>{{ d.download_val }} {{ d.download_unit }}</td></tr>
        <tr><th>Upload</th><td>{{ d.upload_val }} {{ d.upload_unit }}</td></tr>
      </table>
    </div>
    <h2>All Redis fields</h2>
    <div class="section">
      <table>
        {% for k, v in d.raw_fields %}
        <tr><th>{{ k }}</th><td>{{ v }}</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
</body>
</html>
"""

def split_human(n):
    try: n = float(n)
    except: return "0", "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return (f"{n:.1f}" if n < 10 or unit == "B" else f"{n:.0f}"), unit
        n /= 1024
    return f"{n:.1f}", "PB"

def meter_pct(raw, scale=20_000_000):
    try: return max(0.0, min(100.0, (float(raw) / scale) * 100))
    except: return 0.0

def parse_policy(data):
    raw = data.get("policy") or ""
    if not raw: return False, {}
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return False, {}
    monitor = obj.get("monitor")
    if monitor is False or str(monitor).lower() == "false":
        return True, obj
    return False, obj

def parse_tags(data):
    tags = data.get("tags") or data.get("tag") or ""
    if not tags: return []
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

def load_devices():
    devices, total_bytes = [], 0
    for key in r.keys("host:mac:*"):
        data = r.hgetall(key)
        if not data: continue
        mac = key.split(":")[-1].upper()
        name = data.get("name") or data.get("bname") or data.get("dhcpName") or mac
        ip = data.get("ipv4Addr") or data.get("ipv4") or data.get("ip") or ""
        vendor = data.get("macVendor") or data.get("vendor") or ""
        last = float(data.get("lastActiveTimestamp") or data.get("lastActive") or 0)
        online = (datetime.now().timestamp() - last) < 600 if last else False
        last_human = datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S") if last else "—"
        paused, policy = parse_policy(data)
        tags = parse_tags(data)
        group = ", ".join(tags) if tags else (data.get("groupName") or data.get("group") or "Ungrouped")
        upload = int(float(data.get("upload") or data.get("tx") or 0))
        download = int(float(data.get("download") or data.get("rx") or 0))
        if not online:
            glow, badge_class, status_label = "grey", "off", "Offline"
        elif paused:
            glow, badge_class, status_label = "red", "paused", "Paused"
        else:
            glow, badge_class, status_label = "green", "on", "Online"
        devices.append({
            "mac": mac, "name": name, "ip": ip, "vendor": vendor, "online": online, "paused": paused,
            "group": group, "glow": glow, "badge_class": badge_class, "status_label": status_label,
            "last_active_human": last_human, "upload_raw": upload, "download_raw": download,
            "raw_fields": sorted(data.items()),
        })
        total_bytes += upload + download
    for d in devices:
        t = d["upload_raw"] + d["download_raw"]
        d["pct"] = round((t / total_bytes * 100) if total_bytes else 0, 1)
        d["download_val"], d["download_unit"] = split_human(d["download_raw"])
        d["upload_val"], d["upload_unit"] = split_human(d["upload_raw"])
        d["down_pct"] = meter_pct(d["download_raw"])
        d["up_pct"] = meter_pct(d["upload_raw"])
        d["down_arc"] = f"{d['down_pct'] / 100 * ARC_LEN:.2f}"
        d["up_arc"] = f"{d['up_pct'] / 100 * ARC_LEN:.2f}"
    devices.sort(key=lambda x: (not x["online"], x["name"].lower()))
    return devices

def group_devices(devices):
    groups = defaultdict(list)
    for d in devices:
        groups[d["group"] or "Ungrouped"].append(d)
    return dict(sorted(groups.items(), key=lambda kv: (kv[0] == "Ungrouped", kv[0].lower())))

@app.route("/")
def index():
    devices = load_devices()
    return render_template_string(INDEX_TPL, groups=group_devices(devices), total=len(devices),
                                  now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@app.route("/device/<mac>")
def device(mac):
    d = next((x for x in load_devices() if x["mac"] == mac.upper()), None)
    if not d: return "Device not found", 404
    return render_template_string(DETAIL_TPL, d=d)

@app.route("/action", methods=["POST"])
def action():
    print(f"[ACTION] {request.form.get('action')} → {request.form.get('mac')}")
    return redirect(request.form.get("next") or url_for("index"))

if __name__ == "__main__":
    # Match the port you're actually using (8080 in your screenshot)
    app.run(host="0.0.0.0", port=8080, debug=False)
