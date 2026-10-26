#!/usr/bin/env python3
from flask import Flask, render_template_string, request, redirect, url_for
import redis
from datetime import datetime

app = Flask(__name__)
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Firewalla Dashboard</title>
  <meta http-equiv="refresh" content="30">
  <style>
    :root {
      --bg: #070b14;
      --card: rgba(255,255,255,0.06);
      --card-border: rgba(255,255,255,0.12);
      --text: #e8eef9;
      --muted: #8b9bb8;
      --accent: #5b8cff;
      --green: #34d399;
      --amber: #fbbf24;
      --red: #f87171;
      --glass: rgba(255,255,255,0.08);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: "SF Pro Display", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #1a2744 0%, transparent 50%),
                  radial-gradient(900px 500px at 90% 10%, #1e1a3a 0%, transparent 45%),
                  var(--bg);
      color: var(--text);
      min-height: 100vh;
      padding: 28px 20px 60px;
    }

    header {
      max-width: 1400px;
      margin: 0 auto 28px;
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
    }

    h1 {
      font-size: 1.85rem;
      font-weight: 650;
      letter-spacing: -0.03em;
      background: linear-gradient(135deg, #fff 0%, #a8c0ff 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }

    .meta {
      color: var(--muted);
      font-size: 0.9rem;
    }

    .grid {
      max-width: 1400px;
      margin: 0 auto;
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
      gap: 20px;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--card-border);
      border-radius: 22px;
      padding: 22px;
      backdrop-filter: blur(18px);
      -webkit-backdrop-filter: blur(18px);
      box-shadow: 0 8px 32px rgba(0,0,0,0.35);
      transition: transform 0.2s ease, border-color 0.2s ease;
    }

    .card:hover {
      transform: translateY(-3px);
      border-color: rgba(91,140,255,0.35);
    }

    .card-top {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      margin-bottom: 18px;
    }

    .name {
      font-size: 1.15rem;
      font-weight: 600;
      letter-spacing: -0.02em;
      line-height: 1.3;
    }

    .vendor {
      color: var(--muted);
      font-size: 0.8rem;
      margin-top: 4px;
    }

    .badge {
      font-size: 0.72rem;
      font-weight: 600;
      padding: 5px 10px;
      border-radius: 999px;
      white-space: nowrap;
    }

    .badge.on {
      background: rgba(52,211,153,0.15);
      color: var(--green);
      border: 1px solid rgba(52,211,153,0.3);
    }

    .badge.off {
      background: rgba(148,163,184,0.12);
      color: var(--muted);
      border: 1px solid rgba(148,163,184,0.25);
    }

    .info-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 18px;
      font-size: 0.85rem;
    }

    .info-item {
      background: rgba(0,0,0,0.22);
      border-radius: 12px;
      padding: 10px 12px;
    }

    .info-label {
      color: var(--muted);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      margin-bottom: 3px;
    }

    .meters {
      display: flex;
      justify-content: space-around;
      gap: 8px;
      margin: 8px 0 20px;
    }

    .meter {
      text-align: center;
      flex: 1;
    }

    .gauge {
      width: 110px;
      height: 110px;
      margin: 0 auto 8px;
      position: relative;
    }

    .gauge svg {
      width: 100%;
      height: 100%;
      transform: rotate(-90deg);
    }

    .gauge-bg {
      fill: none;
      stroke: rgba(255,255,255,0.08);
      stroke-width: 8;
    }

    .gauge-fill {
      fill: none;
      stroke-width: 8;
      stroke-linecap: round;
      transition: stroke-dashoffset 0.6s ease;
    }

    .gauge-fill.down { stroke: url(#gradDown); }
    .gauge-fill.up { stroke: url(#gradUp); }

    .gauge-center {
      position: absolute;
      inset: 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      pointer-events: none;
    }

    .gauge-value {
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: -0.03em;
    }

    .gauge-unit {
      font-size: 0.65rem;
      color: var(--muted);
      text-transform: uppercase;
    }

    .meter-label {
      font-size: 0.75rem;
      color: var(--muted);
      font-weight: 500;
    }

    .pct-bar {
      height: 6px;
      background: rgba(255,255,255,0.08);
      border-radius: 99px;
      overflow: hidden;
      margin-bottom: 6px;
    }

    .pct-fill {
      height: 100%;
      background: linear-gradient(90deg, #5b8cff, #a78bfa);
      border-radius: 99px;
    }

    .pct-text {
      font-size: 0.78rem;
      color: var(--muted);
      margin-bottom: 16px;
    }

    .actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }

    .btn {
      appearance: none;
      border: 1px solid rgba(255,255,255,0.18);
      border-radius: 14px;
      padding: 12px 10px;
      font-size: 0.82rem;
      font-weight: 600;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(180deg, rgba(255,255,255,0.14) 0%, rgba(255,255,255,0.05) 100%);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.2), 0 4px 16px rgba(0,0,0,0.25);
      transition: transform 0.15s ease, background 0.15s ease, border-color 0.15s ease;
    }

    .btn:hover {
      transform: scale(1.03);
      border-color: rgba(255,255,255,0.35);
    }

    .btn:active { transform: scale(0.98); }

    .btn.pause { background: linear-gradient(180deg, rgba(251,191,36,0.35), rgba(251,191,36,0.12)); }
    .btn.unpause { background: linear-gradient(180deg, rgba(52,211,153,0.35), rgba(52,211,153,0.12)); }
    .btn.quarantine { background: linear-gradient(180deg, rgba(248,113,113,0.4), rgba(248,113,113,0.12)); }
    .btn.unquarantine { background: linear-gradient(180deg, rgba(91,140,255,0.4), rgba(91,140,255,0.12)); }

    footer {
      max-width: 1400px;
      margin: 32px auto 0;
      text-align: center;
      color: var(--muted);
      font-size: 0.8rem;
    }
  </style>
</head>
<body>
  <svg width="0" height="0" style="position:absolute">
    <defs>
      <linearGradient id="gradDown" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#34d399"/>
        <stop offset="100%" stop-color="#5b8cff"/>
      </linearGradient>
      <linearGradient id="gradUp" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#a78bfa"/>
        <stop offset="100%" stop-color="#f472b6"/>
      </linearGradient>
    </defs>
  </svg>

  <header>
    <div>
      <h1>Firewalla Dashboard</h1>
      <div class="meta">{{ devices|length }} devices · updated {{ now }}</div>
    </div>
  </header>

  <div class="grid">
    {% for d in devices %}
    <article class="card">
      <div class="card-top">
        <div>
          <div class="name">{{ d.name }}</div>
          <div class="vendor">{{ d.vendor or "Unknown device" }}</div>
        </div>
        <span class="badge {{ 'on' if d.online else 'off' }}">
          {{ "Online" if d.online else "Offline" }}
        </span>
      </div>

      <div class="info-row">
        <div class="info-item">
          <div class="info-label">IP</div>
          <div>{{ d.ip or "—" }}</div>
        </div>
        <div class="info-item">
          <div class="info-label">MAC</div>
          <div style="font-size:0.78rem">{{ d.mac }}</div>
        </div>
      </div>

      <div class="meters">
        <div class="meter">
          <div class="gauge">
            <svg viewBox="0 0 36 36">
              <circle class="gauge-bg" cx="18" cy="18" r="15.5"/>
              <circle class="gauge-fill down" cx="18" cy="18" r="15.5"
                stroke-dasharray="{{ d.down_dash }} 100"
                stroke-dashoffset="0"/>
            </svg>
            <div class="gauge-center">
              <div class="gauge-value">{{ d.download_val }}</div>
              <div class="gauge-unit">{{ d.download_unit }}</div>
            </div>
          </div>
          <div class="meter-label">Download</div>
        </div>
        <div class="meter">
          <div class="gauge">
            <svg viewBox="0 0 36 36">
              <circle class="gauge-bg" cx="18" cy="18" r="15.5"/>
              <circle class="gauge-fill up" cx="18" cy="18" r="15.5"
                stroke-dasharray="{{ d.up_dash }} 100"
                stroke-dashoffset="0"/>
            </svg>
            <div class="gauge-center">
              <div class="gauge-value">{{ d.upload_val }}</div>
              <div class="gauge-unit">{{ d.upload_unit }}</div>
            </div>
          </div>
          <div class="meter-label">Upload</div>
        </div>
      </div>

      <div class="pct-bar"><div class="pct-fill" style="width:{{ d.pct }}%"></div></div>
      <div class="pct-text">{{ d.pct }}% of visible traffic</div>

      <form class="actions" method="post" action="/action">
        <input type="hidden" name="mac" value="{{ d.mac }}">
        <button class="btn pause" name="action" value="pause">Pause</button>
        <button class="btn unpause" name="action" value="unpause">Unpause</button>
        <button class="btn quarantine" name="action" value="quarantine">Quarantine</button>
        <button class="btn unquarantine" name="action" value="unquarantine">Release</button>
      </form>
    </article>
    {% else %}
    <p style="color:var(--muted)">No devices found in Redis.</p>
    {% endfor %}
  </div>

  <footer>Local Firewalla dashboard · auto-refresh 30s</footer>
</body>
</html>
"""

def split_human(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0", "B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f}", unit
        n /= 1024
    return f"{n:.1f}", "PB"

def rate_dash(raw, scale=50_000_000):
    """Map byte counter to 0–100 arc for the gauge (visual only)."""
    try:
        v = min(100.0, (float(raw) / scale) * 100)
    except (TypeError, ValueError):
        v = 0
    return f"{v:.1f}"

def get_devices():
    devices = []
    total_bytes = 0
    keys = r.keys("host:mac:*")
    raw = []

    for key in keys:
        data = r.hgetall(key)
        if not data:
            continue

        mac = key.split(":")[-1].upper()
        name = data.get("name") or data.get("bname") or data.get("dhcpName") or mac
        ip = data.get("ipv4Addr") or data.get("ipv4") or ""
        vendor = data.get("macVendor") or data.get("vendor") or ""

        last = float(data.get("lastActiveTimestamp") or data.get("lastActive") or 0)
        online = (datetime.now().timestamp() - last) < 600 if last else False

        upload = int(float(data.get("upload") or data.get("tx") or 0))
        download = int(float(data.get("download") or data.get("rx") or 0))

        raw.append({
            "mac": mac,
            "name": name,
            "ip": ip,
            "vendor": vendor,
            "online": online,
            "upload_raw": upload,
            "download_raw": download,
        })
        total_bytes += upload + download

    for d in raw:
        traffic = d["upload_raw"] + d["download_raw"]
        d["pct"] = round((traffic / total_bytes * 100) if total_bytes else 0, 1)

        d["download_val"], d["download_unit"] = split_human(d["download_raw"])
        d["upload_val"], d["upload_unit"] = split_human(d["upload_raw"])
        d["down_dash"] = rate_dash(d["download_raw"])
        d["up_dash"] = rate_dash(d["upload_raw"])

        devices.append(d)

    devices.sort(key=lambda x: (not x["online"], x["name"].lower()))
    return devices

@app.route("/")
def index():
    devices = get_devices()
    return render_template_string(
        TEMPLATE,
        devices=devices,
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

@app.route("/action", methods=["POST"])
def action():
    mac = request.form.get("mac", "").upper()
    act = request.form.get("action")
    print(f"[ACTION] {act} → {mac}")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
