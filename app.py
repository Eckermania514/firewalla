#!/usr/bin/env python3
"""
Firewalla Device Dashboard
Simple web UI showing connected devices + placeholder actions for pause/quarantine.
Runs on the Firewalla box itself.
"""

from flask import Flask, render_template_string, request, redirect, url_for
import redis
from datetime import datetime

app = Flask(__name__)
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <title>Firewalla Device Dashboard</title>
  <meta http-equiv="refresh" content="30">
  <style>
    body { font-family: system-ui, sans-serif; margin: 20px; background: #0f172a; color: #e2e8f0; }
    h1 { color: #38bdf8; }
    table { border-collapse: collapse; width: 100%; margin-top: 20px; }
    th, td { border: 1px solid #334155; padding: 8px 12px; text-align: left; }
    th { background: #1e293b; }
    tr:nth-child(even) { background: #1e293b; }
    .btn { padding: 4px 10px; margin: 2px; border: none; border-radius: 4px; cursor: pointer; font-size: 12px; }
    .pause { background: #f59e0b; color: black; }
    .unpause { background: #22c55e; color: black; }
    .quarantine { background: #ef4444; color: white; }
    .unquarantine { background: #3b82f6; color: white; }
    .online { color: #4ade80; }
    .offline { color: #94a3b8; }
  </style>
</head>
<body>
  <h1>Firewalla Device Dashboard</h1>
  <p>Last updated: {{ now }} | Devices: {{ devices|length }}</p>
  <table>
    <tr>
      <th>Name</th>
      <th>IP</th>
      <th>MAC</th>
      <th>Vendor / Type</th>
      <th>Status</th>
      <th>Upload</th>
      <th>Download</th>
      <th>% Total</th>
      <th>Actions</th>
    </tr>
    {% for d in devices %}
    <tr>
      <td>{{ d.name }}</td>
      <td>{{ d.ip }}</td>
      <td>{{ d.mac }}</td>
      <td>{{ d.vendor }}</td>
      <td class="{{ 'online' if d.online else 'offline' }}">
        {{ 'Online' if d.online else 'Offline' }}
      </td>
      <td>{{ d.upload }}</td>
      <td>{{ d.download }}</td>
      <td>{{ d.pct }}%</td>
      <td>
        <form style="display:inline" method="post" action="/action">
          <input type="hidden" name="mac" value="{{ d.mac }}">
          <button class="btn pause" name="action" value="pause">Pause</button>
          <button class="btn unpause" name="action" value="unpause">Unpause</button>
          <button class="btn quarantine" name="action" value="quarantine">Quarantine</button>
          <button class="btn unquarantine" name="action" value="unquarantine">Unquarantine</button>
        </form>
      </td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
"""

def human(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

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
        name = (
            data.get("name")
            or data.get("bname")
            or data.get("dhcpName")
            or mac
        )
        ip = data.get("ipv4Addr") or data.get("ipv4") or ""
        vendor = data.get("macVendor") or data.get("vendor") or ""

        # Online if seen in the last ~10 minutes
        last = float(
            data.get("lastActiveTimestamp")
            or data.get("lastActive")
            or 0
        )
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
        d["upload"] = human(d["upload_raw"])
        d["download"] = human(d["download_raw"])
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
    # TODO: implement real pause / quarantine logic
    return redirect(url_for("index"))

if __name__ == "__main__":
    # Use port 8080 (no root required). Change to 80 only if you run as root.
    app.run(host="0.0.0.0", port=8080, debug=False)
