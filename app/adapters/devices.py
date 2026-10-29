from typing import List, Optional
import time
import json
from ..redis_client import get_redis
from ..models import Device
from ..config import settings


def _parse_json_field(value: str | None):
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _is_online(last_active: float | None) -> bool:
    if last_active is None:
        return False
    return (time.time() - last_active) <= settings.online_threshold_seconds


def get_all_devices(online_only: bool = False) -> List[Device]:
    r = get_redis()
    keys = list(r.scan_iter(match="host:mac:*", count=200))
    devices = []

    for key in keys:
        data = r.hgetall(key)
        if not data:
            continue

        mac = data.get("mac") or key.split(":")[-1]
        last_active = float(data["lastActiveTimestamp"]) if data.get("lastActiveTimestamp") else None
        online = _is_online(last_active)

        if online_only and not online:
            continue

        ipv6 = _parse_json_field(data.get("ipv6Addr")) or []
        if isinstance(ipv6, str):
            ipv6 = []

        open_ports = _parse_json_field(data.get("openports"))

        # Keep only the most useful fields in the main model;
        # everything else goes into extra
        known = {
            "mac", "bname", "name", "ipv4Addr", "ipv4", "uid",
            "lastActiveTimestamp", "firstFoundTimestamp", "macVendor",
            "localDomain", "intf_uuid", "spoofing", "openports", "ipv6Addr"
        }

        extra = {k: v for k, v in data.items() if k not in known}

        devices.append(Device(
            mac=mac,
            name=data.get("name"),
            bname=data.get("bname"),
            ipv4=data.get("ipv4Addr") or data.get("ipv4") or data.get("uid"),
            ipv6=ipv6 if isinstance(ipv6, list) else [],
            mac_vendor=data.get("macVendor"),
            local_domain=data.get("localDomain"),
            online=online,
            last_active=last_active,
            first_found=float(data["firstFoundTimestamp"]) if data.get("firstFoundTimestamp") else None,
            intf_uuid=data.get("intf_uuid") or data.get("intf"),
            spoofing=data.get("spoofing") == "true" if data.get("spoofing") else None,
            open_ports=open_ports if isinstance(open_ports, dict) else None,
            extra=extra,
        ))

    # Sort online first, then by name/bname
    devices.sort(key=lambda d: (not d.online, (d.bname or d.name or d.mac).lower()))
    return devices


def get_device(mac: str) -> Optional[Device]:
    mac = mac.upper().replace("-", ":")
    devices = get_all_devices()
    for d in devices:
        if d.mac.upper() == mac:
            return d
    return None
