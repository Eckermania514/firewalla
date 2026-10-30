from typing import List, Any
from ..redis_client import get_redis
from ..models import Policy
import json
import logging

log = logging.getLogger(__name__)


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse(v: Any):
    if v is None or v == "":
        return None
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return v


def get_all_policies() -> List[Policy]:
    r = get_redis()
    keys = list(r.scan_iter(match="policy:*", count=500))
    policies: List[Policy] = []

    for key in keys:
        # Skip device-tag style keys for now
        if key.startswith("policy:mac:"):
            continue

        try:
            data = r.hgetall(key)
            if not data:
                continue

            # Make sure everything is a plain string for the model
            clean = {k: (v if isinstance(v, str) else str(v)) for k, v in data.items()}

            policies.append(Policy(
                pid=clean.get("pid") or key.split(":")[-1],
                action=clean.get("action"),
                type=clean.get("type"),
                target=clean.get("target"),
                target_name=clean.get("target_name"),
                target_ip=clean.get("target_ip"),
                scope=_parse(clean.get("scope")),
                direction=clean.get("direction"),
                protocol=clean.get("protocol"),
                local_port=clean.get("localPort"),
                activated_time=_safe_float(clean.get("activatedTime")),
                category=clean.get("category"),
                raw=clean,
            ))
        except Exception as e:
            # Never let one bad key kill the whole endpoint
            log.warning("Skipping bad policy key %s: %s", key, e)
            continue

    return policies
