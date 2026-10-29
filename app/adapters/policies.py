from typing import List
from ..redis_client import get_redis
from ..models import Policy
import json


def get_all_policies() -> List[Policy]:
    r = get_redis()
    keys = list(r.scan_iter(match="policy:*", count=200))
    policies = []

    for key in keys:
        # Skip policy:mac:* for now (device tags) – can be added later
        if key.startswith("policy:mac:"):
            continue

        data = r.hgetall(key)
        if not data:
            continue

        def parse(v):
            try:
                return json.loads(v)
            except Exception:
                return v

        policies.append(Policy(
            pid=data.get("pid") or key.split(":")[-1],
            action=data.get("action"),
            type=data.get("type"),
            target=data.get("target"),
            target_name=data.get("target_name"),
            target_ip=data.get("target_ip"),
            scope=parse(data.get("scope")),
            direction=data.get("direction"),
            protocol=data.get("protocol"),
            local_port=data.get("localPort"),
            activated_time=float(data["activatedTime"]) if data.get("activatedTime") else None,
            category=data.get("category"),
            raw=data,
        ))

    return policies
