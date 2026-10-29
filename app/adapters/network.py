from typing import List
from ..redis_client import get_redis
from ..models import NetworkInfo


def get_networks() -> List[NetworkInfo]:
    r = get_redis()
    keys = list(r.scan_iter(match="network:uuid:*", count=50))
    result = []
    for key in keys:
        data = r.hgetall(key)
        uuid = key.split(":")[-1]
        result.append(NetworkInfo(uuid=uuid, data=data))
    return result
