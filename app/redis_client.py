import redis
from .config import settings


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
    )
