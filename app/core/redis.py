from typing import Optional
import redis
from redis.client import Redis

# Centralized Redis instance sharing a connection pool across the application to minimize TCP connection overhead.
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


def redis_safe_get(redis_client: Redis, key: str) -> Optional[str]:
    # Fail-open on Redis lookup errors ensures API availability if cache goes down, sacrificing cache speed for reliability.
    try:
        return redis_client.get(key)
    except redis.RedisError:
        return None


def redis_safe_set(redis_client: Redis, key: str, value: str, ex: int) -> None:
    # Silently catching exceptions on Redis writes prevents non-critical cache update failures from breaking HTTP requests.
    try:
        redis_client.set(key, value, ex=ex)
    except redis.RedisError:
        pass
