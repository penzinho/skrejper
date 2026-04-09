import os
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from uuid import uuid4

from fastapi import HTTPException, Request, status
from redis import Redis
from redis.exceptions import RedisError

RATE_LIMIT_BACKEND_ENV_VAR = "RATE_LIMIT_BACKEND"
RATE_LIMIT_REDIS_URL_ENV_VAR = "RATE_LIMIT_REDIS_URL"
REDIS_URL_ENV_VAR = "REDIS_URL"

_REDIS_RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)
local count = redis.call("ZCARD", key)

if count >= limit then
  local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
  local retry_after = 1
  if oldest[2] then
    retry_after = math.max(1, math.ceil(window - (now - tonumber(oldest[2]))))
  end
  return {0, count, retry_after}
end

redis.call("ZADD", key, now, member)
redis.call("EXPIRE", key, math.ceil(window))
return {1, count + 1, 0}
"""


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int


class InMemoryRateLimitBackend:
    def __init__(self) -> None:
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, key: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        cutoff = now - window_seconds

        with self._lock:
            bucket = self._buckets[key]
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                retry_after = max(1, int(bucket[0] + window_seconds - now))
                return RateLimitResult(allowed=False, retry_after_seconds=retry_after)

            bucket.append(now)
            return RateLimitResult(allowed=True, retry_after_seconds=0)

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()


class RedisRateLimitBackend:
    def __init__(self, redis_url: str) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)

    def check(self, key: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        member = f"{now}:{uuid4().hex}"
        allowed, _, retry_after = self._redis.eval(
            _REDIS_RATE_LIMIT_SCRIPT,
            1,
            key,
            str(now),
            str(window_seconds),
            str(limit),
            member,
        )
        return RateLimitResult(allowed=bool(int(allowed)), retry_after_seconds=int(retry_after))

    def reset(self) -> None:
        return None


class RateLimiter:
    def __init__(self) -> None:
        self._memory_backend = InMemoryRateLimitBackend()
        self._backend = self._build_backend()

    def dependency(self, limit: int, window_seconds: int = 60, scope: str | None = None) -> Callable[[Request], None]:
        def _limit_request(request: Request) -> None:
            endpoint_scope = scope or self._resolve_scope(request)
            client_ip = self._get_client_ip(request)
            key = f"rate-limit:{endpoint_scope}:{client_ip}"

            try:
                result = self._backend.check(key, limit=limit, window_seconds=window_seconds)
            except RedisError:
                result = self._memory_backend.check(key, limit=limit, window_seconds=window_seconds)

            if result.allowed:
                return

            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(result.retry_after_seconds)},
            )

        return _limit_request

    def reset(self) -> None:
        self._memory_backend.reset()

    def _build_backend(self):
        backend_name = os.getenv(RATE_LIMIT_BACKEND_ENV_VAR, "").strip().lower()
        redis_url = os.getenv(RATE_LIMIT_REDIS_URL_ENV_VAR) or os.getenv(REDIS_URL_ENV_VAR)

        if backend_name == "redis" and redis_url:
            return RedisRateLimitBackend(redis_url)

        if backend_name in {"", "memory"}:
            return self._memory_backend

        if redis_url and backend_name != "memory":
            return RedisRateLimitBackend(redis_url)

        return self._memory_backend

    @staticmethod
    def _get_client_ip(request: Request) -> str:
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            first_hop = forwarded_for.split(",")[0].strip()
            if first_hop:
                return first_hop

        if request.client and request.client.host:
            return request.client.host

        return "unknown"

    @staticmethod
    def _resolve_scope(request: Request) -> str:
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        return f"{request.method}:{route_path}"


rate_limiter = RateLimiter()
