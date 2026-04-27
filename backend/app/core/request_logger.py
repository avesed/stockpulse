"""Request logging middleware — tracks API usage per consumer in Redis.

Stores lightweight counters:
- sp:stats:requests:{consumer_id}:{date} — request count per day (TTL 30d)
- sp:stats:errors:{consumer_id}:{date} — error count per day (TTL 30d)
- sp:stats:latency:{date} — sorted set of response times (TTL 7d)
- sp:stats:total:{date} — total request count per day (TTL 30d)

Best-effort: Redis errors never block API responses.
"""
from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)

_TTL_30D = 30 * 24 * 3600
_TTL_7D = 7 * 24 * 3600


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """Middleware that logs API request metrics to Redis."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Only log API data requests (skip health, auth, admin, docs)
        path = request.url.path
        if not path.startswith("/api/v1/data"):
            return await call_next(request)

        t0 = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        # Extract consumer identity from X-API-Key header (hashed for key)
        api_key = request.headers.get("X-API-Key", "")
        if api_key:
            consumer_id = hashlib.sha256(api_key.encode()).hexdigest()[:16]
        else:
            consumer_id = "anonymous"

        # Fire-and-forget stats update
        try:
            from app.core.redis import get_redis
            r = await get_redis()
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            pipe = r.pipeline(transaction=False)

            # Per-consumer request count
            req_key = f"sp:stats:requests:{consumer_id}:{today}"
            pipe.incr(req_key)
            pipe.expire(req_key, _TTL_30D)

            # Total request count
            total_key = f"sp:stats:total:{today}"
            pipe.incr(total_key)
            pipe.expire(total_key, _TTL_30D)

            # Error count
            if response.status_code >= 400:
                err_key = f"sp:stats:errors:{consumer_id}:{today}"
                pipe.incr(err_key)
                pipe.expire(err_key, _TTL_30D)

            # Latency sample (sorted set, score = response time)
            latency_key = f"sp:stats:latency:{today}"
            pipe.zadd(latency_key, {f"{consumer_id}:{t0}": elapsed_ms})
            pipe.expire(latency_key, _TTL_7D)
            # Trim to keep only latest 10000 samples
            pipe.zremrangebyrank(latency_key, 0, -10001)

            await pipe.execute()
        except Exception:
            pass  # Best-effort, never block

        return response
