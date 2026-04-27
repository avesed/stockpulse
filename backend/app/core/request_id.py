"""Per-request ID for distributed tracing.

Reads X-Request-ID from incoming requests or generates a new one.
Provides a logging filter to inject request_id into log records.
"""

import contextvars
import logging
import uuid
from typing import Optional

request_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "request_id", default=None
)


def generate_request_id() -> str:
    """Generate a new request ID (UUID4 hex, 32 chars)."""
    return uuid.uuid4().hex


def get_request_id() -> Optional[str]:
    """Get the full request ID for the current context."""
    return request_id_var.get()


def get_request_id_short() -> str:
    """Get first 8 chars of request ID for log output, or '-' if unset."""
    rid = request_id_var.get()
    return rid[:8] if rid else "-"


class RequestIdFilter(logging.Filter):
    """Inject request_id into every log record."""

    def filter(self, record):
        record.request_id = get_request_id_short()
        return True


class RequestIdMiddleware:
    """Pure ASGI middleware that extracts or generates X-Request-ID."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        rid = None
        for name, value in scope.get("headers", []):
            if name == b"x-request-id":
                rid = value.decode("latin-1")
                break
        if not rid:
            rid = generate_request_id()

        token = request_id_var.set(rid)
        try:
            await self.app(scope, receive, send)
        finally:
            request_id_var.reset(token)
