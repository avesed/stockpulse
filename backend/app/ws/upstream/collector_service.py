"""Upstream collector lifecycle management.

Only the scheduler leader should run upstream collectors to avoid
duplicate connections to paid WebSocket APIs.
"""
from __future__ import annotations

import logging
from typing import Any

from app.ws.upstream.base import BaseUpstreamCollector

logger = logging.getLogger(__name__)

# Active collector instances
_collectors: dict[str, BaseUpstreamCollector] = {}

# Registry of available collector classes
_COLLECTOR_CLASSES: dict[str, type] = {}


def _ensure_registry() -> None:
    """Lazily populate the collector class registry."""
    if _COLLECTOR_CLASSES:
        return
    from app.ws.upstream.finnhub_collector import FinnhubCollector
    from app.ws.upstream.polygon_collector import PolygonCollector
    from app.ws.upstream.yfinance_collector import YFinanceCollector
    _COLLECTOR_CLASSES["finnhub"] = FinnhubCollector
    _COLLECTOR_CLASSES["polygon"] = PolygonCollector
    _COLLECTOR_CLASSES["yfinance"] = YFinanceCollector


async def start_collector(provider: str, symbols: list[str] | None = None) -> dict[str, Any]:
    """Start an upstream collector for the given provider.

    Returns a result dict with success status and message.
    """
    _ensure_registry()

    if provider in _collectors and _collectors[provider].is_connected:
        # Already running — add new symbols if provided
        if symbols:
            await _collectors[provider].subscribe_symbols(symbols)
            return {
                "success": True,
                "message": f"{provider} collector already running, added {len(symbols)} symbols",
            }
        return {"success": True, "message": f"{provider} collector already running"}

    cls = _COLLECTOR_CLASSES.get(provider)
    if cls is None:
        return {"success": False, "message": f"Unknown provider: {provider}"}

    collector = cls()
    _collectors[provider] = collector

    try:
        await collector.start(symbols)
        return {"success": True, "message": f"{provider} collector started with {len(symbols or [])} symbols"}
    except Exception as e:
        logger.error("Failed to start %s collector: %s", provider, e)
        _collectors.pop(provider, None)
        return {"success": False, "message": f"Failed to start: {e}"}


async def stop_collector(provider: str) -> dict[str, Any]:
    """Stop an upstream collector."""
    collector = _collectors.pop(provider, None)
    if collector is None:
        return {"success": False, "message": f"{provider} collector not running"}

    await collector.stop()
    return {"success": True, "message": f"{provider} collector stopped"}


async def get_all_status() -> list[dict[str, Any]]:
    """Get status of all registered collectors."""
    _ensure_registry()
    statuses = []
    for provider_name in _COLLECTOR_CLASSES:
        collector = _collectors.get(provider_name)
        if collector is not None:
            statuses.append(collector.get_status())
        else:
            statuses.append({
                "provider": provider_name,
                "status": "stopped",
                "symbols_count": 0,
                "message_count": 0,
                "last_message_at": None,
            })
    return statuses


async def stop_all_collectors() -> None:
    """Stop all running collectors. Called during application shutdown."""
    for provider, collector in list(_collectors.items()):
        try:
            await collector.stop()
            logger.info("Stopped %s collector", provider)
        except Exception:
            logger.warning("Error stopping %s collector", provider, exc_info=True)
    _collectors.clear()
