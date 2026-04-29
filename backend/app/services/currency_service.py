"""Currency conversion service using Finnhub Forex API.

Provides real-time exchange rates with Redis caching (1h TTL).
Falls back to hardcoded approximate rates when the API is unavailable.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

from app.config import get_settings
from app.core.redis import cache_get, cache_set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Currency definitions
# ---------------------------------------------------------------------------

class Currency(str, Enum):
    """Supported currencies for conversion."""

    USD = "USD"
    EUR = "EUR"
    CNY = "CNY"
    GBP = "GBP"
    JPY = "JPY"
    HKD = "HKD"
    CHF = "CHF"
    AUD = "AUD"
    CAD = "CAD"
    SGD = "SGD"


CURRENCY_SYMBOLS: Dict[Currency, str] = {
    Currency.USD: "$",
    Currency.EUR: "\u20ac",
    Currency.CNY: "\u00a5",
    Currency.GBP: "\u00a3",
    Currency.JPY: "\u00a5",
    Currency.HKD: "HK$",
    Currency.CHF: "CHF",
    Currency.AUD: "A$",
    Currency.CAD: "C$",
    Currency.SGD: "S$",
}

CURRENCY_NAMES: Dict[Currency, Dict[str, str]] = {
    Currency.USD: {"en": "US Dollar", "zh": "\u7f8e\u5143"},
    Currency.EUR: {"en": "Euro", "zh": "\u6b27\u5143"},
    Currency.CNY: {"en": "Chinese Yuan", "zh": "\u4eba\u6c11\u5e01"},
    Currency.GBP: {"en": "British Pound", "zh": "\u82f1\u938a"},
    Currency.JPY: {"en": "Japanese Yen", "zh": "\u65e5\u5143"},
    Currency.HKD: {"en": "Hong Kong Dollar", "zh": "\u6e2f\u5e01"},
    Currency.CHF: {"en": "Swiss Franc", "zh": "\u745e\u58eb\u6cd5\u90ce"},
    Currency.AUD: {"en": "Australian Dollar", "zh": "\u6fb3\u5143"},
    Currency.CAD: {"en": "Canadian Dollar", "zh": "\u52a0\u5143"},
    Currency.SGD: {"en": "Singapore Dollar", "zh": "\u65b0\u5143"},
}

# Fallback rates (approximate, used when API is unavailable)
FALLBACK_RATES: Dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,
    "CNY": 7.25,
    "GBP": 0.79,
    "JPY": 149.5,
    "HKD": 7.82,
    "CHF": 0.88,
    "AUD": 1.53,
    "CAD": 1.36,
    "SGD": 1.34,
}


# Redis cache key and TTL
_CACHE_KEY = "ds:forex:rates:usd"
_CACHE_TTL = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Core rate fetching
# ---------------------------------------------------------------------------

async def get_exchange_rates(use_fallback: bool = True) -> Dict[str, float]:
    """Get exchange rates from USD to other currencies.

    Uses Redis cache (1h TTL) to minimise Finnhub API calls.
    Falls back to hardcoded approximate rates on failure.

    Returns:
        Dict mapping currency code to rate from USD.
        E.g. {"EUR": 0.92, "CNY": 7.25, ...}
    """
    # Try cache first
    cached = await cache_get(_CACHE_KEY)
    if cached and isinstance(cached, dict):
        logger.debug("Using cached exchange rates")
        return cached

    # Fetch from Finnhub
    from app.core.api_keys import get_next_api_key, mark_key_rate_limited
    api_key = get_next_api_key("finnhub")
    if not api_key:
        logger.warning("Finnhub API key not configured, using fallback rates")
        return dict(FALLBACK_RATES) if use_fallback else {}

    try:
        url = f"https://finnhub.io/api/v1/forex/rates?base=USD&token={api_key}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        raw_rates = data.get("quote", {})
        if not raw_rates:
            logger.warning("Empty rates response from Finnhub")
            return dict(FALLBACK_RATES) if use_fallback else {}

        # Convert to plain float dict for JSON serialization
        rates = {k: float(v) for k, v in raw_rates.items()}
        logger.info("Fetched exchange rates from Finnhub: %d currencies", len(rates))

        # Cache the rates
        await cache_set(_CACHE_KEY, rates, _CACHE_TTL)
        return rates

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            mark_key_rate_limited("finnhub", api_key)
        logger.error("Finnhub API error: %d - %s", e.response.status_code, e.response.text)
    except httpx.RequestError as e:
        logger.error("Finnhub request error: %s", e)
    except Exception as e:
        logger.error("Failed to fetch exchange rates: %s", e)

    if use_fallback:
        logger.warning("Using fallback exchange rates")
        return dict(FALLBACK_RATES)
    return {}


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

async def convert_to_currency(
    usd_amount: float,
    target_currency: str,
    precision: int = 2,
) -> Optional[float]:
    """Convert USD amount to target currency.

    Args:
        usd_amount: Amount in USD.
        target_currency: Target currency code (e.g. "EUR").
        precision: Decimal places to round to.

    Returns:
        Converted amount, or None if rate unavailable.
    """
    if target_currency.upper() == "USD":
        return round(usd_amount, precision)

    rates = await get_exchange_rates()
    rate = rates.get(target_currency.upper())
    if rate is None:
        logger.warning("No rate for %s, returning None", target_currency)
        return None

    converted = Decimal(str(usd_amount)) * Decimal(str(rate))
    quantize_str = "0." + "0" * precision
    result = converted.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
    return float(result)


async def convert_from_currency(
    amount: float,
    source_currency: str,
    precision: int = 2,
) -> Optional[float]:
    """Convert amount from source currency to USD.

    Args:
        amount: Amount in source currency.
        source_currency: Source currency code (e.g. "EUR").
        precision: Decimal places to round to.

    Returns:
        Converted USD amount, or None if rate unavailable.
    """
    if source_currency.upper() == "USD":
        return round(amount, precision)

    rates = await get_exchange_rates()
    rate = rates.get(source_currency.upper())
    if rate is None or rate == 0:
        logger.warning("No rate for %s, returning None", source_currency)
        return None

    converted = Decimal(str(amount)) / Decimal(str(rate))
    quantize_str = "0." + "0" * precision
    result = converted.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
    return float(result)


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------

def get_supported_currencies() -> List[Dict[str, Any]]:
    """Return list of supported currencies with metadata."""
    result = []
    for c in Currency:
        result.append({
            "code": c.value,
            "symbol": CURRENCY_SYMBOLS.get(c, c.value),
            "name_en": CURRENCY_NAMES.get(c, {}).get("en", c.value),
            "name_zh": CURRENCY_NAMES.get(c, {}).get("zh", c.value),
        })
    return result
