"""Persistent multiprocessing pool for all yfinance calls.

Isolates yfinance in subprocesses to prevent memory leaks from accumulating
in the main FastAPI process.  Workers are automatically recycled after
``maxtasksperchild`` tasks, which caps per-worker memory growth.

Architecture::

    FastAPI async method
      → provider_queue.submit()          # rate-limit + priority
        → ThreadPool thread              # blocks on IPC, no CPU
          → yf_call(method, kwargs_dict) # sync, calls pool.apply_async
            → mp.Pool worker             # yfinance runs here (isolated)
              → _yf_dispatch(method, kw) # module-level, picklable
            ← serializable result
          ← result
        ← result
      ← result

All yfinance data stays serializable (dicts, lists, primitives).
DataFrames are converted to records before crossing the process boundary.
"""
from __future__ import annotations

import asyncio
import logging
import math
import multiprocessing as mp
import os
import threading
from functools import partial
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Worker-side globals (live inside each subprocess)
# ---------------------------------------------------------------------------
_yf = None  # yfinance module
_pd = None  # pandas module


def _yf_worker_init(proxy: Optional[str]) -> None:
    """Called once per worker process at fork time.

    Patches curl_cffi.Session.request so every HTTP call in this process
    goes through the proxy — no per-Ticker configuration needed.
    """
    global _yf, _pd
    if proxy:
        from curl_cffi.requests import Session
        _orig_request = Session.request

        def _proxied_request(self, *args, **kwargs):
            if "proxy" not in kwargs:
                kwargs["proxy"] = proxy
            return _orig_request(self, *args, **kwargs)

        Session.request = _proxied_request

    import yfinance
    import pandas
    _yf = yfinance
    _pd = pandas
    # Invalidate cookie cache so each Ticker gets a fresh cookie from Yahoo.
    # Must store None (not {}) — storing {} causes _load_cookie_curlCffi to
    # find a non-None entry with empty 'cookie' dict, then crash on keys()[0].
    try:
        yfinance.cache.get_cookie_cache().store('curlCffi', None)
    except Exception:
        pass


def _nan_safe(v: Any) -> Any:
    """Convert float NaN/Inf to None for JSON-safe serialization."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _df_to_records(df, date_col: str = "index") -> list[dict]:
    """Convert a DataFrame to a list of dicts with NaN → None."""
    if df is None or df.empty:
        return []
    records = []
    for idx, row in df.iterrows():
        rec = {}
        if date_col == "index":
            rec["_index"] = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)
        for col in df.columns:
            rec[col] = _nan_safe(row[col])
        records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Dispatch table — runs inside subprocess
# ---------------------------------------------------------------------------

def _yf_dispatch(method: str, kw: dict) -> Any:
    """Module-level dispatcher. Must be picklable (no closures)."""
    try:
        if method == "ticker_info":
            return _do_ticker_info(kw)
        elif method == "ticker_history":
            return _do_ticker_history(kw)
        elif method == "ticker_news":
            return _do_ticker_news(kw)
        elif method == "ticker_institutional":
            return _do_ticker_institutional(kw)
        elif method == "ticker_insider_tx":
            return _do_ticker_insider_tx(kw)
        elif method == "ticker_insider_purchases":
            return _do_ticker_insider_purchases(kw)
        elif method == "ticker_options_chain":
            return _do_ticker_options_chain(kw)
        elif method == "ticker_options_detail":
            return _do_ticker_options_detail(kw)
        elif method == "ticker_upgrades":
            return _do_ticker_upgrades(kw)
        elif method == "ticker_valuation":
            return _do_ticker_valuation(kw)
        elif method == "ticker_market_index":
            return _do_ticker_market_index(kw)
        elif method == "ticker_data_get":
            return _do_ticker_data_get(kw)
        elif method == "batch_download":
            return _do_batch_download(kw)
        else:
            raise ValueError(f"Unknown yf dispatch method: {method}")
    except Exception:
        raise  # let mp.Pool propagate the exception to the caller



def _clear_cache(ticker) -> None:
    """Clear yfinance internal caches: LRU data cache + cookie/crumb session state.

    Clearing cookie forces the next Ticker to re-authenticate with Yahoo,
    getting a fresh session cookie. This prevents Yahoo from throttling
    a single session that has made too many requests.
    """
    try:
        ticker._data.cache_get.cache_clear()
    except Exception:
        pass
    try:
        ticker._data._cookie = None
        ticker._data._crumb = None
        ticker._data._session.cookies.clear()
        # Store None (not {}) — storing {} leaves a non-None cache entry
        # with empty 'cookie' dict, causing _load_cookie_curlCffi to crash
        # on list(cookies.keys())[0] when cookies is empty.
        _yf.cache.get_cookie_cache().store('curlCffi', None)
    except Exception:
        pass


def _do_ticker_info(kw: dict) -> Optional[dict]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    info = ticker.info
    _clear_cache(ticker)
    return info if info else None


def _do_ticker_history(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)

    period = kw.get("period")
    interval = kw.get("interval", "1d")
    start = kw.get("start")
    end = kw.get("end")

    if start and end:
        yf_start = start[:10] if len(start) > 10 else start
        yf_end = end[:10] if len(end) > 10 else end
        if yf_start == yf_end:
            from datetime import datetime as _dt, timedelta as _td
            yf_end = (_dt.strptime(yf_end, "%Y-%m-%d") + _td(days=1)).strftime("%Y-%m-%d")
        df = ticker.history(start=yf_start, end=yf_end, interval=interval)
    else:
        df = ticker.history(period=period, interval=interval)

    _clear_cache(ticker)

    if df is None or df.empty:
        return None

    df = df.dropna(subset=["Open", "High", "Low", "Close"])

    bars = []
    for idx, row in df.iterrows():
        bars.append({
            "date": idx.to_pydatetime().isoformat() if hasattr(idx, "to_pydatetime") else str(idx),
            "open": _nan_safe(row["Open"]),
            "high": _nan_safe(row["High"]),
            "low": _nan_safe(row["Low"]),
            "close": _nan_safe(row["Close"]),
            "volume": int(row["Volume"]) if _pd.notna(row["Volume"]) else 0,
        })
    return bars


def _do_ticker_news(kw: dict) -> list[dict]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    news = ticker.news or []
    _clear_cache(ticker)
    return news


def _do_ticker_institutional(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    df = ticker.institutional_holders
    _clear_cache(ticker)
    if df is None or df.empty:
        return None
    return _df_to_records(df)


def _do_ticker_insider_tx(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    df = ticker.insider_transactions
    _clear_cache(ticker)
    if df is None or df.empty:
        return None
    return _df_to_records(df)


def _do_ticker_insider_purchases(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    df = ticker.insider_purchases
    _clear_cache(ticker)
    if df is None or df.empty:
        return None
    return _df_to_records(df)


def _do_ticker_options_chain(kw: dict) -> Optional[dict]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    expiries = ticker.options
    if not expiries:
        _clear_cache(ticker)
        return None

    nearest = expiries[0]
    chain = ticker.option_chain(nearest)
    _clear_cache(ticker)

    call_vol = int(chain.calls["volume"].fillna(0).sum())
    put_vol = int(chain.puts["volume"].fillna(0).sum())
    call_oi = int(chain.calls["openInterest"].fillna(0).sum())
    put_oi = int(chain.puts["openInterest"].fillna(0).sum())

    pc_ratio = round(put_vol / call_vol, 4) if call_vol > 0 else None
    pc_oi_ratio = round(put_oi / call_oi, 4) if call_oi > 0 else None

    return {
        "put_volume": put_vol,
        "call_volume": call_vol,
        "put_call_ratio": pc_ratio,
        "put_oi": put_oi,
        "call_oi": call_oi,
        "put_call_oi_ratio": pc_oi_ratio,
        "expiry": nearest,
    }


def _do_ticker_options_detail(kw: dict) -> Optional[dict]:
    """Full options chain for a specific expiry (or nearest if not specified)."""
    symbol = kw["symbol"]
    expiry = kw.get("expiry")

    ticker = _yf.Ticker(symbol)
    expiries = ticker.options
    if not expiries:
        _clear_cache(ticker)
        return None

    target_expiry = expiry if expiry and expiry in expiries else expiries[0]
    chain = ticker.option_chain(target_expiry)
    _clear_cache(ticker)

    def _contracts(df) -> list[dict]:
        if df is None or df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            ltd = row.get("lastTradeDate")
            if hasattr(ltd, "isoformat"):
                ltd = ltd.isoformat()
            elif ltd is not None:
                ltd = str(ltd)
            records.append({
                "contract_symbol": str(row.get("contractSymbol", "")),
                "strike": _nan_safe(row.get("strike")),
                "last_price": _nan_safe(row.get("lastPrice")),
                "bid": _nan_safe(row.get("bid")),
                "ask": _nan_safe(row.get("ask")),
                "change": _nan_safe(row.get("change")),
                "percent_change": _nan_safe(row.get("percentChange")),
                "volume": int(row["volume"]) if _pd.notna(row.get("volume")) else None,
                "open_interest": int(row["openInterest"]) if _pd.notna(row.get("openInterest")) else None,
                "implied_volatility": _nan_safe(row.get("impliedVolatility")),
                "in_the_money": bool(row["inTheMoney"]) if row.get("inTheMoney") is not None else None,
                "last_trade_date": ltd,
            })
        return records

    return {
        "symbol": symbol,
        "expiries": list(expiries),
        "chain": {
            "expiry": target_expiry,
            "calls": _contracts(chain.calls),
            "puts": _contracts(chain.puts),
        },
    }


def _do_ticker_upgrades(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    df = ticker.upgrades_downgrades
    _clear_cache(ticker)
    if df is None or df.empty:
        return None
    df = df.head(200)
    results = []
    for idx, row in df.iterrows():
        results.append({
            "date": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
            "Firm": _nan_safe(row.get("Firm")),
            "ToGrade": _nan_safe(row.get("ToGrade")),
            "FromGrade": _nan_safe(row.get("FromGrade")),
            "Action": _nan_safe(row.get("Action")),
        })
    return results


def _do_ticker_valuation(kw: dict) -> Optional[list[dict]]:
    symbol = kw["symbol"]
    ticker = _yf.Ticker(symbol)
    df = ticker.get_valuation_measures()
    _clear_cache(ticker)
    if df is None or df.empty:
        return None

    metric_map = {
        "Market Cap": "market_cap", "Enterprise Value": "enterprise_value",
        "Trailing P/E": "trailing_pe", "Forward P/E": "forward_pe",
        "PEG Ratio (5yr expected)": "peg_ratio", "Price/Sales": "price_to_sales",
        "Price/Book": "price_to_book", "Enterprise Value/Revenue": "ev_to_revenue",
        "Enterprise Value/EBITDA": "ev_to_ebitda",
    }

    def _parse_val(v):
        if v is None or (isinstance(v, float) and _pd.isna(v)):
            return None
        s = str(v).strip()
        if not s or s == "—":
            return None
        mult = 1.0
        if s[-1] in "TBMK":
            mult = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}[s[-1]]
            s = s[:-1]
        try:
            return float(s) * mult
        except ValueError:
            return None

    results = []
    for col in df.columns:
        col_str = str(col)
        if col_str.lower() == "current":
            continue
        entry: dict[str, Any] = {"date": col_str}
        for idx_label, key in metric_map.items():
            val = df.loc[idx_label, col] if idx_label in df.index else None
            entry[key] = _parse_val(val)
        results.append(entry)
    return results or None


def _do_ticker_market_index(kw: dict) -> Optional[dict]:
    """Combined history + info for a market index (single subprocess call)."""
    symbol = kw["symbol"]
    period = kw.get("period", "5d")
    ticker = _yf.Ticker(symbol)

    df = ticker.history(period=period)
    if df is None or df.empty:
        _clear_cache(ticker)
        return None

    info = ticker.info
    _clear_cache(ticker)

    name = (info.get("shortName", symbol) if info else symbol)

    bars = []
    for idx, row in df.iterrows():
        bars.append({
            "date": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
        })

    latest_close = bars[-1]["close"] if bars else None
    prev_close = bars[-2]["close"] if len(bars) >= 2 else None
    change_pct = None
    if latest_close and prev_close:
        change_pct = round((latest_close - prev_close) / prev_close * 100, 2)

    return {
        "symbol": symbol,
        "name": name,
        "bars": bars,
        "latest_close": latest_close,
        "change_pct": change_pct,
        "source": "yfinance",
    }


def _do_ticker_data_get(kw: dict) -> Optional[dict]:
    """Raw HTTP via yfinance's internal session (for quoteSummary etc.)."""
    symbol = kw["symbol"]
    url = kw["url"]
    params = kw.get("params", {})
    ticker = _yf.Ticker(symbol)
    resp = ticker._data.get(url=url, params=params)
    _clear_cache(ticker)
    if resp is None:
        return None
    return resp.json()


def _do_batch_download(kw: dict) -> Optional[dict[str, list[dict]]]:
    """yf.download() for multiple tickers, returns serialized bars per ticker."""
    tickers = kw["tickers"]
    start = kw.get("start")
    end = kw.get("end")
    period = kw.get("period")
    auto_adjust = kw.get("auto_adjust", True)

    dl_kwargs: dict[str, Any] = {
        "tickers": tickers,
        "auto_adjust": auto_adjust,
        "progress": False,
    }
    if start:
        dl_kwargs["start"] = start
    if end:
        dl_kwargs["end"] = end
    if period and not start:
        dl_kwargs["period"] = period

    df = _yf.download(**dl_kwargs)
    if df is None or df.empty:
        return None

    is_multi = isinstance(df.columns, _pd.MultiIndex)
    result: dict[str, list[dict]] = {}

    ticker_list = tickers if isinstance(tickers, list) else [tickers]
    for ticker in ticker_list:
        try:
            if is_multi:
                cols_needed = ["Open", "High", "Low", "Close", "Volume"]
                available = [c for c in cols_needed if c in df.columns.get_level_values(0)]
                if "Close" not in available or ticker not in df["Close"].columns:
                    continue
                bars = []
                for idx in df.index:
                    close_val = df["Close"][ticker][idx] if ticker in df["Close"].columns else None
                    if close_val is None or _pd.isna(close_val):
                        continue
                    bar = {
                        "date": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                        "close": float(close_val),
                    }
                    for col in ("Open", "High", "Low"):
                        if col in available and ticker in df[col].columns:
                            v = df[col][ticker][idx]
                            bar[col.lower()] = float(v) if _pd.notna(v) else None
                        else:
                            bar[col.lower()] = None
                    if "Volume" in available and ticker in df["Volume"].columns:
                        v = df["Volume"][ticker][idx]
                        bar["volume"] = int(v) if _pd.notna(v) else None
                    else:
                        bar["volume"] = None
                    bars.append(bar)
                result[ticker] = bars
            else:
                bars = []
                for idx in df.index:
                    close_val = df.get("Close")
                    if close_val is None:
                        continue
                    cv = close_val[idx]
                    if _pd.isna(cv):
                        continue
                    bar = {
                        "date": idx.isoformat() if hasattr(idx, "isoformat") else str(idx),
                        "open": float(df["Open"][idx]) if "Open" in df and _pd.notna(df["Open"][idx]) else None,
                        "high": float(df["High"][idx]) if "High" in df and _pd.notna(df["High"][idx]) else None,
                        "low": float(df["Low"][idx]) if "Low" in df and _pd.notna(df["Low"][idx]) else None,
                        "close": float(cv),
                        "volume": int(df["Volume"][idx]) if "Volume" in df and _pd.notna(df["Volume"][idx]) else None,
                    }
                    bars.append(bar)
                result[ticker] = bars
        except Exception:
            continue

    return result if result else None


# ---------------------------------------------------------------------------
# Pool lifecycle (main process side)
# ---------------------------------------------------------------------------
_pool: Optional[mp.pool.Pool] = None
_pool_lock = threading.Lock()
_pool_proxy: Optional[str] = None
_pool_workers: int = 4
_pool_max_tasks: int = 50


def start_pool(
    workers: int = 4,
    maxtasksperchild: int = 50,
    proxy: Optional[str] = None,
) -> None:
    """Create the persistent yfinance process pool. Call from app lifespan."""
    global _pool, _pool_proxy, _pool_workers, _pool_max_tasks
    with _pool_lock:
        if _pool is not None:
            return
        _pool_workers = workers
        _pool_max_tasks = maxtasksperchild
        _pool_proxy = proxy
        _pool = mp.Pool(
            processes=workers,
            initializer=_yf_worker_init,
            initargs=(proxy,),
            maxtasksperchild=maxtasksperchild or None,
        )
        logger.info(
            "yfinance process pool started: workers=%d, maxtasksperchild=%s, proxy=%s",
            workers, maxtasksperchild, proxy or "none",
        )


def stop_pool() -> None:
    """Terminate the pool. Call from app lifespan shutdown."""
    global _pool
    with _pool_lock:
        if _pool is None:
            return
        try:
            _pool.terminate()
            _pool.join()
        except Exception:
            pass
        _pool = None
        logger.info("yfinance process pool stopped")


def restart_pool(proxy: Optional[str] = None) -> None:
    """Restart the pool (e.g. after proxy config change)."""
    global _pool, _pool_proxy
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.terminate()
                _pool.join()
            except Exception:
                pass
            _pool = None
        _pool_proxy = proxy if proxy is not None else _pool_proxy
        _pool = mp.Pool(
            processes=_pool_workers,
            initializer=_yf_worker_init,
            initargs=(_pool_proxy,),
            maxtasksperchild=_pool_max_tasks,
        )
        logger.info(
            "yfinance process pool restarted: workers=%d, proxy=%s",
            _pool_workers, _pool_proxy or "none",
        )


def _get_pool() -> mp.pool.Pool:
    if _pool is None:
        raise RuntimeError("yfinance process pool not started — call start_pool() first")
    return _pool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def yf_call(method: str, kwargs_dict: dict, timeout: float = 55.0) -> Any:
    """Synchronous: submit work to the process pool, block until result.

    Designed to be called from a ThreadPool thread (via provider_queue).
    The thread blocks on IPC while the subprocess does the yfinance work.
    """
    pool = _get_pool()
    async_result = pool.apply_async(_yf_dispatch, (method, kwargs_dict))
    try:
        return async_result.get(timeout=timeout)
    except mp.TimeoutError:
        raise TimeoutError(f"yfinance subprocess timed out: {method} {kwargs_dict.get('symbol', '')}")


async def yf_call_async(
    method: str,
    kwargs_dict: dict,
    timeout: float = 30.0,
) -> Any:
    """Async wrapper for call sites that bypass provider_queue."""
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(None, yf_call, method, kwargs_dict),
        timeout=timeout,
    )
