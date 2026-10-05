"""Byte-level JSON rendering of the PSYGRID equity contract from a ``NodeState``.

The output is the same contract ``output.py`` produces for the full PSYGRID (schema 4.0):

    {"service": "PSYGRID", "schema_version": "4.0", "status", "session", "universe_size",
     "stock_count", "data_policy", "synthetic_candles", "stocks": {SYMBOL: {"symbol",
     "security_id", "previous_close", "today_open", "candles_1m": [{"timestamp", "open",
     "high", "low", "close", "volume"}]}}}

Only completed candles are published, prices are rounded to 4 places and timestamps are
``YYYY-MM-DD HH:MM:SS IST`` - all via the helpers in ``output.py``. Each stock's JSON fragment is
cached and extended in place as candles complete, so serving a 495-stock partition does not
re-serialize the whole session on every request; it is only concatenated.
"""

from __future__ import annotations

from datetime import datetime

import orjson

from output import PUBLIC_TIMEZONE, PUBLIC_TIMEZONE_NAME, _ist_timestamp, _price

SCHEMA_VERSION = "4.0"
DATA_POLICY = "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN"

_TIMESTAMP_CACHE: dict[int, bytes] = {}


def _timestamp_bytes(epoch: int) -> bytes:
    cached = _TIMESTAMP_CACHE.get(epoch)
    if cached is None:
        if len(_TIMESTAMP_CACHE) > 4096:
            _TIMESTAMP_CACHE.clear()
        cached = orjson.dumps(_ist_timestamp(epoch))
        _TIMESTAMP_CACHE[epoch] = cached
    return cached


def _candle_bytes(series, position: int) -> bytes:
    return b'{"timestamp":%s,"open":%s,"high":%s,"low":%s,"close":%s,"volume":%d}' % (
        _timestamp_bytes(int(series.epochs[position])),
        orjson.dumps(_price(series.opens[position])),
        orjson.dumps(_price(series.highs[position])),
        orjson.dumps(_price(series.lows[position])),
        orjson.dumps(_price(series.closes[position])),
        int(series.volumes[position]),
    )


def stock_fragment(state, series) -> bytes:
    """The stock's ``{"symbol": ..., "candles_1m": [...]}`` object as JSON bytes, cached per revision.

    Only one encoded copy is kept per stock. A newly completed candle is appended to it; a rewrite
    (historical merge, late trade, reference price change) re-encodes the stock once.
    """
    with state.lock:
        if series._fragment is not None and series._fragment_revision == series.revision:
            return series._fragment
        count = len(series.epochs)
        refs = (series.previous_close, series.today_open)
        fragment = series._fragment
        if (
            fragment is not None
            and series._fragment_generation == series.generation
            and series._fragment_refs == refs
            and series._fragment_count <= count
        ):
            if count > series._fragment_count:
                added = b",".join(_candle_bytes(series, p) for p in range(series._fragment_count, count))
                fragment = fragment[:-2] + (b"," if series._fragment_count else b"") + added + b"]}"
        else:
            fragment = b'{"symbol":%s,"security_id":%s,"previous_close":%s,"today_open":%s,"candles_1m":[%s]}' % (
                orjson.dumps(series.symbol),
                orjson.dumps(series.security_id),
                orjson.dumps(_price(series.previous_close)),
                orjson.dumps(_price(series.today_open)),
                b",".join(_candle_bytes(series, p) for p in range(count)),
            )
        series._fragment = fragment
        series._fragment_revision = series.revision
        series._fragment_generation = series.generation
        series._fragment_refs = refs
        series._fragment_count = count
        return fragment


def local_fragments(state, start: int, end: int) -> list[tuple[int, str, bytes]]:
    """``(universe_index, symbol, fragment)`` for this node's stocks inside the canonical range."""
    with state.lock:
        selected = [series for series in state.ordered if start <= series.index < end]
    return [(series.index, series.symbol, stock_fragment(state, series)) for series in selected]


def payload_status(session_status: str) -> str:
    return "OK" if session_status == "LIVE" else session_status


def assemble(
    *,
    status: str,
    session_status: str,
    session_date: str | None,
    universe_size: int,
    items: list[tuple[int, str, bytes]],
    sort_by_symbol: bool,
    extra: dict | None = None,
) -> bytes:
    """Build the full endpoint body from per-stock fragments without re-parsing any of them."""
    items = sorted(items, key=lambda item: item[1] if sort_by_symbol else item[0])
    head = {
        "service": "PSYGRID",
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "session": {
            "status": session_status,
            "date": session_date,
            "timezone": PUBLIC_TIMEZONE_NAME,
            "current_time_ist": datetime.now(PUBLIC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST"),
        },
        "universe_size": universe_size,
        "stock_count": len(items),
        "data_policy": DATA_POLICY,
        "synthetic_candles": False,
    }
    if extra:
        head.update(extra)
    parts = [orjson.dumps(head)[:-1], b',"stocks":{']
    for position, (_index, symbol, fragment) in enumerate(items):
        if position:
            parts.append(b",")
        parts.append(orjson.dumps(symbol))
        parts.append(b":")
        parts.append(fragment)
    parts.append(b"}}\n")
    return b"".join(parts)


def stock_body(state, symbol: str) -> bytes:
    """``/public/stock/{symbol}.json`` for a locally owned stock (same shape as ``output.stock_json``)."""
    symbol = symbol.upper()
    with state.lock:
        series = state.by_symbol.get(symbol)
    if series is None:
        return orjson.dumps(
            {"service": "PSYGRID", "symbol": symbol, "status": "NOT_FOUND"}, option=orjson.OPT_APPEND_NEWLINE
        )
    fragment = stock_fragment(state, series)
    return b'{"service":"PSYGRID","schema_version":"4.0","status":"OK",' + fragment[1:] + b"\n"
