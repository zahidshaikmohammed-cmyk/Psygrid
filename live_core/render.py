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
MAX_SYMBOL_LENGTH = 32


def _timestamp_bytes(epoch: int) -> bytes:
    cached = _TIMESTAMP_CACHE.get(epoch)
    if cached is None:
        if len(_TIMESTAMP_CACHE) > 4096:
            _TIMESTAMP_CACHE.clear()
        cached = orjson.dumps(_ist_timestamp(epoch))
        _TIMESTAMP_CACHE[epoch] = cached
    return cached


def clear_caches() -> None:
    """Drop the minute->timestamp string cache (called at session end with the market state)."""
    _TIMESTAMP_CACHE.clear()


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
    (historical merge, reference price change) re-encodes the stock once.
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


def safe_fragment(state, series) -> bytes:
    """``stock_fragment`` that can never fail the whole response because of one stock.

    If encoding this stock raises, its last good encoding is served (or, if it never had one, its
    identity with no candles), the failure is counted, and every other stock is unaffected.
    """
    try:
        return stock_fragment(state, series)
    except Exception as exc:
        with state.lock:
            state.render_errors += 1
            if state.render_errors <= 3:
                state.record_error(f"render {series.symbol}: {type(exc).__name__}: {exc}")
        if series._fragment is not None:
            return series._fragment
        return b'{"symbol":%s,"security_id":%s,"previous_close":null,"today_open":null,"candles_1m":[]}' % (
            orjson.dumps(series.symbol),
            orjson.dumps(series.security_id),
        )


class Snapshot:
    """A coherent view of this node's stocks: every fragment and the version are from one instant."""

    __slots__ = ("items", "session_date", "session_status", "version")

    def __init__(self, version: str, session_status: str, session_date: str | None, items):
        self.version = version
        self.session_status = session_status
        self.session_date = session_date
        self.items = items


def snapshot(state, start: int, end: int) -> Snapshot:
    """Snapshot of this node's stocks in the canonical range ``[start, end)``.

    Phase 1 brings every stock's cached encoding up to date while the feed keeps running (the lock
    is taken per stock). Phase 2 takes the lock once and captures the version, session state and
    every fragment together, re-encoding only the few stocks that changed in between. A response
    therefore never mixes stocks from different moments, and no full deep copy is made: fragments
    are immutable ``bytes`` shared by reference.
    """
    with state.lock:
        selected = [series for series in state.ordered if start <= series.index < end]
    for series in selected:
        safe_fragment(state, series)
    with state.lock:
        selected = [series for series in state.ordered if start <= series.index < end]
        items = [(series.index, series.symbol, safe_fragment(state, series)) for series in selected]
        return Snapshot(f"{state.instance}:{state.version}", state.session_status, state.session_date, items)


def local_fragments(state, start: int, end: int) -> list[tuple[int, str, bytes]]:
    """``(universe_index, symbol, fragment)`` for this node's stocks inside the canonical range."""
    return snapshot(state, start, end).items


_CANDLES_KEY = b'"candles_1m":['
_CANDLE_START = b'{"timestamp":'
MAX_LATEST_CANDLES = 60


def fragment_last(fragment: bytes, count: int) -> bytes:
    """The stock's JSON object with only its last ``count`` completed candles (a byte slice).

    Stock fragments end with ``"candles_1m":[{...},...]}`` and a candle object never nests, so the
    last ``count`` candles are found by searching backwards for their opening bytes - no JSON is
    parsed or re-encoded. Anything unexpected falls back to a parse, never to a wrong slice.
    """
    count = max(0, count)
    head_end = fragment.rfind(_CANDLES_KEY)
    if head_end < 0 or not fragment.endswith(b"]}"):
        try:
            data = orjson.loads(fragment)
            candles = data.get("candles_1m") or []
            data["candles_1m"] = candles[len(candles) - count :] if count else []
            return orjson.dumps(data)
        except Exception:
            return fragment
    body_start = head_end + len(_CANDLES_KEY)
    body_end = len(fragment) - 2
    if count == 0 or body_start >= body_end:
        return fragment[:body_start] + b"]}"
    position = body_end
    for _ in range(count):
        found = fragment.rfind(_CANDLE_START, body_start, position)
        if found < 0:
            return fragment  # fewer candles than asked for: all of them
        position = found
    return fragment[:body_start] + fragment[position:body_end] + b"]}"


def payload_status(session_status: str) -> str:
    return "OK" if session_status == "LIVE" else session_status


def assemble_head(
    *,
    status: str,
    session_status: str,
    session_date: str | None,
    universe_size: int,
    stock_count: int,
    extra: dict | None = None,
) -> bytes:
    """Everything before the stocks object, ending with ``,"stocks":{`` (rebuilt for every response)."""
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
        "stock_count": stock_count,
        "data_policy": DATA_POLICY,
        "synthetic_candles": False,
    }
    if extra:
        head.update(extra)
    return orjson.dumps(head)[:-1] + b',"stocks":{'


def tail_parts(items: list[tuple[int, str, bytes]], *, sort_by_symbol: bool) -> list[bytes]:
    """The stocks object's members and closing braces as a list of byte pieces.

    Each stock fragment appears by reference (no copy); fragments are never re-parsed.
    """
    items = sorted(items, key=lambda item: item[1] if sort_by_symbol else item[0])
    parts = []
    for position, (_index, symbol, fragment) in enumerate(items):
        parts.append(b"%s%s:" % (b"," if position else b"", orjson.dumps(symbol)))
        parts.append(fragment)
    parts.append(b"}}\n")
    return parts


def assemble_tail(items: list[tuple[int, str, bytes]], *, sort_by_symbol: bool) -> bytes:
    """The stocks object's members and the closing braces as one ``bytes``."""
    return b"".join(tail_parts(items, sort_by_symbol=sort_by_symbol))


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
    head = assemble_head(
        status=status,
        session_status=session_status,
        session_date=session_date,
        universe_size=universe_size,
        stock_count=len(items),
        extra=extra,
    )
    return head + assemble_tail(items, sort_by_symbol=sort_by_symbol)


def stock_body(state, symbol: str) -> bytes:
    """``/public/stock/{symbol}.json`` for a locally owned stock (same shape as ``output.stock_json``)."""
    symbol = symbol.upper()[:MAX_SYMBOL_LENGTH]  # an unknown symbol is echoed back, bounded
    with state.lock:
        series = state.by_symbol.get(symbol)
    if series is None:
        return orjson.dumps(
            {"service": "PSYGRID", "symbol": symbol, "status": "NOT_FOUND"}, option=orjson.OPT_APPEND_NEWLINE
        )
    fragment = safe_fragment(state, series)
    return b'{"service":"PSYGRID","schema_version":"4.0","status":"OK",' + fragment[1:] + b"\n"
