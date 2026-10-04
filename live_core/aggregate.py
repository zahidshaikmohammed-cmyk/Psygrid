"""Exchange of current RAM state between Live Core nodes.

Wire format of ``GET /internal/live-core/fragments?start=S&end=E`` (canonical universe indices):

    line 1:  JSON header (node id/count, partition and universe fingerprints, session state)
    line n:  ``<universe_index>\\t<SYMBOL>\\t<stock JSON object>``

Each stock line is the exact JSON object that goes under ``stocks`` in ``/public/live.json``, so
the aggregating node splices them into its response as bytes: nothing is parsed or re-encoded,
and only the peer's *current session* (completed candles so far today) ever crosses the wire.
There is no historical dataset to transfer.

A peer that is down never takes the aggregating node down: fetches use a short timeout, a
recent good answer is reused for ``stale_seconds``, and after that the peer's stocks are
reported as missing (``status: PARTIAL``) - never invented.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from urllib.parse import quote

import orjson

FRAGMENTS_PATH = "/internal/live-core/fragments"
NODE_HEALTH_PATH = "/health/node"


class PeerUnavailable(RuntimeError):
    """The peer could not supply a usable answer."""


def encode_fragments(header: dict, items: list[tuple[int, str, bytes]]) -> bytes:
    parts = [orjson.dumps(header), b"\n"]
    for index, symbol, fragment in items:
        parts.append(b"%d\t%s\t%s\n" % (index, symbol.encode("utf-8"), fragment))
    return b"".join(parts)


def decode_fragments(body: bytes) -> tuple[dict, list[tuple[int, str, bytes]]]:
    head, _, rest = body.partition(b"\n")
    try:
        header = orjson.loads(head)
    except orjson.JSONDecodeError as exc:
        raise PeerUnavailable("peer fragments header is not JSON") from exc
    if not isinstance(header, dict):
        raise PeerUnavailable("peer fragments header is not an object")
    items: list[tuple[int, str, bytes]] = []
    for line in rest.split(b"\n"):
        if not line:
            continue
        index, sep1, remainder = line.partition(b"\t")
        symbol, sep2, fragment = remainder.partition(b"\t")
        if not sep1 or not sep2 or not fragment.startswith(b"{") or not fragment.endswith(b"}"):
            raise PeerUnavailable("peer fragments body is malformed")
        try:
            items.append((int(index), symbol.decode("utf-8"), fragment))
        except (ValueError, UnicodeDecodeError) as exc:
            raise PeerUnavailable("peer fragments line is malformed") from exc
    if not isinstance(header.get("item_count"), int) or header["item_count"] != len(items):
        raise PeerUnavailable("peer fragments body is truncated")
    return header, items


def _requests_get(session_holder: dict, url: str, timeout: float) -> tuple[int, bytes]:
    import requests

    session = session_holder.get("session")
    if session is None:
        session = session_holder["session"] = requests.Session()
    response = session.get(url, timeout=timeout, headers={"User-Agent": "psygrid-live-core-peer"})
    return response.status_code, response.content


@dataclass
class PeerFragments:
    header: dict
    items: list[tuple[int, str, bytes]]
    fetched_monotonic: float
    stale: bool = False

    def age_seconds(self, now: float) -> float:
        return round(max(0.0, now - self.fetched_monotonic), 3)


class PeerClient:
    """Fetches one peer's current state; bounded, cached and failure-tolerant."""

    def __init__(
        self,
        node_id: int,
        base_url: str,
        *,
        expected_partition: dict,
        timeout_seconds: float = 4.0,
        cache_seconds: float = 1.0,
        stale_seconds: float = 15.0,
        get=None,
        monotonic=time.monotonic,
    ):
        self.node_id = node_id
        self.base_url = base_url.rstrip("/")
        self.expected_partition = expected_partition
        self.timeout_seconds = timeout_seconds
        self.cache_seconds = cache_seconds
        self.stale_seconds = stale_seconds
        self._session_holder: dict = {}
        self._get = get or (lambda url, timeout: _requests_get(self._session_holder, url, timeout))
        self._monotonic = monotonic
        # One fetch at a time per peer: concurrent requests reuse the result instead of multiplying load.
        self._fetch_lock = threading.Lock()
        self._snapshot: PeerFragments | None = None
        self._health: tuple[float, dict] | None = None
        self.failures = 0
        self.last_error = ""
        self.last_success_monotonic: float | None = None

    def _verify(self, header: dict) -> None:
        expected = self.expected_partition
        mismatches = [
            key
            for key in ("node_id", "node_count", "start_index", "end_index", "fingerprint", "universe_fingerprint")
            if header.get("partition", {}).get(key) != expected.get(key)
        ]
        if mismatches:
            raise PeerUnavailable(
                f"peer node {self.node_id} serves a different partition/universe ({', '.join(mismatches)}); "
                "both nodes must run the same stocks.json and LIVE_CORE_NODE_COUNT"
            )

    def snapshot(self) -> PeerFragments:
        """The peer's whole partition. Refetched only when its content version changed.

        A poll sends ``if_version``; an unchanged peer answers with a header-only ``not_modified``
        reply, so steady-state polling moves a few hundred bytes, not the session's candles.
        """
        with self._fetch_lock:
            now = self._monotonic()
            cached = self._snapshot
            if cached is not None and not cached.stale and now - cached.fetched_monotonic <= self.cache_seconds:
                return cached
            start = self.expected_partition["start_index"]
            end = self.expected_partition["end_index"]
            url = f"{self.base_url}{FRAGMENTS_PATH}?start={start}&end={end}"
            if cached is not None:
                url += f"&if_version={quote(str(cached.header.get('version', '')), safe='')}"
            try:
                status, body = self._get(url, self.timeout_seconds)
                if status != 200:
                    raise PeerUnavailable(f"HTTP {status} from {url}")
                header, items = decode_fragments(body)
                self._verify(header)
                if header.get("not_modified"):
                    if cached is None or header.get("version") != cached.header.get("version"):
                        raise PeerUnavailable("peer answered not_modified for a version this node does not hold")
                    items = cached.items
                    header = {**header, "item_count": len(items)}
            except Exception as exc:
                self.failures += 1
                self.last_error = f"{type(exc).__name__}: {exc}"[:300]
                if cached is not None and now - cached.fetched_monotonic <= self.stale_seconds:
                    cached.stale = True
                    return cached
                self._snapshot = None
                raise PeerUnavailable(self.last_error) from exc
            result = PeerFragments(header, items, self._monotonic())
            self._snapshot = result
            self.last_success_monotonic = result.fetched_monotonic
            self.last_error = ""
            return result

    def forget(self) -> None:
        """Drop the cached peer snapshot (session end): no market data outlives the session."""
        with self._fetch_lock:
            self._snapshot = None

    def fragments(self, start: int, end: int) -> PeerFragments:
        """The peer's stocks inside the canonical range ``[start, end)``, sliced from its snapshot."""
        snapshot = self.snapshot()
        if start <= self.expected_partition["start_index"] and end >= self.expected_partition["end_index"]:
            return snapshot
        items = [item for item in snapshot.items if start <= item[0] < end]
        return PeerFragments(snapshot.header, items, snapshot.fetched_monotonic, snapshot.stale)

    def stock(self, symbol: str) -> tuple[int, bytes]:
        url = f"{self.base_url}/public/stock/{quote(symbol, safe='')}.json?local=1"
        try:
            status, body = self._get(url, self.timeout_seconds)
        except Exception as exc:
            self.failures += 1
            self.last_error = f"{type(exc).__name__}: {exc}"[:300]
            raise PeerUnavailable(self.last_error) from exc
        if status >= 500:
            raise PeerUnavailable(f"HTTP {status} from {url}")
        return status, body

    def health(self, cache_seconds: float = 5.0, timeout_seconds: float = 2.0) -> dict:
        now = self._monotonic()
        cached = self._health
        if cached is not None and now - cached[0] <= cache_seconds:
            return cached[1]
        url = f"{self.base_url}{NODE_HEALTH_PATH}"
        try:
            status, body = self._get(url, min(timeout_seconds, self.timeout_seconds))
            payload = orjson.loads(body)
            if status != 200 or not isinstance(payload, dict):
                raise PeerUnavailable(f"HTTP {status} from {url}")
            result = {"reachable": True, "health": payload, "error": ""}
        except Exception as exc:
            result = {"reachable": False, "health": None, "error": f"{type(exc).__name__}: {exc}"[:300]}
        self._health = (self._monotonic(), result)
        return result

    def status(self) -> dict:
        now = self._monotonic()
        return {
            "node_id": self.node_id,
            "base_url": self.base_url,
            "failures": self.failures,
            "last_error": self.last_error,
            "last_success_age_seconds": round(now - self.last_success_monotonic, 3)
            if self.last_success_monotonic is not None
            else None,
        }
