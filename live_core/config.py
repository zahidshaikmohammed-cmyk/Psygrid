"""Live Core node identity, topology and resource settings, read once from the environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlparse


class LiveCoreConfigError(RuntimeError):
    """The node's environment is not a valid Live Core configuration."""


def _int(environ: Mapping[str, str], name: str, default: int | None = None) -> int:
    raw = str(environ.get(name, "")).strip()
    if not raw:
        if default is None:
            raise LiveCoreConfigError(f"Missing required environment variable: {name}")
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise LiveCoreConfigError(f"{name} must be an integer; got {raw!r}") from exc


def _float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw = str(environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise LiveCoreConfigError(f"{name} must be a number; got {raw!r}") from exc
    if value < 0:
        raise LiveCoreConfigError(f"{name} must not be negative; got {raw!r}")
    return value


def _flag(environ: Mapping[str, str], name: str, default: bool) -> bool:
    raw = str(environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def parse_peers(raw: str, node_id: int, node_count: int) -> dict[int, str]:
    """Parse ``LIVE_CORE_PEERS`` (``"1=http://10.0.0.12:10000,2=..."``) into ``{node_id: base_url}``."""
    peers: dict[int, str] = {}
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        key, sep, url = item.partition("=")
        if not sep:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS entry {item!r} must look like '<node_id>=<base_url>'")
        try:
            peer_id = int(key.strip())
        except ValueError as exc:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS node id {key!r} is not an integer") from exc
        url = url.strip().rstrip("/")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS url for node {peer_id} is not an http(s) URL: {url!r}")
        if not 0 <= peer_id < node_count:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS node {peer_id} is outside 0..{node_count - 1}")
        if peer_id == node_id:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS must not list this node ({node_id}) as its own peer")
        if peer_id in peers:
            raise LiveCoreConfigError(f"LIVE_CORE_PEERS lists node {peer_id} twice")
        peers[peer_id] = url
    return peers


def _token_source(environ: Mapping[str, str]) -> str:
    raw = str(environ.get("LIVE_CORE_TOKEN_SOURCE", "")).strip().rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise LiveCoreConfigError(f"LIVE_CORE_TOKEN_SOURCE is not an http(s) URL: {raw!r}")
    return raw


@dataclass(frozen=True)
class LiveCoreConfig:
    node_id: int
    node_count: int
    host: str = "0.0.0.0"
    port: int = 10000
    peers: dict[int, str] = field(default_factory=dict)
    # Peer exchange: only the peer's current RAM state is ever fetched, never history.
    peer_timeout_seconds: float = 4.0
    peer_cache_seconds: float = 1.0
    peer_stale_seconds: float = 15.0
    # A large response is reused outright for render_cache_seconds; after that its small head is
    # rebuilt and its stocks part is re-encoded only if the content changed.
    render_cache_seconds: float = 1.0
    # Genuine Dhan 1m history for today only, after a mid-session (re)start or a feed gap.
    history_bootstrap: bool = True
    history_interval_seconds: float = 0.5
    # A minute's candle is published once its minute has ended plus this grace, without waiting
    # for the stock's next trade.
    finalize_grace_seconds: float = 3.0
    http_threads: int = 8
    max_rss_mb: float = 600.0
    # Off: the node only consumes the shared DHAN_ACCESS_TOKEN and never mints one (see live_core/auth.py).
    token_generation: bool = False
    # Base URL of the account's token authority (the full PSYGRID on the private network); the node
    # takes the Dhan token it currently holds from there. Empty: DHAN_ACCESS_TOKEN from the env file.
    token_source: str = ""
    token_poll_seconds: float = 15.0

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> LiveCoreConfig:
        environ = os.environ if environ is None else environ
        node_count = _int(environ, "LIVE_CORE_NODE_COUNT")
        node_id = _int(environ, "LIVE_CORE_NODE_ID")
        if node_count < 1:
            raise LiveCoreConfigError(f"LIVE_CORE_NODE_COUNT must be >= 1; got {node_count}")
        if not 0 <= node_id < node_count:
            raise LiveCoreConfigError(f"LIVE_CORE_NODE_ID must be in 0..{node_count - 1}; got {node_id}")
        port = _int(environ, "LIVE_CORE_PORT", _int(environ, "PORT", 10000))
        if not 0 < port < 65536:
            raise LiveCoreConfigError(f"LIVE_CORE_PORT out of range: {port}")
        http_threads = _int(environ, "LIVE_CORE_HTTP_THREADS", 8)
        if not 1 <= http_threads <= 64:
            raise LiveCoreConfigError(f"LIVE_CORE_HTTP_THREADS must be in 1..64; got {http_threads}")
        return cls(
            node_id=node_id,
            node_count=node_count,
            host=str(environ.get("LIVE_CORE_HOST", "") or "0.0.0.0").strip(),
            port=port,
            peers=parse_peers(str(environ.get("LIVE_CORE_PEERS", "")), node_id, node_count),
            peer_timeout_seconds=_float(environ, "LIVE_CORE_PEER_TIMEOUT_SECONDS", 4.0),
            peer_cache_seconds=_float(environ, "LIVE_CORE_PEER_CACHE_SECONDS", 1.0),
            peer_stale_seconds=_float(environ, "LIVE_CORE_PEER_STALE_SECONDS", 15.0),
            render_cache_seconds=_float(environ, "LIVE_CORE_RENDER_CACHE_SECONDS", 1.0),
            history_bootstrap=_flag(environ, "LIVE_CORE_HISTORY_BOOTSTRAP", True),
            history_interval_seconds=_float(environ, "LIVE_CORE_HISTORY_INTERVAL_SECONDS", 0.5),
            finalize_grace_seconds=_float(environ, "LIVE_CORE_FINALIZE_GRACE_SECONDS", 3.0),
            http_threads=http_threads,
            max_rss_mb=_float(environ, "LIVE_CORE_MAX_RSS_MB", 600.0),
            token_generation=_flag(environ, "LIVE_CORE_TOKEN_GENERATION", False),
            token_source=_token_source(environ),
            token_poll_seconds=_float(environ, "LIVE_CORE_TOKEN_POLL_SECONDS", 15.0),
        )

    def missing_peers(self) -> list[int]:
        return [node for node in range(self.node_count) if node != self.node_id and node not in self.peers]
