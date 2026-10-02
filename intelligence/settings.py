"""Configuration of the intelligence service, from environment variables only.

Every setting has a safe default, so the service runs with no configuration:
it binds to localhost, requires API keys for every data endpoint, and reads
the archive PSYGRID already writes. See ``docs/intelligence/configuration.md``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from daily_archive import archive_dir_from_environment
from intelligence.history import store_dir


def _int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except ValueError:
        return default
    return max(low, min(high, value))


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    archive_dir: Path
    store_dir: Path
    psygrid_url: str  # the production service, read-only, for derivatives snapshots
    host: str
    port: int
    require_keys: bool
    rate_per_minute: int
    rate_burst: int
    max_streams: int
    max_streams_per_key: int
    live_enabled: bool
    record_derivatives: bool
    similarity_lookback: int
    backup_keep: int
    use_stream: bool = True  # follow PSYGRID's per-minute stream (seconds) as well as its archive (minutes)

    @property
    def events_db(self) -> Path:
        return self.store_dir / "events.db"

    @property
    def keys_file(self) -> Path:
        return self.store_dir / "api_keys.json"

    @classmethod
    def from_environment(cls) -> Settings:
        return cls(
            archive_dir=archive_dir_from_environment(),
            store_dir=store_dir(),
            psygrid_url=os.environ.get("PSYGRID_INTELLIGENCE_SOURCE_URL", "http://127.0.0.1:10000").rstrip("/"),
            host=os.environ.get("PSYGRID_INTELLIGENCE_HOST", "127.0.0.1"),
            port=_int("PSYGRID_INTELLIGENCE_PORT", 18101, 1, 65535),
            require_keys=_bool("PSYGRID_INTELLIGENCE_REQUIRE_KEYS", True),
            rate_per_minute=_int("PSYGRID_INTELLIGENCE_RATE_PER_MINUTE", 120, 1, 100_000),
            rate_burst=_int("PSYGRID_INTELLIGENCE_RATE_BURST", 30, 1, 10_000),
            max_streams=_int("PSYGRID_INTELLIGENCE_MAX_STREAMS", 50, 0, 10_000),
            max_streams_per_key=_int("PSYGRID_INTELLIGENCE_MAX_STREAMS_PER_KEY", 5, 0, 1_000),
            live_enabled=_bool("PSYGRID_INTELLIGENCE_LIVE", True),
            record_derivatives=_bool("PSYGRID_INTELLIGENCE_RECORD_DERIVATIVES", True),
            similarity_lookback=_int("PSYGRID_INTELLIGENCE_SIMILARITY_SESSIONS", 60, 5, 1_000),
            backup_keep=_int("PSYGRID_INTELLIGENCE_BACKUP_KEEP", 14, 1, 365),
            use_stream=_bool("PSYGRID_INTELLIGENCE_STREAM", True),
        )
