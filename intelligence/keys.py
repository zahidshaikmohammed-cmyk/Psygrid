"""API keys: issued once, stored only as hashes, checked in constant time.

A key looks like ``psg_<id>_<secret>``. The id (8 hex characters) names the key
in logs and in the key file; the secret (43 URL-safe characters, 256 bits) is
shown once at creation and never stored. The file holds SHA-256 of the full
key: a random 256-bit secret needs no slow hash, because it cannot be guessed
from a dictionary. The file is written atomically with mode 0600.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
from datetime import UTC, datetime
from pathlib import Path

KEY_PATTERN = re.compile(r"^psg_([0-9a-f]{8})_([A-Za-z0-9_-]{43})$")


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


class KeyStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._cache: tuple[float, dict] | None = None

    def _load(self) -> dict:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            return {}
        if self._cache and self._cache[0] == mtime:
            return self._cache[1]
        data = json.loads(self.path.read_text())
        self._cache = (mtime, data)
        return data

    def _save(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
        os.replace(tmp, self.path)
        self._cache = None

    def create(self, name: str, rate_per_minute: int | None = None) -> tuple[str, str]:
        """A new key; returns (key id, full key). The full key is not recoverable later."""
        if not name or len(name) > 100:
            raise ValueError("name must be 1-100 characters")
        with self._lock:
            data = dict(self._load())
            key_id = secrets.token_hex(4)
            while key_id in data:
                key_id = secrets.token_hex(4)
            key = f"psg_{key_id}_{secrets.token_urlsafe(32)}"
            data[key_id] = {
                "name": name,
                "hash": _hash(key),
                "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "revoked_at": None,
                "rate_per_minute": rate_per_minute,
            }
            self._save(data)
        return key_id, key

    def revoke(self, key_id: str) -> bool:
        with self._lock:
            data = dict(self._load())
            if key_id not in data or data[key_id]["revoked_at"]:
                return False
            data[key_id] = {**data[key_id], "revoked_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}
            self._save(data)
        return True

    def list(self) -> list[dict]:
        return [
            {"id": key_id, **{k: v for k, v in entry.items() if k != "hash"}}
            for key_id, entry in sorted(self._load().items())
        ]

    def verify(self, key: str | None) -> dict | None:
        """The key's record (with ``id``) if the key is valid and not revoked, else None."""
        if not key:
            return None
        match = KEY_PATTERN.match(key)
        if not match:
            return None
        entry = self._load().get(match.group(1))
        if not entry or entry.get("revoked_at"):
            return None
        if not hmac.compare_digest(entry["hash"], _hash(key)):
            return None
        return {"id": match.group(1), **entry}

    def configured(self) -> bool:
        return any(not e.get("revoked_at") for e in self._load().values())


class RateLimiter:
    """Token bucket per key: ``per_minute`` sustained, ``burst`` at once."""

    def __init__(self, per_minute: int, burst: int, clock=None):
        self.per_minute, self.burst = per_minute, burst
        self._clock = clock or __import__("time").monotonic
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def check(self, key_id: str, per_minute: int | None = None) -> tuple[bool, int, float]:
        """(allowed, tokens remaining, seconds until the next token)."""
        rate = (per_minute or self.per_minute) / 60.0
        now = self._clock()
        with self._lock:
            tokens, last = self._buckets.get(key_id, (float(self.burst), now))
            tokens = min(float(self.burst), tokens + (now - last) * rate)
            allowed = tokens >= 1.0
            if allowed:
                tokens -= 1.0
            self._buckets[key_id] = (tokens, now)
            if len(self._buckets) > 10_000:  # forget idle keys rather than grow without bound
                self._buckets = {k: v for k, v in self._buckets.items() if now - v[1] < 600}
        wait = 0.0 if allowed else (1.0 - tokens) / rate
        return allowed, int(tokens), wait
