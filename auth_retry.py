"""Retry a Dhan API call once after refreshing an expired or rejected access token."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import TypeVar

from config import refresh_access_token
from dhan_auth import DhanTokenRateLimited

T = TypeVar("T")

# Dhan HTTP/WebSocket auth failures: 401, plus Dhan error codes 807 (token expired),
# 808 (authentication failed), 809 (token invalid) and 810 (client id invalid).
_AUTH_FAILURE_MARKERS = (
    "401",
    "807",
    "808",
    "809",
    "810",
    "expired",
    "invalid token",
    "authentication failed",
    "unauthorized",
)


def looks_like_auth_failure(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _AUTH_FAILURE_MARKERS)


class AuthRetryGuard:
    """Run Dhan API calls, force-refreshing the access token once on an auth failure.

    If Dhan rate-limits token generation, the guard enters a cooldown for the
    period Dhan asked for; calls during the cooldown fail fast instead of
    hammering the token endpoint.
    """

    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self._retry_at = 0.0
        self._lock = threading.Lock()

    def _raise_if_cooling_down(self, cause: Exception | None = None) -> None:
        """Raise while a token-generation cooldown is active. Caller must hold ``self._lock``."""
        now = time.monotonic()
        if now < self._retry_at:
            message = f"Dhan authentication refresh cooldown active: {int(self._retry_at - now)}s"
            raise RuntimeError(message) from cause

    def call(self, operation: Callable[[], T]) -> T:
        with self._lock:
            self._raise_if_cooling_down()
        try:
            return operation()
        except Exception as first_exc:
            if not looks_like_auth_failure(first_exc):
                raise
            # Re-check and refresh under one lock acquisition so concurrent
            # callers cannot refresh again after a rate-limited attempt.
            with self._lock:
                self._raise_if_cooling_down(first_exc)
                try:
                    refresh_access_token(self.settings, force=True)
                except DhanTokenRateLimited as exc:
                    self._retry_at = time.monotonic() + exc.retry_after
                    raise
                self.dhan_api.settings = self.settings
                self._retry_at = 0.0
            return operation()
