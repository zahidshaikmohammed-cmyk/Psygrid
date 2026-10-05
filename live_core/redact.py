"""Strip credentials from any text the Live Core may expose (health payloads, errors, logs).

Dhan credentials reach error strings in practice. dhanhq puts the access token and client id in
the WebSocket URL (``?version=2&token=...&clientId=...``), and Dhan's token endpoint takes the PIN
and TOTP as URL query parameters, so a connection error that prints its URL carries them. Every
error the Live Core stores or logs passes through ``redact`` first.
"""

from __future__ import annotations

import logging
import os
import re
import threading

REDACTED = "<redacted>"
_SECRET_ENV = ("DHAN_ACCESS_TOKEN", "DHAN_PIN", "DHAN_TOTP_SECRET", "DHAN_CLIENT_ID")
_QUERY = re.compile(r"\?[^\s'\"<>)\]]+")
_JWT = re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}")
_KEY_VALUE = re.compile(
    r"(?i)\b(access[-_]?token|token|totp|pin|password|secret|dhan[-_]?client[-_]?id|client[-_]?id)"
    r"(\s*[=:]\s*['\"]?)[^\s&,'\"}]+"
)
_LONG_HEX_OR_B64 = re.compile(r"\b[A-Za-z0-9_-]{40,}\b")
_extra_secrets: set[str] = set()
_lock = threading.Lock()


def register_secret(value) -> None:
    """Also redact this exact value (e.g. a token generated at runtime)."""
    text = str(value or "").strip()
    if len(text) >= 4:
        with _lock:
            _extra_secrets.add(text)


def _secret_values() -> list[str]:
    values = set()
    token_var = os.getenv("DHAN_TOKEN_VAR", "").strip()
    for name in (*_SECRET_ENV, token_var):
        if name:
            value = os.getenv(name, "").strip()
            if len(value) >= 4:
                values.add(value)
    with _lock:
        values |= _extra_secrets
    return sorted(values, key=len, reverse=True)


def redact(text) -> str:
    if text is None:
        return ""
    out = str(text)
    for value in _secret_values():
        out = out.replace(value, REDACTED)
    out = _QUERY.sub("?" + REDACTED, out)
    out = _JWT.sub(REDACTED, out)
    out = _KEY_VALUE.sub(lambda m: m.group(1) + m.group(2) + REDACTED, out)
    out = _LONG_HEX_OR_B64.sub(REDACTED, out)
    return out


class RedactingFilter(logging.Filter):
    """Logging filter that redacts the fully formatted message of every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        record.msg = redact(message)
        record.args = ()
        if record.exc_info and record.exc_info[1] is not None:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True
