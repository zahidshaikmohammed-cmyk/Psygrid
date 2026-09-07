from __future__ import annotations

import os
import re
from typing import Any

import pyotp
import requests

TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"


class DhanTokenRateLimited(RuntimeError):
    """Dhan rejected token generation because another token was generated recently."""

    def __init__(self, message: str, retry_after: int = 120):
        super().__init__(message)
        self.retry_after = max(120, int(retry_after))


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _generate_totp(secret: str) -> str:
    normalized = "".join(secret.split()).upper()
    if not normalized:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET")
    try:
        return pyotp.TOTP(normalized).now()
    except Exception as exc:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET") from exc


def _request_token(client_id: str, pin: str, totp: str) -> tuple[str | None, str | None, str]:
    response = requests.post(
        TOKEN_URL,
        params={"dhanClientId": client_id, "pin": pin, "totp": totp},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    try:
        payload: Any = response.json()
    except ValueError:
        payload = {"message": response.text[:500]}
    if not isinstance(payload, dict):
        payload = {"message": str(payload)}
    token = str(payload.get("accessToken", "")).strip()
    expiry = str(payload.get("expiryTime", "")).strip() or None
    if token:
        return token, expiry, ""
    message = payload.get("errorMessage") or payload.get("message")
    if not message and isinstance(payload.get("remarks"), dict):
        message = payload["remarks"].get("error_message") or payload["remarks"].get("errorMessage")
    if not message:
        message = payload
    return None, None, str(message).strip()


def _rate_limit_seconds(message: str) -> int | None:
    """Extract Dhan's temporary token-generation cooldown from its message."""
    text = message.lower()
    rate_limit_hint = any(
        phrase in text
        for phrase in (
            "once every 2 minutes",
            "once every two minutes",
            "token can be generated once",
            "too frequently",
            "rate limit",
            "rate-limited",
            "retry after",
            "retry in",
            "try again after",
            "try again in",
            "temporarily blocked",
        )
    )
    if not rate_limit_hint:
        return None
    seconds_match = re.search(r"(\d+)\s*(?:second|seconds|sec|secs)", text)
    if seconds_match:
        return max(120, int(seconds_match.group(1)) + 2)
    minutes_match = re.search(r"(\d+)\s*(?:minute|minutes|min|mins)", text)
    if minutes_match:
        return max(120, int(minutes_match.group(1)) * 60 + 2)
    return 120


def generate_access_token(client_id: str, pin: str, totp_secret: str) -> tuple[str, str | None]:
    """Generate one fresh Dhan 24-hour token using PIN + TOTP."""
    if not client_id or not pin or not totp_secret:
        raise RuntimeError(
            "Dhan automatic token generation requires DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET"
        )
    normalized_secret = "".join(totp_secret.split()).upper()
    if not normalized_secret:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET")
    try:
        pyotp.TOTP(normalized_secret).now()
    except Exception as exc:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET") from exc

    # Dhan limits token generation frequency. Make exactly one request per call;
    # temporary cooldowns are returned to SessionManager for deferred retry.
    totp = _generate_totp(normalized_secret)
    try:
        token, expiry, message = _request_token(client_id, pin, totp)
    except requests.RequestException as exc:
        raise RuntimeError(f"Dhan access-token generation failed: {exc}") from exc
    if token:
        return token, expiry
    retry_after = _rate_limit_seconds(message)
    if retry_after is not None:
        raise DhanTokenRateLimited(
            "Dhan token generation is temporarily rate-limited; retry will be deferred.",
            retry_after,
        )
    if "totp" in message.lower() and "invalid" in message.lower():
        raise RuntimeError(
            "Dhan rejected the current TOTP. Check DHAN_TOTP_SECRET and Dhan TOTP setup."
        )
    raise RuntimeError(f"Dhan access-token generation returned no token: {message}")


def token_from_environment(client_id: str) -> tuple[str, str | None, str]:
    """Use an explicit token first; otherwise prepare TOTP auth for session start."""
    token_var = _env("DHAN_TOKEN_VAR") or "DHAN_ACCESS_TOKEN"
    existing = _env(token_var)
    if existing:
        return existing, None, "ENVIRONMENT_TOKEN"
    pin = _env("DHAN_PIN")
    totp_secret = _env("DHAN_TOTP_SECRET")
    if pin and totp_secret:
        return "", None, "TOTP_PENDING"
    raise RuntimeError(
        f"Missing Dhan token. Expected {token_var}, or provide DHAN_PIN + DHAN_TOTP_SECRET for automatic daily token generation"
    )
