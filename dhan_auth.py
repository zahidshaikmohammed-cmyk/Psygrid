from __future__ import annotations

import os
import re
import time
from typing import Any

import pyotp
import requests

TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"


class DhanTokenRateLimited(RuntimeError):
    """Dhan rejected token generation because another token was generated recently."""

    def __init__(self, message: str, retry_after: int = 120):
        super().__init__(message)
        self.retry_after = max(30, int(retry_after))


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

    message = str(
        payload.get("errorMessage")
        or payload.get("message")
        or payload.get("remarks", {}).get("error_message") if isinstance(payload.get("remarks"), dict) else ""
        or payload
    ).strip()
    return None, None, message


def _rate_limit_seconds(message: str) -> int | None:
    text = message.lower()
    if "once every 2 minutes" not in text and "2 minutes" not in text and "two minutes" not in text:
        return None
    match = re.search(r"(\d+)\s*(?:second|seconds|sec|secs)", text)
    if match:
        return max(30, int(match.group(1)) + 2)
    return 120


def generate_access_token(client_id: str, pin: str, totp_secret: str) -> tuple[str, str | None]:
    """Generate a fresh Dhan 24-hour token using PIN + TOTP.

    The Dhan endpoint is rate-limited. A rate-limit response is surfaced as a
    structured exception so the application can wait without repeatedly
    hammering authentication or killing the HTTP server.
    """
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

    for attempt in range(2):
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
                "Dhan token generation is temporarily rate-limited; no credentials were exposed.",
                retry_after,
            )

        if attempt == 0 and "totp" in message.lower():
            remaining = 30.0 - (time.time() % 30.0)
            time.sleep(max(0.75, remaining + 0.25))
            continue

        raise RuntimeError(f"Dhan access-token generation returned no token: {message}")

    raise RuntimeError("Dhan access-token generation failed after TOTP rollover retry")


def token_from_environment(client_id: str) -> tuple[str, str | None, str]:
    """Use an explicitly supplied token first; otherwise prepare TOTP auth for session start."""
    token_var = _env("DHAN_TOKEN_VAR") or "DHAN_ACCESS_TOKEN"
    existing = _env(token_var)
    if existing:
        return existing, None, "ENVIRONMENT_TOKEN"

    pin = _env("DHAN_PIN")
    totp_secret = _env("DHAN_TOTP_SECRET")
    if pin and totp_secret:
        # Do not generate during FastAPI startup. SessionManager owns the
        # market-session authentication attempt and can safely back off.
        return "", None, "TOTP_PENDING"

    raise RuntimeError(
        f"Missing Dhan token. Expected {token_var}, or provide DHAN_PIN + DHAN_TOTP_SECRET for automatic daily token generation"
    )
