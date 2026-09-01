from __future__ import annotations

import os
import time
from typing import Any

import pyotp
import requests

TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _generate_totp(secret: str) -> str:
    # Dhan supplies a Base32 TOTP secret. Ignore visual spaces from copied secrets.
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

    message = str(payload.get("errorMessage") or payload.get("message") or payload).strip()
    return None, None, message


def generate_access_token(client_id: str, pin: str, totp_secret: str) -> tuple[str, str | None]:
    """Generate a fresh Dhan 24-hour access token using PIN + TOTP.

    Dhan TOTP codes rotate every 30 seconds. If the first request lands across
    that boundary and Dhan rejects the just-expired code, wait for the next
    code and retry once. No token or credential is ever logged.
    """
    if not client_id or not pin or not totp_secret:
        raise RuntimeError(
            "Dhan automatic token generation requires DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET"
        )

    normalized_secret = "".join(totp_secret.split()).upper()
    if not normalized_secret:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET")

    try:
        # Validate the secret locally before making an HTTP request.
        pyotp.TOTP(normalized_secret).now()
    except Exception as exc:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET") from exc

    last_message = ""
    for attempt in range(2):
        totp = _generate_totp(normalized_secret)
        try:
            token, expiry, message = _request_token(client_id, pin, totp)
        except requests.RequestException as exc:
            raise RuntimeError(f"Dhan access-token generation failed: {exc}") from exc

        if token:
            return token, expiry

        last_message = message
        if attempt == 0 and "totp" in message.lower():
            # Wait into the next RFC-6238 30-second window so the retry cannot
            # reuse the same code at a rollover boundary.
            remaining = 30.0 - (time.time() % 30.0)
            time.sleep(max(0.75, remaining + 0.25))
            continue
        break

    raise RuntimeError(f"Dhan access-token generation returned no token: {last_message}")


def token_from_environment(client_id: str) -> tuple[str, str | None, str]:
    """Return an existing token or generate one from the linked Render group."""
    token_var = _env("DHAN_TOKEN_VAR") or "DHAN_ACCESS_TOKEN"
    existing = _env(token_var)
    if existing:
        return existing, None, "ENVIRONMENT_TOKEN"

    pin = _env("DHAN_PIN")
    totp_secret = _env("DHAN_TOTP_SECRET")
    if pin and totp_secret:
        token, expiry = generate_access_token(client_id, pin, totp_secret)
        return token, expiry, "AUTO_GENERATED_TOTP"

    raise RuntimeError(
        f"Missing Dhan token. Expected {token_var}, or provide DHAN_PIN + DHAN_TOTP_SECRET for automatic daily token generation"
    )
