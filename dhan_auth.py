from __future__ import annotations

import os
from typing import Any

import pyotp
import requests

TOKEN_URL = "https://auth.dhan.co/app/generateAccessToken"


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def generate_access_token(client_id: str, pin: str, totp_secret: str) -> tuple[str, str | None]:
    """Generate a fresh 24-hour Dhan access token using PIN + TOTP."""
    if not client_id or not pin or not totp_secret:
        raise RuntimeError("Dhan automatic token generation requires DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET")

    try:
        totp = pyotp.TOTP(totp_secret.replace(" ", "")).now()
    except Exception as exc:
        raise RuntimeError("Invalid DHAN_TOTP_SECRET") from exc

    try:
        response = requests.post(
            TOKEN_URL,
            params={"dhanClientId": client_id, "pin": pin, "totp": totp},
            headers={"Accept": "application/json"},
            timeout=20,
        )
        response.raise_for_status()
        payload: Any = response.json()
    except Exception as exc:
        raise RuntimeError(f"Dhan access-token generation failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Dhan access-token response was not an object")

    token = str(payload.get("accessToken", "")).strip()
    if not token:
        message = payload.get("errorMessage") or payload.get("message") or payload
        raise RuntimeError(f"Dhan access-token generation returned no token: {message}")

    return token, str(payload.get("expiryTime", "")).strip() or None


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
