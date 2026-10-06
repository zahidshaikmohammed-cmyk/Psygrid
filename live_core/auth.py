"""Dhan authentication for Live Core nodes: consume one shared access token, never generate one.

Dhan keeps one live access token per client. The full PSYGRID generates its own token with PIN +
TOTP and regenerates it on any 401/807. If a Live Core node also generated tokens, each generation
could invalidate the other process's token and the two would knock each other out all session.

So, by default, a node only *consumes* the token it is given (``DHAN_CLIENT_ID`` +
``DHAN_ACCESS_TOKEN`` in ``/etc/psygrid-live-core.env``). ``DHAN_PIN`` / ``DHAN_TOTP_SECRET`` are
ignored even if present. When Dhan rejects the token, the node reports ``AUTH_ERROR`` and keeps
serving HTTP; it never tries to mint a replacement. ``LIVE_CORE_TOKEN_GENERATION=1`` restores the
full app's PIN + TOTP behaviour for a deployment where the node is the only token authority.
"""

from __future__ import annotations

import os

import config as psygrid_config

TOKEN_MODE_SHARED = "SHARED_TOKEN_CONSUMER"
TOKEN_MODE_GENERATE = "TOTP_GENERATION"


class SharedTokenRejected(RuntimeError):
    """The shared Dhan access token is missing, expired or invalid; this node will not replace it."""


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def load_shared_settings():
    """``Settings`` from ``DHAN_CLIENT_ID`` + the shared ``DHAN_ACCESS_TOKEN`` (no PIN/TOTP fallback)."""
    client_id = _env("DHAN_CLIENT_ID")
    token_var = _env("DHAN_TOKEN_VAR") or "DHAN_ACCESS_TOKEN"
    token = _env(token_var)
    missing = [name for name, value in (("DHAN_CLIENT_ID", client_id), (token_var, token)) if not value]
    if missing:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing)} "
            "(shared-token mode: this node never generates Dhan tokens)"
        )
    return psygrid_config.Settings(
        client_id=client_id,
        access_token=token,
        token_source="SHARED_ENVIRONMENT_TOKEN",
        token_expiry=None,
    )


def shared_token_refresher(settings, force: bool = False) -> None:
    """Token "refresh" for a consumer: accept the configured token, refuse to generate a new one."""
    if force or not getattr(settings, "access_token", ""):
        raise SharedTokenRejected(
            "Dhan rejected the shared access token (expired or invalid). This node does not generate "
            "tokens; install the current token as DHAN_ACCESS_TOKEN (deploy-live-core action=configure)"
        )


def describe(token_generation: bool) -> dict:
    """Non-secret description of the node's token mode for health payloads."""
    pin_totp_present = bool(_env("DHAN_PIN") and _env("DHAN_TOTP_SECRET"))
    return {
        "token_mode": TOKEN_MODE_GENERATE if token_generation else TOKEN_MODE_SHARED,
        "token_generation": token_generation,
        "client_id_configured": bool(_env("DHAN_CLIENT_ID")),
        "access_token_configured": bool(_env(_env("DHAN_TOKEN_VAR") or "DHAN_ACCESS_TOKEN")),
        "pin_totp_ignored": bool(pin_totp_present and not token_generation),
    }
