"""Share this process's current Dhan access token with the PSYGRID Live Core nodes.

Dhan keeps one live access token per client, and any new login invalidates the others. This
process is the account's only token authority (PIN + TOTP); the Live Core nodes must never log in
themselves, so they fetch the token it currently holds from ``GET /internal/dhan-token``.

The endpoint is off unless ``PSYGRID_TOKEN_SHARE_CLIENTS`` lists the nodes' *private* IPs. It
answers only a direct connection from one of them: private addresses only, and any request that
carries proxy headers is refused, so a request relayed through a local proxy cannot claim to be a
node. The token is never logged and the response is marked ``no-store``.
"""

from __future__ import annotations

import ipaddress
import os

_PROXY_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded", "x-forwarded-host")


def allowed_clients(raw: str | None = None) -> frozenset[str]:
    """Private IPs from ``PSYGRID_TOKEN_SHARE_CLIENTS``; anything else is ignored."""
    raw = os.getenv("PSYGRID_TOKEN_SHARE_CLIENTS", "") if raw is None else raw
    allowed = set()
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            address = ipaddress.ip_address(item)
        except ValueError:
            continue
        if address.is_private and not address.is_loopback:
            allowed.add(str(address))
    return frozenset(allowed)


def token_share(settings, client_host: str | None, headers, allowed: frozenset[str]) -> tuple[int, dict]:
    """``(http_status, payload)`` for one request. Only an allowed node ever sees the token."""
    if not allowed:
        return 404, {"status": "NOT_FOUND"}
    if any(name in headers for name in _PROXY_HEADERS) or client_host not in allowed:
        return 403, {"status": "FORBIDDEN"}
    token = str(getattr(settings, "access_token", "") or "").strip() if settings is not None else ""
    client_id = str(getattr(settings, "client_id", "") or "").strip() if settings is not None else ""
    if not token or not client_id:
        return 503, {"status": "TOKEN_NOT_READY"}
    return 200, {
        "status": "OK",
        "client_id": client_id,
        "access_token": token,
        "token_source": str(getattr(settings, "token_source", "") or ""),
        "token_expiry": getattr(settings, "token_expiry", None),
    }
