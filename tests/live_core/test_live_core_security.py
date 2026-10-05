"""No Dhan credential can reach a Live Core response or log, whatever error carries it."""

import logging

import pytest
from fastapi.testclient import TestClient
from live_core_helpers import ist

from live_core.api import create_app
from live_core.redact import RedactingFilter, redact, register_secret

CLIENT_ID = "1100223344"
PIN = "4321"
TOTP_SECRET = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"
TOTP_NOW = "987654"
JWT = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwiZGhhbkNsaWVudElkIjoiMTEwMDIyMzM0NCJ9.c2lnbmF0dXJlc2lnbmF0dXJl"
SECRETS = (CLIENT_ID, PIN, TOTP_SECRET, TOTP_NOW, JWT)

AUTH_URL_ERROR = (
    "Dhan access-token generation failed: HTTPSConnectionPool(host='auth.dhan.co', port=443): Max retries exceeded "
    f"with url: /app/generateAccessToken?dhanClientId={CLIENT_ID}&pin={PIN}&totp={TOTP_NOW} "
    "(Caused by NewConnectionError('Failed to establish a new connection: [Errno 101] Network is unreachable'))"
)
WS_URL_ERROR = f"InvalidURI: wss://api-feed.dhan.co?version=2&token={JWT}&clientId={CLIENT_ID}&authType=2 isn't valid"


@pytest.fixture(autouse=True)
def dhan_env(monkeypatch):
    monkeypatch.setenv("DHAN_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("DHAN_PIN", PIN)
    monkeypatch.setenv("DHAN_TOTP_SECRET", TOTP_SECRET)
    monkeypatch.delenv("DHAN_ACCESS_TOKEN", raising=False)


def _assert_clean(text: str):
    for secret in SECRETS:
        assert secret not in text, f"credential leaked: {secret[:6]}..."


@pytest.mark.parametrize("message", [AUTH_URL_ERROR, WS_URL_ERROR, f"token={JWT}", f"pin: {PIN} totp={TOTP_NOW}"])
def test_redact_removes_every_credential_form(message):
    cleaned = redact(message)
    _assert_clean(cleaned)
    assert "<redacted>" in cleaned


def test_redact_keeps_ordinary_diagnostics_readable():
    text = "Dhan feed error code=805: Too many connections; websocket closed by peer"
    assert redact(text) == text


def test_credentials_in_auth_and_feed_errors_never_reach_any_endpoint(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))

    def failing_refresh(settings, force=False):
        raise RuntimeError(AUTH_URL_ERROR)

    runtime._token_refresher = failing_refresh
    runtime.tick()
    assert runtime.state.session_status == "AUTH_ERROR"
    runtime.state.mark_websocket_error("websocket:" + WS_URL_ERROR)
    runtime.state.mark_websocket_reconnecting("websocket reconnect; cause=" + WS_URL_ERROR)
    runtime.config_error = ""
    client = TestClient(create_app(runtime, start_runtime=False))
    paths = ["/", "/health", "/health/node", "/public/health.json", "/ready", "/public/live.json",
             "/public/live-a.json", "/public/stock/RELIANCE.json", "/internal/live-core/fragments"]  # fmt: skip
    for path in paths:
        _assert_clean(client.get(path).text)
    health = client.get("/health/node").json()
    assert "<redacted>" in health["feed"]["last_feed_error"]
    assert any("<redacted>" in e["error"] for e in health["errors"])


def test_a_token_generated_at_runtime_is_redacted_too(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))

    def generate(settings, force=False):
        settings.access_token = "runtime-generated-token-value-1234567"

    runtime._token_refresher = generate
    runtime.tick()
    runtime.state.mark_websocket_error("rejected token runtime-generated-token-value-1234567 by server")
    assert (
        "runtime-generated-token-value-1234567"
        not in TestClient(create_app(runtime, start_runtime=False)).get("/health").text
    )


def test_log_records_are_redacted(caplog):
    logger = logging.getLogger("live_core.test")
    redacting = RedactingFilter()
    caplog.handler.addFilter(redacting)
    try:
        logger.error("auth failed: %s", AUTH_URL_ERROR)
        try:
            raise RuntimeError(WS_URL_ERROR)
        except RuntimeError:
            logger.exception("feed crashed")
    finally:
        caplog.handler.removeFilter(redacting)
    _assert_clean(caplog.text)


def test_register_secret_ignores_trivial_values():
    register_secret("")
    register_secret("ab")
    assert redact("ab cd") == "ab cd"
