"""Shared-token mode: a Live Core node consumes the one Dhan token it is given and never mints one."""

from __future__ import annotations

import pytest
from live_core_helpers import ist

import dhan_auth
from live_core import auth
from live_core.config import LiveCoreConfig
from live_core.partition import build_partition
from live_core.runtime import LiveCoreRuntime

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjQxMDI0NDQ4MDB9.c2lnbmF0dXJlLW5vdC1yZWFs"


@pytest.fixture
def no_token_generation(monkeypatch):
    """Fail the test if anything tries to contact Dhan's token endpoint."""
    calls = []

    def forbidden(*args, **kwargs):
        calls.append(args)
        raise AssertionError("a Live Core node must not generate a Dhan token")

    monkeypatch.setattr(dhan_auth, "generate_access_token", forbidden)
    monkeypatch.setattr(dhan_auth, "_request_token", forbidden)
    return calls


def _clear(monkeypatch):
    for name in ("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN", "DHAN_TOKEN_VAR", "DHAN_PIN", "DHAN_TOTP_SECRET"):
        monkeypatch.delenv(name, raising=False)


def test_token_generation_is_off_by_default():
    assert (
        LiveCoreConfig.from_environment({"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "2"}).token_generation
        is False
    )
    cfg = LiveCoreConfig.from_environment(
        {"LIVE_CORE_NODE_ID": "0", "LIVE_CORE_NODE_COUNT": "2", "LIVE_CORE_TOKEN_GENERATION": "1"}
    )
    assert cfg.token_generation is True


def test_shared_settings_need_client_id_and_token_and_ignore_pin_totp(monkeypatch, no_token_generation):
    _clear(monkeypatch)
    monkeypatch.setenv("DHAN_PIN", "123456")
    monkeypatch.setenv("DHAN_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    with pytest.raises(RuntimeError) as missing:
        auth.load_shared_settings()
    assert "DHAN_CLIENT_ID" in str(missing.value) and "DHAN_ACCESS_TOKEN" in str(missing.value)
    assert "123456" not in str(missing.value) and "JBSWY3DPEHPK3PXP" not in str(missing.value)

    monkeypatch.setenv("DHAN_CLIENT_ID", "1100000001")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", TOKEN)
    settings = auth.load_shared_settings()
    assert settings.client_id == "1100000001" and settings.access_token == TOKEN
    assert settings.token_source == "SHARED_ENVIRONMENT_TOKEN"
    described = auth.describe(token_generation=False)
    assert described == {
        "token_mode": "SHARED_TOKEN_CONSUMER",
        "token_generation": False,
        "client_id_configured": True,
        "access_token_configured": True,
        "pin_totp_ignored": True,
    }
    assert no_token_generation == []


def test_shared_refresher_accepts_the_token_and_never_generates(monkeypatch, no_token_generation):
    _clear(monkeypatch)
    monkeypatch.setenv("DHAN_PIN", "123456")
    monkeypatch.setenv("DHAN_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    settings = type("S", (), {"access_token": TOKEN})()
    auth.shared_token_refresher(settings)  # a configured token is used as is
    with pytest.raises(auth.SharedTokenRejected):
        auth.shared_token_refresher(settings, force=True)
    settings.access_token = ""
    with pytest.raises(auth.SharedTokenRejected):
        auth.shared_token_refresher(settings)
    assert no_token_generation == []


def test_rejected_shared_token_is_auth_error_not_a_token_war(monkeypatch, universe, instruments, no_token_generation):
    """Dhan rejects the token: the node reports AUTH_ERROR, keeps serving, retries later, never mints."""
    from live_core_helpers import Clock, FakeDhanAPI, FakeFeed

    _clear(monkeypatch)
    monkeypatch.setenv("DHAN_CLIENT_ID", "1100000001")
    monkeypatch.setenv("DHAN_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("DHAN_PIN", "123456")
    monkeypatch.setenv("DHAN_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    clock = Clock(ist(9, 15))
    api = FakeDhanAPI()
    api.verify_error = RuntimeError("Dhan API HTTP error: 401 token expired (807)")
    runtime = LiveCoreRuntime(
        LiveCoreConfig(node_id=0, node_count=2, history_interval_seconds=0.0),
        universe,
        build_partition(universe, 0, 2),
        instrument_loader=lambda: list(instruments),
        api_factory=lambda settings: api,
        feed_factory=FakeFeed,
        now=clock.now,
        clock=clock.epoch,
    )
    try:
        runtime.tick()
        assert runtime.state.session_status == "AUTH_ERROR"
        assert runtime.feed is None
        health = runtime.node_health()
        assert health["auth"]["token_mode"] == "SHARED_TOKEN_CONSUMER"
        assert health["auth"]["pin_totp_ignored"] is True
        text = str(health)
        for secret in (TOKEN, "123456", "JBSWY3DPEHPK3PXP"):
            assert secret not in text
        assert "does not generate tokens" in health["feed"]["last_feed_error"]

        # A new shared token (installed + restart in production) is simply consumed on the retry.
        api.verify_error = None
        clock.set(ist(9, 17))
        runtime.tick()
        assert runtime.state.session_status == "LIVE"
        assert no_token_generation == []
    finally:
        runtime.stop()


class Authority:
    """The full PSYGRID's /internal/dhan-token as seen from a node."""

    def __init__(self, token=TOKEN):
        self.token = token
        self.down = False
        self.calls = 0

    def get(self, url, timeout):
        self.calls += 1
        assert url == "http://10.0.0.10:10000/internal/dhan-token"
        if self.down:
            raise ConnectionRefusedError("connect to 10.0.0.10:10000 refused")
        return 200, {"status": "OK", "client_id": "1100000001", "access_token": self.token}


def _authority_runtime(monkeypatch, universe, instruments, authority, api, when):
    from live_core_helpers import Clock, FakeFeed

    _clear(monkeypatch)
    clock = Clock(when)
    runtime = LiveCoreRuntime(
        LiveCoreConfig(node_id=0, node_count=2, history_interval_seconds=0.0, token_source="http://10.0.0.10:10000"),
        universe,
        build_partition(universe, 0, 2),
        instrument_loader=lambda: list(instruments),
        api_factory=lambda settings: api,
        feed_factory=FakeFeed,
        now=clock.now,
        clock=clock.epoch,
        token_get=authority.get,
    )
    return runtime, clock


def test_node_takes_the_authoritys_token_and_follows_its_renewal(
    monkeypatch, universe, instruments, no_token_generation
):
    from live_core_helpers import FakeDhanAPI

    authority, api = Authority(), FakeDhanAPI()
    runtime, clock = _authority_runtime(monkeypatch, universe, instruments, authority, api, ist(9, 15))
    try:
        runtime.tick()
        assert runtime.state.session_status == "LIVE"
        assert runtime.settings.access_token == TOKEN and runtime.settings.client_id == "1100000001"
        feed = runtime.feed

        # The authority renews its token mid-session: within one poll interval the old feed is
        # stopped and a completely new one is built with the renewed token (never half-reconnected).
        renewed = TOKEN[:-4] + "NEWW"
        authority.token = renewed
        clock.set(ist(9, 15, 20))
        runtime.tick()
        assert runtime.settings.access_token == renewed
        assert feed.stopped and runtime.feed is not feed
        replacement = runtime.feed
        assert replacement.started and replacement.settings.access_token == renewed
        assert replacement.instruments == feed.instruments  # the same partition
        assert getattr(feed, "reconnect_tokens", []) == []  # the old feed object was not reused
        clock.set(ist(9, 15, 25))
        runtime.tick()  # polled at most every 15 s, and an unchanged token never replaces the feed
        assert runtime.feed is replacement and runtime.token_renewals == 1
        assert runtime.state.session_status == "LIVE"
        feed = replacement

        health = runtime.node_health()
        assert health["auth"]["token_mode"] == "SHARED_FROM_AUTHORITY"
        assert health["auth"]["token_rotations"] == 1
        for secret in (TOKEN, renewed):
            assert secret not in str(health)

        # The authority being briefly unreachable never stops the running feed.
        authority.down = True
        clock.set(ist(9, 16, 0))
        runtime.tick()
        assert runtime.state.session_status == "LIVE" and runtime.feed is feed
        assert "refused" in runtime.node_health()["auth"]["last_error"]
        assert no_token_generation == []
    finally:
        runtime.stop()


def test_rejected_token_waits_for_the_authority_then_goes_live(monkeypatch, universe, instruments, no_token_generation):
    from live_core_helpers import FakeDhanAPI

    authority, api = Authority(), FakeDhanAPI()
    api.verify_error = RuntimeError("Dhan API HTTP error: 401 token expired (807)")
    runtime, clock = _authority_runtime(monkeypatch, universe, instruments, authority, api, ist(9, 15))
    try:
        runtime.tick()
        assert runtime.state.session_status == "AUTH_ERROR"  # never mints its own token
        authority.token = TOKEN[:-4] + "NEWW"
        api.verify_error = None
        clock.set(ist(9, 15, 20))
        runtime.tick()  # sees the renewed token and retries at once (not after the 30 s back-off)
        runtime.tick()
        assert runtime.state.session_status == "LIVE"
        assert runtime.settings.access_token.endswith("NEWW")
        assert no_token_generation == []
    finally:
        runtime.stop()


def test_authority_unreachable_at_the_open_is_a_visible_config_error(monkeypatch, universe, instruments):
    from live_core_helpers import FakeDhanAPI

    authority, api = Authority(), FakeDhanAPI()
    authority.down = True
    runtime, clock = _authority_runtime(monkeypatch, universe, instruments, authority, api, ist(9, 15))
    try:
        runtime.tick()
        assert runtime.state.session_status == "CONFIG_ERROR"
        assert "token authority" in runtime.config_error
        authority.down = False
        clock.set(ist(9, 16, 5))
        runtime.tick()
        assert runtime.state.session_status == "LIVE"
    finally:
        runtime.stop()
