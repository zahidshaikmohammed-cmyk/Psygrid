"""The full PSYGRID holds a Dhan token before the open (the Live Core nodes connect at 09:05 with it)."""

import base64
import json
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import session as session_module
from dhan_auth import DhanTokenRateLimited
from session import SessionManager

IST = ZoneInfo("Asia/Kolkata")


def _jwt(exp: datetime) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": int(exp.timestamp())}).encode()).decode().rstrip("=")
    return f"eyJhbGciOiJIUzUxMiJ9.{payload}.c2ln"


@pytest.fixture
def manager(monkeypatch):
    monkeypatch.setenv("DHAN_PIN", "123456")
    monkeypatch.setenv("DHAN_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    settings = SimpleNamespace(
        timezone="Asia/Kolkata", market_start="09:15", market_end="15:15", client_id="1100000001", access_token=""
    )
    generated = []

    def fake_refresh(target, force=False):
        generated.append(force)
        if getattr(fake_refresh, "error", None):
            raise fake_refresh.error
        target.access_token = _jwt(datetime(2026, 10, 9, 8, 50, tzinfo=IST))

    monkeypatch.setattr(session_module, "refresh_access_token", fake_refresh)
    feed = SimpleNamespace(dhan_api=None, settings=None)
    manager = SessionManager(settings, SimpleNamespace(last_feed_error=""), SimpleNamespace(), feed, [])
    manager.generated = generated
    manager.fake_refresh = fake_refresh
    return manager


def test_a_restarted_authority_without_a_token_generates_one_outside_market_hours(manager):
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 30, tzinfo=IST))
    assert manager.generated == [True] and manager.settings.access_token
    assert manager.feed.settings is manager.settings and manager.dhan_api.settings is manager.settings
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 30, 2, tzinfo=IST))
    assert manager.generated == [True]  # a token is held: nothing more


def test_a_token_expiring_before_the_close_is_replaced_before_the_open(manager):
    manager.settings.access_token = _jwt(datetime(2026, 10, 8, 9, 20, tzinfo=IST))  # yesterday's 09:20 token
    manager._keep_token_ready(datetime(2026, 10, 8, 8, 30, tzinfo=IST))
    assert manager.generated == []  # not yet in the preparation window
    manager._keep_token_ready(datetime(2026, 10, 8, 8, 45, tzinfo=IST))
    assert manager.generated == [True]
    manager._keep_token_ready(datetime(2026, 10, 8, 8, 46, tzinfo=IST))
    assert manager.generated == [True]  # the new token lasts through the close


def test_a_token_valid_through_the_close_is_kept(manager):
    manager.settings.access_token = token = _jwt(datetime(2026, 10, 8, 19, 30, tzinfo=IST))
    manager._keep_token_ready(datetime(2026, 10, 8, 8, 50, tzinfo=IST))
    assert manager.generated == [] and manager.settings.access_token == token


def test_rate_limited_generation_waits_for_dhan(manager):
    manager.fake_refresh.error = DhanTokenRateLimited("rate limited", 120)
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 30, tzinfo=IST))
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 31, tzinfo=IST))
    assert manager.generated == [True]
    manager.fake_refresh.error = None
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 32, 1, tzinfo=IST))
    assert manager.generated == [True, True] and manager.settings.access_token


def test_an_explicit_environment_token_is_left_alone(manager, monkeypatch):
    monkeypatch.delenv("DHAN_TOTP_SECRET")
    manager._keep_token_ready(datetime(2026, 10, 7, 19, 30, tzinfo=IST))
    assert manager.generated == []
