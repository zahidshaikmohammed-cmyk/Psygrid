"""Regression coverage for IndexLayerManager's session-start auth handling.

IndexLayerManager runs its own independent loop, sharing the same Settings/
DhanAPI instances as the sealed equity SessionManager but racing it
independently to refresh the Dhan access token at the start of each day.
The sealed equity session.py self-heals a 401/expired token by generating a
fresh one via TOTP before giving up; index_layer.py's _start_session had no
equivalent - a stale token at that one moment got baked into the WebSocket
connection built by feed.start() right after, with no retry until the next
day. That's what a real production instance surfaced: feed.status stuck at
"STARTING" with "history bootstrap: ... 401 Client Error: Unauthorized".
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from index_layer import IndexLayerManager, IndexInstrument, IndexState


def _settings():
    return SimpleNamespace(
        timezone="Asia/Kolkata", market_start="09:15", market_end="15:15",
        client_id="x", access_token="stale", token_expiry=None, token_source="TEST",
    )


def _manager_with_one_state():
    manager = IndexLayerManager.__new__(IndexLayerManager)
    manager.settings = _settings()
    manager.dhan_api = MagicMock()
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    state = IndexState(manager.settings, "nifty", "NIFTY", instrument)
    manager.states = {"nifty": state}
    manager.feed = MagicMock()
    return manager, state


def test_start_session_proactively_refreshes_the_token_before_bootstrapping():
    manager, state = _manager_with_one_state()
    manager.dhan_api.quote_snapshot.return_value = {}
    manager.dhan_api.load_today_completed_intraday.return_value = []

    with patch("index_layer.refresh_access_token") as fake_refresh:
        manager._start_session(__import__("datetime").datetime(2026, 9, 24))

    fake_refresh.assert_called_once_with(manager.settings)
    manager.feed.start.assert_called_once()


def test_start_session_force_refreshes_once_on_a_401_and_still_starts_the_feed():
    manager, state = _manager_with_one_state()
    manager.dhan_api.quote_snapshot.side_effect = RuntimeError(
        "Dhan API HTTP error 401 Client Error: Unauthorized for url: https://api.dhan.co/v2/marketfeed/quote"
    )

    with patch("index_layer.refresh_access_token") as fake_refresh:
        manager._start_session(__import__("datetime").datetime(2026, 9, 24))

    # First call is the unconditional proactive refresh, second is the
    # forced retry triggered by recognizing the 401 as an auth failure.
    assert fake_refresh.call_count == 2
    assert fake_refresh.call_args_list[1].kwargs.get("force") is True or fake_refresh.call_args_list[1].args[1:] == (True,)
    # The feed still starts afterwards - a bootstrap failure never blocks
    # the live WebSocket connection from being attempted with the (now
    # hopefully fresh) token.
    manager.feed.start.assert_called_once()
    assert "history bootstrap" in state.last_feed_error


def test_start_session_only_force_refreshes_once_even_with_many_states_failing():
    manager, state = _manager_with_one_state()
    instrument2 = IndexInstrument(security_id="25", exchange_segment="IDX_I")
    state2 = IndexState(manager.settings, "banknifty", "BANKNIFTY", instrument2)
    manager.states["banknifty"] = state2
    manager.dhan_api.quote_snapshot.side_effect = RuntimeError("401 Unauthorized")

    with patch("index_layer.refresh_access_token") as fake_refresh:
        manager._start_session(__import__("datetime").datetime(2026, 9, 24))

    # One proactive call + exactly one forced retry, not one per failing state.
    assert fake_refresh.call_count == 2


def test_looks_like_auth_failure_recognizes_dhan_error_codes():
    assert IndexLayerManager._looks_like_auth_failure(RuntimeError("401 Client Error"))
    assert IndexLayerManager._looks_like_auth_failure(RuntimeError("Disconnected: Access Token is expired (807)"))
    assert IndexLayerManager._looks_like_auth_failure(RuntimeError("Invalid Client ID (808)"))
    assert IndexLayerManager._looks_like_auth_failure(RuntimeError("Authentication Failed (809)"))
    assert not IndexLayerManager._looks_like_auth_failure(RuntimeError("Dhan API HTTP 429 rate limit"))
    assert not IndexLayerManager._looks_like_auth_failure(RuntimeError("Dhan API network failure: timeout"))
