from types import SimpleNamespace
from unittest.mock import patch

import pytest

from auth_retry import AuthRetryGuard, looks_like_auth_failure


def test_recognizes_dhan_auth_failures():
    assert looks_like_auth_failure(RuntimeError("401 Client Error: Unauthorized"))
    assert looks_like_auth_failure(RuntimeError("Access Token is expired (807)"))
    assert looks_like_auth_failure(RuntimeError("Invalid Client ID (810)"))
    assert not looks_like_auth_failure(RuntimeError("Dhan API HTTP 429 rate limit"))
    assert not looks_like_auth_failure(RuntimeError("network timeout"))


def test_non_auth_error_propagates_without_token_refresh():
    guard = AuthRetryGuard(SimpleNamespace(), SimpleNamespace())

    def failing():
        raise RuntimeError("Dhan API HTTP 429 rate limit")

    with patch("auth_retry.refresh_access_token") as refresh, pytest.raises(RuntimeError, match="429"):
        guard.call(failing)
    refresh.assert_not_called()


def test_success_needs_no_refresh():
    guard = AuthRetryGuard(SimpleNamespace(), SimpleNamespace())
    with patch("auth_retry.refresh_access_token") as refresh:
        assert guard.call(lambda: 42) == 42
    refresh.assert_not_called()
