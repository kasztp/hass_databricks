"""Tests for the async_test_databricks_connection helper."""

import asyncio
from unittest import mock

import aiohttp

from custom_components.hass_databricks import async_test_databricks_connection


def _make_response(status=200, json_data=None):
    """Create a mock aiohttp response."""
    resp = mock.AsyncMock()
    resp.status = status
    resp.json = mock.AsyncMock(
        return_value=json_data or {"status": {"state": "SUCCEEDED"}}
    )
    resp.raise_for_status = mock.MagicMock()
    return resp


def _make_cm(resp_or_exc):
    """Build an async context manager from a response or exception."""
    cm = mock.AsyncMock()
    if isinstance(resp_or_exc, BaseException):
        cm.__aenter__ = mock.AsyncMock(side_effect=resp_or_exc)
    else:
        cm.__aenter__ = mock.AsyncMock(return_value=resp_or_exc)
    cm.__aexit__ = mock.AsyncMock()
    return cm


def test_connection_success():
    session = mock.AsyncMock()
    resp = _make_response(200, {"status": {"state": "SUCCEEDED"}})
    session.post = mock.MagicMock(return_value=_make_cm(resp))

    result = asyncio.run(
        async_test_databricks_connection("host", "/path", "token", session=session)
    )
    assert result == {"success": True}


def test_connection_401_returns_invalid_auth_immediately():
    """Auth errors must NOT retry — should return immediately."""
    session = mock.AsyncMock()
    resp = _make_response(401)
    session.post = mock.MagicMock(return_value=_make_cm(resp))

    result = asyncio.run(
        async_test_databricks_connection("host", "/path", "bad-token", session=session)
    )
    assert result == {"success": False, "error": "invalid_auth"}
    # Should only have been called once (no retries for auth errors)
    assert session.post.call_count == 1


def test_connection_403_returns_invalid_auth():
    session = mock.AsyncMock()
    resp = _make_response(403)
    session.post = mock.MagicMock(return_value=_make_cm(resp))

    result = asyncio.run(
        async_test_databricks_connection("host", "/path", "bad-token", session=session)
    )
    assert result == {"success": False, "error": "invalid_auth"}


def test_connection_retries_on_failed_state_then_succeeds():
    """Cluster cold-start: first attempt FAILED, second attempt SUCCEEDED."""
    session = mock.AsyncMock()
    resp_fail = _make_response(200, {"status": {"state": "FAILED"}})
    resp_ok = _make_response(200, {"status": {"state": "SUCCEEDED"}})
    session.post = mock.MagicMock(side_effect=[_make_cm(resp_fail), _make_cm(resp_ok)])

    with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
        result = asyncio.run(
            async_test_databricks_connection("host", "/path", "token", session=session)
        )
    assert result == {"success": True}
    assert session.post.call_count == 2


def test_connection_retries_exhausted_returns_cannot_connect():
    """All retries fail → cannot_connect."""
    session = mock.AsyncMock()
    resp_fail = _make_response(200, {"status": {"state": "FAILED"}})
    session.post = mock.MagicMock(return_value=_make_cm(resp_fail))

    with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
        result = asyncio.run(
            async_test_databricks_connection("host", "/path", "token", session=session)
        )
    assert result == {"success": False, "error": "cannot_connect"}
    # 1 initial + 4 retries = 5 total
    assert session.post.call_count == 5


def test_connection_client_response_error_401():
    session = mock.AsyncMock()
    error = aiohttp.ClientResponseError(
        request_info=mock.MagicMock(),
        history=(),
        status=401,
    )
    session.post = mock.MagicMock(return_value=_make_cm(error))

    result = asyncio.run(
        async_test_databricks_connection("host", "/path", "token", session=session)
    )
    assert result == {"success": False, "error": "invalid_auth"}


def test_connection_client_response_error_500_retries():
    session = mock.AsyncMock()
    error = aiohttp.ClientResponseError(
        request_info=mock.MagicMock(),
        history=(),
        status=500,
    )
    session.post = mock.MagicMock(return_value=_make_cm(error))

    with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
        result = asyncio.run(
            async_test_databricks_connection("host", "/path", "token", session=session)
        )
    assert result == {"success": False, "error": "cannot_connect"}
    assert session.post.call_count == 5


def test_connection_generic_exception_retries():
    session = mock.AsyncMock()
    session.post = mock.MagicMock(return_value=_make_cm(OSError("Network unreachable")))

    with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
        result = asyncio.run(
            async_test_databricks_connection("host", "/path", "token", session=session)
        )
    assert result == {"success": False, "error": "cannot_connect"}


def test_connection_creates_and_closes_session_when_none_provided():
    """When no session is provided, the function creates and closes its own."""
    resp = _make_response(200, {"status": {"state": "SUCCEEDED"}})
    mock_session = mock.AsyncMock()
    mock_session.post = mock.MagicMock(return_value=_make_cm(resp))
    mock_session.close = mock.AsyncMock()

    with mock.patch("aiohttp.ClientSession", return_value=mock_session):
        result = asyncio.run(
            async_test_databricks_connection("host", "/path", "token", session=None)
        )

    assert result == {"success": True}
    mock_session.close.assert_awaited_once()


def test_connection_sends_warehouse_id_from_http_path():
    """Verify the warehouse_id is extracted from http_path and sent in the payload."""
    session = mock.AsyncMock()
    resp = _make_response(200, {"status": {"state": "SUCCEEDED"}})
    session.post = mock.MagicMock(return_value=_make_cm(resp))

    http_path = "/sql/1.0/warehouses/abc123def"
    result = asyncio.run(
        async_test_databricks_connection("host", http_path, "token", session=session)
    )
    assert result == {"success": True}

    # Verify the payload includes the correct warehouse_id
    call_kwargs = session.post.call_args
    payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
    assert payload["warehouse_id"] == "abc123def"
    assert payload["statement"] == "SELECT 1"
