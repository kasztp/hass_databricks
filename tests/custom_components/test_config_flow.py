import asyncio
from types import SimpleNamespace
from unittest import mock

from typing import cast
from homeassistant.core import HomeAssistant
from custom_components.hass_databricks.config_flow import (
    HassDataBricksConfigFlow,
    HassDataBricksOptionsFlow,
)
from custom_components.hass_databricks.const import (
    CONF_ACCESS_TOKEN,
    CONF_AUTO_SYNC_ENABLED,
    CONF_AUTO_SYNC_INTERVAL_MINUTES,
    CONF_CATALOG,
    CONF_CHUNK_SIZE,
    CONF_DBX_VOLUMES_PATH,
    CONF_ENTITY_LIKE,
    CONF_HTTP_PATH,
    CONF_KEEP_LOCAL_FILE,
    CONF_LOCAL_PATH,
    CONF_SCHEMA,
    CONF_SERVER_HOSTNAME,
    CONF_TABLE,
)

_USER_INPUT = {
    CONF_SERVER_HOSTNAME: "adb-example.azuredatabricks.net",
    CONF_HTTP_PATH: "/sql/1.0/warehouses/abc",
    CONF_ACCESS_TOKEN: "token",
    CONF_CATALOG: "main",
    CONF_SCHEMA: "ha",
    CONF_TABLE: "states",
    CONF_DBX_VOLUMES_PATH: "/Volumes/main/ha/ingest",
}


def _flow_with_hass():
    """Build a config flow with a mocked hass instance."""
    flow = HassDataBricksConfigFlow()
    flow.hass = SimpleNamespace()
    return flow


def test_user_step_shows_form_when_no_input():
    flow = HassDataBricksConfigFlow()
    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "user"}
    )

    result = asyncio.run(flow.async_step_user(None))

    assert result["type"] == "form"
    flow.async_show_form.assert_called_once()


def test_user_step_creates_entry_on_successful_connection():
    flow = _flow_with_hass()
    flow.async_set_unique_id = mock.AsyncMock()
    flow._abort_if_unique_id_configured = mock.MagicMock()
    flow.async_create_entry = mock.MagicMock(return_value={"type": "create_entry"})

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.config_flow.async_get_clientsession"
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": True},
            ):
                return await flow.async_step_user(_USER_INPUT)

    result = asyncio.run(_run())

    assert result["type"] == "create_entry"
    flow.async_set_unique_id.assert_awaited_once()
    unique_id = flow.async_set_unique_id.call_args.args[0]
    assert unique_id == "adb-example.azuredatabricks.net|main.ha.states"


def test_user_step_shows_error_on_cannot_connect():
    flow = _flow_with_hass()
    flow.async_show_form = mock.MagicMock(
        return_value={
            "type": "form",
            "step_id": "user",
            "errors": {"base": "cannot_connect"},
        }
    )

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.config_flow.async_get_clientsession"
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": False, "error": "cannot_connect"},
            ):
                return await flow.async_step_user(_USER_INPUT)

    result = asyncio.run(_run())

    assert result["type"] == "form"
    call_kwargs = flow.async_show_form.call_args.kwargs
    assert call_kwargs["errors"] == {"base": "cannot_connect"}


def test_user_step_shows_error_on_invalid_auth():
    flow = _flow_with_hass()
    flow.async_show_form = mock.MagicMock(
        return_value={
            "type": "form",
            "step_id": "user",
            "errors": {"base": "invalid_auth"},
        }
    )

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.config_flow.async_get_clientsession"
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": False, "error": "invalid_auth"},
            ):
                return await flow.async_step_user(_USER_INPUT)

    result = asyncio.run(_run())

    assert result["type"] == "form"
    call_kwargs = flow.async_show_form.call_args.kwargs
    assert call_kwargs["errors"] == {"base": "invalid_auth"}


def test_reauth_step_and_confirm_update_entry():
    flow = HassDataBricksConfigFlow()
    entry = SimpleNamespace(entry_id="abc")
    flow.hass = SimpleNamespace(
        config_entries=SimpleNamespace(
            async_get_entry=mock.MagicMock(return_value=entry)
        )
    )
    flow.context = {"entry_id": "abc"}

    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "reauth_confirm"}
    )
    result = asyncio.run(flow.async_step_reauth({}))
    assert result["step_id"] == "reauth_confirm"

    flow.async_update_reload_and_abort = mock.MagicMock(
        return_value={"type": "abort", "reason": "reauth_successful"}
    )
    confirm = asyncio.run(
        flow.async_step_reauth_confirm({CONF_ACCESS_TOKEN: "new-token"})
    )
    assert confirm["type"] == "abort"
    flow.async_update_reload_and_abort.assert_called_once()


def test_reauth_confirm_shows_form_without_input():
    flow = HassDataBricksConfigFlow()
    flow._reauth_entry = None
    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "reauth_confirm"}
    )

    result = asyncio.run(flow.async_step_reauth_confirm(None))

    assert result["type"] == "form"
    assert result["step_id"] == "reauth_confirm"


def test_options_flow_init_paths():
    config_entry = SimpleNamespace(
        options={
            CONF_ENTITY_LIKE: "sensor.%",
            CONF_CHUNK_SIZE: 1000,
            CONF_KEEP_LOCAL_FILE: False,
            CONF_LOCAL_PATH: "/tmp/",
            CONF_AUTO_SYNC_ENABLED: True,
            CONF_AUTO_SYNC_INTERVAL_MINUTES: 60,
        }
    )
    with mock.patch("homeassistant.config_entries.report_usage", create=True):
        flow = HassDataBricksOptionsFlow(config_entry)
    flow.handler = "entry-1"
    flow.hass = SimpleNamespace(
        config_entries=SimpleNamespace(
            async_get_known_entry=mock.MagicMock(return_value=config_entry)
        )
    )

    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "init"}
    )
    result_form = asyncio.run(flow.async_step_init(None))
    assert result_form["type"] == "form"

    flow.async_create_entry = mock.MagicMock(return_value={"type": "create_entry"})
    result_create = asyncio.run(flow.async_step_init({CONF_CHUNK_SIZE: 2000}))
    assert result_create["type"] == "create_entry"


def test_reconfigure_flow_success():
    """Test reconfiguration flow preserves existing data and updates correctly."""
    flow = HassDataBricksConfigFlow()
    entry = SimpleNamespace(
        entry_id="abc",
        data={
            CONF_SERVER_HOSTNAME: "old-host",
            CONF_HTTP_PATH: "/old/path",
            CONF_ACCESS_TOKEN: "old-token",
            CONF_CATALOG: "main",
            CONF_SCHEMA: "ha",
            CONF_TABLE: "states",
        },
    )
    flow.hass = cast(
        HomeAssistant,
        SimpleNamespace(
            config_entries=SimpleNamespace(
                async_get_entry=mock.MagicMock(return_value=entry)
            )
        ),
    )
    flow.context = {"entry_id": "abc"}
    flow._get_reconfigure_entry = mock.MagicMock(return_value=entry)

    # 1. Show form
    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "reconfigure"}
    )
    result = asyncio.run(flow.async_step_reconfigure(None))
    assert result["type"] == "form"

    # 2. Submit success
    flow.async_update_reload_and_abort = mock.MagicMock(
        return_value={"type": "abort", "reason": "reconfigure_successful"}
    )
    new_input = {CONF_ACCESS_TOKEN: "new-token"}

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.config_flow.async_get_clientsession"
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": True},
            ):
                return await flow.async_step_reconfigure(new_input)

    result_submit = asyncio.run(_run())
    assert result_submit["type"] == "abort"
    # Verify we merged data
    flow.async_update_reload_and_abort.assert_called_once()
    merged_data = flow.async_update_reload_and_abort.call_args.kwargs["data"]
    assert merged_data[CONF_ACCESS_TOKEN] == "new-token"
    assert merged_data[CONF_SERVER_HOSTNAME] == "old-host"
