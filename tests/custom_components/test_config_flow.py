import asyncio
from types import SimpleNamespace
from unittest import mock

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


def test_user_step_shows_form_when_no_input():
    flow = HassDataBricksConfigFlow()
    flow.async_show_form = mock.MagicMock(
        return_value={"type": "form", "step_id": "user"}
    )

    result = asyncio.run(flow.async_step_user(None))

    assert result["type"] == "form"
    flow.async_show_form.assert_called_once()


def test_user_step_creates_entry_with_unique_id():
    flow = HassDataBricksConfigFlow()
    flow.async_set_unique_id = mock.AsyncMock()
    flow._abort_if_unique_id_configured = mock.MagicMock()
    flow.async_create_entry = mock.MagicMock(return_value={"type": "create_entry"})
    user_input = {
        CONF_SERVER_HOSTNAME: "adb-example.azuredatabricks.net",
        CONF_HTTP_PATH: "/sql/1.0/warehouses/abc",
        CONF_ACCESS_TOKEN: "token",
        CONF_CATALOG: "main",
        CONF_SCHEMA: "ha",
        CONF_TABLE: "states",
        CONF_DBX_VOLUMES_PATH: "/Volumes/main/ha/ingest",
    }

    result = asyncio.run(flow.async_step_user(user_input))

    assert result["type"] == "create_entry"
    flow.async_set_unique_id.assert_awaited_once()
    unique_id = flow.async_set_unique_id.call_args.args[0]
    assert unique_id == "adb-example.azuredatabricks.net|main.ha.states"


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
