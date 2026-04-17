"""Tests for the HASS Databricks button platform."""

from unittest import mock
import asyncio
from types import SimpleNamespace
from typing import cast

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.hass_databricks import HassDataBricksRuntimeData
from custom_components.hass_databricks.const import DOMAIN
from custom_components.hass_databricks.button import (
    HassDataBricksSyncButton,
    async_setup_entry,
)


def _entry(entry_id: str = "entry-1", catalog="main", schema="ha", table="states"):
    return cast(
        ConfigEntry,
        SimpleNamespace(
            entry_id=entry_id,
            data={
                "catalog": catalog,
                "schema": schema,
                "table": table,
            },
            runtime_data=None,
        ),
    )


def test_async_setup_entry_adds_one_button():
    hass = cast(HomeAssistant, SimpleNamespace(data={}))
    entry = _entry("entry-1")
    added = []

    asyncio.run(async_setup_entry(hass, entry, lambda entities: added.extend(entities)))

    assert len(added) == 1
    assert isinstance(added[0], HassDataBricksSyncButton)


def test_button_properties():
    entry = _entry("entry-1", catalog="CAT", schema="SCH", table="TAB")
    button = HassDataBricksSyncButton(entry)

    assert button.has_entity_name is True
    assert button.translation_key == "sync_now"
    assert button.icon == "mdi:sync"
    assert button.unique_id == "entry-1_sync_now"

    device_info = button.device_info
    assert device_info["name"] == "HASS Databricks (CAT.SCH.TAB)"
    assert device_info["identifiers"] == {(DOMAIN, "entry-1")}


def test_button_press_calls_sync_func():
    sync_mock = mock.AsyncMock()
    entry = _entry("entry-1")
    entry.runtime_data = HassDataBricksRuntimeData(
        store=mock.MagicMock(), sync_func=sync_mock
    )
    button = HassDataBricksSyncButton(entry)
    button.hass = cast(HomeAssistant, SimpleNamespace(data={}))

    asyncio.run(button.async_press())

    sync_mock.assert_awaited_once_with({}, "button")


def test_button_press_logs_error_if_sync_func_missing(caplog):
    entry = _entry("entry-1")
    entry.runtime_data = HassDataBricksRuntimeData(
        store=mock.MagicMock(), sync_func=None
    )
    button = HassDataBricksSyncButton(entry)
    button.hass = cast(HomeAssistant, SimpleNamespace(data={}))

    asyncio.run(button.async_press())

    assert "Sync function not found" in caplog.text


def test_button_press_logs_error_if_runtime_data_missing(caplog):
    entry = _entry("entry-1")
    # runtime_data is None (not set yet)
    button = HassDataBricksSyncButton(entry)
    button.hass = cast(HomeAssistant, SimpleNamespace(data={}))

    asyncio.run(button.async_press())

    assert "Sync function not found" in caplog.text
