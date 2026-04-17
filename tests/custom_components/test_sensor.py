from types import SimpleNamespace
from typing import cast
import asyncio

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.hass_databricks import HassDataBricksRuntimeData
from custom_components.hass_databricks.const import (
    SYNC_META_LAST_ERROR,
    SYNC_META_LAST_FILENAME,
    SYNC_META_LAST_ROWS,
    SYNC_META_LAST_RUN_TS,
    SYNC_META_LAST_SINCE_TS,
    SYNC_META_LAST_STATUS,
    SYNC_META_LAST_SUCCESS_TS,
    SYNC_META_LAST_TARGET,
    SYNC_META_LAST_TRIGGER,
)
from custom_components.hass_databricks.sensor import (
    HassDatabricksLastRowsSensor,
    HassDatabricksLastRunSensor,
    HassDatabricksLastSuccessSensor,
    async_setup_entry,
    _iso_from_ts,
)


def _entry_with_meta(meta, entry_id="entry-1"):
    entry = SimpleNamespace(
        entry_id=entry_id,
        runtime_data=HassDataBricksRuntimeData(store=None, sync_meta=meta),
    )
    return cast(ConfigEntry, entry)


def _hass():
    return cast(HomeAssistant, SimpleNamespace(data={}))


def test_iso_from_ts_handles_invalid_values():
    assert _iso_from_ts(None) is None
    assert _iso_from_ts("not-a-number") is None


def test_last_run_sensor_exposes_status_and_attributes():
    meta = {
        SYNC_META_LAST_STATUS: "success",
        SYNC_META_LAST_RUN_TS: 1700000100.0,
        SYNC_META_LAST_SUCCESS_TS: 1700000200.0,
        SYNC_META_LAST_SINCE_TS: 1700000000.0,
        SYNC_META_LAST_ROWS: 42,
        SYNC_META_LAST_FILENAME: "upload.parquet",
        SYNC_META_LAST_TARGET: "main.ha.states",
        SYNC_META_LAST_TRIGGER: "scheduled",
        SYNC_META_LAST_ERROR: None,
    }
    hass = _hass()
    entry = _entry_with_meta(meta)

    sensor = HassDatabricksLastRunSensor(hass, entry)

    assert sensor.native_value == "success"
    assert sensor.entity_category == "diagnostic"
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert attrs["last_rows"] == 42
    assert attrs["last_filename"] == "upload.parquet"
    assert attrs["last_target"] == "main.ha.states"


def test_last_rows_and_last_success_sensors_refresh_from_meta():
    meta = {
        SYNC_META_LAST_ROWS: 7,
        SYNC_META_LAST_SUCCESS_TS: 1700000300.0,
    }
    hass = _hass()
    entry = _entry_with_meta(meta)

    rows_sensor = HassDatabricksLastRowsSensor(hass, entry)
    success_sensor = HassDatabricksLastSuccessSensor(hass, entry)

    assert rows_sensor.native_value == 7
    assert success_sensor.native_value is not None

    meta[SYNC_META_LAST_ROWS] = 9
    meta[SYNC_META_LAST_SUCCESS_TS] = 1700000400.0

    rows_sensor._refresh_from_meta()
    success_sensor._refresh_from_meta()

    assert rows_sensor.native_value == 9
    assert success_sensor.native_value is not None


def test_sensor_availability_toggles_from_sync_event():
    meta = {
        SYNC_META_LAST_STATUS: "success",
        SYNC_META_LAST_ROWS: 1,
    }
    hass = _hass()
    entry = _entry_with_meta(meta)
    sensor = HassDatabricksLastRunSensor(hass, entry)
    sensor.async_write_ha_state = lambda: None

    sensor._handle_sync_event(
        SimpleNamespace(data={"entry_id": "entry-1", "success": False})
    )
    assert sensor.available is False

    sensor._handle_sync_event(
        SimpleNamespace(data={"entry_id": "entry-1", "success": True})
    )
    assert sensor.available is True


def test_sensor_has_entity_name_enabled():
    meta = {SYNC_META_LAST_STATUS: "never"}
    hass = _hass()
    entry = _entry_with_meta(meta)
    sensor = HassDatabricksLastRowsSensor(hass, entry)

    assert sensor.has_entity_name is True


def test_async_setup_entry_adds_three_entities():
    hass = _hass()
    entry = _entry_with_meta({})
    added = []

    asyncio.run(async_setup_entry(hass, entry, lambda entities: added.extend(entities)))

    assert len(added) == 3


def test_added_to_hass_registers_event_listener():
    remove_callbacks = []

    hass = cast(
        HomeAssistant,
        SimpleNamespace(
            data={},
            bus=SimpleNamespace(async_listen=lambda *_: lambda: None),
        ),
    )
    entry = _entry_with_meta({})
    sensor = HassDatabricksLastRunSensor(hass, entry)
    sensor.async_on_remove = lambda cb: remove_callbacks.append(cb)

    asyncio.run(sensor.async_added_to_hass())

    assert len(remove_callbacks) == 1


def test_handle_sync_event_ignores_other_entry_ids():
    meta = {SYNC_META_LAST_STATUS: "success"}
    hass = _hass()
    entry = _entry_with_meta(meta)
    sensor = HassDatabricksLastRunSensor(hass, entry)
    sensor.async_write_ha_state = lambda: None

    sensor._handle_sync_event(
        SimpleNamespace(data={"entry_id": "other", "success": False})
    )

    assert sensor.available is True


def test_last_success_sensor_none_when_missing_timestamp():
    meta = {}
    hass = _hass()
    entry = _entry_with_meta(meta)

    sensor = HassDatabricksLastSuccessSensor(hass, entry)

    assert sensor.native_value is None
