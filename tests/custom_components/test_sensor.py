from types import SimpleNamespace
from typing import cast

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from custom_components.hass_databricks.const import (
    DOMAIN,
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
    _iso_from_ts,
)


def _hass_with_meta(meta):
    return cast(
        HomeAssistant,
        SimpleNamespace(data={DOMAIN: {"entry-1": {"sync_meta": meta}}}),
    )


def _entry(entry_id: str = "entry-1"):
    return cast(ConfigEntry, SimpleNamespace(entry_id=entry_id))


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
    hass = _hass_with_meta(meta)
    entry = _entry("entry-1")

    sensor = HassDatabricksLastRunSensor(hass, entry)

    assert sensor.native_value == "success"
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
    hass = _hass_with_meta(meta)
    entry = _entry("entry-1")

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
