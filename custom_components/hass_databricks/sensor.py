"""Sensor entities for hass_databricks integration."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DOMAIN,
    EVENT_SYNC_RESULT,
    PARALLEL_UPDATES,
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


def _iso_from_ts(value: Any) -> str | None:
    """Convert unix timestamp to UTC ISO string."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except TypeError, ValueError:
        return None


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up hass_databricks sensor entities from config entry."""
    async_add_entities(
        [
            HassDatabricksLastRunSensor(hass, entry),
            HassDatabricksLastRowsSensor(hass, entry),
            HassDatabricksLastSuccessSensor(hass, entry),
        ]
    )


class HassDatabricksBaseSensor(SensorEntity):
    """Base class that refreshes from sync metadata and listens for updates."""

    _attr_has_entity_name = True
    _attr_parallel_updates = PARALLEL_UPDATES

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self._entry = entry
        self._attr_available = True

    @property
    def sync_meta(self) -> dict[str, Any]:
        """Return current sync metadata for this config entry."""
        return self.hass.data[DOMAIN][self._entry.entry_id]["sync_meta"]

    async def async_added_to_hass(self) -> None:
        """Subscribe to sync result events for immediate state refresh."""
        self.async_on_remove(
            self.hass.bus.async_listen(EVENT_SYNC_RESULT, self._handle_sync_event)
        )

    @callback
    def _handle_sync_event(self, event: Event) -> None:
        """Handle integration sync event and update state."""
        if event.data.get("entry_id") != self._entry.entry_id:
            return
        self._attr_available = event.data.get("success", True)
        self._refresh_from_meta()
        self.async_write_ha_state()

    def _refresh_from_meta(self) -> None:
        """Refresh entity attributes from sync metadata."""


class HassDatabricksLastRunSensor(HassDatabricksBaseSensor):
    """Expose last sync run status and details."""

    _attr_icon = "mdi:database-sync"

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(hass, entry)
        self._attr_unique_id = f"{entry.entry_id}_last_run"
        self._attr_name = "Last Run"
        self._attr_native_value = "never"
        self._attr_extra_state_attributes = {}
        self._refresh_from_meta()

    def _refresh_from_meta(self) -> None:
        """Load sensor state from persisted sync metadata."""
        meta = self.sync_meta
        self._attr_native_value = str(meta.get(SYNC_META_LAST_STATUS, "never"))
        self._attr_extra_state_attributes = {
            "last_run": _iso_from_ts(meta.get(SYNC_META_LAST_RUN_TS)),
            "last_success": _iso_from_ts(meta.get(SYNC_META_LAST_SUCCESS_TS)),
            "last_since": _iso_from_ts(meta.get(SYNC_META_LAST_SINCE_TS)),
            "last_rows": meta.get(SYNC_META_LAST_ROWS),
            "last_filename": meta.get(SYNC_META_LAST_FILENAME),
            "last_target": meta.get(SYNC_META_LAST_TARGET),
            "last_trigger": meta.get(SYNC_META_LAST_TRIGGER),
            "last_error": meta.get(SYNC_META_LAST_ERROR),
        }


class HassDatabricksLastRowsSensor(HassDatabricksBaseSensor):
    """Expose number of rows uploaded in the last sync run."""

    _attr_icon = "mdi:database-arrow-up"

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(hass, entry)
        self._attr_unique_id = f"{entry.entry_id}_last_rows"
        self._attr_name = "Last Rows"
        self._attr_native_value = None
        self._refresh_from_meta()

    def _refresh_from_meta(self) -> None:
        """Load row-count sensor state from persisted sync metadata."""
        rows = self.sync_meta.get(SYNC_META_LAST_ROWS)
        self._attr_native_value = int(rows) if rows is not None else None


class HassDatabricksLastSuccessSensor(HassDatabricksBaseSensor):
    """Expose timestamp of the last successful sync run."""

    _attr_icon = "mdi:clock-check-outline"
    _attr_device_class = SensorDeviceClass.TIMESTAMP

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        super().__init__(hass, entry)
        self._attr_unique_id = f"{entry.entry_id}_last_success"
        self._attr_name = "Last Success"
        self._attr_native_value = None
        self._refresh_from_meta()

    def _refresh_from_meta(self) -> None:
        """Load last-success timestamp from persisted sync metadata."""
        last_success_ts = self.sync_meta.get(SYNC_META_LAST_SUCCESS_TS)
        if last_success_ts is None:
            self._attr_native_value = None
            return
        self._attr_native_value = datetime.fromtimestamp(
            float(last_success_ts), tz=timezone.utc
        )
