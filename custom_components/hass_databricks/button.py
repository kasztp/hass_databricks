"""Button platform for HASS Databricks."""

from __future__ import annotations

import logging

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the HASS Databricks button platform."""
    async_add_entities([HassDataBricksSyncButton(entry)])


class HassDataBricksSyncButton(ButtonEntity):
    """Button to trigger a manual sync."""

    _attr_has_entity_name = True
    _attr_translation_key = "sync_now"
    _attr_icon = "mdi:sync"

    def __init__(self, entry: ConfigEntry) -> None:
        """Initialize the button."""
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_sync_now"
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry.entry_id)},
            "name": f"HASS Databricks ({entry.data.get('catalog')}.{entry.data.get('schema')}.{entry.data.get('table')})",
            "manufacturer": "Peter Kaszt",
            "model": "Databricks Sync",
        }

    async def async_press(self) -> None:
        """Handle the button press."""
        runtime_data = getattr(self._entry, "runtime_data", None)
        sync_func = getattr(runtime_data, "sync_func", None) if runtime_data else None
        if sync_func:
            _LOGGER.info(
                "Manual sync triggered via button for %s", self._entry.entry_id
            )
            await sync_func({}, "button")
        else:
            _LOGGER.error("Sync function not found for %s", self._entry.entry_id)
