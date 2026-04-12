"""Config flow for the hass_databricks integration."""

from __future__ import annotations

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.core import callback

from .const import (
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
    DEFAULT_CHUNK_SIZE,
    DEFAULT_ENTITY_LIKE,
    DEFAULT_LOCAL_PATH,
    DEFAULT_AUTO_SYNC_ENABLED,
    DEFAULT_AUTO_SYNC_INTERVAL_MINUTES,
    DOMAIN,
)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_SERVER_HOSTNAME): str,
        vol.Required(CONF_HTTP_PATH): str,
        vol.Required(CONF_ACCESS_TOKEN): str,
        vol.Required(CONF_CATALOG): str,
        vol.Required(CONF_SCHEMA): str,
        vol.Required(CONF_TABLE): str,
        vol.Required(CONF_DBX_VOLUMES_PATH): str,
    }
)

STEP_REAUTH_CONFIRM_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ACCESS_TOKEN): str,
    }
)


class HassDataBricksConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the initial configuration for hass_databricks."""

    VERSION = 1
    _reauth_entry: config_entries.ConfigEntry | None = None

    async def async_step_user(self, user_input: dict | None = None) -> ConfigFlowResult:
        """Handle the user step."""
        if user_input is not None:
            await self.async_set_unique_id(
                f"{user_input[CONF_SERVER_HOSTNAME]}"
                f"|{user_input[CONF_CATALOG]}"
                f".{user_input[CONF_SCHEMA]}"
                f".{user_input[CONF_TABLE]}"
            )
            self._abort_if_unique_id_configured()
            return self.async_create_entry(
                title=(
                    f"{user_input[CONF_CATALOG]}"
                    f".{user_input[CONF_SCHEMA]}"
                    f".{user_input[CONF_TABLE]}"
                ),
                data=user_input,
            )

        return self.async_show_form(step_id="user", data_schema=STEP_USER_SCHEMA)

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> HassDataBricksOptionsFlow:
        """Return the options flow handler."""
        return HassDataBricksOptionsFlow(config_entry)

    async def async_step_reauth(self, entry_data: dict) -> ConfigFlowResult:
        """Start reauthentication flow."""
        entry_id = self.context.get("entry_id")
        if entry_id is None:
            return self.async_abort(reason="unknown")
        self._reauth_entry = self.hass.config_entries.async_get_entry(entry_id)
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict | None = None
    ) -> ConfigFlowResult:
        """Confirm and store a new access token for reauthentication."""
        if user_input is not None and self._reauth_entry is not None:
            return self.async_update_reload_and_abort(
                self._reauth_entry,
                data_updates={CONF_ACCESS_TOKEN: user_input[CONF_ACCESS_TOKEN]},
            )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=STEP_REAUTH_CONFIRM_SCHEMA,
        )


class HassDataBricksOptionsFlow(config_entries.OptionsFlowWithConfigEntry):
    """Handle option updates for hass_databricks."""

    async def async_step_init(self, user_input: dict | None = None) -> ConfigFlowResult:
        """Handle options form."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_ENTITY_LIKE,
                    default=current.get(CONF_ENTITY_LIKE, DEFAULT_ENTITY_LIKE),
                ): str,
                vol.Optional(
                    CONF_CHUNK_SIZE,
                    default=current.get(CONF_CHUNK_SIZE, DEFAULT_CHUNK_SIZE),
                ): int,
                vol.Optional(
                    CONF_KEEP_LOCAL_FILE,
                    default=current.get(CONF_KEEP_LOCAL_FILE, False),
                ): bool,
                vol.Optional(
                    CONF_LOCAL_PATH,
                    default=current.get(CONF_LOCAL_PATH, DEFAULT_LOCAL_PATH),
                ): str,
                vol.Optional(
                    CONF_AUTO_SYNC_ENABLED,
                    default=current.get(
                        CONF_AUTO_SYNC_ENABLED, DEFAULT_AUTO_SYNC_ENABLED
                    ),
                ): bool,
                vol.Optional(
                    CONF_AUTO_SYNC_INTERVAL_MINUTES,
                    default=current.get(
                        CONF_AUTO_SYNC_INTERVAL_MINUTES,
                        DEFAULT_AUTO_SYNC_INTERVAL_MINUTES,
                    ),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=1440)),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
