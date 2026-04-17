"""Config flow for the hass_databricks integration."""

from __future__ import annotations
from typing import Any
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession

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


def _schema_with_defaults(user_input: dict | None) -> vol.Schema:
    """Rebuild user schema with suggested values from previous input."""
    if user_input is None:
        return STEP_USER_SCHEMA

    return vol.Schema(
        {
            vol.Required(
                CONF_SERVER_HOSTNAME,
                description={
                    "suggested_value": user_input.get(CONF_SERVER_HOSTNAME, "")
                },
            ): str,
            vol.Required(
                CONF_HTTP_PATH,
                description={"suggested_value": user_input.get(CONF_HTTP_PATH, "")},
            ): str,
            vol.Required(
                CONF_ACCESS_TOKEN,
                description={"suggested_value": user_input.get(CONF_ACCESS_TOKEN, "")},
            ): str,
            vol.Required(
                CONF_CATALOG,
                description={"suggested_value": user_input.get(CONF_CATALOG, "")},
            ): str,
            vol.Required(
                CONF_SCHEMA,
                description={"suggested_value": user_input.get(CONF_SCHEMA, "")},
            ): str,
            vol.Required(
                CONF_TABLE,
                description={"suggested_value": user_input.get(CONF_TABLE, "")},
            ): str,
            vol.Required(
                CONF_DBX_VOLUMES_PATH,
                description={
                    "suggested_value": user_input.get(CONF_DBX_VOLUMES_PATH, "")
                },
            ): str,
        }
    )


class HassDataBricksConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle the initial configuration for hass_databricks."""

    VERSION = 1
    _reauth_entry: config_entries.ConfigEntry | None = None

    async def async_step_user(self, user_input: dict | None = None) -> ConfigFlowResult:
        """Handle the user step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Test connection before creating entry
            from . import async_test_databricks_connection

            session = async_get_clientsession(self.hass)
            conn_result = await async_test_databricks_connection(
                server_hostname=user_input[CONF_SERVER_HOSTNAME],
                http_path=user_input[CONF_HTTP_PATH],
                access_token=user_input[CONF_ACCESS_TOKEN],
                session=session,
            )

            if not conn_result["success"]:
                errors["base"] = conn_result["error"]
            else:
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

        # Build schema with previously entered values pre-filled
        schema = _schema_with_defaults(user_input)

        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

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

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reconfiguration of the integration."""
        errors: dict[str, str] = {}
        config_entry = self._get_reconfigure_entry()

        if user_input is not None:
            # Join existing and new data to perform connection test
            test_input = {**config_entry.data, **user_input}
            from . import async_test_databricks_connection

            session = async_get_clientsession(self.hass)
            conn_result = await async_test_databricks_connection(
                server_hostname=test_input[CONF_SERVER_HOSTNAME],
                http_path=test_input[CONF_HTTP_PATH],
                access_token=test_input[CONF_ACCESS_TOKEN],
                session=session,
            )

            if not conn_result["success"]:
                errors["base"] = conn_result["error"]
            else:
                return self.async_update_reload_and_abort(config_entry, data=test_input)

        # Pre-fill form with existing values
        schema = _schema_with_defaults(user_input or config_entry.data)

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=schema,
            errors=errors,
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
