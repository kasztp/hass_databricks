# Integration Quality Scale — HASS Databricks

## Bronze
- [x] `action-setup` — Note: Service actions are registered in `async_setup_entry` (todo: move to `async_setup`)
- [x] `appropriate-polling` — N/A (exempt: not a polling integration)
- [x] `brands` — Branding assets available in `custom_components/hass_databricks/brand/`
- [x] `common-modules` — Common patterns placed in `const.py` and `pipeline.py`
- [x] `config-flow-test-coverage` — Full test coverage in `test_config_flow.py`
- [x] `config-flow` — UI-based setup via config flow with `data_description` fields
- [x] Uses `data_description` to give context to fields
- [x] Uses `ConfigEntry.data` and `ConfigEntry.options` correctly
- [x] `dependency-transparency` — No hidden dependencies; `requirements` list is empty in `manifest.json`
- [x] `docs-actions` — Service actions documented in README and `services.yaml`
- [x] `docs-high-level-description` — README includes project architecture overview
- [x] `docs-installation-instructions` — Step-by-step HACS and manual install instructions
- [x] `docs-removal-instructions` — Removal section in README
- [x] `entity-event-setup` — `async_added_to_hass` subscribes to `hass_databricks_sync_result` events
- [x] `entity-unique-id` — All entities use `{entry_id}_{suffix}` unique IDs
- [x] `has-entity-name` — All entities set `_attr_has_entity_name = True`
- [x] `runtime-data` — Uses `ConfigEntry.runtime_data` with `HassDataBricksRuntimeData` dataclass
- [x] `test-before-configure` — Config flow tests Databricks with `SELECT 1` before creating entry
- [x] `test-before-setup` — `async_setup_entry` verifies connectivity; raises `ConfigEntryNotReady` or triggers reauth
- [x] `unique-config-entry` — Unique ID based on `hostname|catalog.schema.table` prevents duplicates

## Silver
- [x] `action-exceptions` — Sync service raises `HomeAssistantError` on failure
- [x] `config-entry-unloading` — `async_unload_entry` unloads platforms and cancels auto-sync timers
- [x] `docs-configuration-parameters` — Options flow parameters documented in README
- [x] `docs-installation-parameters` — Setup parameters (hostname, token, catalog, etc.) documented
- [x] `entity-unavailable` — Sensors marked unavailable on sync failure, restored on success
- [x] `integration-owner` — `@kasztp` listed as code owner in `manifest.json`
- [x] `log-when-unavailable` — Logs once on unavailability, logs once on reconnect
- [x] `parallel-updates` — `PARALLEL_UPDATES = 1` set in `const.py`
- [x] `reauthentication-flow` — `async_step_reauth` / `async_step_reauth_confirm` implemented
- [x] `test-coverage` — 98% coverage across all integration modules (592 statements, 9 missed)

## Gold
- [x] `devices` — Button entity creates device via `device_info` with identifiers and metadata
- [ ] `diagnostics` — No diagnostics download support implemented
- [x] `discovery-update-info` — N/A (exempt: cloud service, no discoverable network devices)
- [x] `discovery` — N/A (exempt: cloud service, not locally discoverable)
- [x] `docs-data-update` — README describes sync mechanism and data flow
- [ ] `docs-examples` — Missing automation and dashboard examples
- [x] `docs-known-limitations` — Dedicated section added to README
- [x] `docs-supported-devices` — N/A (exempt: no hardware devices)
- [x] `docs-supported-functions` — README lists all provided entities (sensors/buttons) and their functions
- [ ] `docs-troubleshooting` — Missing troubleshooting section
- [ ] `docs-use-cases` — Missing use-case examples
- [x] `dynamic-devices` — N/A (exempt: static device per config entry)
- [x] `entity-category` — Status sensors (run, rows, success) are assigned to `EntityCategory.DIAGNOSTIC`
- [ ] `entity-device-class` — Only `last_success` uses `SensorDeviceClass.TIMESTAMP`
- [x] `entity-disabled-by-default` — N/A (exempt: all entities provide essential sync info)
- [ ] `entity-translations` — Sensor names use `_attr_name` literals, not translation keys
- [ ] `exception-translations` — Exception messages are plain strings
- [ ] `icon-translations` — Icons use hard-coded `_attr_icon` strings
- [x] `reconfiguration-flow` — `async_step_reconfigure` is implemented in the config flow
- [ ] `repair-issues` — No repair issues or repair flows used
- [x] `stale-devices` — N/A (exempt: one device per entry, removed on unload)

## Platinum
- [x] `async-dependency` — All Databricks API calls use `aiohttp` (async); file I/O via executor jobs
- [x] `inject-websession` — HA session injected via `async_get_clientsession(hass)`
- [ ] `strict-typing` — Type hints present but not enforced with strict mypy settings
