"""Home Assistant integration setup for hass_databricks."""

from __future__ import annotations

from datetime import timedelta
import logging
import time

from homeassistant.config_entries import ConfigEntry, SOURCE_REAUTH
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.const import Platform

from .const import (
    CONF_ACCESS_TOKEN,
    CONF_AUTO_SYNC_ENABLED,
    CONF_AUTO_SYNC_INTERVAL_MINUTES,
    CONF_CATALOG,
    CONF_CHUNK_SIZE,
    CONF_DB_PATH,
    CONF_DBX_VOLUMES_PATH,
    CONF_ENTITY_LIKE,
    CONF_HTTP_PATH,
    CONF_HOT_COPY_DB,
    CONF_KEEP_LOCAL_FILE,
    CONF_LOCAL_PATH,
    CONF_SCHEMA,
    CONF_SERVER_HOSTNAME,
    CONF_TABLE,
    DEFAULT_AUTO_SYNC_ENABLED,
    DEFAULT_AUTO_SYNC_INTERVAL_MINUTES,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_DB_FILENAME,
    DEFAULT_ENTITY_LIKE,
    DEFAULT_INCREMENTAL_LOOKBACK_MINUTES,
    DEFAULT_LOCAL_PATH,
    DOMAIN,
    EVENT_SYNC_RESULT,
    SERVICE_SYNC,
    SYNC_META_AVAILABLE,
    SYNC_META_LAST_ERROR,
    SYNC_META_LAST_FILENAME,
    SYNC_META_LAST_ROWS,
    SYNC_META_LAST_RUN_TS,
    SYNC_META_LAST_SINCE_TS,
    SYNC_META_LAST_STATUS,
    SYNC_META_LAST_UNAVAILABLE_LOG,
    STORAGE_KEY_PREFIX,
    STORAGE_VERSION,
    SYNC_META_LAST_SUCCESS_TS,
    SYNC_META_LAST_TARGET,
    SYNC_META_LAST_TRIGGER,
)

_LOGGER = logging.getLogger(__name__)
PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up hass_databricks from a config entry."""
    store = Store[dict](hass, STORAGE_VERSION, f"{STORAGE_KEY_PREFIX}.{entry.entry_id}")
    sync_meta = await store.async_load() or {}

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "entry": entry,
        "unsub_auto_sync": None,
        "store": store,
        "sync_meta": sync_meta,
    }

    async def _record_sync_result(
        *,
        success: bool,
        trigger: str,
        target: str,
        since_ts: float | None = None,
        run_ts: float | None = None,
        last_success_ts: float | None = None,
        rows: int | None = None,
        filename: str | None = None,
        error: str | None = None,
    ) -> None:
        """Record one sync run for Activity/Logbook and automations."""
        sync_meta = hass.data[DOMAIN][entry.entry_id]["sync_meta"]

        # Log once when service becomes unavailable
        if not success:
            was_available = sync_meta.get(SYNC_META_AVAILABLE, True)
            if was_available:
                _LOGGER.warning(
                    "hass_databricks sync unavailable: %s", error or "unknown error"
                )
                sync_meta[SYNC_META_AVAILABLE] = False
                sync_meta[SYNC_META_LAST_UNAVAILABLE_LOG] = run_ts
        else:
            was_available = sync_meta.get(SYNC_META_AVAILABLE, True)
            if not was_available:
                _LOGGER.info("hass_databricks sync reconnected and operational")
                sync_meta[SYNC_META_AVAILABLE] = True

        if success:
            message = f"Sync succeeded ({rows} rows) -> {target}"
            if filename:
                message = f"{message} ({filename})"
        else:
            message = f"Sync failed -> {target}: {error or 'unknown error'}"

        await hass.services.async_call(
            "logbook",
            "log",
            {
                "name": "HASS Databricks",
                "message": message,
                "domain": DOMAIN,
            },
            blocking=False,
        )

        hass.bus.async_fire(
            EVENT_SYNC_RESULT,
            {
                "entry_id": entry.entry_id,
                "success": success,
                "trigger": trigger,
                "target": target,
                "since_ts": since_ts,
                "run_ts": run_ts,
                "last_success_ts": last_success_ts,
                "rows": rows,
                "filename": filename,
                "error": error,
            },
        )

    async def _run_sync(call_data: dict, trigger: str) -> None:
        """Run one sync execution for this config entry."""
        try:
            from .pipeline import SyncRequest, run_sync_pipeline
        except ImportError as err:
            raise HomeAssistantError(
                "Sync dependencies are unavailable in this Home Assistant runtime. "
                "Check integration requirements and container build dependencies."
            ) from err

        data = entry.data
        opts = entry.options

        db_path = call_data.get(CONF_DB_PATH) or hass.config.path(DEFAULT_DB_FILENAME)

        request = SyncRequest(
            db_path=db_path,
            server_hostname=data[CONF_SERVER_HOSTNAME],
            http_path=data[CONF_HTTP_PATH],
            access_token=data[CONF_ACCESS_TOKEN],
            catalog=call_data.get(CONF_CATALOG, data[CONF_CATALOG]),
            schema=call_data.get(CONF_SCHEMA, data[CONF_SCHEMA]),
            table=call_data.get(CONF_TABLE, data[CONF_TABLE]),
            local_path=call_data.get(
                CONF_LOCAL_PATH, opts.get(CONF_LOCAL_PATH, DEFAULT_LOCAL_PATH)
            ),
            dbx_volumes_path=call_data.get(
                CONF_DBX_VOLUMES_PATH, data[CONF_DBX_VOLUMES_PATH]
            ),
            entity_like=call_data.get(
                CONF_ENTITY_LIKE, opts.get(CONF_ENTITY_LIKE, DEFAULT_ENTITY_LIKE)
            ),
            chunk_size=int(
                call_data.get(
                    CONF_CHUNK_SIZE, opts.get(CONF_CHUNK_SIZE, DEFAULT_CHUNK_SIZE)
                )
            ),
            keep_local_file=bool(
                call_data.get(
                    CONF_KEEP_LOCAL_FILE, opts.get(CONF_KEEP_LOCAL_FILE, False)
                )
            ),
            hot_copy_db=call_data.get(CONF_HOT_COPY_DB),
            min_last_updated_ts=None,
            session=async_get_clientsession(hass),
            hass=hass,
        )

        last_success_ts = hass.data[DOMAIN][entry.entry_id]["sync_meta"].get(
            SYNC_META_LAST_SUCCESS_TS
        )
        if last_success_ts is not None:
            lookback_seconds = DEFAULT_INCREMENTAL_LOOKBACK_MINUTES * 60
            request.min_last_updated_ts = float(last_success_ts) - lookback_seconds

        target = f"{request.catalog}.{request.schema}.{request.table}"
        run_ts = time.time()

        async def _start_reauth_if_needed(err: Exception) -> None:
            """Trigger reauthentication flow on auth-related failures."""
            err_text = str(err).lower()
            auth_markers = ("auth", "token", "unauthorized", "forbidden", "401")
            if not any(marker in err_text for marker in auth_markers):
                return
            await hass.config_entries.flow.async_init(
                DOMAIN,
                context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
                data={
                    CONF_SERVER_HOSTNAME: data[CONF_SERVER_HOSTNAME],
                    CONF_HTTP_PATH: data[CONF_HTTP_PATH],
                },
            )

        _LOGGER.info("Starting hass_databricks sync service")
        try:
            result = await run_sync_pipeline(request)
        except Exception as err:
            try:
                await _start_reauth_if_needed(err)
            except Exception:
                _LOGGER.debug("Failed to start reauthentication flow", exc_info=True)
            sync_meta = hass.data[DOMAIN][entry.entry_id]["sync_meta"]
            sync_meta[SYNC_META_LAST_RUN_TS] = run_ts
            sync_meta[SYNC_META_LAST_STATUS] = "failed"
            sync_meta[SYNC_META_LAST_TRIGGER] = trigger
            sync_meta[SYNC_META_LAST_TARGET] = target
            sync_meta[SYNC_META_LAST_SINCE_TS] = request.min_last_updated_ts
            sync_meta[SYNC_META_LAST_ROWS] = None
            sync_meta[SYNC_META_LAST_FILENAME] = None
            sync_meta[SYNC_META_LAST_ERROR] = str(err)
            await store.async_save(sync_meta)
            await _record_sync_result(
                success=False,
                trigger=trigger,
                target=target,
                since_ts=request.min_last_updated_ts,
                run_ts=run_ts,
                last_success_ts=sync_meta.get(SYNC_META_LAST_SUCCESS_TS),
                error=str(err),
            )
            raise

        _LOGGER.info(
            "Completed hass_databricks sync: file=%s rows=%s",
            result["filename"],
            result["rows"],
        )
        sync_meta = hass.data[DOMAIN][entry.entry_id]["sync_meta"]
        new_last_success_ts = float(result.get("max_last_updated_ts") or time.time())
        sync_meta[SYNC_META_LAST_RUN_TS] = run_ts
        sync_meta[SYNC_META_LAST_STATUS] = "success"
        sync_meta[SYNC_META_LAST_TRIGGER] = trigger
        sync_meta[SYNC_META_LAST_TARGET] = target
        sync_meta[SYNC_META_LAST_SINCE_TS] = request.min_last_updated_ts
        sync_meta[SYNC_META_LAST_ROWS] = int(result["rows"])
        sync_meta[SYNC_META_LAST_FILENAME] = str(result["filename"])
        sync_meta[SYNC_META_LAST_ERROR] = None
        sync_meta[SYNC_META_LAST_SUCCESS_TS] = new_last_success_ts
        await store.async_save(sync_meta)

        await _record_sync_result(
            success=True,
            trigger=trigger,
            target=target,
            since_ts=request.min_last_updated_ts,
            run_ts=run_ts,
            last_success_ts=new_last_success_ts,
            rows=int(result["rows"]),
            filename=str(result["filename"]),
        )

    async def handle_sync(call: ServiceCall) -> None:
        await _run_sync(call.data, "service")

    auto_sync_enabled = bool(
        entry.options.get(CONF_AUTO_SYNC_ENABLED, DEFAULT_AUTO_SYNC_ENABLED)
    )
    auto_sync_interval_minutes = int(
        entry.options.get(
            CONF_AUTO_SYNC_INTERVAL_MINUTES, DEFAULT_AUTO_SYNC_INTERVAL_MINUTES
        )
    )
    if auto_sync_enabled:
        interval = timedelta(minutes=max(1, auto_sync_interval_minutes))

        async def _handle_scheduled_sync(_now) -> None:
            try:
                await _run_sync({}, "scheduled")
            except HomeAssistantError:
                _LOGGER.exception("Scheduled hass_databricks sync failed")
            except Exception:
                _LOGGER.exception("Unexpected scheduled hass_databricks sync failure")

        unsub_auto_sync = async_track_time_interval(
            hass, _handle_scheduled_sync, interval
        )
        hass.data[DOMAIN][entry.entry_id]["unsub_auto_sync"] = unsub_auto_sync
        _LOGGER.debug(
            "Automatic sync enabled for %s every %s minutes",
            entry.entry_id,
            auto_sync_interval_minutes,
        )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    hass.services.async_register(DOMAIN, SERVICE_SYNC, handle_sync)
    _LOGGER.debug("Service registered: %s.%s", DOMAIN, SERVICE_SYNC)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    entry_data = hass.data[DOMAIN].pop(entry.entry_id, None)
    if entry_data and entry_data.get("unsub_auto_sync") is not None:
        entry_data["unsub_auto_sync"]()
    if not hass.data[DOMAIN]:
        hass.services.async_remove(DOMAIN, SERVICE_SYNC)
    return unload_ok
