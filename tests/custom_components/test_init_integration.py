import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest
from homeassistant.exceptions import HomeAssistantError

from custom_components.hass_databricks import (
    _async_update_listener,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.hass_databricks.const import (
    CONF_ACCESS_TOKEN,
    CONF_AUTO_SYNC_ENABLED,
    CONF_AUTO_SYNC_INTERVAL_MINUTES,
    CONF_CATALOG,
    CONF_DBX_VOLUMES_PATH,
    CONF_HTTP_PATH,
    CONF_SCHEMA,
    CONF_SERVER_HOSTNAME,
    CONF_TABLE,
    DOMAIN,
    SERVICE_SYNC,
    SYNC_META_AVAILABLE,
    SYNC_META_LAST_STATUS,
)


class DummyStore:
    initial_data = {}
    instances = []

    def __class_getitem__(cls, _item):
        return cls

    def __init__(self, *_args, **_kwargs):
        self._data = dict(self.__class__.initial_data)
        self.saved = []
        self.__class__.instances.append(self)

    async def async_load(self):
        return self._data

    async def async_save(self, data):
        self.saved.append(dict(data))


def _build_hass():
    services = SimpleNamespace(
        async_register=mock.MagicMock(),
        async_remove=mock.MagicMock(),
        async_call=mock.AsyncMock(),
    )
    flow = SimpleNamespace(async_init=mock.AsyncMock())
    config_entries = SimpleNamespace(
        async_forward_entry_setups=mock.AsyncMock(return_value=True),
        async_unload_platforms=mock.AsyncMock(return_value=True),
        async_reload=mock.AsyncMock(return_value=True),
        flow=flow,
    )

    async def _exec_in_executor(func, *args):
        return func(*args)

    hass = mock.MagicMock()
    hass.data = {}
    hass.config = SimpleNamespace(path=lambda name: f"/config/{name}")
    hass.services = services
    hass.config_entries = config_entries
    hass.bus = SimpleNamespace(async_fire=mock.MagicMock())
    hass.async_add_executor_job = mock.AsyncMock(side_effect=_exec_in_executor)
    return hass


def _build_entry(auto_sync=False):
    return SimpleNamespace(
        entry_id="entry-1",
        data={
            CONF_SERVER_HOSTNAME: "adb.example",
            CONF_HTTP_PATH: "/sql/1.0/warehouses/1",
            CONF_ACCESS_TOKEN: "token",
            CONF_CATALOG: "main",
            CONF_SCHEMA: "ha",
            CONF_TABLE: "states",
            CONF_DBX_VOLUMES_PATH: "/Volumes/main/ha/ingest",
        },
        options={
            CONF_AUTO_SYNC_ENABLED: auto_sync,
            CONF_AUTO_SYNC_INTERVAL_MINUTES: 5,
        },
        async_on_unload=mock.MagicMock(),
        add_update_listener=mock.MagicMock(return_value=mock.MagicMock()),
    )


def test_setup_registers_service_and_unload_removes_it():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    with mock.patch("custom_components.hass_databricks.Store", DummyStore):
        ok = asyncio.run(async_setup_entry(hass, entry))

    assert ok is True
    assert DOMAIN in hass.data
    hass.services.async_register.assert_called_once()
    assert hass.services.async_register.call_args.args[1] == SERVICE_SYNC

    unload_ok = asyncio.run(async_unload_entry(hass, entry))
    assert unload_ok is True
    hass.services.async_remove.assert_called_once_with(DOMAIN, SERVICE_SYNC)


def test_service_handler_success_updates_sync_meta():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                return_value={
                    "rows": 12,
                    "filename": "upload.csv.gz",
                    "max_last_updated_ts": 1700.0,
                    "used_hot_copy": False,
                },
            ):
                with mock.patch(
                    "custom_components.hass_databricks.async_get_clientsession"
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    await handler(SimpleNamespace(data={}))

    asyncio.run(_run())

    meta = hass.data[DOMAIN][entry.entry_id]["sync_meta"]
    assert meta[SYNC_META_LAST_STATUS] == "success"
    assert meta.get(SYNC_META_AVAILABLE, True) is True
    assert DummyStore.instances[-1].saved


def test_service_handler_failure_sets_unavailable_and_triggers_reauth():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                side_effect=Exception("authentication failed: invalid token"),
            ):
                with mock.patch(
                    "custom_components.hass_databricks.async_get_clientsession"
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    with pytest.raises(Exception, match="authentication failed"):
                        await handler(SimpleNamespace(data={}))

    asyncio.run(_run())

    meta = hass.data[DOMAIN][entry.entry_id]["sync_meta"]
    assert meta[SYNC_META_LAST_STATUS] == "failed"
    assert meta[SYNC_META_AVAILABLE] is False
    hass.config_entries.flow.async_init.assert_awaited_once()


def test_auto_sync_registration_stores_unsubscribe_callback():
    hass = _build_hass()
    entry = _build_entry(auto_sync=True)
    DummyStore.initial_data = {}
    DummyStore.instances = []
    unsub = mock.MagicMock()

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.async_track_time_interval",
                return_value=unsub,
            ):
                await async_setup_entry(hass, entry)

    asyncio.run(_run())

    assert hass.data[DOMAIN][entry.entry_id]["unsub_auto_sync"] is unsub


def test_service_handler_uses_incremental_lookback_when_last_success_exists():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {"last_success_ts": 2000.0}
    DummyStore.instances = []

    captured = {}

    def _fake_pipeline(request):
        captured["min_last_updated_ts"] = request.min_last_updated_ts
        return {
            "rows": 1,
            "filename": "upload.csv.gz",
            "max_last_updated_ts": 2100.0,
            "used_hot_copy": False,
        }

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                side_effect=_fake_pipeline,
            ):
                with mock.patch(
                    "custom_components.hass_databricks.async_get_clientsession"
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    await handler(SimpleNamespace(data={}))

    asyncio.run(_run())
    assert captured["min_last_updated_ts"] == 1400.0


def test_failure_with_non_auth_error_does_not_start_reauth():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                side_effect=Exception("disk full"),
            ):
                with mock.patch(
                    "custom_components.hass_databricks.async_get_clientsession"
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    with pytest.raises(Exception, match="disk full"):
                        await handler(SimpleNamespace(data={}))

    asyncio.run(_run())
    hass.config_entries.flow.async_init.assert_not_awaited()


def test_failure_when_reauth_init_raises_is_handled():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []
    hass.config_entries.flow.async_init = mock.AsyncMock(
        side_effect=RuntimeError("flow down")
    )

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                side_effect=Exception("auth failed"),
            ):
                with mock.patch(
                    "custom_components.hass_databricks.async_get_clientsession"
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    with pytest.raises(Exception, match="auth failed"):
                        await handler(SimpleNamespace(data={}))

    asyncio.run(_run())


def test_scheduled_sync_homeassistant_error_logged():
    hass = _build_hass()
    entry = _build_entry(auto_sync=True)
    DummyStore.initial_data = {}
    DummyStore.instances = []
    scheduled = {}

    def _capture_scheduler(_hass, callback, _interval):
        scheduled["callback"] = callback
        return mock.MagicMock()

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with mock.patch(
                "custom_components.hass_databricks.async_track_time_interval",
                side_effect=_capture_scheduler,
            ):
                with mock.patch(
                    "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                    side_effect=HomeAssistantError("boom"),
                ):
                    await async_setup_entry(hass, entry)
                    await scheduled["callback"](None)

    asyncio.run(_run())


def test_update_listener_reloads_entry():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)

    asyncio.run(_async_update_listener(hass, entry))

    hass.config_entries.async_reload.assert_awaited_once_with(entry.entry_id)


def test_unload_calls_unsubscribe_when_present():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    unsub = mock.MagicMock()
    hass.data = {DOMAIN: {entry.entry_id: {"unsub_auto_sync": unsub}}}

    asyncio.run(async_unload_entry(hass, entry))

    unsub.assert_called_once()
