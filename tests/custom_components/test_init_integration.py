import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

import pytest
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError

from custom_components.hass_databricks import (
    HassDataBricksRuntimeData,
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
    CONF_SYNC_METADATA,
    CONF_SYNC_STATISTICS,
    SYNC_META_LAST_STATS_TS,
    SERVICE_SYNC,
    SYNC_META_AVAILABLE,
    SYNC_META_LAST_STATUS,
)


@pytest.fixture(autouse=True)
def issue_registry():
    """hass is mocked in these tests, so stub the repairs registry."""
    with mock.patch("custom_components.hass_databricks.ir") as ir:
        yield ir


@pytest.fixture(autouse=True)
def metadata_sync():
    """Stub the metadata sync; dedicated tests below exercise its wiring."""
    with mock.patch(
        "custom_components.hass_databricks.metadata.run_metadata_sync",
        new=mock.AsyncMock(return_value={"tables": {"ha_floors": 1}, "errors": {}}),
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def statistics_sync():
    """Stub the long-term statistics sync; dedicated tests exercise the wiring."""
    with mock.patch(
        "custom_components.hass_databricks.long_term_stats.run_statistics_sync",
        new=mock.AsyncMock(return_value={"rows": 5, "max_start_ts": 5000.0}),
    ) as m:
        yield m


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
    entry = SimpleNamespace(
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
        runtime_data=None,
    )
    return entry


# Helper for setup tests


@contextmanager
def _mock_conn_success():
    """Mock both the connection test and session creation for setup tests."""
    with mock.patch(
        "custom_components.hass_databricks.async_get_clientsession",
    ):
        with mock.patch(
            "custom_components.hass_databricks.async_test_databricks_connection",
            return_value={"success": True},
        ):
            yield


def test_setup_registers_service_and_unload_removes_it():
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    with mock.patch("custom_components.hass_databricks.Store", DummyStore):
        with _mock_conn_success():
            ok = asyncio.run(async_setup_entry(hass, entry))

    assert ok is True
    assert DOMAIN in hass.data
    hass.services.async_register.assert_called_once()
    assert hass.services.async_register.call_args.args[1] == SERVICE_SYNC
    assert isinstance(entry.runtime_data, HassDataBricksRuntimeData)

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
            with _mock_conn_success():
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

    meta = entry.runtime_data.sync_meta
    assert meta[SYNC_META_LAST_STATUS] == "success"
    assert meta.get(SYNC_META_AVAILABLE, True) is True
    assert DummyStore.instances[-1].saved


def test_service_handler_failure_sets_unavailable_and_triggers_reauth(issue_registry):
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
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

    meta = entry.runtime_data.sync_meta
    assert meta[SYNC_META_LAST_STATUS] == "failed"
    assert meta[SYNC_META_AVAILABLE] is False
    hass.config_entries.flow.async_init.assert_awaited_once()
    issue_registry.async_create_issue.assert_called_once()


def test_auto_sync_registration_stores_unsubscribe_callback():
    hass = _build_hass()
    entry = _build_entry(auto_sync=True)
    DummyStore.initial_data = {}
    DummyStore.instances = []
    unsub = mock.MagicMock()

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
                with mock.patch(
                    "custom_components.hass_databricks.async_track_time_interval",
                    return_value=unsub,
                ):
                    await async_setup_entry(hass, entry)

    asyncio.run(_run())

    assert entry.runtime_data.unsub_auto_sync is unsub


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
            with _mock_conn_success():
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
            with _mock_conn_success():
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
            with _mock_conn_success():
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
            with _mock_conn_success():
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
    entry.runtime_data = HassDataBricksRuntimeData(
        store=mock.MagicMock(), unsub_auto_sync=unsub
    )
    hass.data = {DOMAIN: {"entry-1"}}

    asyncio.run(async_unload_entry(hass, entry))

    unsub.assert_called_once()


def test_sync_recovers_availability_and_logs(issue_registry):
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    # We must mock that it was originally unavailable to hit lines 102-103
    DummyStore.initial_data = {
        SYNC_META_AVAILABLE: False,
        "last_success_ts": 1000.0,
    }
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
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

    meta = entry.runtime_data.sync_meta
    assert meta[SYNC_META_AVAILABLE] is True
    assert meta[SYNC_META_LAST_STATUS] == "success"
    issue_registry.async_delete_issue.assert_called_once()


def test_import_error_in_sync():
    """Test importing pipeline fails with HomeAssistantError."""
    # To hit line 143-144, sys.modules must fail import
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
                with mock.patch.dict(
                    "sys.modules", {"custom_components.hass_databricks.pipeline": None}
                ):
                    with mock.patch(
                        "custom_components.hass_databricks.async_get_clientsession"
                    ):
                        await async_setup_entry(hass, entry)
                        handler = hass.services.async_register.call_args.args[2]
                        with pytest.raises(
                            Exception,
                            match="Sync dependencies are unavailable in this Home Assistant runtime",
                        ):
                            await handler(SimpleNamespace(data={}))

    asyncio.run(_run())


def test_run_sync_pipeline_exception_without_reauth():
    # Target line 217-218 (except Exception on reauthentication)
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)
    DummyStore.initial_data = {}
    DummyStore.instances = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
                with mock.patch(
                    "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                    side_effect=Exception(
                        "Unauthorized"
                    ),  # this triggers _start_reauth_if_needed
                ):
                    with mock.patch(
                        "custom_components.hass_databricks.async_get_clientsession"
                    ):
                        await async_setup_entry(hass, entry)
                        handler = hass.services.async_register.call_args.args[2]

                        # mock reauth so it throws Exception
                        with mock.patch.object(
                            hass.config_entries.flow,
                            "async_init",
                            side_effect=Exception("reauth broken"),
                        ):
                            with pytest.raises(Exception, match="Unauthorized"):
                                await handler(SimpleNamespace(data={}))

    asyncio.run(_run())


# --- New tests for Bronze requirements ---


def test_setup_entry_raises_config_entry_not_ready_on_connection_failure(
    issue_registry,
):
    """test-before-setup: ConfigEntryNotReady when Databricks is unreachable."""
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.async_get_clientsession",
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": False, "error": "cannot_connect"},
            ):
                with pytest.raises(ConfigEntryNotReady):
                    await async_setup_entry(hass, entry)

    asyncio.run(_run())
    issue_registry.async_create_issue.assert_called_once()
    assert (
        issue_registry.async_create_issue.call_args.kwargs["translation_key"]
        == "sync_failed"
    )


def test_setup_entry_triggers_reauth_on_invalid_auth():
    """test-before-setup: triggers reauth when credentials are invalid."""
    hass = _build_hass()
    entry = _build_entry(auto_sync=False)

    async def _run():
        with mock.patch(
            "custom_components.hass_databricks.async_get_clientsession",
        ):
            with mock.patch(
                "custom_components.hass_databricks.async_test_databricks_connection",
                return_value={"success": False, "error": "invalid_auth"},
            ):
                result = await async_setup_entry(hass, entry)
                assert result is False
                hass.config_entries.flow.async_init.assert_awaited_once()

    asyncio.run(_run())


def _run_handler(entry, pipeline_effect, options=None, keep_store=False):
    """Set up the entry and call the sync service once; return the raised error."""
    hass = _build_hass()
    if options:
        entry.options.update(options)
    if not keep_store:
        DummyStore.initial_data = {}
    DummyStore.instances = []
    raised = []

    async def _run():
        with mock.patch("custom_components.hass_databricks.Store", DummyStore):
            with _mock_conn_success():
                with mock.patch(
                    "custom_components.hass_databricks.pipeline.run_sync_pipeline",
                    **pipeline_effect,
                ):
                    await async_setup_entry(hass, entry)
                    handler = hass.services.async_register.call_args.args[2]
                    try:
                        await handler(SimpleNamespace(data={}))
                    except Exception as err:  # pylint: disable=broad-except
                        raised.append(err)

    asyncio.run(_run())
    return hass, raised


_OK = {
    "return_value": {
        "rows": 1,
        "filename": "f",
        "max_last_updated_ts": 1.0,
        "used_hot_copy": False,
    }
}


def test_metadata_result_is_in_sync_event(metadata_sync):
    hass, raised = _run_handler(_build_entry(), _OK)
    assert not raised
    metadata_sync.assert_awaited_once()
    event_data = hass.bus.async_fire.call_args.args[1]
    assert event_data["metadata"] == {"tables": {"ha_floors": 1}, "errors": {}}


def test_metadata_failure_does_not_fail_sensor_sync(metadata_sync):
    metadata_sync.side_effect = RuntimeError("boom")
    entry = _build_entry()
    hass, raised = _run_handler(entry, _OK)
    assert not raised
    assert entry.runtime_data.sync_meta[SYNC_META_LAST_STATUS] == "success"
    assert hass.bus.async_fire.call_args.args[1]["metadata"]["errors"] == {
        "all": "boom"
    }


def test_sensor_no_rows_does_not_stop_metadata(metadata_sync):
    entry = _build_entry()
    _, raised = _run_handler(
        entry, {"side_effect": ValueError("No rows were extracted")}
    )
    metadata_sync.assert_awaited_once()
    assert isinstance(raised[0], ValueError)
    assert entry.runtime_data.sync_meta[SYNC_META_LAST_STATUS] == "failed"


def test_metadata_sync_skipped_when_option_off(metadata_sync):
    _, raised = _run_handler(_build_entry(), _OK, {CONF_SYNC_METADATA: False})
    assert not raised
    metadata_sync.assert_not_awaited()


def test_statistics_first_run_is_full_and_stores_high_water_mark(statistics_sync):
    entry = _build_entry()
    hass, raised = _run_handler(entry, _OK)
    assert not raised
    assert statistics_sync.await_args.args[1] is None  # no watermark: full history
    assert entry.runtime_data.sync_meta[SYNC_META_LAST_STATS_TS] == 5000.0
    assert hass.bus.async_fire.call_args.args[1]["statistics"]["rows"] == 5


def test_statistics_next_run_uses_watermark_minus_lookback(statistics_sync):
    entry = _build_entry()
    DummyStore.initial_data = {SYNC_META_LAST_STATS_TS: 100000.0}
    _, raised = _run_handler(entry, _OK, keep_store=True)
    assert not raised
    assert statistics_sync.await_args.args[1] == 100000.0 - 24 * 3600


def test_statistics_failure_does_not_fail_sensor_sync(statistics_sync):
    statistics_sync.side_effect = RuntimeError("stats boom")
    entry = _build_entry()
    hass, raised = _run_handler(entry, _OK)
    assert not raised
    assert entry.runtime_data.sync_meta[SYNC_META_LAST_STATUS] == "success"
    assert SYNC_META_LAST_STATS_TS not in entry.runtime_data.sync_meta
    assert hass.bus.async_fire.call_args.args[1]["statistics"]["error"] == "stats boom"


def test_statistics_with_no_new_rows_keeps_watermark(statistics_sync):
    statistics_sync.return_value = {"rows": 0, "max_start_ts": None}
    entry = _build_entry()
    _run_handler(entry, _OK)
    assert SYNC_META_LAST_STATS_TS not in entry.runtime_data.sync_meta


def test_statistics_skipped_when_option_off(statistics_sync):
    _, raised = _run_handler(_build_entry(), _OK, {CONF_SYNC_STATISTICS: False})
    assert not raised
    statistics_sync.assert_not_awaited()
