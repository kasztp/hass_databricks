"""Tests for the HA metadata export."""

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest

from custom_components.hass_databricks import metadata


def _entity(entity_id="sensor.a", **kw):
    base = dict(
        entity_id=entity_id,
        name=None,
        original_name=None,
        area_id=None,
        device_id=None,
        device_class=None,
        original_device_class=None,
        unit_of_measurement=None,
        capabilities=None,
        platform="demo",
        labels=set(),
        disabled_by=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


DEVICE = SimpleNamespace(area_id="dev_area", name="Dev", name_by_user="My Dev")


def _rows(entities, devices=None, names=None, labels=None):
    return metadata.build_entity_rows(
        entities, devices or {}, names or {}, labels or {}
    )


def test_entity_own_area_wins_over_device_area():
    row = _rows([_entity(area_id="own", device_id="d1")], {"d1": DEVICE})[0]
    assert row["area_id"] == "own"


def test_entity_falls_back_to_device_area():
    row = _rows([_entity(device_id="d1")], {"d1": DEVICE})[0]
    assert row["area_id"] == "dev_area"
    assert row["device_name"] == "My Dev"


def test_entity_area_null_when_neither_set():
    device = SimpleNamespace(area_id=None, name="Dev", name_by_user=None)
    row = _rows([_entity(device_id="d1")], {"d1": device})[0]
    assert row["area_id"] is None
    assert row["device_name"] == "Dev"
    assert _rows([_entity()])[0]["area_id"] is None


def test_entity_fields_and_friendly_name_precedence():
    e = _entity(
        "sensor.power",
        name="Reg name",
        original_name="Orig",
        original_device_class="power",
        unit_of_measurement="W",
        capabilities={"state_class": "measurement"},
        disabled_by="user",
        labels={"l1", "gone"},
    )
    row = _rows([e], names={"sensor.power": "State name"}, labels={"l1": "Solar"})[0]
    assert row["friendly_name"] == "State name"
    assert _rows([e])[0]["friendly_name"] == "Reg name"
    e.name = None
    assert _rows([e])[0]["friendly_name"] == "Orig"
    assert row["domain"] == "sensor"
    assert row["device_class"] == "power"
    assert row["unit"] == "W"
    assert row["state_class"] == "measurement"
    assert row["disabled"] is True
    assert row["labels"] == ["Solar"]  # names, not ids; unknown ids dropped


def test_area_without_entities_is_exported_with_label_names():
    area = SimpleNamespace(
        id="a1",
        name="Attic",
        floor_id=None,
        aliases={"loft"},
        icon=None,
        labels={"l1"},
    )
    rows = metadata.build_area_rows([area], {"l1": "Cold"})
    assert rows == [
        {
            "area_id": "a1",
            "name": "Attic",
            "floor_id": None,
            "aliases": ["loft"],
            "icon": None,
            "labels": ["Cold"],
        }
    ]


def test_floor_rows():
    f = SimpleNamespace(
        floor_id="f1", name="Ground", level=0, aliases=set(), icon="mdi:home"
    )
    assert metadata.build_floor_rows([f])[0]["level"] == 0


def test_energy_mapping_grid_solar_battery_devices():
    prefs = {
        "energy_sources": [
            {
                "type": "grid",
                "flow_from": [
                    {
                        "stat_energy_from": "sensor.import",
                        "stat_cost": "sensor.cost",
                        "entity_energy_price": None,
                        "number_energy_price": 0.3,
                    }
                ],
                "flow_to": [
                    {
                        "stat_energy_to": "sensor.export",
                        "stat_compensation": "sensor.comp",
                        "entity_energy_price": "sensor.price",
                        "number_energy_price": None,
                    }
                ],
                "cost_adjustment_day": 0,
            },
            {"type": "solar", "stat_energy_from": "sensor.pv"},
            {
                "type": "battery",
                "stat_energy_from": "sensor.bat_out",
                "stat_energy_to": "sensor.bat_in",
            },
            {"type": "future_thing"},
        ],
        "device_consumption": [{"stat_consumption": "sensor.oven", "name": "Oven"}],
    }
    got = {
        (r["role"], r["stat_id"], r["source_index"])
        for r in metadata.build_energy_rows(prefs)
    }
    assert got == {
        ("grid_import", "sensor.import", 0),
        ("grid_export", "sensor.export", 0),
        ("solar", "sensor.pv", 1),
        ("battery_out", "sensor.bat_out", 2),
        ("battery_in", "sensor.bat_in", 2),
        ("device_consumption", "sensor.oven", 0),
    }
    rows = {r["role"]: r for r in metadata.build_energy_rows(prefs)}
    assert rows["grid_import"]["money_stat_id"] == "sensor.cost"
    assert rows["grid_import"]["fixed_price"] == 0.3
    assert rows["grid_export"]["money_stat_id"] == "sensor.comp"
    assert rows["grid_export"]["price_entity_id"] == "sensor.price"
    assert rows["device_consumption"]["name"] == "Oven"


@pytest.mark.parametrize(
    "prefs", [None, {}, {"energy_sources": [], "device_consumption": []}]
)
def test_energy_empty_preferences_give_no_rows(prefs):
    assert metadata.build_energy_rows(prefs) == []


def test_energy_component_missing_gives_empty_prefs():
    hass = SimpleNamespace(config=SimpleNamespace(components={"sensor"}))
    assert asyncio.run(metadata._energy_preferences(hass)) is None


def test_sql_uses_explicit_schema_and_create_or_replace():
    sql = metadata.build_load_sql("main", "ha", "ha_floors", "/Volumes/x/f.jsonl")
    assert sql.startswith("CREATE OR REPLACE TABLE `main`.`ha`.`ha_floors`")
    assert "schema => 'floor_id STRING, name STRING, level INT" in sql
    assert "current_timestamp() AS synced_at" in sql
    ddl = metadata.build_empty_ddl("main", "ha", "ha_areas")
    assert ddl.startswith("CREATE OR REPLACE TABLE") and "synced_at TIMESTAMP" in ddl


def test_sql_rejects_quote_in_path():
    with pytest.raises(ValueError):
        metadata.build_load_sql("c", "s", "ha_floors", "/Volumes/a'b")


def _request(tmp_path):
    return SimpleNamespace(
        hass=None,
        catalog="main",
        schema="ha",
        table="states",
        local_path=str(tmp_path),
        dbx_volumes_path="/Volumes/main/ha/ingest",
        server_hostname="h",
        http_path="/p",
        access_token="t",
        session=None,
    )


def _run_sync(tmp_path, snapshots, execute_effect=None):
    target = mock.MagicMock()
    target.create_schema = mock.AsyncMock()
    target._execute_sql = mock.AsyncMock(side_effect=execute_effect)
    target._upload_file = mock.AsyncMock()
    target.delete_volume_folder = mock.AsyncMock()
    with mock.patch.object(
        metadata, "collect_snapshots", mock.AsyncMock(return_value=snapshots)
    ):
        with mock.patch.object(metadata, "DatabricksTarget", return_value=target):
            result = asyncio.run(metadata.run_metadata_sync(_request(tmp_path)))
    return result, target


def test_zero_row_snapshot_uses_ddl_and_uploads_nothing(tmp_path):
    snaps = {name: [] for name in metadata.TABLE_SCHEMAS}
    result, target = _run_sync(tmp_path, snaps)
    target._upload_file.assert_not_awaited()
    target.delete_volume_folder.assert_not_awaited()
    assert target._execute_sql.await_count == 4
    assert all(
        "synced_at TIMESTAMP" in c.args[0] for c in target._execute_sql.await_args_list
    )
    assert result == {"tables": {n: 0 for n in snaps}, "errors": {}}
    assert list(tmp_path.iterdir()) == []  # local staging cleaned up


def test_rows_are_uploaded_loaded_and_folder_deleted(tmp_path):
    snaps = {name: [] for name in metadata.TABLE_SCHEMAS}
    snaps["ha_floors"] = [
        {"floor_id": "f", "name": "G", "level": 0, "aliases": [], "icon": None}
    ]
    result, target = _run_sync(tmp_path, snaps)
    target._upload_file.assert_awaited_once()
    assert target._upload_file.await_args.args[1].endswith("/ha_floors.jsonl")
    assert any("read_files" in c.args[0] for c in target._execute_sql.await_args_list)
    target.delete_volume_folder.assert_awaited_once()
    assert result["tables"]["ha_floors"] == 1


def test_one_table_failure_does_not_stop_the_others(tmp_path):
    snaps = {name: [] for name in metadata.TABLE_SCHEMAS}
    calls = []

    async def effect(sql):
        calls.append(sql)
        if "ha_floors" in sql:
            raise RuntimeError("bad")

    result, _ = _run_sync(tmp_path, snaps, effect)
    assert set(result["errors"]) == {"ha_floors"}
    assert set(result["tables"]) == {"ha_areas", "ha_entities", "ha_energy_config"}


def test_collect_snapshots_reads_registries_without_energy():
    hass = SimpleNamespace(
        config=SimpleNamespace(components=set()),
        states=SimpleNamespace(
            async_all=lambda: [
                SimpleNamespace(entity_id="sensor.a", attributes={"friendly_name": "A"})
            ]
        ),
    )
    reg = lambda **kw: mock.MagicMock(return_value=SimpleNamespace(**kw))  # noqa: E731
    with mock.patch.multiple(
        metadata,
        fr=SimpleNamespace(async_get=reg(floors=SimpleNamespace(values=lambda: []))),
        ar=SimpleNamespace(async_get=reg(areas=SimpleNamespace(values=lambda: []))),
        lr=SimpleNamespace(async_get=reg(labels=SimpleNamespace(values=lambda: []))),
        dr=SimpleNamespace(async_get=reg(devices={})),
        er=SimpleNamespace(
            async_get=reg(
                entities=SimpleNamespace(values=lambda: [_entity("sensor.a")])
            )
        ),
    ):
        snaps = asyncio.run(metadata.collect_snapshots(hass))
    assert snaps["ha_entities"][0]["friendly_name"] == "A"
    assert snaps["ha_energy_config"] == []
    assert snaps["ha_floors"] == []


def test_energy_preferences_read_from_manager():
    hass = SimpleNamespace(config=SimpleNamespace(components={"energy"}))
    manager = SimpleNamespace(data={"energy_sources": [], "device_consumption": []})
    # Fake the module: the real one is not importable on every test interpreter.
    fake = SimpleNamespace(async_get_manager=mock.AsyncMock(return_value=manager))
    with mock.patch.dict("sys.modules", {"homeassistant.components.energy.data": fake}):
        assert asyncio.run(metadata._energy_preferences(hass)) == manager.data


def test_energy_read_error_gives_empty_config():
    hass = SimpleNamespace(
        config=SimpleNamespace(components={"energy"}),
        states=SimpleNamespace(async_all=lambda: []),
    )
    empty = SimpleNamespace(
        async_get=mock.MagicMock(
            return_value=SimpleNamespace(
                floors=SimpleNamespace(values=lambda: []),
                areas=SimpleNamespace(values=lambda: []),
                labels=SimpleNamespace(values=lambda: []),
                entities=SimpleNamespace(values=lambda: []),
                devices={},
            )
        )
    )
    with mock.patch.multiple(
        metadata, fr=empty, ar=empty, lr=empty, dr=empty, er=empty
    ):
        with mock.patch.object(
            metadata, "_energy_preferences", mock.AsyncMock(side_effect=RuntimeError)
        ):
            snaps = asyncio.run(metadata.collect_snapshots(hass))
    assert snaps["ha_energy_config"] == []


def test_folder_delete_failure_is_ignored(tmp_path):
    snaps = {name: [] for name in metadata.TABLE_SCHEMAS}
    snaps["ha_floors"] = [
        {"floor_id": "f", "name": "G", "level": None, "aliases": [], "icon": None}
    ]
    target = mock.MagicMock()
    target.create_schema = mock.AsyncMock()
    target._execute_sql = mock.AsyncMock()
    target._upload_file = mock.AsyncMock()
    target.delete_volume_folder = mock.AsyncMock(side_effect=RuntimeError)
    with mock.patch.object(
        metadata, "collect_snapshots", mock.AsyncMock(return_value=snaps)
    ):
        with mock.patch.object(metadata, "DatabricksTarget", return_value=target):
            result = asyncio.run(metadata.run_metadata_sync(_request(tmp_path)))
    assert result["errors"] == {}


def test_energy_mapping_gas_and_water_with_prices():
    prefs = {
        "energy_sources": [
            {
                "type": "gas",
                "stat_energy_from": "sensor.gas",
                "stat_cost": "sensor.gas_cost",
                "entity_energy_price": None,
                "number_energy_price": 1.5,
            },
            {"type": "water", "stat_energy_from": "sensor.water"},
        ],
        "device_consumption": [],
    }
    rows = {r["role"]: r for r in metadata.build_energy_rows(prefs)}
    assert rows["gas"]["stat_id"] == "sensor.gas"
    assert rows["gas"]["money_stat_id"] == "sensor.gas_cost"
    assert rows["gas"]["fixed_price"] == 1.5
    assert rows["water"]["source_index"] == 1


def test_energy_preferences_none_when_energy_not_importable():
    hass = SimpleNamespace(config=SimpleNamespace(components={"energy"}))
    with mock.patch.dict("sys.modules", {"homeassistant.components.energy.data": None}):
        assert asyncio.run(metadata._energy_preferences(hass)) is None
