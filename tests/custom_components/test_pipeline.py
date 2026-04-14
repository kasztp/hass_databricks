from unittest import mock

import pytest

from custom_components.hass_databricks import pipeline


def _request(**overrides):
    data = {
        "db_path": "/tmp/ha.db",
        "server_hostname": "host",
        "http_path": "/sql/path",
        "access_token": "token",
        "catalog": "main",
        "schema": "ha",
        "table": "states",
        "local_path": "/tmp/",
        "dbx_volumes_path": "/Volumes/main/ha/ingest",
        "entity_like": "sensor.%",
        "chunk_size": 5000,
        "keep_local_file": False,
        "hot_copy_db": None,
        "min_last_updated_ts": 1700000000.0,
    }
    data.update(overrides)
    return pipeline.SyncRequest(**data)


@mock.patch("custom_components.hass_databricks.pipeline.os.remove")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_states_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_success(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-00"
    mock_extract.return_value = (7, 1700001111.0)
    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target.upload_to_databricks = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value=[["ok"]])

    import asyncio

    result = asyncio.run(pipeline.run_sync_pipeline(_request()))

    assert result["rows"] == 7
    assert result["max_last_updated_ts"] == 1700001111.0
    assert result["filename"] == "upload_2026-04-12-10-00-00.csv.gz"
    mock_extract.assert_called_once()
    args, _ = mock_extract.call_args
    assert args[4] == 1700000000.0
    assert args[5] is False
    mock_remove.assert_called_once()


@mock.patch("custom_components.hass_databricks.pipeline.os.remove")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_states_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_initial_defaults_to_hot_copy(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    _mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-01"
    mock_extract.return_value = (3, 1700002111.0)
    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target.upload_to_databricks = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value=[["ok"]])

    import asyncio

    result = asyncio.run(
        pipeline.run_sync_pipeline(_request(min_last_updated_ts=None, hot_copy_db=None))
    )

    assert result["used_hot_copy"] is True
    args, _ = mock_extract.call_args
    assert args[5] is True


@mock.patch("custom_components.hass_databricks.pipeline.os.remove")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_states_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_can_force_hot_copy_override(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    _mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-02"
    mock_extract.return_value = (4, 1700003111.0)
    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target.upload_to_databricks = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value=[["ok"]])

    import asyncio

    result = asyncio.run(pipeline.run_sync_pipeline(_request(hot_copy_db=True)))

    assert result["used_hot_copy"] is True
    args, _ = mock_extract.call_args
    assert args[5] is True


@mock.patch("custom_components.hass_databricks.pipeline._extract_states_to_csv")
def test_run_sync_pipeline_raises_when_no_rows(mock_extract):
    mock_extract.return_value = (0, None)

    import asyncio

    with pytest.raises(ValueError, match="No rows were extracted"):
        asyncio.run(pipeline.run_sync_pipeline(_request()))


def test_extract_states_to_csv_filters_and_returns_max_ts(tmp_path):
    source_db = tmp_path / "source.db"
    out_csv = tmp_path / "out.csv.gz"

    conn = pipeline.sqlite3.connect(source_db)
    try:
        conn.execute(
            "CREATE TABLE states_meta (metadata_id INTEGER PRIMARY KEY, entity_id TEXT)"
        )
        conn.execute(
            "CREATE TABLE states (state_id INTEGER PRIMARY KEY, metadata_id INTEGER, state TEXT, last_updated_ts REAL)"
        )
        conn.execute(
            "INSERT INTO states_meta(metadata_id, entity_id) VALUES (1, 'sensor.a')"
        )
        conn.execute(
            "INSERT INTO states_meta(metadata_id, entity_id) VALUES (2, 'light.b')"
        )
        conn.execute(
            "INSERT INTO states(state_id, metadata_id, state, last_updated_ts) VALUES (1, 1, '1', 1000.0)"
        )
        conn.execute(
            "INSERT INTO states(state_id, metadata_id, state, last_updated_ts) VALUES (2, 1, '2', 2000.0)"
        )
        conn.execute(
            "INSERT INTO states(state_id, metadata_id, state, last_updated_ts) VALUES (3, 2, '3', 3000.0)"
        )
        conn.commit()
    finally:
        conn.close()

    rows, max_ts = pipeline._extract_states_to_csv(
        source_db_path=str(source_db),
        output_csv_path=str(out_csv),
        entity_like="sensor.%",
        chunk_size=100,
        min_last_updated_ts=1500.0,
    )

    assert rows == 1
    assert max_ts == 2000.0
    assert out_csv.exists()


def test_databricks_target_methods_execute_queries():
    cfg = pipeline.RuntimeSyncConfig(
        catalog="main",
        schema="ha",
        table="states",
        local_path="/tmp/",
        dbx_path="/Volumes/main/ha/ingest",
    )
    target = pipeline.DatabricksTarget(
        cfg,
        server_hostname="host",
        http_path="/sql/path",
        access_token="token",
    )

    import asyncio

    with mock.patch.object(
        target, "_execute_sql", new_callable=mock.AsyncMock
    ) as mock_sql:
        with mock.patch.object(
            target, "_upload_file", new_callable=mock.AsyncMock
        ) as mock_upload:
            mock_sql.return_value = {"status": {"state": "SUCCESS"}}
            mock_upload.return_value = {"status": 200, "response": "ok"}

            assert asyncio.run(target.create_schema()) == {
                "status": {"state": "SUCCESS"}
            }
            assert asyncio.run(target.create_table()) == {
                "status": {"state": "SUCCESS"}
            }
            assert asyncio.run(
                target.upload_to_databricks("/tmp/file.csv.gz", "file.csv.gz")
            ) == {"status": 200, "response": "ok"}
            assert asyncio.run(target.upsert_new_data("file.csv.gz")) == {
                "status": {"state": "SUCCESS"}
            }


@mock.patch("custom_components.hass_databricks.pipeline.os.remove")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_states_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_keep_local_file_true_does_not_remove(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-03"
    mock_extract.return_value = (2, 1700004111.0)
    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target.upload_to_databricks = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value=[["ok"]])

    import asyncio

    result = asyncio.run(pipeline.run_sync_pipeline(_request(keep_local_file=True)))

    assert result["rows"] == 2
    mock_remove.assert_not_called()


def test_extract_states_to_csv_without_hot_copy(tmp_path):
    source_db = tmp_path / "source_no_copy.db"
    out_csv = tmp_path / "out_no_copy.csv.gz"

    conn = pipeline.sqlite3.connect(source_db)
    try:
        conn.execute(
            "CREATE TABLE states_meta (metadata_id INTEGER PRIMARY KEY, entity_id TEXT)"
        )
        conn.execute(
            "CREATE TABLE states (state_id INTEGER PRIMARY KEY, metadata_id INTEGER, state TEXT, last_updated_ts REAL)"
        )
        conn.execute(
            "INSERT INTO states_meta(metadata_id, entity_id) VALUES (1, 'sensor.a')"
        )
        conn.execute(
            "INSERT INTO states(state_id, metadata_id, state, last_updated_ts) VALUES (1, 1, '12', 5000.0)"
        )
        conn.commit()
    finally:
        conn.close()

    rows, max_ts = pipeline._extract_states_to_csv(
        source_db_path=str(source_db),
        output_csv_path=str(out_csv),
        entity_like="sensor.%",
        chunk_size=10,
        min_last_updated_ts=None,
        use_hot_copy=False,
    )

    assert rows == 1
    assert max_ts == 5000.0
