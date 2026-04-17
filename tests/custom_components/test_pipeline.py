from unittest import mock

import pytest
import aiohttp
import asyncio

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
@mock.patch("custom_components.hass_databricks.pipeline.os.rmdir")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_chunk_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_success(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    mock_rmdir,
    mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-00"

    # First chunk returns 7 rows, second chunk returns 0 (EOF)
    mock_extract.side_effect = [(7, 1700001111.0, 7), (0, None, 7)]

    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target._upload_file = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value={"status": "SUCCESS"})
    target.delete_volume_folder = mock.AsyncMock(return_value={"status": "SUCCESS"})

    import asyncio

    result = asyncio.run(pipeline.run_sync_pipeline(_request()))

    assert result["rows"] == 7
    assert result["max_last_updated_ts"] == 1700001111.0
    assert result["filename"] == "upload_2026-04-12-10-00-00"

    assert mock_extract.call_count == 2
    args_call_1, _ = mock_extract.call_args_list[0]
    assert args_call_1[4] == 1700000000.0
    assert args_call_1[5] == 0

    args_call_2, _ = mock_extract.call_args_list[1]
    assert args_call_2[5] == 7

    mock_remove.assert_called()


@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_chunk_to_csv")
def test_run_sync_pipeline_raises_when_no_rows(mock_extract, mock_target):
    mock_extract.return_value = (0, None, 0)

    target = mock_target.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])

    import asyncio

    with pytest.raises(
        ValueError, match="No rows were extracted from Home Assistant database."
    ):
        asyncio.run(pipeline.run_sync_pipeline(_request()))


def test_extract_chunk_to_csv_filters_and_returns_max_ts(tmp_path):
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

    rows, max_ts, next_id = pipeline._extract_chunk_to_csv(
        source_db_path=str(source_db),
        output_csv_path=str(out_csv),
        entity_like="sensor.%",
        chunk_size=100,
        min_last_updated_ts=1500.0,
        last_state_id=0,
    )

    assert rows == 1
    assert max_ts == 2000.0
    assert next_id == 2
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
            assert asyncio.run(target.upsert_new_data("vol/*")) == {
                "status": {"state": "SUCCESS"}
            }


@mock.patch("custom_components.hass_databricks.pipeline.os.remove")
@mock.patch("custom_components.hass_databricks.pipeline.os.rmdir")
@mock.patch(
    "custom_components.hass_databricks.pipeline.os.path.exists", return_value=True
)
@mock.patch("custom_components.hass_databricks.pipeline.DatabricksTarget")
@mock.patch("custom_components.hass_databricks.pipeline._extract_chunk_to_csv")
@mock.patch("custom_components.hass_databricks.pipeline.datetime")
def test_run_sync_pipeline_keep_local_file_true_does_not_remove(
    mock_datetime,
    mock_extract,
    mock_target_cls,
    _mock_exists,
    _mock_rmdir,
    mock_remove,
):
    mock_datetime.now.return_value.strftime.return_value = "2026-04-12-10-00-03"
    mock_extract.side_effect = [(2, 1700004111.0, 2), (0, None, 2)]

    target = mock_target_cls.return_value
    target.create_schema = mock.AsyncMock(return_value=[])
    target.create_table = mock.AsyncMock(return_value=[])
    target._upload_file = mock.AsyncMock(return_value=[["ok"]])
    target.upsert_new_data = mock.AsyncMock(return_value={"status": "SUCCESS"})
    target.delete_volume_folder = mock.AsyncMock(return_value={"status": "SUCCESS"})

    import asyncio

    result = asyncio.run(pipeline.run_sync_pipeline(_request(keep_local_file=True)))

    assert result["rows"] == 2
    mock_remove.assert_called_once_with(
        "/tmp/upload_2026-04-12-10-00-03/part_00002.csv.gz"
    )


class MockResponse:
    def __init__(self, status, text_data="{}", json_data=None):
        self.status = status
        self._text = text_data
        self._json = json_data or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def text(self):
        return self._text

    async def json(self):
        return self._json

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=mock.Mock(),
                history=(),
                status=self.status,
                message="Error",
            )


def test_execute_sql():
    cfg = pipeline.RuntimeSyncConfig("main", "ha", "states", "/tmp", "/dbx")

    # Test without a provided session (creates its own)
    target = pipeline.DatabricksTarget(
        cfg,
        server_hostname="host",
        http_path="/sql/path",
        access_token="token",
    )

    async def _run():
        with mock.patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value = MockResponse(
                200, json_data={"status": {"state": "SUCCESS"}}
            )

            res = await target._execute_sql("SELECT 1")
            assert res["status"]["state"] == "SUCCESS"
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            assert kwargs["json"]["statement"] == "SELECT 1"
            assert kwargs["headers"]["Authorization"] == "Bearer token"

    asyncio.run(_run())


def test_execute_sql_failure():
    cfg = pipeline.RuntimeSyncConfig("main", "ha", "states", "/tmp", "/dbx")
    target = pipeline.DatabricksTarget(
        cfg,
        server_hostname="host",
        http_path="/sql/path",
        access_token="token",
    )

    async def _run():
        with mock.patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value = MockResponse(
                200, json_data={"status": {"state": "FAILED"}}
            )
            with pytest.raises(Exception, match="SQL execution failed"):
                await target._execute_sql("SELECT 1")

    asyncio.run(_run())


def test_upload_file(tmp_path):
    cfg = pipeline.RuntimeSyncConfig("main", "ha", "states", "/tmp", "/dbx")
    target = pipeline.DatabricksTarget(
        cfg,
        server_hostname="host",
        http_path="/sql/path",
        access_token="token",
    )

    test_file = tmp_path / "test.csv"
    test_file.write_text("1,2,3")

    async def _run():
        with mock.patch("aiohttp.ClientSession.put") as mock_put:
            mock_put.return_value = MockResponse(200, text_data="ok")
            res = await target._upload_file(str(test_file), "/api/path")
            assert res["status"] == 200
            assert res["response"] == "ok"

    asyncio.run(_run())


def test_delete_volume_folder():
    cfg = pipeline.RuntimeSyncConfig("main", "ha", "states", "/tmp", "/dbx")
    target = pipeline.DatabricksTarget(
        cfg,
        server_hostname="host",
        http_path="/sql/path",
        access_token="token",
    )

    async def _run():
        with mock.patch("aiohttp.ClientSession.delete") as mock_delete:
            mock_delete.return_value = MockResponse(200, text_data="deleted")
            res = await target.delete_volume_folder("/api/path")
            assert res["status"] == 200
            assert res["response"] == "deleted"

    asyncio.run(_run())


def test_schema_errors(tmp_path):
    source_db = tmp_path / "source.db"
    out_csv = tmp_path / "out.csv.gz"

    conn = pipeline.sqlite3.connect(source_db)
    # create states but nothing else
    conn.execute(
        "CREATE TABLE states (state_id INTEGER PRIMARY KEY, metadata_id INTEGER, state TEXT, last_updated_ts REAL)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(Exception, match="Home Assistant metadata table not found"):
        pipeline._extract_chunk_to_csv(str(source_db), str(out_csv), "sensor.%", 100)

    # Remove states table to get the other error
    conn = pipeline.sqlite3.connect(source_db)
    conn.execute("DROP TABLE states")
    conn.commit()
    conn.close()

    with pytest.raises(Exception, match="schema not found"):
        pipeline._extract_chunk_to_csv(str(source_db), str(out_csv), "sensor.%", 100)


def test_remove_path_coverage(tmp_path):
    d = tmp_path / "dir"
    d.mkdir()
    f = tmp_path / "file.txt"
    f.write_text("Hello")

    # Test valid removes
    pipeline._remove_path(str(d))
    assert not d.exists()

    pipeline._remove_path(str(f))
    assert not f.exists()

    # Test os error bypass
    with mock.patch("os.remove", side_effect=OSError("Boom")):
        f2 = tmp_path / "file2.txt"
        f2.write_text("Hello")
        pipeline._remove_path(str(f2))  # should not raise
