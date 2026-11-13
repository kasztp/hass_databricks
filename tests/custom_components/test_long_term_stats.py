"""Tests for the incremental long-term statistics export."""

import asyncio
import gzip
import csv
import sqlite3
from types import SimpleNamespace
from unittest import mock

import pytest

from custom_components.hass_databricks import long_term_stats as lts


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "ha.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE statistics_meta (id INTEGER PRIMARY KEY, statistic_id TEXT,
            source TEXT, unit_of_measurement TEXT);
        CREATE TABLE statistics (id INTEGER PRIMARY KEY, metadata_id INTEGER,
            start_ts REAL, mean REAL, min REAL, max REAL, sum REAL, state REAL,
            last_reset_ts REAL);
        INSERT INTO statistics_meta VALUES (1, 'sensor.power', 'recorder', 'W'),
                                           (2, 'sensor.energy', 'recorder', 'kWh');
        INSERT INTO statistics VALUES
            (1, 1, 1000.0, 5.0, 1.0, 9.0, NULL, NULL, NULL),
            (2, 2, 1000.0, NULL, NULL, NULL, 10.5, 10.5, 900.0),
            (3, 1, 4600.0, 6.0, 2.0, 8.0, NULL, NULL, NULL),
            (4, 2, 4600.0, NULL, NULL, NULL, 12.0, 12.0, 900.0);
        """
    )
    con.commit()
    con.close()
    return path


def _read(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_extract_all_rows_with_iso_timestamps_and_meta(db, tmp_path):
    out = str(tmp_path / "o.csv.gz")
    n, max_ts, last_id = lts._extract_statistics_chunk(db, out, 100)
    rows = _read(out)
    assert (n, max_ts, last_id) == (4, 4600.0, 4)
    assert rows[0]["statistic_id"] == "sensor.power"
    assert rows[0]["start_ts"] == "1970-01-01T00:16:40+00:00"
    assert rows[0]["unit"] == "W" and rows[0]["source"] == "recorder"
    assert rows[0]["sum"] == "" and rows[1]["mean"] == ""  # NULL stays empty
    assert rows[1]["last_reset_ts"] == "1970-01-01T00:15:00+00:00"


def test_extract_filters_by_min_start_ts_and_pages_by_id(db, tmp_path):
    out = str(tmp_path / "o.csv.gz")
    n, max_ts, _ = lts._extract_statistics_chunk(db, out, 100, min_start_ts=4000.0)
    assert n == 2 and max_ts == 4600.0

    n, _, last_id = lts._extract_statistics_chunk(db, out, 3)
    assert (n, last_id) == (3, 3)
    n, _, last_id = lts._extract_statistics_chunk(db, out, 3, last_id=last_id)
    assert (n, last_id) == (1, 4)


def test_extract_empty_returns_zero_and_keeps_last_id(db, tmp_path):
    out = str(tmp_path / "o.csv.gz")
    assert lts._extract_statistics_chunk(db, out, 10, min_start_ts=1e12, last_id=7) == (
        0,
        None,
        7,
    )
    assert _read(out) == []


def test_sql_is_idempotent_merge_on_statistic_and_start():
    sql = lts.build_merge_sql("main", "ha", "/Volumes/x/stats/*.csv.gz")
    assert "MERGE INTO `main`.`ha`.ha_statistics" in sql
    assert "target.statistic_id = source.statistic_id" in sql
    assert "target.start_ts = source.start_ts" in sql
    assert "UPDATE SET *" in sql and "INSERT *" in sql
    assert "CREATE TABLE IF NOT EXISTS" in lts.build_create_table_sql("main", "ha")
    with pytest.raises(ValueError):
        lts.build_merge_sql("main", "ha", "/Volumes/a'b")


def _request(db, tmp_path, chunk_size=100):
    return SimpleNamespace(
        hass=None,
        db_path=db,
        catalog="main",
        schema="ha",
        table="states",
        local_path=str(tmp_path / "stage"),
        dbx_volumes_path="/Volumes/main/ha/ingest",
        server_hostname="h",
        http_path="/p",
        access_token="t",
        session=None,
        chunk_size=chunk_size,
    )


def _run(db, tmp_path, since=None, chunk_size=100, delete_effect=None):
    target = mock.MagicMock()
    target.create_schema = mock.AsyncMock()
    target._execute_sql = mock.AsyncMock()
    target._upload_file = mock.AsyncMock()
    target.delete_volume_folder = mock.AsyncMock(side_effect=delete_effect)
    with mock.patch.object(lts, "DatabricksTarget", return_value=target):
        result = asyncio.run(
            lts.run_statistics_sync(_request(db, tmp_path, chunk_size), since)
        )
    return result, target


def test_sync_uploads_chunks_then_merges_once_and_cleans_up(db, tmp_path):
    result, target = _run(db, tmp_path, chunk_size=3)
    assert result == {"rows": 4, "max_start_ts": 4600.0}
    assert target._upload_file.await_count == 2  # 3 + 1 rows
    sqls = [c.args[0] for c in target._execute_sql.await_args_list]
    assert sum("MERGE INTO" in s for s in sqls) == 1
    assert "*.csv.gz" in sqls[-1]
    target.delete_volume_folder.assert_awaited_once()
    assert list((tmp_path / "stage").iterdir()) == []  # local staging cleaned


def test_sync_incremental_window_and_delete_failure_ignored(db, tmp_path):
    result, target = _run(db, tmp_path, since=4000.0, delete_effect=RuntimeError)
    assert result == {"rows": 2, "max_start_ts": 4600.0}


def test_sync_with_nothing_new_creates_table_but_does_not_merge(db, tmp_path):
    result, target = _run(db, tmp_path, since=1e12)
    assert result == {"rows": 0, "max_start_ts": None}
    target._upload_file.assert_not_awaited()
    sqls = [c.args[0] for c in target._execute_sql.await_args_list]
    assert len(sqls) == 1 and "CREATE TABLE IF NOT EXISTS" in sqls[0]
    target.delete_volume_folder.assert_not_awaited()


def test_sync_failure_propagates_and_cleans_local_dir(db, tmp_path):
    target = mock.MagicMock()
    target.create_schema = mock.AsyncMock()
    target._execute_sql = mock.AsyncMock(
        side_effect=[None, RuntimeError("merge failed")]
    )
    target._upload_file = mock.AsyncMock()
    with mock.patch.object(lts, "DatabricksTarget", return_value=target):
        with pytest.raises(RuntimeError, match="merge failed"):
            asyncio.run(lts.run_statistics_sync(_request(db, tmp_path), None))
    assert list((tmp_path / "stage").iterdir()) == []
