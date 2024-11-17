from unittest import mock
from pytest import mark

from hass_databricks.core.upload import (
    create_data_pack,
)


@mark.parametrize("staging_path, expected_filename", [
    ("/tmp/", "upload_2023-10-10-10-10-10.parquet"),
    ("/data/", "upload_2023-10-10-10-10-10.parquet"),
])
@mock.patch("hass_databricks.core.upload.datetime")
@mock.patch("hass_databricks.core.upload.detective")
def test_create_data_pack(mock_detective, mock_datetime, staging_path, expected_filename):
    mock_datetime.now.return_value.strftime.return_value = "2023-10-10-10-10-10"
    mock_db = mock.Mock()
    mock_detective.db_from_hass_config.return_value = mock_db
    mock_hadf = mock.Mock()
    mock_db.fetch_all_sensor_data.return_value = mock_hadf

    config = mock.Mock()
    config.staging_allowed_local_path = staging_path

    full_path, filename_parquet = create_data_pack(config)

    assert full_path == staging_path + expected_filename
    assert filename_parquet == expected_filename
    mock_hadf.to_parquet.assert_called_once_with(full_path, index=False)
