from unittest import mock
from pytest import mark

from hass_databricks.core.upload import (
    create_data_pack,
)


@mark.parametrize(
    "staging_path, expected_filename",
    [
        ("/tmp/", "upload_2023-10-10-10-10-10.csv.gz"),
        ("/data/", "upload_2023-10-10-10-10-10.csv.gz"),
    ],
)
@mock.patch("hass_databricks.core.upload.datetime")
@mock.patch("hass_databricks.core.upload._extract_states_to_csv")
def test_create_data_pack(mock_extract, mock_datetime, staging_path, expected_filename):
    mock_datetime.now.return_value.strftime.return_value = "2023-10-10-10-10-10"
    mock_extract.return_value = 10

    config = mock.Mock()
    config.local_path = staging_path

    full_path, filename_csv_gz = create_data_pack(config)

    assert full_path == staging_path + expected_filename
    assert filename_csv_gz == expected_filename
    mock_extract.assert_called_once_with("home-assistant_v2.db", full_path)
