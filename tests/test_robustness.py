from unittest.mock import MagicMock, patch
from typer.testing import CliRunner
from rds_cli.main import app
from botocore.exceptions import EndpointConnectionError, ClientError

runner = CliRunner()


@patch("rds_cli.client.get_s3_client")
def test_ls_limit_bounding(mock_get_client):
    # Mock S3 client paginator response containing 3 items
    mock_s3 = MagicMock()
    mock_get_client.return_value = mock_s3

    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "file1.txt", "Size": 10},
                {"Key": "file2.txt", "Size": 20},
            ]
        },
        {"Contents": [{"Key": "file3.txt", "Size": 30}]},
    ]

    # Run with limit=2 (should truncate listing)
    result = runner.invoke(app, ["ls", "-b", "test-bucket", "--limit", "2"])
    assert result.exit_code == 0
    assert "file1.txt" in result.output
    assert "file2.txt" in result.output
    assert "file3.txt" not in result.output  # file3.txt should be truncated
    assert "Truncated listing at limit of 2 objects" in result.output

    # Run with limit=-1 (should show all)
    result_unlimited = runner.invoke(app, ["ls", "-b", "test-bucket", "--limit", "-1"])
    assert result_unlimited.exit_code == 0
    assert "file1.txt" in result_unlimited.output
    assert "file2.txt" in result_unlimited.output
    assert "file3.txt" in result_unlimited.output
    assert "Found 3 objects" in result_unlimited.output


@patch("rds_cli.client.get_s3_client")
def test_info_endpoint_error_handling(mock_get_client):
    # Mock get_s3_client to raise EndpointConnectionError (VPN disconnected or Starlink drop)
    mock_get_client.side_effect = EndpointConnectionError(
        endpoint_url="https://rds.ucr.edu"
    )

    result = runner.invoke(app, ["info", "-b", "test-bucket"])
    assert result.exit_code == 0
    assert "Connection/Transport Error" in result.output


@patch("rds_cli.client.get_s3_client")
def test_ls_client_error_handling(mock_get_client):
    # Mock client head/list to raise a ClientError (e.g. 403 Forbidden)
    mock_s3 = MagicMock()
    mock_get_client.return_value = mock_s3
    mock_s3.list_buckets.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        operation_name="ListBuckets",
    )

    result = runner.invoke(app, ["ls"])
    assert result.exit_code == 0
    assert "S3 API Access Error" in result.output


@patch("rds_cli.client.get_s3_client")
@patch("os.path.exists")
@patch("os.path.isdir")
@patch("os.walk")
def test_upload_directory_partial_failures(
    mock_walk, mock_isdir, mock_exists, mock_get_client
):
    # Set up mocks for directory structure
    mock_exists.return_value = True
    mock_isdir.return_value = True
    mock_walk.return_value = [
        ("/local/path", [], ["good.txt", "bad.txt"]),
    ]

    mock_s3 = MagicMock()
    mock_get_client.return_value = mock_s3

    # Ensure one upload succeeds and one fails
    def mock_upload_file(Filename, Bucket, Key, **kwargs):
        if "bad.txt" in Filename:
            raise ClientError(
                error_response={
                    "Error": {"Code": "AccessDenied", "Message": "Access Denied"}
                },
                operation_name="PutObject",
            )
        return True

    mock_s3.upload_file.side_effect = mock_upload_file

    result = runner.invoke(app, ["upload", "/local/path", "-b", "test-bucket"])
    assert result.exit_code != 0  # Should return non-zero exit code due to failures
    assert "Upload complete with partial failures" in result.output
    assert "1 succeeded, 1 failed" in result.output
    assert "bad.txt" in result.output
