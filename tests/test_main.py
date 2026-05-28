from unittest.mock import patch
from typer.testing import CliRunner
from rds_cli.main import app

runner = CliRunner()


def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "CephRDS Command Line Interface" in result.output
    # Ensure all required commands are present
    assert "auth" in result.output
    assert "info" in result.output
    assert "ls" in result.output
    assert "upload" in result.output
    assert "download" in result.output
    assert "rm" in result.output
    assert "share" in result.output
    assert "stat" in result.output
    assert "cp" in result.output
    assert "mv" in result.output
    # Ensure new renames and aliases are listed in help
    assert "bucket-info" in result.output
    assert "file-info" in result.output
    assert "list" in result.output
    assert "copy" in result.output
    assert "move" in result.output
    assert "delete" in result.output
    assert "remove" in result.output


@patch("rds_cli.main.bucket_info")
def test_info_deprecation_warning(mock_bucket_info):
    result = runner.invoke(app, ["info", "-b", "test-bucket"])
    assert (
        "Warning: 'info' is deprecated. Please use 'bucket-info' instead."
        in result.output
    )
    mock_bucket_info.assert_called_once_with(bucket="test-bucket")


@patch("rds_cli.main.file_info")
def test_stat_deprecation_warning(mock_file_info):
    result = runner.invoke(app, ["stat", "my-key", "-b", "test-bucket"])
    assert (
        "Warning: 'stat' is deprecated. Please use 'file-info' instead."
        in result.output
    )
    mock_file_info.assert_called_once_with(key="my-key", bucket="test-bucket")


@patch("rds_cli.main.ls")
def test_list_alias(mock_ls):
    runner.invoke(app, ["list", "-b", "test-bucket", "-p", "prefix/", "-l", "10"])
    mock_ls.assert_called_once_with(bucket="test-bucket", prefix="prefix/", limit=10)


@patch("rds_cli.main.cp")
def test_copy_alias(mock_cp):
    runner.invoke(app, ["copy", "src", "dst", "--recursive", "--multipart"])
    mock_cp.assert_called_once_with(
        source="src", destination="dst", recursive=True, multipart=True
    )


@patch("rds_cli.main.mv")
def test_move_alias(mock_mv):
    runner.invoke(app, ["move", "src", "dst", "--recursive"])
    mock_mv.assert_called_once_with(source="src", destination="dst", recursive=True)


@patch("rds_cli.main.rm")
def test_delete_alias(mock_rm):
    runner.invoke(app, ["delete", "my-key", "-b", "test-bucket", "--recursive"])
    mock_rm.assert_called_once_with(key="my-key", bucket="test-bucket", recursive=True)


@patch("rds_cli.main.rm")
def test_remove_alias(mock_rm):
    runner.invoke(app, ["remove", "my-key", "-b", "test-bucket", "--recursive"])
    mock_rm.assert_called_once_with(key="my-key", bucket="test-bucket", recursive=True)
