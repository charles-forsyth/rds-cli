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
