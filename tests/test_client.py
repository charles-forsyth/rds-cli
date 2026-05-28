import pytest
from unittest.mock import patch
from rds_cli.client import get_s3_client
from rds_cli.config import Settings


def test_get_s3_client_missing_credentials():
    # If credentials are not set, should raise ValueError
    with patch("rds_cli.client.get_settings") as mock_get_settings:
        mock_get_settings.return_value = Settings(
            s3_access_key="", s3_secret_key="", _env_file=None
        )
        with pytest.raises(ValueError, match="AWS credentials not found"):
            get_s3_client()


def test_get_s3_client_config():
    # If credentials are set, client should be initialized with correct config
    with patch("rds_cli.client.get_settings") as mock_get_settings:
        mock_get_settings.return_value = Settings(
            s3_access_key="test_key",
            s3_secret_key="test_secret",
            s3_endpoint_url="https://test-endpoint.example.com",
            _env_file=None,
        )
        client = get_s3_client()
        assert client.meta.endpoint_url == "https://test-endpoint.example.com"
        assert client.meta.config.connect_timeout == 5
        assert client.meta.config.read_timeout == 15
        assert client.meta.config.retries.get("total_max_attempts") == 4
        assert client.meta.config.retries.get("mode") == "standard"
        assert client.meta.config.s3 == {"addressing_style": "path"}
