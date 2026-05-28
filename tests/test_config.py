import os
from unittest.mock import patch
from rds_cli.config import Settings


def test_settings_aliases_fallback():
    # Test fallback when custom keys are not set but standard AWS env vars are
    env_mock = {
        "AWS_ACCESS_KEY_ID": "mock_aws_id",
        "AWS_SECRET_ACCESS_KEY": "mock_aws_secret",
        "AWS_S3_ENDPOINT_URL": "https://aws-s3.example.com",
    }
    with patch.dict(os.environ, env_mock, clear=True):
        # Pass _env_file=None to bypass loading the real config file on disk during testing
        settings = Settings(_env_file=None)
        assert settings.s3_access_key == "mock_aws_id"
        assert settings.s3_secret_key == "mock_aws_secret"
        assert settings.s3_endpoint_url == "https://aws-s3.example.com"


def test_settings_custom_priority():
    # Test that custom keys have higher priority if both are set
    env_mock = {
        "S3_ACCESS_KEY": "custom_id",
        "S3_SECRET_KEY": "custom_secret",
        "S3_ENDPOINT_URL": "https://custom.example.com",
        "AWS_ACCESS_KEY_ID": "mock_aws_id",
        "AWS_SECRET_ACCESS_KEY": "mock_aws_secret",
        "AWS_S3_ENDPOINT_URL": "https://aws-s3.example.com",
    }
    with patch.dict(os.environ, env_mock, clear=True):
        settings = Settings(_env_file=None)
        assert settings.s3_access_key == "custom_id"
        assert settings.s3_secret_key == "custom_secret"
        assert settings.s3_endpoint_url == "https://custom.example.com"
