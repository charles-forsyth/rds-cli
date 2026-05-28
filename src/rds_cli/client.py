import boto3
from botocore.config import Config
from .config import get_settings


def get_s3_client():
    settings = get_settings()

    if not settings.s3_access_key or not settings.s3_secret_key:
        raise ValueError("AWS credentials not found. Please run 'rds-cli auth' first.")

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(
            connect_timeout=5,  # Fail quickly if endpoint is unreachable (e.g. VPN off)
            read_timeout=15,  # Wait up to 15s for socket/data read response
            retries={
                "max_attempts": 3,  # Retry up to 3 times on transient socket errors
                "mode": "standard",  # Use standard exponential backoff jitter
            },
            s3={"addressing_style": "path"},
        ),
    )
