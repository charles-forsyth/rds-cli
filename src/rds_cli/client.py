import boto3
from botocore.client import Config
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
        config=Config(s3={"addressing_style": "path"}),
    )
