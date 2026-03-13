from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

CONFIG_DIR = Path.home() / ".config" / "rds-cli"
ENV_FILE = CONFIG_DIR / ".env"

# Ensure the config directory exists
CONFIG_DIR.mkdir(parents=True, exist_ok=True)


class Settings(BaseSettings):
    s3_access_key: str = Field(default="")
    s3_secret_key: str = Field(default="")
    s3_endpoint_url: str = Field(default="https://rds.ucr.edu")

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE), env_file_encoding="utf-8", extra="ignore"
    )


def get_settings() -> Settings:
    return Settings()
