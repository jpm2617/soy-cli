import os
from enum import Enum
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Environments(Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class EnvSettings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent / ".env",
        env_file_encoding="utf-8"
    )

    DATABRICKS_PROFILE_ID: str
    DATABRICKS_CLUSTER_ID: str
    LOG_LEVEL: str = "INFO"
    LOG_TO_JSON: bool = False


def get_env() -> EnvSettings:
    """Get the environment settings based on the current environment."""
    _env = os.getenv("ENV_FILE_PATH", "dev")
    if _env not in list(Environments._value2member_map_):
        raise ValueError(
            f"Invalid environment: {_env}. Must be one of {list(Environments._value2member_map_)}.")

    # Ensure the .env file is loaded from the correct path
    env_file_path = Path(__file__).parent / f".env.{_env}"
    if not env_file_path.exists():
        raise FileNotFoundError(
            f"Environment file {env_file_path} does not exist.")
    return EnvSettings(_env_file=env_file_path)


env = get_env()
