import os
from dataclasses import dataclass
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


@dataclass(frozen=True)
class DBConfig:
    server: str
    database: str
    username: str
    password: str
    driver: str 


def _get_env_variable(key: str) -> str:
    """
    Fetch environment variable and validate.
    Raises ValueError if missing.
    """
    value = os.getenv(key)
    if not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


def get_db_config() -> DBConfig:
    """
    Reads DB configuration from environment variables
    and returns a DBConfig object.
    """
    return DBConfig(
        server=_get_env_variable("DB_SERVER"),
        database=_get_env_variable("DB_NAME"),
        username=_get_env_variable("DB_USER"),
        password=_get_env_variable("DB_PASSWORD"),
        driver=_get_env_variable("DB_DRIVER")
    )