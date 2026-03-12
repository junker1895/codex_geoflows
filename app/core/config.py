from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field(default="local", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/geoflows",
        alias="DATABASE_URL",
    )
    forecast_default_provider: str = Field(default="geoglows", alias="FORECAST_DEFAULT_PROVIDER")
    forecast_enabled_providers: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["geoglows"], alias="FORECAST_ENABLED_PROVIDERS"
    )
    forecast_summary_default_limit: int = Field(default=200, alias="FORECAST_SUMMARY_DEFAULT_LIMIT")
    geoglows_enabled: bool = Field(default=True, alias="GEOGLOWS_ENABLED")
    geoglows_source_type: str = Field(default="geoglows_api", alias="GEOGLOWS_SOURCE_TYPE")
    geoglows_default_run_selector: str = Field(default="latest", alias="GEOGLOWS_DEFAULT_RUN_SELECTOR")
    geoglows_request_timeout_seconds: int = Field(default=30, alias="GEOGLOWS_REQUEST_TIMEOUT_SECONDS")
    geoglows_data_source: str = Field(default="rest", alias="GEOGLOWS_DATA_SOURCE")

    @field_validator("forecast_enabled_providers", mode="before")
    @classmethod
    def parse_enabled_providers(cls, value: object) -> object:
        if isinstance(value, str):
            return [x.strip() for x in value.split(",") if x.strip()]
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
