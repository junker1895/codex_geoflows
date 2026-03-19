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
    forecast_bulk_ingest_batch_size: int = Field(default=5000, alias="FORECAST_BULK_INGEST_BATCH_SIZE")
    forecast_bulk_artifact_dir: str = Field(default="./data/forecast_artifacts", alias="FORECAST_BULK_ARTIFACT_DIR")
    forecast_bulk_artifact_write_batch_size: int = Field(default=10000, alias="FORECAST_BULK_ARTIFACT_WRITE_BATCH_SIZE")
    forecast_bulk_artifact_retention_runs: int = Field(default=5, alias="FORECAST_BULK_ARTIFACT_RETENTION_RUNS")
    forecast_bulk_artifact_format: str = Field(default="parquet", alias="FORECAST_BULK_ARTIFACT_FORMAT")
    forecast_environment: str = Field(default="local", alias="FORECAST_ENVIRONMENT")
    forecast_cache_dir: str = Field(default="./data/forecast_cache", alias="FORECAST_CACHE_DIR")
    forecast_cache_max_gb: float = Field(default=2.0, alias="FORECAST_CACHE_MAX_GB")
    forecast_cleanup_cache_after_run: bool = Field(default=True, alias="FORECAST_CLEANUP_CACHE_AFTER_RUN")
    forecast_default_max_reaches: int = Field(default=25000, alias="FORECAST_DEFAULT_MAX_REACHES")
    forecast_default_max_blocks: int = Field(default=20, alias="FORECAST_DEFAULT_MAX_BLOCKS")
    forecast_default_max_seconds: int = Field(default=900, alias="FORECAST_DEFAULT_MAX_SECONDS")
    forecast_detail_cache_ttl_seconds: int = Field(default=300, alias="FORECAST_DETAIL_CACHE_TTL_SECONDS")
    forecast_detail_cache_max_items: int = Field(default=128, alias="FORECAST_DETAIL_CACHE_MAX_ITEMS")
    geoglows_enabled: bool = Field(default=True, alias="GEOGLOWS_ENABLED")
    geoglows_source_type: str = Field(default="geoglows_api", alias="GEOGLOWS_SOURCE_TYPE")
    geoglows_default_run_selector: str = Field(default="latest", alias="GEOGLOWS_DEFAULT_RUN_SELECTOR")
    geoglows_request_timeout_seconds: int = Field(default=30, alias="GEOGLOWS_REQUEST_TIMEOUT_SECONDS")
    geoglows_data_source: str = Field(default="rest", alias="GEOGLOWS_DATA_SOURCE")
    geoglows_return_period_method: str = Field(default="gumbel", alias="GEOGLOWS_RETURN_PERIOD_METHOD")
    geoglows_return_period_zarr_path: str = Field(
        default="s3://geoglows-v2/retrospective/return-periods.zarr",
        alias="GEOGLOWS_RETURN_PERIOD_ZARR_PATH",
    )
    geoglows_return_period_import_batch_size: int = Field(
        default=10000, alias="GEOGLOWS_RETURN_PERIOD_IMPORT_BATCH_SIZE"
    )
    geoglows_bulk_forecast_source: str | None = Field(default=None, alias="GEOGLOWS_BULK_FORECAST_SOURCE")
    geoglows_forecast_bucket: str = Field(default="geoglows-v2-forecasts", alias="GEOGLOWS_FORECAST_BUCKET")
    geoglows_forecast_region: str = Field(default="us-west-2", alias="GEOGLOWS_FORECAST_REGION")
    geoglows_forecast_use_anon: bool = Field(default=True, alias="GEOGLOWS_FORECAST_USE_ANON")
    geoglows_forecast_variable: str = Field(default="Qout", alias="GEOGLOWS_FORECAST_VARIABLE")
    geoglows_forecast_run_suffix: str = Field(default=".zarr", alias="GEOGLOWS_FORECAST_RUN_SUFFIX")
    geoglows_bulk_acquisition_mode: str = Field(default="aws_public_zarr", alias="GEOGLOWS_BULK_ACQUISITION_MODE")
    geoglows_bulk_raw_source_uri: str | None = Field(default=None, alias="GEOGLOWS_BULK_RAW_SOURCE_URI")
    geoglows_bulk_remote_auth_token: str | None = Field(default=None, alias="GEOGLOWS_BULK_REMOTE_AUTH_TOKEN")
    geoglows_bulk_staging_dir: str = Field(default="./data/geoglows_raw", alias="GEOGLOWS_BULK_STAGING_DIR")
    geoglows_bulk_download_timeout_seconds: int = Field(default=120, alias="GEOGLOWS_BULK_DOWNLOAD_TIMEOUT_SECONDS")
    geoglows_bulk_download_max_retries: int = Field(default=2, alias="GEOGLOWS_BULK_DOWNLOAD_MAX_RETRIES")
    geoglows_bulk_overwrite_existing_raw: bool = Field(default=False, alias="GEOGLOWS_BULK_OVERWRITE_EXISTING_RAW")
    geoglows_bulk_raw_retention_runs: int = Field(default=5, alias="GEOGLOWS_BULK_RAW_RETENTION_RUNS")

    @field_validator("forecast_enabled_providers", mode="before")
    @classmethod
    def parse_enabled_providers(cls, value: object) -> object:
        if isinstance(value, str):
            return [x.strip() for x in value.split(",") if x.strip()]
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
