from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ORMBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class ForecastRunSchema(ORMBaseModel):
    provider: str
    run_id: str
    run_date_utc: datetime
    issued_at_utc: datetime | None = None
    source_type: str = "geoglows_api"
    ingest_status: str = "pending"
    metadata_json: dict[str, Any] | None = None


class ReturnPeriodSchema(ORMBaseModel):
    provider: str
    provider_reach_id: str
    rp_2: float | None = None
    rp_5: float | None = None
    rp_10: float | None = None
    rp_25: float | None = None
    rp_50: float | None = None
    rp_100: float | None = None
    metadata_json: dict[str, Any] | None = None


class TimeseriesPointSchema(ORMBaseModel):
    provider: str
    run_id: str
    provider_reach_id: str
    forecast_time_utc: datetime
    flow_mean_cms: float | None = None
    flow_median_cms: float | None = None
    flow_p25_cms: float | None = None
    flow_p75_cms: float | None = None
    flow_max_cms: float | None = None
    raw_payload_json: dict[str, Any] | None = None


class ReachSummarySchema(ORMBaseModel):
    provider: str
    run_id: str
    provider_reach_id: str
    peak_time_utc: datetime | None = None
    first_exceedance_time_utc: datetime | None = None
    peak_mean_cms: float | None = None
    peak_median_cms: float | None = None
    peak_max_cms: float | None = None
    return_period_band: str | None = None
    severity_score: int = 0
    is_flagged: bool = False
    metadata_json: dict[str, Any] | None = None


class MapReachSummarySchema(ORMBaseModel):
    provider: str
    run_id: str
    provider_reach_id: str
    peak_time_utc: datetime | None = None
    peak_mean_cms: float | None = None
    peak_median_cms: float | None = None
    peak_max_cms: float | None = None
    return_period_band: str | None = None
    severity_score: int = 0
    is_flagged: bool = False


class ForecastMapFilters(ORMBaseModel):
    bbox: str | None = None
    flagged_only: bool = False
    min_severity_score: float | None = None


class ForecastMapMeta(ORMBaseModel):
    provider: str
    run_id: str
    count: int
    filters: ForecastMapFilters


class ForecastMapReachesResponse(ORMBaseModel):
    data: list[MapReachSummarySchema] = Field(default_factory=list)
    meta: ForecastMapMeta




class BulkForecastArtifactRowSchema(ORMBaseModel):
    provider: str
    run_id: str
    provider_reach_id: str
    forecast_time_utc: datetime
    flow_mean_cms: float | None = None
    flow_median_cms: float | None = None
    flow_p25_cms: float | None = None
    flow_p75_cms: float | None = None
    flow_max_cms: float | None = None
    raw_payload_json: dict | None = None

    @field_validator("provider_reach_id", mode="before")
    @classmethod
    def _normalize_reach_id(cls, value: object) -> str:
        return str(value).strip()


class BulkForecastSummaryArtifactRowSchema(ORMBaseModel):
    provider: str
    run_id: str
    provider_reach_id: str
    peak_time_utc: datetime | None = None
    peak_mean_cms: float | None = None
    peak_median_cms: float | None = None
    peak_max_cms: float | None = None
    return_period_band: str | None = None
    severity_score: int = 0
    is_flagged: bool = False
    raw_payload_json: dict | None = None

    @field_validator("provider_reach_id", mode="before")
    @classmethod
    def _normalize_summary_reach_id(cls, value: object) -> str:
        return str(value).strip()


class ClassificationResult(ORMBaseModel):
    return_period_band: str = "unknown"
    severity_score: int = 0
    is_flagged: bool = False


class ReachDetailResponse(ORMBaseModel):
    provider: str
    run: ForecastRunSchema
    return_periods: ReturnPeriodSchema | None
    timeseries: list[TimeseriesPointSchema] = Field(default_factory=list)
    summary: ReachSummarySchema | None


class ProviderHealthResponse(ORMBaseModel):
    provider: str
    enabled: bool
    latest_run: ForecastRunSchema | None
    ingest_status: str | None
    summary_count: int = 0
    supports_forecast_stats_rest: bool = False
    supports_return_periods_current_backend: bool = False
    supports_bulk_forecast_ingest: bool = False
    bulk_acquisition_configured: bool = False
    bulk_acquisition_mode: str = "unknown"
    bulk_raw_source_reachable: bool | None = None
    local_return_periods_available: bool = False
    latest_run_has_timeseries: bool = False
    latest_run_timeseries_row_count: int = 0
    latest_run_reach_count: int = 0
    latest_run_has_summaries: bool = False
    latest_run_artifact_exists: bool = False
    latest_run_artifact_row_count: int = 0
    latest_run_map_ready: bool = False
    latest_run_summary_count: int = 0
    latest_run_map_count: int = 0
    latest_run_status: str | None = None
    latest_run_missing_stages: list[str] = Field(default_factory=list)
    latest_run_failure_stage: str | None = None
    latest_run_failure_message: str | None = None
    authoritative_latest_upstream_run_id: str | None = None
    latest_upstream_run_exists: bool | None = None
    source_bucket: str | None = None
    source_zarr_path: str | None = None
    bounded_run: bool | None = None
    configured_limits: dict[str, Any] = Field(default_factory=dict)


class RawAcquisitionStatus(ORMBaseModel):
    attempted: bool = False
    succeeded: bool = False
    mode: str | None = None
    source_uri: str | None = None
    staged_raw_path: str | None = None


class ArtifactStatus(ORMBaseModel):
    exists: bool = False
    path: str | None = None
    row_count: int = 0
    size_bytes: int = 0


class IngestStatus(ORMBaseModel):
    completed: bool = False
    timeseries_row_count: int = 0


class SummarizeStatus(ORMBaseModel):
    completed: bool = False
    summary_row_count: int = 0


class RunReadinessStatusResponse(ORMBaseModel):
    provider: str
    run_id: str
    current_status: str
    completed_stages: list[str] = Field(default_factory=list)
    missing_stages: list[str] = Field(default_factory=list)
    raw_acquisition: RawAcquisitionStatus
    artifact: ArtifactStatus
    ingest: IngestStatus
    summarize: SummarizeStatus
    map_row_count: int = 0
    map_ready: bool = False
    map_ready_definition: str
    failure_stage: str | None = None
    failure_message: str | None = None
    last_updated_utc: datetime | None = None
    authoritative_latest_upstream_run_id: str | None = None
    upstream_run_exists: bool | None = None
    acquisition_mode: str | None = None
    source_bucket: str | None = None
    source_zarr_path: str | None = None
    bounded_run: bool | None = None
    configured_limits: dict[str, Any] = Field(default_factory=dict)
