from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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
    local_return_periods_available: bool = False
