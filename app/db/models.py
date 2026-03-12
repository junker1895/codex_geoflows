from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Float, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastRun(Base):
    __tablename__ = "forecast_runs"
    __table_args__ = (UniqueConstraint("provider", "run_id", name="uq_forecast_run_provider_run_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    run_id: Mapped[str] = mapped_column(String(128), index=True)
    run_date_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    issued_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_type: Mapped[str] = mapped_column(String(64))
    ingest_status: Mapped[str] = mapped_column(String(32), default="pending")
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ForecastProviderReturnPeriod(Base):
    __tablename__ = "forecast_provider_return_periods"
    __table_args__ = (
        UniqueConstraint("provider", "provider_reach_id", name="uq_rp_provider_reach"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    provider_reach_id: Mapped[str] = mapped_column(String(128), index=True)
    rp_2: Mapped[float | None] = mapped_column(Float, nullable=True)
    rp_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    rp_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    rp_25: Mapped[float | None] = mapped_column(Float, nullable=True)
    rp_50: Mapped[float | None] = mapped_column(Float, nullable=True)
    rp_100: Mapped[float | None] = mapped_column(Float, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ForecastProviderReachTimeseries(Base):
    __tablename__ = "forecast_provider_reach_timeseries"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "run_id",
            "provider_reach_id",
            "forecast_time_utc",
            name="uq_ts_provider_run_reach_time",
        ),
        Index("idx_ts_provider_run", "provider", "run_id"),
        Index("idx_ts_provider_reach", "provider", "provider_reach_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str] = mapped_column(String(128))
    provider_reach_id: Mapped[str] = mapped_column(String(128))
    forecast_time_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    flow_mean_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_median_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_p25_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_p75_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    flow_max_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ForecastProviderReachSummary(Base):
    __tablename__ = "forecast_provider_reach_summaries"
    __table_args__ = (
        UniqueConstraint("provider", "run_id", "provider_reach_id", name="uq_summary_provider_run_reach"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(64), index=True)
    run_id: Mapped[str] = mapped_column(String(128), index=True)
    provider_reach_id: Mapped[str] = mapped_column(String(128), index=True)
    peak_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_exceedance_time_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    peak_mean_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    peak_median_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    peak_max_cms: Mapped[float | None] = mapped_column(Float, nullable=True)
    return_period_band: Mapped[str | None] = mapped_column(String(16), nullable=True)
    severity_score: Mapped[int] = mapped_column(Integer, default=0)
    is_flagged: Mapped[bool] = mapped_column(Boolean, default=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
