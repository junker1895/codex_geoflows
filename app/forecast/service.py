import logging
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db import models
from app.db.repositories import ForecastRepository
from app.forecast.base import ForecastProviderAdapter
from app.forecast.schemas import (
    ForecastRunSchema,
    ProviderHealthResponse,
    ReachDetailResponse,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


logger = logging.getLogger(__name__)


class ForecastService:
    def __init__(self, db: Session, settings: Settings, providers: dict[str, ForecastProviderAdapter]) -> None:
        self.db = db
        self.settings = settings
        self.repo = ForecastRepository(db)
        self.providers = providers

    def list_providers(self) -> list[str]:
        return sorted(self.providers.keys())

    def _get_provider(self, provider: str) -> ForecastProviderAdapter:
        if provider not in self.providers:
            raise ValueError(f"Provider '{provider}' is not enabled")
        return self.providers[provider]

    def discover_latest_run(self, provider: str) -> ForecastRunSchema:
        run = self._get_provider(provider).discover_latest_run()
        self.repo.upsert_run(run)
        self.db.commit()
        return run

    def ingest_return_periods(self, provider: str, reach_ids: list[str]) -> int:
        self._get_provider(provider)
        rows = self.providers[provider].fetch_return_periods(reach_ids)
        count = self.repo.upsert_return_periods(rows)
        self.db.commit()
        logger.info("upserted return periods", extra={"provider": provider, "count": count})
        return count

    def ingest_forecast_run(self, provider: str, run_id: str, reach_ids: list[str]) -> int:
        self._get_provider(provider)
        resolved_run = self._resolve_run(provider, run_id)
        rows = self.providers[provider].fetch_forecast_timeseries(resolved_run.run_id, reach_ids)
        count = self.repo.bulk_upsert_timeseries(rows)
        run_row = self.repo.get_run(provider, resolved_run.run_id)
        if run_row:
            run_row.ingest_status = "partial" if count == 0 else "complete"
        self.db.commit()
        logger.info("upserted forecast timeseries rows", extra={"provider": provider, "run_id": resolved_run.run_id, "count": count})
        return count

    def summarize_run(self, provider: str, run_id: str, reach_ids: list[str] | None = None) -> int:
        adapter = self._get_provider(provider)
        resolved_run = self._resolve_run(provider, run_id)

        if reach_ids is None:
            reach_ids = list(
                self.db.execute(
                    select(models.ForecastProviderReachTimeseries.provider_reach_id)
                    .where(
                        models.ForecastProviderReachTimeseries.provider == provider,
                        models.ForecastProviderReachTimeseries.run_id == resolved_run.run_id,
                    )
                    .distinct()
                ).scalars()
            )

        summaries: list[ReachSummarySchema] = []
        for reach_id in reach_ids:
            ts_rows = [to_timeseries_schema(x) for x in self.repo.get_timeseries(provider, resolved_run.run_id, reach_id)]
            rp_model = self.repo.get_return_period(provider, reach_id)
            rp_schema = None if not rp_model else to_return_period_schema(rp_model)
            if rp_schema is None:
                logger.info("generating summary without return periods", extra={"provider": provider, "run_id": resolved_run.run_id, "reach_id": reach_id})
            summary = adapter.summarize_reach(resolved_run.run_id, reach_id, ts_rows, rp_schema)
            logger.info("summary peak values", extra={"provider": provider, "run_id": resolved_run.run_id, "reach_id": reach_id, "peak_mean_cms": summary.peak_mean_cms, "peak_median_cms": summary.peak_median_cms, "peak_max_cms": summary.peak_max_cms})
            summaries.append(summary)

        count = self.repo.upsert_summaries(summaries)
        self.db.commit()
        return count

    def get_latest_run(self, provider: str) -> ForecastRunSchema | None:
        self._get_provider(provider)
        row = self.repo.get_latest_run(provider)
        return None if row is None else to_run_schema(row)

    def get_reach_detail(
        self, provider: str, provider_reach_id: str, run_id: str | None = None, timeseries_limit: int | None = None
    ) -> ReachDetailResponse:
        self._get_provider(provider)
        run = self._resolve_run(provider, run_id or "latest")
        rp_row = self.repo.get_return_period(provider, provider_reach_id)
        ts_rows = self.repo.get_timeseries(provider, run.run_id, provider_reach_id, limit=timeseries_limit)
        summary = self.repo.get_summary(provider, run.run_id, provider_reach_id)
        return ReachDetailResponse(
            provider=provider,
            run=run,
            return_periods=None if rp_row is None else to_return_period_schema(rp_row),
            timeseries=[to_timeseries_schema(x) for x in ts_rows],
            summary=None if summary is None else to_summary_schema(summary),
        )

    def get_reach_summaries(
        self, provider: str, run_id: str | None = None, severity_min: int | None = None, limit: int | None = None
    ) -> list[ReachSummarySchema]:
        self._get_provider(provider)
        run = self._resolve_run(provider, run_id or "latest", require_existing=False)
        if not run:
            return []
        rows = self.repo.get_summaries(
            provider,
            run.run_id,
            severity_min=severity_min,
            limit=limit or self.settings.forecast_summary_default_limit,
        )
        return [to_summary_schema(x) for x in rows]

    def get_provider_health(self, provider: str) -> ProviderHealthResponse:
        adapter = self._get_provider(provider)
        latest = self.get_latest_run(provider)
        summary_count = 0
        status = None
        if latest:
            status = latest.ingest_status
            summary_count = len(self.get_reach_summaries(provider, run_id=latest.run_id, limit=10000))

        capabilities = getattr(adapter, "capabilities", None)
        supports_forecast_stats_rest = bool(
            getattr(capabilities, "supports_forecast_stats_rest", False)
        )
        source = self.settings.geoglows_data_source.lower() if provider == "geoglows" else "unknown"
        supports_return_periods_current_backend = bool(
            getattr(capabilities, f"supports_return_periods_{source}", False)
        )

        return ProviderHealthResponse(
            provider=provider,
            enabled=provider in self.providers,
            latest_run=latest,
            ingest_status=status,
            summary_count=summary_count,
            supports_forecast_stats_rest=supports_forecast_stats_rest,
            supports_return_periods_current_backend=supports_return_periods_current_backend,
        )

    def _resolve_run(
        self, provider: str, run_id: str, require_existing: bool = True
    ) -> ForecastRunSchema | None:
        if run_id == "latest":
            latest = self.repo.get_latest_run(provider)
            if latest:
                return to_run_schema(latest)
            if require_existing:
                return self.discover_latest_run(provider)
            return None

        run = self.repo.get_run(provider, run_id)
        if run:
            return to_run_schema(run)
        if require_existing:
            raise ValueError(f"Run '{run_id}' not found for provider '{provider}'")
        return None


def to_run_schema(row: models.ForecastRun) -> ForecastRunSchema:
    return ForecastRunSchema.model_validate(row, from_attributes=True)


def to_return_period_schema(row: models.ForecastProviderReturnPeriod) -> ReturnPeriodSchema:
    return ReturnPeriodSchema.model_validate(row, from_attributes=True)


def to_timeseries_schema(row: models.ForecastProviderReachTimeseries) -> TimeseriesPointSchema:
    return TimeseriesPointSchema.model_validate(row, from_attributes=True)


def to_summary_schema(row: models.ForecastProviderReachSummary) -> ReachSummarySchema:
    return ReachSummarySchema.model_validate(row, from_attributes=True)
