import logging
from time import perf_counter
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db import models
from app.db.repositories import ForecastRepository
from app.forecast.base import ForecastProviderAdapter
from app.forecast.providers.geoglows_return_periods import (
    iter_geoglows_return_periods_from_zarr,
    load_geoglows_return_periods_from_path,
)
from app.forecast.schemas import (
    ForecastMapFilters,
    ForecastMapMeta,
    ForecastMapReachesResponse,
    ForecastRunSchema,
    MapReachSummarySchema,
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

    def import_geoglows_return_periods(self, dataset_path: str) -> int:
        rows = load_geoglows_return_periods_from_path(dataset_path)
        count = self.repo.upsert_return_periods(rows)
        self.db.commit()
        logger.info("imported local GEOGLOWS return periods", extra={"count": count, "path": dataset_path})
        return count

    def import_geoglows_return_periods_zarr(
        self,
        zarr_path: str | None = None,
        method: str | None = None,
        batch_size: int | None = None,
    ) -> int:
        selected_path = zarr_path or self.settings.geoglows_return_period_zarr_path
        selected_method = method or self.settings.geoglows_return_period_method
        selected_batch_size = batch_size or self.settings.geoglows_return_period_import_batch_size

        logger.info(
            "starting GEOGLOWS Zarr return-period import",
            extra={
                "zarr_path": selected_path,
                "method": selected_method,
                "batch_size": selected_batch_size,
            },
        )

        total_upserted = 0
        total_processed = 0
        for rows in iter_geoglows_return_periods_from_zarr(
            zarr_path=selected_path,
            method=selected_method,
            batch_size=selected_batch_size,
        ):
            total_processed += len(rows)
            upserted = self.repo.upsert_return_periods(rows)
            total_upserted += upserted
            self.db.commit()
            logger.info(
                "upserted GEOGLOWS return-period batch",
                extra={
                    "method": selected_method,
                    "batch_size": selected_batch_size,
                    "batch_rows": len(rows),
                    "batch_upserted": upserted,
                    "total_processed": total_processed,
                    "total_upserted": total_upserted,
                },
            )

        logger.info(
            "completed GEOGLOWS Zarr return-period import",
            extra={
                "method": selected_method,
                "batch_size": selected_batch_size,
                "total_reaches_processed": total_processed,
                "total_upserted": total_upserted,
                "classification_available": self.repo.has_return_periods("geoglows"),
            },
        )
        return total_upserted

    def ingest_forecast_run(
        self,
        provider: str,
        run_id: str,
        reach_ids: list[str] | None = None,
        ingest_mode: Literal["rest_single", "bulk"] | None = None,
    ) -> int:
        adapter = self._get_provider(provider)
        resolved_run = self._resolve_run(provider, run_id)
        selected_mode = ingest_mode or ("rest_single" if reach_ids else "bulk")

        if selected_mode == "rest_single":
            if not reach_ids:
                raise ValueError("ingest_mode=rest_single requires at least one --reach-id")
            return self._ingest_via_rest(adapter, provider, resolved_run.run_id, list(reach_ids))

        if selected_mode != "bulk":
            raise ValueError(f"Unsupported ingest mode '{selected_mode}'")
        if reach_ids:
            raise ValueError("ingest_mode=bulk does not accept explicit reach_ids; use rest_single for targeted ingest")
        return self._ingest_via_bulk(adapter, provider, resolved_run.run_id)

    def _ingest_via_rest(
        self,
        adapter: ForecastProviderAdapter,
        provider: str,
        run_id: str,
        reach_ids: list[str],
    ) -> int:
        logger.info(
            "starting forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "rest_single",
                "source": "forecast_stats_rest",
                "total_reach_count": len(reach_ids),
            },
        )
        started_at = perf_counter()
        rows = adapter.fetch_forecast_timeseries(run_id, reach_ids)
        rows_written = self.repo.bulk_upsert_timeseries(rows)
        run_row = self.repo.get_run(provider, run_id)
        if run_row:
            run_row.ingest_status = "partial" if rows_written == 0 else "complete"
        self.db.commit()
        logger.info(
            "completed forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "rest_single",
                "source": "forecast_stats_rest",
                "rows_written": rows_written,
                "elapsed_seconds": round(perf_counter() - started_at, 3),
            },
        )
        return rows_written

    def _ingest_via_bulk(self, adapter: ForecastProviderAdapter, provider: str, run_id: str) -> int:
        if not adapter.supports_bulk_forecast_ingest():
            raise ValueError(
                "Bulk ingest was requested, but no provider bulk forecast source is configured. "
                "REST forecast_stats ingest is intentionally limited to single/small batches and is not used "
                "for full-network runs due to upstream throttling."
            )

        supported_reach_count = self.repo.count_supported_reaches(provider)
        if supported_reach_count == 0:
            raise ValueError(
                f"No supported reaches found for provider '{provider}'. "
                "Import return periods first to establish supported map reaches."
            )

        logger.info(
            "starting forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "bulk",
                "source": "provider_bulk",
                "supported_reach_count": supported_reach_count,
                "batch_size": self.settings.forecast_bulk_ingest_batch_size,
            },
        )

        started_at = perf_counter()
        total_rows = 0
        for chunk_index, rows in enumerate(
            adapter.iter_bulk_forecast_timeseries(
                run_id=run_id,
                supported_reach_ids=self.repo.iter_supported_reach_ids(provider, as_chunks=False),
                batch_size=self.settings.forecast_bulk_ingest_batch_size,
            ),
            start=1,
        ):
            chunk_rows = self.repo.bulk_upsert_timeseries(rows)
            total_rows += chunk_rows
            self.db.commit()
            logger.info(
                "ingest chunk complete",
                extra={
                    "provider": provider,
                    "run_id": run_id,
                    "ingest_mode": "bulk",
                    "source": "provider_bulk",
                    "chunk_number": chunk_index,
                    "chunk_rows_written": chunk_rows,
                    "total_rows_written": total_rows,
                },
            )

        run_row = self.repo.get_run(provider, run_id)
        if run_row:
            run_row.ingest_status = "partial" if total_rows == 0 else "complete"
        self.db.commit()

        logger.info(
            "completed forecast ingest",
            extra={
                "provider": provider,
                "run_id": run_id,
                "ingest_mode": "bulk",
                "source": "provider_bulk",
                "supported_reach_count": supported_reach_count,
                "rows_written": total_rows,
                "elapsed_seconds": round(perf_counter() - started_at, 3),
            },
        )
        return total_rows

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


    def list_forecast_map_reaches(
        self,
        provider: str,
        run_id: str | None = None,
        bbox: str | None = None,
        limit: int | None = None,
        flagged_only: bool = False,
        min_severity_score: float | None = None,
    ) -> ForecastMapReachesResponse:
        """
        Map endpoint contract: serve lightweight forecast attributes only.

        River geometry is intentionally excluded from this backend because geometry is
        provided by frontend tiles (PMTiles/vector tiles). The detailed reach endpoint
        remains the heavy view for timeseries and return-period payloads.

        NOTE: bbox is accepted for forward compatibility; this repository currently has
        no reach geometry/bounds table, so bbox filtering is not applied yet.
        """
        self._get_provider(provider)
        run = self._resolve_run(provider, run_id or "latest", require_existing=False)
        if not run:
            return ForecastMapReachesResponse(
                data=[],
                meta=ForecastMapMeta(
                    provider=provider,
                    run_id="",
                    count=0,
                    filters=ForecastMapFilters(
                        bbox=bbox,
                        flagged_only=flagged_only,
                        min_severity_score=min_severity_score,
                    ),
                ),
            )

        rows = self.repo.get_map_summaries(
            provider=provider,
            run_id=run.run_id,
            flagged_only=flagged_only,
            min_severity_score=min_severity_score,
            limit=limit or self.settings.forecast_summary_default_limit,
        )
        data = [to_map_summary_schema(x) for x in rows]
        return ForecastMapReachesResponse(
            data=data,
            meta=ForecastMapMeta(
                provider=provider,
                run_id=run.run_id,
                count=len(data),
                filters=ForecastMapFilters(
                    bbox=bbox,
                    flagged_only=flagged_only,
                    min_severity_score=min_severity_score,
                ),
            ),
        )

    def get_provider_health(self, provider: str) -> ProviderHealthResponse:
        adapter = self._get_provider(provider)
        latest = self.get_latest_run(provider)
        summary_count = 0
        status = None
        latest_run_timeseries_row_count = 0
        latest_run_reach_count = 0
        if latest:
            status = latest.ingest_status
            summary_count = self.repo.count_summaries_for_run(provider, latest.run_id)
            latest_run_timeseries_row_count = self.repo.count_timeseries_rows_for_run(provider, latest.run_id)
            latest_run_reach_count = self.repo.count_timeseries_reaches_for_run(provider, latest.run_id)

        capabilities = getattr(adapter, "capabilities", None)
        supports_forecast_stats_rest = bool(
            getattr(capabilities, "supports_forecast_stats_rest", False)
        )
        supports_bulk_forecast_ingest = bool(adapter.supports_bulk_forecast_ingest())
        source = self.settings.geoglows_data_source.lower() if provider == "geoglows" else "unknown"
        supports_return_periods_current_backend = bool(
            getattr(capabilities, f"supports_return_periods_{source}", False)
        )
        local_return_periods_available = self.repo.has_return_periods(provider)

        return ProviderHealthResponse(
            provider=provider,
            enabled=provider in self.providers,
            latest_run=latest,
            ingest_status=status,
            summary_count=summary_count,
            supports_forecast_stats_rest=supports_forecast_stats_rest,
            supports_return_periods_current_backend=supports_return_periods_current_backend,
            supports_bulk_forecast_ingest=supports_bulk_forecast_ingest,
            local_return_periods_available=local_return_periods_available,
            latest_run_has_timeseries=latest_run_timeseries_row_count > 0,
            latest_run_timeseries_row_count=latest_run_timeseries_row_count,
            latest_run_reach_count=latest_run_reach_count,
            latest_run_has_summaries=summary_count > 0,
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


def to_map_summary_schema(row: models.ForecastProviderReachSummary) -> MapReachSummarySchema:
    return MapReachSummarySchema(
        provider=row.provider,
        run_id=row.run_id,
        provider_reach_id=row.provider_reach_id,
        peak_time_utc=row.peak_time_utc,
        peak_mean_cms=row.peak_mean_cms,
        peak_median_cms=row.peak_median_cms,
        peak_max_cms=row.peak_max_cms,
        return_period_band=row.return_period_band,
        severity_score=row.severity_score,
        is_flagged=row.is_flagged,
    )
