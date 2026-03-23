from collections.abc import Iterable

from sqlalchemy import Select, and_, delete, desc, exists, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db import models
from app.forecast.schemas import (
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


# PostgreSQL supports at most 32,767 bind parameters per statement.
# We leave headroom and cap at 30,000 to be safe.
_PG_MAX_PARAMS = 30_000


def _chunked(values: list[dict], cols: int) -> list[list[dict]]:
    """Split *values* into sub-lists that fit within the PG parameter limit."""
    if cols <= 0:
        return [values] if values else []
    chunk_size = max(1, _PG_MAX_PARAMS // cols)
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


class ForecastRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert_run(self, payload: ForecastRunSchema) -> models.ForecastRun:
        row = self.db.execute(
            select(models.ForecastRun).where(
                and_(models.ForecastRun.provider == payload.provider, models.ForecastRun.run_id == payload.run_id)
            )
        ).scalar_one_or_none()
        if not row:
            row = models.ForecastRun(provider=payload.provider, run_id=payload.run_id)
            self.db.add(row)
        row.run_date_utc = payload.run_date_utc
        row.issued_at_utc = payload.issued_at_utc
        row.source_type = payload.source_type
        row.ingest_status = payload.ingest_status
        row.metadata_json = payload.metadata_json
        self.db.flush()
        return row

    def upsert_return_periods(self, rows: Iterable[ReturnPeriodSchema]) -> int:
        values = [
            {
                "provider": payload.provider,
                "provider_reach_id": payload.provider_reach_id,
                "rp_2": payload.rp_2,
                "rp_5": payload.rp_5,
                "rp_10": payload.rp_10,
                "rp_25": payload.rp_25,
                "rp_50": payload.rp_50,
                "rp_100": payload.rp_100,
                "metadata_json": payload.metadata_json,
            }
            for payload in rows
        ]
        if not values:
            return 0
        update_cols = {"rp_2", "rp_5", "rp_10", "rp_25", "rp_50", "rp_100", "metadata_json"}
        for chunk in _chunked(values, cols=9):
            stmt = pg_insert(models.ForecastProviderReturnPeriod).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_rp_provider_reach",
                set_={col: stmt.excluded[col] for col in update_cols},
            )
            self.db.execute(stmt)
        self.db.flush()
        return len(values)

    def bulk_upsert_timeseries(self, rows: Iterable[TimeseriesPointSchema]) -> int:
        values = [
            {
                "provider": payload.provider,
                "run_id": payload.run_id,
                "provider_reach_id": payload.provider_reach_id,
                "forecast_time_utc": payload.forecast_time_utc,
                "flow_mean_cms": payload.flow_mean_cms,
                "flow_median_cms": payload.flow_median_cms,
                "flow_p25_cms": payload.flow_p25_cms,
                "flow_p75_cms": payload.flow_p75_cms,
                "flow_max_cms": payload.flow_max_cms,
                "raw_payload_json": payload.raw_payload_json,
            }
            for payload in rows
        ]
        if not values:
            return 0
        update_cols = {
            "flow_mean_cms", "flow_median_cms", "flow_p25_cms",
            "flow_p75_cms", "flow_max_cms", "raw_payload_json",
        }
        for chunk in _chunked(values, cols=10):
            stmt = pg_insert(models.ForecastProviderReachTimeseries).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_ts_provider_run_reach_time",
                set_={col: stmt.excluded[col] for col in update_cols},
            )
            self.db.execute(stmt)
        self.db.flush()
        return len(values)

    def upsert_summaries(self, rows: Iterable[ReachSummarySchema]) -> int:
        values = [
            {
                "provider": payload.provider,
                "run_id": payload.run_id,
                "provider_reach_id": payload.provider_reach_id,
                "peak_time_utc": payload.peak_time_utc,
                "first_exceedance_time_utc": payload.first_exceedance_time_utc,
                "peak_mean_cms": payload.peak_mean_cms,
                "peak_median_cms": payload.peak_median_cms,
                "peak_max_cms": payload.peak_max_cms,
                "now_mean_cms": payload.now_mean_cms,
                "now_max_cms": payload.now_max_cms,
                "return_period_band": payload.return_period_band,
                "severity_score": payload.severity_score,
                "is_flagged": payload.is_flagged,
                "metadata_json": payload.metadata_json,
            }
            for payload in rows
        ]
        if not values:
            return 0
        update_cols = {
            "peak_time_utc", "first_exceedance_time_utc",
            "peak_mean_cms", "peak_median_cms", "peak_max_cms",
            "now_mean_cms", "now_max_cms",
            "return_period_band", "severity_score", "is_flagged", "metadata_json",
        }
        for chunk in _chunked(values, cols=14):
            stmt = pg_insert(models.ForecastProviderReachSummary).values(chunk)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_summary_provider_run_reach",
                set_={col: stmt.excluded[col] for col in update_cols},
            )
            self.db.execute(stmt)
        return len(values)


    def delete_summaries_for_run(self, provider: str, run_id: str) -> int:
        stmt = delete(models.ForecastProviderReachSummary).where(
            and_(
                models.ForecastProviderReachSummary.provider == provider,
                models.ForecastProviderReachSummary.run_id == run_id,
            )
        )
        result = self.db.execute(stmt)
        return result.rowcount

    def count_timeseries_rows_for_run(self, provider: str, run_id: str) -> int:
        stmt = select(func.count(models.ForecastProviderReachTimeseries.id)).where(
            and_(
                models.ForecastProviderReachTimeseries.provider == provider,
                models.ForecastProviderReachTimeseries.run_id == run_id,
            )
        )
        return int(self.db.execute(stmt).scalar_one())

    def count_timeseries_reaches_for_run(self, provider: str, run_id: str) -> int:
        stmt = select(func.count(models.ForecastProviderReachTimeseries.provider_reach_id.distinct())).where(
            and_(
                models.ForecastProviderReachTimeseries.provider == provider,
                models.ForecastProviderReachTimeseries.run_id == run_id,
            )
        )
        return int(self.db.execute(stmt).scalar_one())

    def count_summaries_for_run(self, provider: str, run_id: str) -> int:
        stmt = select(func.count(models.ForecastProviderReachSummary.id)).where(
            and_(
                models.ForecastProviderReachSummary.provider == provider,
                models.ForecastProviderReachSummary.run_id == run_id,
            )
        )
        return int(self.db.execute(stmt).scalar_one())

    def get_latest_run(self, provider: str, *, require_has_data: bool = False) -> models.ForecastRun | None:
        stmt = (
            select(models.ForecastRun)
            .where(models.ForecastRun.provider == provider)
        )
        if require_has_data:
            # Only return runs that have at least one summary row
            S = models.ForecastProviderReachSummary
            stmt = stmt.where(
                exists(
                    select(S.id).where(
                        and_(S.provider == models.ForecastRun.provider, S.run_id == models.ForecastRun.run_id)
                    )
                )
            )
        return self.db.execute(
            stmt.order_by(desc(models.ForecastRun.run_date_utc)).limit(1)
        ).scalar_one_or_none()

    def get_run(self, provider: str, run_id: str) -> models.ForecastRun | None:
        return self.db.execute(
            select(models.ForecastRun).where(
                and_(models.ForecastRun.provider == provider, models.ForecastRun.run_id == run_id)
            )
        ).scalar_one_or_none()


    def count_supported_reaches(self, provider: str) -> int:
        stmt = select(func.count(models.ForecastProviderReturnPeriod.id)).where(
            models.ForecastProviderReturnPeriod.provider == provider
        )
        return int(self.db.execute(stmt).scalar_one())

    def iter_supported_reach_ids(
        self,
        provider: str,
        chunk_size: int = 1000,
        as_chunks: bool = True,
    ):
        stmt = (
            select(models.ForecastProviderReturnPeriod.provider_reach_id)
            .where(models.ForecastProviderReturnPeriod.provider == provider)
            .order_by(models.ForecastProviderReturnPeriod.provider_reach_id)
        )
        stream = self.db.execute(stmt).scalars()

        if not as_chunks:
            for reach_id in stream:
                yield str(reach_id)
            return

        batch: list[str] = []
        for reach_id in stream:
            batch.append(str(reach_id))
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def has_return_periods(self, provider: str) -> bool:
        stmt = select(models.ForecastProviderReturnPeriod.id).where(
            models.ForecastProviderReturnPeriod.provider == provider
        ).limit(1)
        return self.db.execute(stmt).scalar_one_or_none() is not None

    def get_return_period(self, provider: str, reach_id: str) -> models.ForecastProviderReturnPeriod | None:
        return self.db.execute(
            select(models.ForecastProviderReturnPeriod).where(
                and_(
                    models.ForecastProviderReturnPeriod.provider == provider,
                    models.ForecastProviderReturnPeriod.provider_reach_id == reach_id,
                )
            )
        ).scalar_one_or_none()

    def get_all_return_periods(self, provider: str) -> dict[str, models.ForecastProviderReturnPeriod]:
        """Load all return periods for a provider into a dict keyed by reach_id."""
        stmt = select(models.ForecastProviderReturnPeriod).where(
            models.ForecastProviderReturnPeriod.provider == provider
        )
        rows = self.db.execute(stmt).scalars().all()
        return {str(row.provider_reach_id): row for row in rows}

    def get_timeseries(
        self, provider: str, run_id: str, reach_id: str, limit: int | None = None
    ) -> list[models.ForecastProviderReachTimeseries]:
        stmt: Select[tuple[models.ForecastProviderReachTimeseries]] = (
            select(models.ForecastProviderReachTimeseries)
            .where(
                and_(
                    models.ForecastProviderReachTimeseries.provider == provider,
                    models.ForecastProviderReachTimeseries.run_id == run_id,
                    models.ForecastProviderReachTimeseries.provider_reach_id == reach_id,
                )
            )
            .order_by(models.ForecastProviderReachTimeseries.forecast_time_utc)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def get_summary(self, provider: str, run_id: str, reach_id: str) -> models.ForecastProviderReachSummary | None:
        return self.db.execute(
            select(models.ForecastProviderReachSummary).where(
                and_(
                    models.ForecastProviderReachSummary.provider == provider,
                    models.ForecastProviderReachSummary.run_id == run_id,
                    models.ForecastProviderReachSummary.provider_reach_id == reach_id,
                )
            )
        ).scalar_one_or_none()


    def get_map_summaries(
        self,
        provider: str,
        run_id: str,
        flagged_only: bool = False,
        min_severity_score: float | None = None,
        limit: int | None = None,
    ) -> list[models.ForecastProviderReachSummary]:
        stmt = select(models.ForecastProviderReachSummary).where(
            and_(
                models.ForecastProviderReachSummary.provider == provider,
                models.ForecastProviderReachSummary.run_id == run_id,
            )
        )
        if flagged_only:
            stmt = stmt.where(models.ForecastProviderReachSummary.is_flagged.is_(True))
        if min_severity_score is not None:
            stmt = stmt.where(models.ForecastProviderReachSummary.severity_score >= min_severity_score)
        stmt = stmt.order_by(desc(models.ForecastProviderReachSummary.severity_score))
        if limit:
            stmt = stmt.limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def get_severity_map(
        self,
        provider: str,
        run_id: str,
        min_severity_score: int = 1,
        limit: int | None = None,
    ) -> dict[str, int]:
        """Return {provider_reach_id: severity_score} for flagged reaches.

        Only selects two columns – no ORM hydration, minimal serialisation cost.
        Results ordered by severity DESC so the limit keeps the most critical reaches.
        """
        S = models.ForecastProviderReachSummary
        stmt = (
            select(S.provider_reach_id, S.severity_score)
            .where(
                and_(
                    S.provider == provider,
                    S.run_id == run_id,
                    S.severity_score >= min_severity_score,
                )
            )
            .order_by(desc(S.severity_score))
        )
        if limit:
            stmt = stmt.limit(limit)
        rows = self.db.execute(stmt).all()
        return {str(r[0]): int(r[1]) for r in rows}

    def get_summaries(
        self,
        provider: str,
        run_id: str,
        severity_min: int | None = None,
        limit: int | None = None,
    ) -> list[models.ForecastProviderReachSummary]:
        stmt = select(models.ForecastProviderReachSummary).where(
            and_(
                models.ForecastProviderReachSummary.provider == provider,
                models.ForecastProviderReachSummary.run_id == run_id,
            )
        )
        if severity_min is not None:
            stmt = stmt.where(models.ForecastProviderReachSummary.severity_score >= severity_min)
        stmt = stmt.order_by(desc(models.ForecastProviderReachSummary.severity_score))
        if limit:
            stmt = stmt.limit(limit)
        return list(self.db.execute(stmt).scalars().all())
