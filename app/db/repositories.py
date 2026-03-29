from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from sqlalchemy import Select, and_, delete, desc, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db import models
from app.forecast.schemas import (
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


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

    # ------------------------------------------------------------------
    # COPY-based fast path (PostgreSQL + psycopg3 only)
    # ------------------------------------------------------------------

    _SUMMARY_COPY_COLS = (
        "provider", "run_id", "provider_reach_id",
        "peak_time_utc", "first_exceedance_time_utc",
        "peak_mean_cms", "peak_median_cms", "peak_max_cms",
        "now_mean_cms", "now_max_cms",
        "return_period_band", "severity_score", "is_flagged", "metadata_json",
    )

    _SUMMARY_UPDATE_COLS = (
        "peak_time_utc", "first_exceedance_time_utc",
        "peak_mean_cms", "peak_median_cms", "peak_max_cms",
        "now_mean_cms", "now_max_cms",
        "return_period_band", "severity_score", "is_flagged", "metadata_json",
    )

    def copy_upsert_summaries_from_table(self, table: pa.Table) -> int:
        """Bulk-load an Arrow table into forecast_provider_reach_summaries
        using PostgreSQL COPY (via psycopg3) + a temp staging table for upsert.

        Falls back to the regular ``upsert_summaries`` path when the
        underlying connection is not psycopg3 / PostgreSQL.
        """
        num_rows = table.num_rows
        if num_rows == 0:
            return 0

        raw_conn = self._get_psycopg_connection()
        if raw_conn is None:
            return self._fallback_upsert_from_table(table)

        cols = self._SUMMARY_COPY_COLS
        col_list = ", ".join(cols)
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in self._SUMMARY_UPDATE_COLS)

        # Create an UNLOGGED temp table (no WAL overhead) matching the target schema.
        self.db.execute(text(
            "CREATE TEMP TABLE _stg_summaries (LIKE forecast_provider_reach_summaries INCLUDING DEFAULTS) "
            "ON COMMIT DROP"
        ))
        self.db.flush()

        # Build CSV in-memory from the Arrow table.
        buf = self._arrow_table_to_csv(table, cols)

        # COPY into the temp table.
        raw_cursor = raw_conn.cursor()
        with raw_cursor.copy(
            f"COPY _stg_summaries ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        ) as copy:
            copy.write(buf.getvalue())

        # Merge from staging into the real table.
        self.db.execute(text(
            f"INSERT INTO forecast_provider_reach_summaries ({col_list}) "
            f"SELECT {col_list} FROM _stg_summaries "
            f"ON CONFLICT ON CONSTRAINT uq_summary_provider_run_reach "
            f"DO UPDATE SET {update_set}"
        ))

        return num_rows

    def _get_psycopg_connection(self):
        """Return the underlying psycopg connection, or None if unavailable."""
        try:
            sa_conn = self.db.connection()
            dbapi_conn = sa_conn.connection.dbapi_connection
            # psycopg3 connections have a .pgconn attribute
            if hasattr(dbapi_conn, "pgconn"):
                return dbapi_conn
        except Exception:
            pass
        return None

    def _arrow_table_to_csv(self, table: pa.Table, cols: tuple[str, ...]) -> io.BytesIO:
        """Serialize selected columns of an Arrow table to CSV bytes."""
        buf = io.BytesIO()
        wrapper = io.TextIOWrapper(buf, encoding="utf-8", newline="")
        writer = csv.writer(wrapper)
        writer.writerow(cols)

        # Convert to Python rows — Arrow's to_pylist() is faster than per-row iteration.
        for row in table.to_pylist():
            writer.writerow(self._format_csv_value(row.get(c)) for c in cols)

        wrapper.flush()
        wrapper.detach()  # prevent close from closing buf
        buf.seek(0)
        return buf

    @staticmethod
    def _format_csv_value(val) -> str:
        if val is None:
            return ""
        # Floats that are whole numbers must be written without the decimal
        # so PostgreSQL COPY accepts them for integer columns (e.g. severity_score).
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        return str(val)

    def _fallback_upsert_from_table(self, table: pa.Table) -> int:
        """Convert Arrow table to ReachSummarySchema list and use the regular path."""
        rows = []
        for r in table.to_pylist():
            rows.append(ReachSummarySchema(
                provider=str(r["provider"]),
                run_id=str(r["run_id"]),
                provider_reach_id=str(r["provider_reach_id"]),
                peak_time_utc=r.get("peak_time_utc"),
                first_exceedance_time_utc=r.get("first_exceedance_time_utc"),
                peak_mean_cms=r.get("peak_mean_cms"),
                peak_median_cms=r.get("peak_median_cms"),
                peak_max_cms=r.get("peak_max_cms"),
                now_mean_cms=r.get("now_mean_cms"),
                now_max_cms=r.get("now_max_cms"),
                return_period_band=r.get("return_period_band"),
                severity_score=int(r.get("severity_score", 0) or 0),
                is_flagged=bool(r.get("is_flagged", False)),
                metadata_json=r.get("metadata_json"),
            ))
        return self.upsert_summaries(rows)

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
        reach_ids: list[str] | None = None,
        bbox: str | None = None,
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
        if reach_ids:
            stmt = stmt.where(S.provider_reach_id.in_(reach_ids))
        if bbox:
            try:
                min_lon, min_lat, max_lon, max_lat = [float(x) for x in bbox.split(",")]
            except Exception:
                min_lon = min_lat = max_lon = max_lat = None
            if None not in (min_lon, min_lat, max_lon, max_lat):
                C = models.ReachGridCrosswalk
                crosswalk_provider = "glofas" if provider == "geoglows" else provider
                spatial_match = (
                    select(1)
                    .select_from(C)
                    .where(
                        and_(
                            C.reach_id == S.provider_reach_id,
                            C.target_provider == crosswalk_provider,
                            C.grid_lon.is_not(None),
                            C.grid_lat.is_not(None),
                            C.grid_lon >= min_lon,
                            C.grid_lon <= max_lon,
                            C.grid_lat >= min_lat,
                            C.grid_lat <= max_lat,
                        )
                    )
                    .correlate(S)
                    .exists()
                )
                stmt = stmt.where(spatial_match)
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
