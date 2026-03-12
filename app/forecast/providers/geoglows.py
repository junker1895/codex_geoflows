from datetime import UTC, datetime
from typing import Any

import pandas as pd

from app.core.config import Settings
from app.forecast.base import ForecastProviderAdapter
from app.forecast.classify import classify_peak_flow
from app.forecast.schemas import (
    ForecastRunSchema,
    ReachSummarySchema,
    ReturnPeriodSchema,
    TimeseriesPointSchema,
)


class GeoglowsForecastProvider(ForecastProviderAdapter):
    def __init__(self, settings: Settings, geoglows_module: Any | None = None) -> None:
        self.settings = settings
        self._geoglows = geoglows_module

    def get_provider_name(self) -> str:
        return "geoglows"

    def discover_latest_run(self) -> ForecastRunSchema:
        # GEOGLOWS run discovery endpoint support varies by package/version; use a deterministic
        # UTC-hour run identifier as a safe default until a dedicated run metadata source is wired.
        run_date = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        run_id = run_date.strftime("%Y%m%d%H")
        return ForecastRunSchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            run_date_utc=run_date,
            issued_at_utc=run_date,
            source_type=self.settings.geoglows_source_type,
            ingest_status="pending",
            metadata_json={"selector": self.settings.geoglows_default_run_selector},
        )

    def fetch_return_periods(self, reach_ids: list[str | int]) -> list[ReturnPeriodSchema]:
        geoglows = self._get_geoglows()
        data = geoglows.streamflow.return_periods(comid=[int(r) for r in reach_ids])
        if isinstance(data, pd.Series):
            data = data.to_frame().T

        output: list[ReturnPeriodSchema] = []
        for idx, row in data.iterrows():
            reach_id = str(row.get("rivid", idx))
            output.append(
                ReturnPeriodSchema(
                    provider=self.get_provider_name(),
                    provider_reach_id=reach_id,
                    rp_2=_safe_float(row.get("return_period_2")),
                    rp_5=_safe_float(row.get("return_period_5")),
                    rp_10=_safe_float(row.get("return_period_10")),
                    rp_25=_safe_float(row.get("return_period_25")),
                    rp_50=_safe_float(row.get("return_period_50")),
                    rp_100=_safe_float(row.get("return_period_100")),
                    metadata_json={"source": "geoglows.streamflow.return_periods"},
                )
            )
        return output

    def fetch_forecast_timeseries(
        self, run_id: str, reach_ids: list[str | int]
    ) -> list[TimeseriesPointSchema]:
        geoglows = self._get_geoglows()
        rows: list[TimeseriesPointSchema] = []
        for reach_id in reach_ids:
            df = geoglows.streamflow.forecast_stats(comid=int(reach_id))
            if isinstance(df.index, pd.DatetimeIndex):
                iter_rows = df.reset_index(names="forecast_time_utc").to_dict(orient="records")
            else:
                iter_rows = df.to_dict(orient="records")

            for item in iter_rows:
                dt = item.get("forecast_time_utc") or item.get("time")
                if not isinstance(dt, datetime):
                    dt = datetime.fromisoformat(str(dt)).replace(tzinfo=UTC)
                rows.append(
                    TimeseriesPointSchema(
                        provider=self.get_provider_name(),
                        run_id=run_id,
                        provider_reach_id=str(reach_id),
                        forecast_time_utc=dt,
                        flow_mean_cms=_safe_float(item.get("flow_avg_m^3/s") or item.get("mean")),
                        flow_median_cms=_safe_float(item.get("flow_med_m^3/s") or item.get("median")),
                        flow_p25_cms=_safe_float(item.get("flow_25%_m^3/s") or item.get("p25")),
                        flow_p75_cms=_safe_float(item.get("flow_75%_m^3/s") or item.get("p75")),
                        flow_max_cms=_safe_float(item.get("flow_max_m^3/s") or item.get("max")),
                        raw_payload_json={"provider_row": {k: str(v) for k, v in item.items()}},
                    )
                )
        return rows

    def summarize_reach(
        self,
        run_id: str,
        reach_id: str | int,
        timeseries_rows: list[TimeseriesPointSchema],
        return_period_row: ReturnPeriodSchema | None,
    ) -> ReachSummarySchema:
        peak_row = max(timeseries_rows, key=lambda r: r.flow_max_cms or r.flow_mean_cms or -1, default=None)
        peak_flow = None if peak_row is None else (peak_row.flow_max_cms or peak_row.flow_mean_cms)
        classification = classify_peak_flow(peak_flow, return_period_row)

        first_exceedance = None
        if return_period_row and return_period_row.rp_2 is not None:
            for row in sorted(timeseries_rows, key=lambda r: r.forecast_time_utc):
                candidate = row.flow_max_cms or row.flow_mean_cms
                if candidate is not None and candidate >= return_period_row.rp_2:
                    first_exceedance = row.forecast_time_utc
                    break

        return ReachSummarySchema(
            provider=self.get_provider_name(),
            run_id=run_id,
            provider_reach_id=str(reach_id),
            peak_time_utc=None if peak_row is None else peak_row.forecast_time_utc,
            first_exceedance_time_utc=first_exceedance,
            peak_mean_cms=None if peak_row is None else peak_row.flow_mean_cms,
            peak_median_cms=None if peak_row is None else peak_row.flow_median_cms,
            peak_max_cms=None if peak_row is None else peak_row.flow_max_cms,
            return_period_band=classification.return_period_band,
            severity_score=classification.severity_score,
            is_flagged=classification.is_flagged,
            metadata_json={"points": len(timeseries_rows)},
        )

    def _get_geoglows(self) -> Any:
        if self._geoglows is not None:
            return self._geoglows
        try:
            import geoglows as geoglows_module
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "geoglows package is required for GEOGLOWS ingestion. Install dependencies first."
            ) from exc
        self._geoglows = geoglows_module
        return geoglows_module


def _safe_float(value: object) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None
