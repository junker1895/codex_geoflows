from typing import Literal

from app.forecast.service import ForecastService


def run(
    service: ForecastService,
    provider: str,
    run_id: str,
    reach_ids: list[str] | None = None,
    ingest_mode: Literal["rest_single", "bulk"] | None = None,
) -> int:
    return service.ingest_forecast_run(provider, run_id, reach_ids, ingest_mode=ingest_mode)
