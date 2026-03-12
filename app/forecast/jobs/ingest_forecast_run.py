from app.forecast.service import ForecastService


def run(service: ForecastService, provider: str, run_id: str, reach_ids: list[str]) -> int:
    return service.ingest_forecast_run(provider, run_id, reach_ids)
