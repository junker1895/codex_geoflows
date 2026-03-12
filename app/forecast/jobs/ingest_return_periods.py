from app.forecast.service import ForecastService


def run(service: ForecastService, provider: str, reach_ids: list[str]) -> int:
    return service.ingest_return_periods(provider, reach_ids)
