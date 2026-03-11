from sqlalchemy.orm import Session

from app.forecast.service import ForecastService


def run(service: ForecastService, provider: str) -> str:
    result = service.discover_latest_run(provider)
    return result.run_id
