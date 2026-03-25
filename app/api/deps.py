from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.providers.glofas import GlofasForecastProvider
from app.forecast.service import ForecastService


def get_forecast_service(db: Session) -> ForecastService:
    settings = get_settings()
    providers = {}
    if settings.geoglows_enabled and "geoglows" in settings.forecast_enabled_providers:
        providers["geoglows"] = GeoglowsForecastProvider(settings)
    if settings.glofas_enabled and "glofas" in settings.forecast_enabled_providers:
        providers["glofas"] = GlofasForecastProvider(settings)
    return ForecastService(db=db, settings=settings, providers=providers)
