from datetime import UTC, datetime

import pandas as pd
import pytest

from app.core.config import Settings
from app.forecast.exceptions import ForecastValidationError, ProviderBackendUnavailableError
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.schemas import ReturnPeriodSchema, TimeseriesPointSchema


VALID_RIVER_ID = 123456789


class _MockGeoglowsRestForecastOnly:
    @staticmethod
    def forecast_stats(river_id, data_source=None):
        assert data_source == "rest"
        assert isinstance(river_id, int)
        return pd.DataFrame(
            [
                {
                    "forecast_time_utc": datetime(2024, 1, 1, tzinfo=UTC),
                    "flow_avg_m^3/s": 11,
                    "flow_max_m^3/s": 15,
                },
                {
                    "forecast_time_utc": datetime(2024, 1, 1, 1, tzinfo=UTC),
                    "flow_avg_m^3/s": 19,
                    "flow_max_m^3/s": 25,
                },
            ]
        )

    @staticmethod
    def return_periods(river_id):
        return pd.DataFrame(
            [
                {
                    "river_id": river_id[0],
                    "return_period_2": 10,
                    "return_period_5": 20,
                    "return_period_10": 30,
                    "return_period_25": 40,
                    "return_period_50": 50,
                    "return_period_100": 60,
                }
            ]
        )


class _MockGeoglowsAwsRpBroken:
    @staticmethod
    def return_periods(river_id):
        raise RuntimeError(
            'Could not connect to the endpoint URL: "https://geoglows-v2.s3.auto.amazonaws.com/retrospective/..."'
        )


class _MockGeoglowsInvalid:
    pass


def test_geoglows_valid_9_digit_id_and_forecast_rest_path():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    ts = provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])
    assert isinstance(ts[0], TimeseriesPointSchema)
    assert ts[0].provider_reach_id == str(VALID_RIVER_ID)


def test_geoglows_invalid_id_validation_error():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    with pytest.raises(ForecastValidationError, match="9-digit"):
        provider.fetch_forecast_timeseries("2024010100", [123])


def test_return_periods_unavailable_in_rest_mode():
    settings = Settings(GEOGLOWS_DATA_SOURCE="rest")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsRestForecastOnly())
    with pytest.raises(ProviderBackendUnavailableError, match="not supported in REST mode"):
        provider.fetch_return_periods([VALID_RIVER_ID])


def test_return_periods_aws_backend_failure_message():
    settings = Settings(GEOGLOWS_DATA_SOURCE="aws")
    provider = GeoglowsForecastProvider(settings, geoglows_module=_MockGeoglowsAwsRpBroken())
    with pytest.raises(ProviderBackendUnavailableError, match="retrospective/AWS access"):
        provider.fetch_return_periods([VALID_RIVER_ID])


def test_geoglows_missing_api_surface_raises_runtime_error():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsInvalid())
    with pytest.raises(Exception, match="does not expose 'forecast_stats'"):
        provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])


def test_summary_shape():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    ts = provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])
    rp = ReturnPeriodSchema(
        provider="geoglows",
        provider_reach_id=str(VALID_RIVER_ID),
        rp_2=10,
        rp_5=20,
        rp_10=30,
        rp_25=40,
        rp_50=50,
        rp_100=60,
    )
    summary = provider.summarize_reach("2024010100", str(VALID_RIVER_ID), ts, rp)
    assert summary.severity_score >= 2
