from datetime import UTC, datetime

import pandas as pd
import pytest

from app.core.config import Settings
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.schemas import ReturnPeriodSchema, TimeseriesPointSchema


class _MockStreamflow:
    @staticmethod
    def return_periods(comid):
        return pd.DataFrame(
            [
                {
                    "rivid": 123,
                    "return_period_2": 10,
                    "return_period_5": 20,
                    "return_period_10": 30,
                    "return_period_25": 40,
                    "return_period_50": 50,
                    "return_period_100": 60,
                }
            ]
        )

    @staticmethod
    def forecast_stats(comid):
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


class _MockGeoglowsStreamflow:
    streamflow = _MockStreamflow()


class _MockGeoglowsTopLevel:
    return_periods = staticmethod(_MockStreamflow.return_periods)
    forecast_stats = staticmethod(_MockStreamflow.forecast_stats)


class _MockGeoglowsInvalid:
    pass


def _assert_provider(provider: GeoglowsForecastProvider) -> None:
    rp = provider.fetch_return_periods([123])
    ts = provider.fetch_forecast_timeseries("2024010100", [123])

    assert isinstance(rp[0], ReturnPeriodSchema)
    assert rp[0].provider_reach_id == "123"
    assert isinstance(ts[0], TimeseriesPointSchema)
    summary = provider.summarize_reach("2024010100", "123", ts, rp[0])
    assert summary.run_id == "2024010100"
    assert summary.severity_score >= 2


def test_geoglows_normalization_streamflow_namespace():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsStreamflow())
    _assert_provider(provider)


def test_geoglows_normalization_top_level_namespace():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsTopLevel())
    _assert_provider(provider)


def test_geoglows_missing_api_surface_raises_runtime_error():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsInvalid())
    with pytest.raises(RuntimeError, match="does not expose 'return_periods'"):
        provider.fetch_return_periods([123])
