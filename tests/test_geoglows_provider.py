from datetime import UTC, datetime

import pandas as pd
import pytest

from app.core.config import Settings
from app.forecast.exceptions import ForecastValidationError, ProviderBackendUnavailableError
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.schemas import ReturnPeriodSchema, TimeseriesPointSchema


VALID_RIVER_ID = 760021611


class _MockGeoglowsRestForecastOnly:
    @staticmethod
    def forecast_stats(river_id, data_source=None):
        assert data_source == "rest"
        assert isinstance(river_id, int)
        return pd.DataFrame(
            [
                {
                    "forecast_time_utc": datetime(2024, 1, 1, tzinfo=UTC),
                    "flow_avg": "11.1",
                    "flow_med": "10.5",
                    "flow_25p": "9.2",
                    "flow_75p": "13.0",
                    "flow_max": "15.0",
                    "flow_min": "4.0",
                    "high_res": "14.2",
                },
                {
                    "forecast_time_utc": datetime(2024, 1, 1, 1, tzinfo=UTC),
                    "flow_avg": "nan",
                    "flow_med": "12.3",
                    "flow_25p": "11.1",
                    "flow_75p": "16.0",
                    "flow_max": "18.8",
                    "flow_min": "nan",
                    "high_res": "nan",
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


def test_normalization_maps_provider_columns_and_parses_numbers():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    ts = provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])

    assert isinstance(ts[0], TimeseriesPointSchema)
    assert ts[0].flow_mean_cms == 11.1
    assert ts[0].flow_median_cms == 10.5
    assert ts[0].flow_p25_cms == 9.2
    assert ts[0].flow_p75_cms == 13.0
    assert ts[0].flow_max_cms == 15.0
    assert ts[0].raw_payload_json["high_res"] == 14.2


def test_normalization_nan_strings_become_null():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    ts = provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])

    assert ts[1].flow_mean_cms is None
    assert ts[1].raw_payload_json["flow_min"] is None
    assert ts[1].raw_payload_json["high_res"] is None


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


def test_summary_from_normalized_rows_sets_peak_values_without_rps():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    ts = provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])

    summary = provider.summarize_reach("2024010100", str(VALID_RIVER_ID), ts, None)
    assert summary.peak_time_utc is not None
    assert summary.peak_mean_cms is not None
    assert summary.peak_median_cms is not None
    assert summary.peak_max_cms is not None
    assert summary.return_period_band == "unknown"
    assert summary.severity_score == 0


def test_summary_shape_with_thresholds():
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
    assert summary.return_period_band == "2"
    assert summary.severity_score == 1


def test_geoglows_missing_api_surface_raises_runtime_error():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsInvalid())
    with pytest.raises(Exception, match="does not expose 'forecast_stats'"):
        provider.fetch_forecast_timeseries("2024010100", [VALID_RIVER_ID])


def test_bulk_ingest_requires_configured_source():
    provider = GeoglowsForecastProvider(Settings(), geoglows_module=_MockGeoglowsRestForecastOnly())
    assert provider.supports_bulk_acquisition() is False
    with pytest.raises(ProviderBackendUnavailableError, match="bulk acquisition source is not configured"):
        next(provider.iter_acquired_bulk_records("2024010100"))
