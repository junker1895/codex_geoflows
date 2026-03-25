from app.forecast.classify import classify_peak_flow
from app.forecast.schemas import ReturnPeriodSchema


def _thresholds(**kwargs):
    return ReturnPeriodSchema(provider="geoglows", provider_reach_id="1", **kwargs)


def test_missing_flow_unknown():
    result = classify_peak_flow(None, _thresholds(rp_2=10))
    assert result.severity_score == 0
    assert result.return_period_band == "unknown"
    assert result.is_flagged is False


def test_missing_thresholds_fallback_unknown():
    result = classify_peak_flow(20, _thresholds())
    assert result.return_period_band == "unknown"
    assert result.severity_score == 0
    assert result.is_flagged is False


def test_below_two_year_band():
    result = classify_peak_flow(9.9, _thresholds(rp_2=10, rp_5=20))
    assert result.return_period_band == "below_2"
    assert result.severity_score == 0
    assert result.is_flagged is False


def test_two_year_band_and_flagged():
    result = classify_peak_flow(10, _thresholds(rp_2=10, rp_5=20))
    assert result.return_period_band == "2"
    assert result.severity_score == 1
    assert result.is_flagged is True


def test_five_year_band():
    result = classify_peak_flow(21, _thresholds(rp_2=10, rp_5=20, rp_10=30))
    assert result.return_period_band == "5"
    assert result.severity_score == 2
    assert result.is_flagged is True


def test_each_threshold_band_boundary_values():
    thresholds = _thresholds(rp_2=10, rp_5=20, rp_10=30, rp_25=40, rp_50=50, rp_100=60)

    assert classify_peak_flow(30, thresholds).return_period_band == "10"
    assert classify_peak_flow(40, thresholds).return_period_band == "25"
    assert classify_peak_flow(50, thresholds).return_period_band == "50"

    hundred = classify_peak_flow(60, thresholds)
    assert hundred.return_period_band == "100"
    assert hundred.severity_score == 6
    assert hundred.is_flagged is True


def test_zero_thresholds_treated_as_unknown():
    """Zero-valued return periods are physically impossible and must not
    cause positive peak flows to cascade to severity 6."""
    thresholds = _thresholds(rp_2=0.0, rp_5=0.0, rp_10=0.0, rp_25=0.0, rp_50=0.0, rp_100=0.0)
    result = classify_peak_flow(5.0, thresholds)
    # rp_2=0.0 is treated as invalid → falls back to unknown/severity 0
    assert result.severity_score == 0
    assert result.return_period_band == "unknown"
    assert result.is_flagged is False


def test_near_zero_thresholds_treated_as_invalid():
    """Near-zero values (e.g. 1e-20) from GloFAS return-period datasets are
    physically meaningless and must not cause false severity-6 classifications."""
    thresholds = _thresholds(
        rp_2=1e-20, rp_5=1e-15, rp_10=1e-10,
        rp_25=1e-8, rp_50=1e-6, rp_100=1e-4,
    )
    result = classify_peak_flow(5.0, thresholds)
    # All thresholds below 0.01 m³/s → treated as invalid → unknown
    assert result.severity_score == 0
    assert result.return_period_band == "unknown"
    assert result.is_flagged is False


def test_threshold_at_minimum_boundary():
    """Thresholds exactly at the 0.01 m³/s minimum are accepted."""
    thresholds = _thresholds(rp_2=0.01, rp_5=0.02)
    result = classify_peak_flow(0.015, thresholds)
    assert result.return_period_band == "2"
    assert result.severity_score == 1
