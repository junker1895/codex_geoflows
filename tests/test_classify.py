from app.forecast.classify import classify_peak_flow
from app.forecast.schemas import ReturnPeriodSchema


def _thresholds(**kwargs):
    return ReturnPeriodSchema(provider="geoglows", provider_reach_id="1", **kwargs)


def test_missing_flow_unknown():
    result = classify_peak_flow(None, _thresholds(rp_2=10))
    assert result.severity_score == 0
    assert result.return_period_band == "unknown"


def test_missing_thresholds_fallback():
    result = classify_peak_flow(20, _thresholds())
    assert result.severity_score == 0


def test_below_two_year():
    result = classify_peak_flow(9.9, _thresholds(rp_2=10, rp_5=20))
    assert result.return_period_band == "below_2"
    assert result.severity_score == 1


def test_equal_two_year():
    result = classify_peak_flow(10, _thresholds(rp_2=10, rp_5=20))
    assert result.return_period_band == "ge_2"


def test_five_year_band():
    result = classify_peak_flow(21, _thresholds(rp_2=10, rp_5=20, rp_10=30))
    assert result.return_period_band == "ge_5"
    assert result.severity_score == 3


def test_hundred_year_band():
    result = classify_peak_flow(1000, _thresholds(rp_2=10, rp_5=20, rp_10=30, rp_25=40, rp_50=50, rp_100=60))
    assert result.return_period_band == "ge_100"
    assert result.severity_score == 7


def test_unordered_thresholds_still_works():
    result = classify_peak_flow(55, _thresholds(rp_2=50, rp_5=20, rp_10=45, rp_25=40, rp_50=60, rp_100=80))
    assert result.severity_score >= 3
