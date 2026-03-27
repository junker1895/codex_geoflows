from app.forecast.schemas import ReturnPeriodSchema
from app.forecast.service import _sanitize_glofas_return_period_rows


def _row(**kwargs):
    defaults = dict(rp_2=1, rp_5=2, rp_10=3, rp_25=4, rp_50=5, rp_100=6)
    defaults.update(kwargs)
    return ReturnPeriodSchema(provider="glofas", provider_reach_id="1", **defaults)


def test_sanitize_rejects_nulls_and_nonfinite():
    rows, rejected = _sanitize_glofas_return_period_rows([_row(rp_10=None), _row(rp_25=float("inf"))])
    assert rows == []
    assert rejected == 2


def test_sanitize_rejects_tiny_or_non_increasing_ladder():
    accepted, rejected = _sanitize_glofas_return_period_rows([
        _row(rp_2=0.001),
        _row(rp_2=2, rp_5=2, rp_10=3, rp_25=4, rp_50=5, rp_100=6),
    ])
    assert accepted == []
    assert rejected == 2


def test_sanitize_accepts_valid_increasing_ladder():
    accepted, rejected = _sanitize_glofas_return_period_rows([_row(rp_2=10, rp_5=20, rp_10=30, rp_25=40, rp_50=50, rp_100=60)])
    assert len(accepted) == 1
    assert rejected == 0
