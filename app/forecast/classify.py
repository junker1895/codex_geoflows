from app.forecast.schemas import ClassificationResult, ReturnPeriodSchema


def classify_peak_flow(peak_flow: float | None, thresholds: ReturnPeriodSchema | None) -> ClassificationResult:
    if peak_flow is None or thresholds is None:
        return ClassificationResult()

    rp_2 = _valid_threshold(thresholds.rp_2)
    if rp_2 is None:
        return ClassificationResult()

    rp_5 = _valid_threshold(thresholds.rp_5)
    rp_10 = _valid_threshold(thresholds.rp_10)
    rp_25 = _valid_threshold(thresholds.rp_25)
    rp_50 = _valid_threshold(thresholds.rp_50)
    rp_100 = _valid_threshold(thresholds.rp_100)

    if peak_flow < rp_2:
        return ClassificationResult(return_period_band="below_2", severity_score=0, is_flagged=False)
    if rp_5 is None or peak_flow < rp_5:
        return ClassificationResult(return_period_band="2", severity_score=1, is_flagged=True)
    if rp_10 is None or peak_flow < rp_10:
        return ClassificationResult(return_period_band="5", severity_score=2, is_flagged=True)
    if rp_25 is None or peak_flow < rp_25:
        return ClassificationResult(return_period_band="10", severity_score=3, is_flagged=True)
    if rp_50 is None or peak_flow < rp_50:
        return ClassificationResult(return_period_band="25", severity_score=4, is_flagged=True)
    if rp_100 is None or peak_flow < rp_100:
        return ClassificationResult(return_period_band="50", severity_score=5, is_flagged=True)
    return ClassificationResult(return_period_band="100", severity_score=6, is_flagged=True)


def _valid_threshold(value: float | None) -> float | None:
    if isinstance(value, (float, int)) and value > 0:
        return float(value)
    return None
