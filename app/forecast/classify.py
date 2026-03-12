from app.forecast.schemas import ClassificationResult, ReturnPeriodSchema


def classify_peak_flow(peak_flow: float | None, thresholds: ReturnPeriodSchema | None) -> ClassificationResult:
    if peak_flow is None or thresholds is None:
        return ClassificationResult()

    candidates = {
        2: thresholds.rp_2,
        5: thresholds.rp_5,
        10: thresholds.rp_10,
        25: thresholds.rp_25,
        50: thresholds.rp_50,
        100: thresholds.rp_100,
    }
    clean = {k: v for k, v in candidates.items() if isinstance(v, (float, int)) and v >= 0}
    if not clean:
        return ClassificationResult()

    ordered = sorted(clean.items(), key=lambda item: item[1])
    highest = None
    for rp, threshold in ordered:
        if peak_flow >= threshold:
            highest = rp

    if highest is None:
        return ClassificationResult(return_period_band="below_2", severity_score=1, is_flagged=False)

    band_map = {
        2: ("ge_2", 2),
        5: ("ge_5", 3),
        10: ("ge_10", 4),
        25: ("ge_25", 5),
        50: ("ge_50", 6),
        100: ("ge_100", 7),
    }
    band, score = band_map.get(highest, ("unknown", 0))
    return ClassificationResult(return_period_band=band, severity_score=score, is_flagged=score >= 3)
