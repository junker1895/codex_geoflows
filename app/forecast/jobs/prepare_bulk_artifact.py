from app.forecast.service import ForecastService


def run(
    service: ForecastService,
    provider: str,
    run_id: str,
    filter_to_supported_reaches: bool = True,
    if_present: str = "skip",
    overwrite_raw: bool = False,
) -> tuple[str, int]:
    return service.prepare_bulk_artifact(
        provider=provider,
        run_id=run_id,
        filter_to_supported_reaches=filter_to_supported_reaches,
        if_present=if_present,
        overwrite_raw=overwrite_raw,
    )
