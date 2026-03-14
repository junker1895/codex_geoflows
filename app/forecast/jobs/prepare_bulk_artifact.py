from app.forecast.service import ForecastService


def run(
    service: ForecastService,
    provider: str,
    run_id: str,
    filter_to_supported_reaches: bool = True,
) -> tuple[str, int]:
    return service.prepare_bulk_artifact(
        provider=provider,
        run_id=run_id,
        filter_to_supported_reaches=filter_to_supported_reaches,
    )
