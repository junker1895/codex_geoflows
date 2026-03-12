import typer

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import configure_logging
from app.forecast.jobs import discover_latest_run, ingest_forecast_run, ingest_return_periods, summarize_run
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.service import ForecastService

cli = typer.Typer(help="Forecast ingestion CLI")


def _build_service() -> ForecastService:
    settings = get_settings()
    configure_logging(settings.log_level)
    db = SessionLocal()
    providers = {}
    if settings.geoglows_enabled and "geoglows" in settings.forecast_enabled_providers:
        providers["geoglows"] = GeoglowsForecastProvider(settings)
    return ForecastService(db=db, settings=settings, providers=providers)


def _safe_run(fn):
    try:
        fn()
    except Exception as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


@cli.command("discover-latest-run")
def cli_discover_latest_run(provider: str = typer.Option("geoglows", "--provider")) -> None:
    def _inner() -> None:
        service = _build_service()
        run_id = discover_latest_run.run(service, provider)
        typer.echo(f"discovered run: {run_id}")

    _safe_run(_inner)


@cli.command("ingest-return-periods")
def cli_ingest_return_periods(
    provider: str = typer.Option("geoglows", "--provider"),
    reach_id: list[str] = typer.Option(..., "--reach-id"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        count = ingest_return_periods.run(service, provider, reach_id)
        typer.echo(f"upserted return periods: {count}")

    _safe_run(_inner)


@cli.command("ingest-forecast-run")
def cli_ingest_forecast_run(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    reach_id: list[str] = typer.Option(..., "--reach-id"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        count = ingest_forecast_run.run(service, provider, run_id, reach_id)
        typer.echo(f"upserted timeseries rows: {count}")

    _safe_run(_inner)


@cli.command("summarize-run")
def cli_summarize_run(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        count = summarize_run.run(service, provider, run_id)
        typer.echo(f"upserted summaries: {count}")

    _safe_run(_inner)


if __name__ == "__main__":
    cli()
