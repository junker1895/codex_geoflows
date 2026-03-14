import typer

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.logging import configure_logging
from app.forecast.exceptions import (
    ForecastValidationError,
    ProviderBackendUnavailableError,
    ProviderOperationalError,
)
from app.forecast.jobs import discover_latest_run, ingest_forecast_run, ingest_return_periods, summarize_run
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.providers.geoglows_return_periods import open_geoglows_public_return_periods_zarr
from app.forecast.service import ForecastService

cli = typer.Typer(help="Forecast ingestion CLI")


SAMPLE_GEOGLOWS_RIVER_ID = "123456789"


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
    except ForecastValidationError as exc:
        typer.secho(f"validation error: {exc}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=2) from exc
    except ProviderBackendUnavailableError as exc:
        typer.secho(f"backend unavailable: {exc}", fg=typer.colors.YELLOW)
        raise typer.Exit(code=3) from exc
    except ProviderOperationalError as exc:
        typer.secho(f"provider error: {exc}", fg=typer.colors.RED)
        raise typer.Exit(code=4) from exc
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


@cli.command("import-geoglows-return-periods")
def cli_import_geoglows_return_periods(
    path: str = typer.Option(..., "--path"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        count = service.import_geoglows_return_periods(path)
        typer.echo(f"upserted local GEOGLOWS return periods: {count}")

    _safe_run(_inner)


@cli.command("import-geoglows-return-periods-zarr")
def cli_import_geoglows_return_periods_zarr(
    zarr_path: str | None = typer.Option(None, "--zarr-path"),
    method: str | None = typer.Option(None, "--method"),
    batch_size: int | None = typer.Option(None, "--batch-size"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        count = service.import_geoglows_return_periods_zarr(
            zarr_path=zarr_path, method=method, batch_size=batch_size
        )
        typer.echo(f"upserted GEOGLOWS return periods from Zarr: {count}")

    _safe_run(_inner)




@cli.command("return-periods-zarr-open")
def cli_return_periods_zarr_open(
    zarr_path: str = typer.Option("s3://geoglows-v2/retrospective/return-periods.zarr", "--zarr-path"),
    method: str = typer.Option("gumbel", "--method"),
) -> None:
    def _inner() -> None:
        import xarray as xr

        ds = open_geoglows_public_return_periods_zarr(xr=xr, zarr_path=zarr_path)
        typer.echo(f"selected method: {method}")
        typer.echo(f"dataset dims: {dict(ds.dims)}")
        typer.echo(f"dataset variables: {sorted(ds.data_vars.keys())}")

    _safe_run(_inner)

@cli.command("ingest-forecast-run")
def cli_ingest_forecast_run(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    reach_id: list[str] | None = typer.Option(None, "--reach-id"),
    mode: str | None = typer.Option(
        None,
        "--mode",
        help="Ingest mode: rest_single (debug/small batch) or bulk (full supported network via bulk source)",
    ),
) -> None:
    def _inner() -> None:
        selected_mode = mode
        if selected_mode is None:
            selected_mode = "rest_single" if reach_id else "bulk"
        if selected_mode not in {"rest_single", "bulk"}:
            raise ValueError("--mode must be one of: rest_single, bulk")
        if selected_mode == "rest_single" and not reach_id:
            raise ValueError("--mode rest_single requires at least one --reach-id")
        if selected_mode == "bulk" and reach_id:
            raise ValueError("--mode bulk cannot be combined with --reach-id; remove --reach-id for full ingest")

        service = _build_service()
        count = ingest_forecast_run.run(service, provider, run_id, reach_id, ingest_mode=selected_mode)
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


@cli.command("smoke-geoglows")
def cli_smoke_geoglows(
    river_id: str = typer.Option(SAMPLE_GEOGLOWS_RIVER_ID, "--river-id"),
    run_id: str = typer.Option("latest", "--run-id"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        typer.echo(f"Running GEOGLOWS smoke test with river_id={river_id}")

        forecast_ok = False
        return_ok = False

        try:
            count = service.ingest_forecast_run("geoglows", run_id, [river_id])
            forecast_ok = True
            typer.secho(f"[PASS] forecast_stats REST ingest rows={count}", fg=typer.colors.GREEN)
        except Exception as exc:  # intentionally report but continue
            typer.secho(f"[FAIL] forecast_stats REST: {exc}", fg=typer.colors.RED)

        try:
            count = service.ingest_return_periods("geoglows", [river_id])
            return_ok = True
            typer.secho(f"[PASS] return_periods ingest rows={count}", fg=typer.colors.GREEN)
        except Exception as exc:  # intentionally report but continue
            typer.secho(f"[FAIL] return_periods: {exc}", fg=typer.colors.YELLOW)

        typer.echo("Capability summary:")
        typer.echo(f"- supports_forecast_stats_rest: {forecast_ok}")
        typer.echo(f"- supports_return_periods_current_backend: {return_ok}")

        if not forecast_ok:
            raise typer.Exit(code=5)

    _safe_run(_inner)


if __name__ == "__main__":
    cli()
