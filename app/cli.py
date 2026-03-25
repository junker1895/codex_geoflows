import json
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
from app.forecast.jobs import prepare_bulk_artifact
from app.forecast.providers.geoglows import GeoglowsForecastProvider
from app.forecast.providers.geoglows_forecast_zarr import describe_forecast_dataset, open_geoglows_public_forecast_run_zarr
from app.forecast.providers.geoglows_return_periods import open_geoglows_public_return_periods_zarr
from app.forecast.providers.glofas import GlofasForecastProvider
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
    if settings.glofas_enabled and "glofas" in settings.forecast_enabled_providers:
        providers["glofas"] = GlofasForecastProvider(settings)
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


@cli.command("forecast-zarr-inspect")
def cli_forecast_zarr_inspect(
    run_id: str = typer.Option("latest", "--run-id"),
    bucket: str | None = typer.Option(None, "--bucket"),
    region: str | None = typer.Option(None, "--region"),
    forecast_variable: str | None = typer.Option(None, "--variable"),
) -> None:
    def _inner() -> None:
        import xarray as xr

        service = _build_service()
        settings = get_settings()
        provider = service.providers["geoglows"]
        resolved = service.resolve_requested_run_id("geoglows", run_id)

        selected_bucket = bucket or settings.geoglows_forecast_bucket
        selected_region = region or settings.geoglows_forecast_region
        selected_variable = forecast_variable or settings.geoglows_forecast_variable

        ds = open_geoglows_public_forecast_run_zarr(
            xr=xr,
            run_id=resolved.run_id,
            bucket=selected_bucket,
            region=selected_region,
            use_anon=settings.geoglows_forecast_use_anon,
            run_suffix=settings.geoglows_forecast_run_suffix,
        )
        summary = describe_forecast_dataset(ds, selected_variable)
        summary["run_id"] = resolved.run_id
        summary["source_zarr_path"] = provider.build_source_zarr_path(resolved.run_id)
        typer.echo(json.dumps(summary, indent=2, default=str))

    _safe_run(_inner)



@cli.command("prepare-bulk-artifact")
def cli_prepare_bulk_artifact(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    filter_to_supported_reaches: bool = typer.Option(True, "--filter-supported/--no-filter-supported"),
    if_present: str = typer.Option("skip", "--if-present", help="Behavior if artifact exists: skip|overwrite|error"),
    overwrite_raw: bool = typer.Option(False, "--overwrite-raw"),
) -> None:
    def _inner() -> None:
        if if_present not in {"skip", "overwrite", "error"}:
            raise ValueError("--if-present must be one of: skip, overwrite, error")
        service = _build_service()
        artifact_path, count = prepare_bulk_artifact.run(
            service,
            provider=provider,
            run_id=run_id,
            filter_to_supported_reaches=filter_to_supported_reaches,
            if_present=if_present,
            overwrite_raw=overwrite_raw,
        )
        typer.echo(f"prepared bulk artifact: {artifact_path} (rows={count})")

    _safe_run(_inner)


@cli.command("prepare-bulk-summaries")
def cli_prepare_bulk_summaries(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    filter_to_supported_reaches: bool = typer.Option(True, "--filter-supported/--no-filter-supported"),
    if_present: str = typer.Option("skip", "--if-present", help="Behavior if summary artifact exists: skip|overwrite|error"),
    max_reaches: int | None = typer.Option(None, "--max-reaches"),
    max_blocks: int | None = typer.Option(None, "--max-blocks"),
    max_seconds: int | None = typer.Option(None, "--max-seconds"),
    full_run: bool = typer.Option(False, "--full-run"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        artifact_path, count = service.prepare_bulk_summaries(
            provider=provider,
            run_id=run_id,
            filter_to_supported_reaches=filter_to_supported_reaches,
            if_present=if_present,
            max_reaches=max_reaches,
            max_blocks=max_blocks,
            max_seconds=max_seconds,
            full_run=full_run,
        )
        typer.echo(f"prepared summary bulk artifact: {artifact_path} (rows={count})")

    _safe_run(_inner)


@cli.command("ingest-forecast-summaries")
def cli_ingest_forecast_summaries(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    replace_existing: bool = typer.Option(False, "--replace-existing"),
    skip_reclassify: bool = typer.Option(False, "--skip-reclassify", help="Trust artifact classification; skip re-classification with current return periods."),
    use_copy: bool = typer.Option(False, "--use-copy", help="Use PostgreSQL COPY fast-path for bulk loading (auto-detected when omitted)."),
) -> None:
    def _inner() -> None:
        service = _build_service()
        copy_flag = True if use_copy else None  # None = auto-detect
        count = service.ingest_forecast_summaries(
            provider=provider,
            run_id=run_id,
            replace_existing=replace_existing,
            skip_reclassify=skip_reclassify,
            use_copy=copy_flag,
        )
        typer.echo(f"upserted summary rows: {count}")

    _safe_run(_inner)


@cli.command("inspect-summary-artifact-schema")
def cli_inspect_summary_artifact_schema(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    preview_limit: int = typer.Option(3, "--preview-limit", min=0, max=10),
) -> None:
    def _inner() -> None:
        service = _build_service()
        resolved = service.resolve_requested_run_id(provider, run_id)
        path = service.artifacts.summary_artifact_path(provider, resolved.run_id)
        typer.echo(f"summary_artifact_path: {path}")
        typer.echo(f"summary_schema: {service.artifacts.summary_schema_string(provider, resolved.run_id)}")
        if preview_limit > 0:
            preview = service.artifacts.preview_summary_rows(provider, resolved.run_id, limit=preview_limit)
            typer.echo(json.dumps(preview, indent=2, default=str))

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




@cli.command("inspect-run-artifact")
def cli_inspect_run_artifact(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
    preview_limit: int = typer.Option(0, "--preview-limit", min=0, max=10),
) -> None:
    def _inner() -> None:
        service = _build_service()
        status = service.get_run_status(provider, run_id)
        typer.echo(f"run_id: {status.run_id}")
        typer.echo(f"artifact_exists: {status.artifact.exists}")
        typer.echo(f"artifact_path: {status.artifact.path or ''}")
        typer.echo(f"artifact_row_count: {status.artifact.row_count}")
        if preview_limit > 0 and status.artifact.exists:
            preview = service.artifacts.preview_rows(provider, status.run_id, limit=preview_limit)
            typer.echo("artifact_preview:")
            typer.echo(json.dumps(preview, indent=2))

    _safe_run(_inner)


@cli.command("run-status")
def cli_run_status(
    provider: str = typer.Option("geoglows", "--provider"),
    run_id: str = typer.Option("latest", "--run-id"),
) -> None:
    def _inner() -> None:
        service = _build_service()
        status = service.get_run_status(provider, run_id)
        typer.echo(f"provider: {status.provider}")
        typer.echo(f"run_id: {status.run_id}")
        typer.echo(f"current_status: {status.current_status}")
        typer.echo(f"completed_stages: {', '.join(status.completed_stages) or '(none)'}")
        typer.echo(f"missing_stages: {', '.join(status.missing_stages) or '(none)'}")
        typer.echo(f"artifact: exists={status.artifact.exists} rows={status.artifact.row_count} size_bytes={status.artifact.size_bytes}")
        typer.echo(f"timeseries_rows: {status.ingest.timeseries_row_count}")
        typer.echo(f"summary_rows: {status.summarize.summary_row_count}")
        typer.echo(f"map_rows: {status.map_row_count}")
        typer.echo(f"map_ready: {'yes' if status.map_ready else 'no'}")
        typer.echo(f"authoritative_latest_upstream_run_id: {status.authoritative_latest_upstream_run_id or ''}")
        typer.echo(f"upstream_run_exists: {status.upstream_run_exists}")
        typer.echo(f"acquisition_mode: {status.acquisition_mode or ''}")
        typer.echo(f"source_bucket: {status.source_bucket or ''}")
        typer.echo(f"source_zarr_path: {status.source_zarr_path or ''}")
        typer.echo(f"bounded_run: {status.bounded_run}")
        typer.echo(f"configured_limits: {status.configured_limits}")
        if status.failure_stage or status.failure_message:
            typer.echo(f"failure_stage: {status.failure_stage or ''}")
            typer.echo(f"failure_message: {status.failure_message or ''}")

    _safe_run(_inner)


@cli.command("cleanup-forecast-cache")
def cli_cleanup_forecast_cache() -> None:
    def _inner() -> None:
        service = _build_service()
        removed = service.cleanup_forecast_cache()
        typer.echo(f"removed cache entries: {removed}")

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


@cli.command("import-glofas-return-periods")
def cli_import_glofas_return_periods(
    netcdf_dir: str | None = typer.Option(None, "--netcdf-dir", help="Directory with official GloFAS v4 threshold NetCDF files (flood_threshold_glofas_v4_rl_*.nc)"),
    threshold_path: str | None = typer.Option(None, "--threshold-path", help="Pre-computed threshold file (parquet/CSV) with lat, lon, rp_2, rp_5, rp_20"),
    reanalysis_path: str | None = typer.Option(None, "--reanalysis-path", help="GloFAS reanalysis GRIB to extract thresholds from"),
    batch_size: int = typer.Option(5000, "--batch-size"),
) -> None:
    """Import GloFAS return period thresholds into the database.

    Provide exactly one of:
      --netcdf-dir     (preferred) directory with official GloFAS v4 NetCDF files
      --threshold-path pre-computed parquet/CSV file
      --reanalysis-path GloFAS reanalysis GRIB

    Thresholds are mapped to GeoGloWS reach IDs via the crosswalk table.
    """

    def _inner() -> None:
        service = _build_service()
        count = service.import_glofas_return_periods(
            netcdf_dir=netcdf_dir,
            threshold_path=threshold_path,
            reanalysis_path=reanalysis_path,
            batch_size=batch_size,
        )
        typer.echo(f"upserted GloFAS return periods: {count}")

    _safe_run(_inner)


@cli.command("build-crosswalk")
def cli_build_crosswalk(
    provider: str = typer.Option("glofas", "--provider"),
    metadata_path: str | None = typer.Option(None, "--metadata-path", help="Local path to GeoGloWS metadata parquet (~250 MB)"),
    threshold_dir: str | None = typer.Option(None, "--threshold-dir", help="Dir with GloFAS v4 threshold NetCDF files for river masking"),
    min_river_cms: float = typer.Option(1.0, "--min-river-cms", help="Min rp_2 (m³/s) for a grid cell to be a river cell"),
    max_snap_km: float = typer.Option(10.0, "--max-snap-km"),
    grid_resolution: float = typer.Option(0.05, "--grid-resolution"),
    batch_size: int = typer.Option(5000, "--batch-size"),
) -> None:
    """Build reach-to-grid crosswalk table for a grid-based provider (e.g. GloFAS)."""

    def _inner() -> None:
        from app.forecast.providers.glofas_crosswalk import build_glofas_crosswalk

        configure_logging(get_settings().log_level)
        db = SessionLocal()
        count = build_glofas_crosswalk(
            metadata_parquet_path=metadata_path,
            glofas_threshold_dir=threshold_dir,
            min_river_threshold_cms=min_river_cms,
            glofas_grid_resolution=grid_resolution,
            max_snap_distance_km=max_snap_km,
            batch_size=batch_size,
            db_session=db,
        )
        typer.echo(f"built crosswalk for provider={provider}: {count} rows")

    _safe_run(_inner)


if __name__ == "__main__":
    cli()
