from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_forecast_service
from app.db.session import get_db_session
from app.forecast.exceptions import (
    ForecastValidationError,
    ProviderBackendUnavailableError,
    ProviderOperationalError,
)
from app.forecast.schemas import (
    ForecastMapReachesResponse,
    ForecastRunSchema,
    ProviderHealthResponse,
    ReachDetailResponse,
    ReachSummarySchema,
)

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/providers", response_model=list[str])
def providers(db: Session = Depends(get_db_session)) -> list[str]:
    return get_forecast_service(db).list_providers()


@router.get("/runs/latest", response_model=ForecastRunSchema)
def latest_run(provider: str = Query(...), db: Session = Depends(get_db_session)) -> ForecastRunSchema:
    service = get_forecast_service(db)
    try:
        run = service.get_latest_run(provider)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not run:
        raise HTTPException(status_code=404, detail=f"No run found for provider '{provider}'")
    return run


@router.get("/reaches/{provider}/{provider_reach_id}", response_model=ReachDetailResponse)
def reach_detail(
    provider: str,
    provider_reach_id: str,
    run_id: str | None = Query(default=None),
    timeseries_limit: int | None = Query(default=500, ge=1, le=5000),
    db: Session = Depends(get_db_session),
) -> ReachDetailResponse:
    service = get_forecast_service(db)
    try:
        return service.get_reach_detail(provider, provider_reach_id, run_id=run_id, timeseries_limit=timeseries_limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc



@router.get("/map/reaches", response_model=ForecastMapReachesResponse)
def map_reaches(
    provider: str = Query(...),
    run_id: str | None = Query(default=None),
    bbox: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1),
    flagged_only: bool = Query(default=False),
    min_severity_score: float | None = Query(default=None),
    db: Session = Depends(get_db_session),
) -> ForecastMapReachesResponse:
    service = get_forecast_service(db)
    try:
        return service.list_forecast_map_reaches(
            provider=provider,
            run_id=run_id,
            bbox=bbox,
            limit=limit,
            flagged_only=flagged_only,
            min_severity_score=min_severity_score,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/summary", response_model=list[ReachSummarySchema])
def summary(
    provider: str,
    run_id: str | None = Query(default=None),
    severity_min: int | None = Query(default=None),
    limit: int | None = Query(default=None),
    db: Session = Depends(get_db_session),
) -> list[ReachSummarySchema]:
    service = get_forecast_service(db)
    try:
        return service.get_reach_summaries(provider, run_id=run_id, severity_min=severity_min, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/health", response_model=ProviderHealthResponse)
def forecast_health(provider: str, db: Session = Depends(get_db_session)) -> ProviderHealthResponse:
    service = get_forecast_service(db)
    try:
        return service.get_provider_health(provider)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/smoke/geoglows")
def geoglows_smoke(
    river_id: str = Query("123456789"),
    run_id: str = Query("latest"),
    db: Session = Depends(get_db_session),
) -> dict:
    service = get_forecast_service(db)
    out = {
        "provider": "geoglows",
        "river_id": river_id,
        "forecast_stats_rest": {"ok": False, "error": None},
        "return_periods": {"ok": False, "error": None},
    }
    try:
        service.ingest_forecast_run("geoglows", run_id, [river_id])
        out["forecast_stats_rest"]["ok"] = True
    except (ForecastValidationError, ProviderBackendUnavailableError, ProviderOperationalError, ValueError) as exc:
        out["forecast_stats_rest"]["error"] = str(exc)

    try:
        service.ingest_return_periods("geoglows", [river_id])
        out["return_periods"]["ok"] = True
    except (ForecastValidationError, ProviderBackendUnavailableError, ProviderOperationalError, ValueError) as exc:
        out["return_periods"]["error"] = str(exc)
    return out
