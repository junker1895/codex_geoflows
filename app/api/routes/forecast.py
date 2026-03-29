import logging
from time import perf_counter

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
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
    RunReadinessStatusResponse,
)

router = APIRouter(prefix="/forecast", tags=["forecast"])
logger = logging.getLogger(__name__)


class SeverityFilterRequest(BaseModel):
    provider: str
    run_id: str | None = None
    min_severity_score: int = Field(default=1, ge=1, le=6)
    limit: int | None = Field(default=None, ge=1)
    bbox: str | None = None
    reach_ids: list[str] = Field(default_factory=list)


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
    started = perf_counter()
    service = get_forecast_service(db)
    try:
        response = service.get_reach_detail(provider, provider_reach_id, run_id=run_id, timeseries_limit=timeseries_limit)
        logger.info("forecast reach_detail route completed", extra={"provider": provider, "run_id": run_id or "latest", "elapsed_seconds": round(perf_counter()-started,6)})
        return response
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
    started = perf_counter()
    service = get_forecast_service(db)
    resolved_run_id = None
    try:
        resolved = service.resolve_requested_run_id_local(provider, run_id or "latest", require_existing=False)
        resolved_run_id = None if resolved is None else resolved.run_id
        response = service.list_forecast_map_reaches(
            provider=provider,
            run_id=resolved_run_id,
            bbox=bbox,
            limit=limit,
            flagged_only=flagged_only,
            min_severity_score=min_severity_score,
        )
        elapsed_seconds = round(perf_counter() - started, 6)
        logger.info(
            "forecast map_reaches route completed provider=%s requested_run_id=%s resolved_run_id=%s bbox=%s limit=%s flagged_only=%s min_severity_score=%s count=%s elapsed_seconds=%s",
            provider,
            run_id or "latest",
            resolved_run_id,
            bbox,
            limit,
            flagged_only,
            min_severity_score,
            response.meta.count,
            elapsed_seconds,
            extra={
                "provider": provider,
                "requested_run_id": run_id or "latest",
                "resolved_run_id": resolved_run_id,
                "bbox": bbox,
                "limit": limit,
                "flagged_only": flagged_only,
                "min_severity_score": min_severity_score,
                "count": response.meta.count,
                "elapsed_seconds": elapsed_seconds,
            },
        )
        return response
    except ValueError as exc:
        logger.exception("forecast map_reaches route failed", extra={"provider": provider, "requested_run_id": run_id or "latest", "resolved_run_id": resolved_run_id})
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("forecast map_reaches route failed", extra={"provider": provider, "requested_run_id": run_id or "latest", "resolved_run_id": resolved_run_id})
        raise HTTPException(status_code=500, detail="internal server error") from exc


@router.get("/map/severity")
def map_severity(
    provider: str = Query(...),
    run_id: str | None = Query(default=None),
    min_severity_score: int = Query(default=1, ge=1, le=6),
    limit: int | None = Query(default=None, ge=1),
    bbox: str | None = Query(default=None),
    db: Session = Depends(get_db_session),
) -> Response:
    """Ultra-compact severity payload for map colouring: ``{run_id, severity: {reach_id: score}}``."""
    started = perf_counter()
    service = get_forecast_service(db)
    resolved_run_id, severity = service.get_severity_map(
        provider,
        run_id,
        min_severity_score,
        limit=limit,
        bbox=bbox,
    )
    elapsed_seconds = round(perf_counter() - started, 6)
    count = len(severity)
    logger.info(
        "forecast map_severity route completed provider=%s run_id=%s count=%s elapsed_seconds=%s",
        provider,
        resolved_run_id,
        count,
        elapsed_seconds,
        extra={
            "provider": provider,
            "run_id": resolved_run_id,
            "count": count,
            "elapsed_seconds": elapsed_seconds,
        },
    )
    payload = orjson.dumps({"run_id": resolved_run_id, "severity": severity})
    payload_bytes = len(payload)
    logger.info(
        "forecast map_severity payload prepared provider=%s run_id=%s count=%s payload_bytes=%s",
        provider,
        resolved_run_id,
        count,
        payload_bytes,
        extra={
            "provider": provider,
            "run_id": resolved_run_id,
            "count": count,
            "payload_bytes": payload_bytes,
        },
    )
    return Response(content=payload, media_type="application/json")


@router.post("/map/severity/filter")
def map_severity_filter(
    request: SeverityFilterRequest,
    db: Session = Depends(get_db_session),
) -> Response:
    """Severity payload filtered to a provided set of reach IDs."""
    started = perf_counter()
    service = get_forecast_service(db)
    resolved_run_id, severity = service.get_severity_map(
        request.provider,
        request.run_id,
        request.min_severity_score,
        limit=request.limit,
        reach_ids=request.reach_ids or None,
        bbox=request.bbox,
    )
    elapsed_seconds = round(perf_counter() - started, 6)
    count = len(severity)
    payload = orjson.dumps({"run_id": resolved_run_id, "severity": severity})
    payload_bytes = len(payload)
    logger.info(
        "forecast map_severity_filter route completed provider=%s run_id=%s bbox=%s requested_reach_ids=%s count=%s elapsed_seconds=%s payload_bytes=%s",
        request.provider,
        resolved_run_id,
        request.bbox,
        len(request.reach_ids),
        count,
        elapsed_seconds,
        payload_bytes,
    )
    return Response(content=payload, media_type="application/json")


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
def forecast_health(
    provider: str,
    refresh_upstream: bool = Query(default=False),
    db: Session = Depends(get_db_session),
) -> ProviderHealthResponse:
    started = perf_counter()
    service = get_forecast_service(db)
    try:
        response = service.get_provider_health(provider, refresh_upstream=refresh_upstream)
        logger.info("forecast health route completed", extra={"provider": provider, "refresh_upstream": refresh_upstream, "elapsed_seconds": round(perf_counter()-started,6)})
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc




@router.get("/runs/{provider}/{run_id}/status", response_model=RunReadinessStatusResponse)
def run_status(
    provider: str,
    run_id: str,
    refresh_upstream: bool = Query(default=False),
    db: Session = Depends(get_db_session),
) -> RunReadinessStatusResponse:
    started = perf_counter()
    service = get_forecast_service(db)
    resolved_run_id = None
    try:
        resolved = service.resolve_requested_run_id_local(provider, run_id, require_existing=False)
        resolved_run_id = None if resolved is None else resolved.run_id
        if resolved_run_id is None:
            raise ValueError(f"Run '{run_id}' not found for provider '{provider}'")
        response = service.get_run_status(provider, resolved_run_id, refresh_upstream=refresh_upstream)
        logger.info("forecast run_status route completed", extra={"provider": provider, "requested_run_id": run_id, "resolved_run_id": resolved_run_id, "refresh_upstream": refresh_upstream, "elapsed_seconds": round(perf_counter()-started,6)})
        return response
    except ValueError as exc:
        logger.exception("forecast run_status route failed", extra={"provider": provider, "requested_run_id": run_id, "resolved_run_id": resolved_run_id})
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("forecast run_status route failed", extra={"provider": provider, "requested_run_id": run_id, "resolved_run_id": resolved_run_id})
        raise HTTPException(status_code=500, detail="internal server error") from exc

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
