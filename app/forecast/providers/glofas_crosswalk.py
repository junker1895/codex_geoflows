"""Build a hydrologically constrained GeoGLOWS↔GloFAS crosswalk.

This builder joins GeoGLOWS reach coordinates from the lightweight metadata table
with upstream drainage area (``DSContArea``) from the full attributes table,
then matches each reach to plausible GloFAS river grid cells using:

1. River-cell mask from GloFAS threshold NetCDFs (rp_2 >= configurable minimum)
2. Valid GloFAS upstream area from ``uparea_glofas_v4_0.nc``
3. K-nearest-neighbor candidate search in geographic space
4. Deterministic scoring based on distance + drainage-area mismatch

Weak candidates are rejected instead of forcing nearest-cell snaps.
"""

from __future__ import annotations

import logging
import math
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

GEOGLOWS_METADATA_URL = (
    "https://geoglows-v2.s3.us-west-2.amazonaws.com/tables/package-metadata-table.parquet"
)

_DEFAULT_NEAREST_CANDIDATES = 16
_DEFAULT_MAX_AREA_RATIO = 10.0
_DEFAULT_MIN_RIVER_THRESHOLD_CMS = 1.0
_DEFAULT_AREA_WEIGHT = 20.0


def build_glofas_crosswalk(
    *,
    metadata_parquet_path: str | None = None,
    attributes_parquet_path: str | None = None,
    uparea_netcdf_path: str | None = None,
    glofas_threshold_dir: str | None = None,
    nearest_candidates_k: int = _DEFAULT_NEAREST_CANDIDATES,
    max_area_ratio: float = _DEFAULT_MAX_AREA_RATIO,
    min_river_threshold_cms: float = _DEFAULT_MIN_RIVER_THRESHOLD_CMS,
    area_weight: float = _DEFAULT_AREA_WEIGHT,
    batch_size: int = 5000,
    db_session=None,
) -> int:
    """Build and persist GloFAS crosswalk rows.

    Reaches lacking valid coordinates or ``DSContArea`` are excluded before matching.
    Candidate cells must satisfy river mask and valid positive upstream area.
    """
    import pandas as pd
    from scipy.spatial import cKDTree

    if nearest_candidates_k < 1:
        raise ValueError("nearest_candidates_k must be >= 1")
    if max_area_ratio <= 1.0:
        raise ValueError("max_area_ratio must be > 1.0")

    reaches = _load_geoglows_reaches(metadata_parquet_path, attributes_parquet_path)
    logger.info("Crosswalk reaches ready for matching: %d", len(reaches))

    candidates = _load_glofas_candidates(
        uparea_netcdf_path=uparea_netcdf_path,
        threshold_dir=glofas_threshold_dir,
        min_river_threshold_cms=min_river_threshold_cms,
    )

    tree = cKDTree(np.deg2rad(candidates[:, :2]))

    reach_ids = reaches["reach_id"].values
    reach_lats = reaches["lat"].to_numpy(dtype=np.float64)
    reach_lons = reaches["lon"].to_numpy(dtype=np.float64)
    reach_areas = reaches["reach_upstream_area_km2"].to_numpy(dtype=np.float64)

    query_coords = np.deg2rad(np.column_stack([reach_lats, reach_lons]))
    k = min(nearest_candidates_k, len(candidates))
    distances_rad, indices = tree.query(query_coords, k=k)
    if k == 1:
        distances_rad = distances_rad[:, np.newaxis]
        indices = indices[:, np.newaxis]

    if db_session is None:
        from app.core.database import SessionLocal

        db_session = SessionLocal()
        own_session = True
    else:
        own_session = False

    rows: list[dict] = []
    rejected_examples: list[dict] = []
    matched_count = 0
    rejected_count = 0

    for i, reach_id in enumerate(reach_ids):
        result = _select_best_candidate(
            reach_lat=reach_lats[i],
            reach_lon=reach_lons[i],
            reach_area_km2=reach_areas[i],
            candidate_distances_rad=distances_rad[i],
            candidate_indices=indices[i],
            candidates=candidates,
            max_area_ratio=max_area_ratio,
            area_weight=area_weight,
        )
        if result is None:
            rejected_count += 1
            if len(rejected_examples) < 10:
                rejected_examples.append({
                    "reach_id": str(reach_id),
                    "lat": float(reach_lats[i]),
                    "lon": float(reach_lons[i]),
                    "reach_upstream_area_km2": float(reach_areas[i]),
                })
            rows.append(
                {
                    "reach_id": str(reach_id),
                    "target_provider": "glofas",
                    "grid_lat": None,
                    "grid_lon": None,
                    "upstream_area_km2": float(reach_areas[i]),
                    "reach_upstream_area_km2": float(reach_areas[i]),
                    "grid_upstream_area_km2": None,
                    "area_ratio": None,
                    "distance_km": None,
                    "match_score": None,
                    "match_method": "distance_plus_area_v1",
                    "is_valid_match": False,
                }
            )
        else:
            matched_count += 1
            rows.append(
                {
                    "reach_id": str(reach_id),
                    "target_provider": "glofas",
                    "grid_lat": float(result["grid_lat"]),
                    "grid_lon": float(result["grid_lon"]),
                    "upstream_area_km2": float(reach_areas[i]),
                    "reach_upstream_area_km2": float(reach_areas[i]),
                    "grid_upstream_area_km2": float(result["grid_area_km2"]),
                    "area_ratio": float(result["area_ratio"]),
                    "distance_km": float(result["distance_km"]),
                    "match_score": float(result["match_score"]),
                    "match_method": "distance_plus_area_v1",
                    "is_valid_match": True,
                }
            )

        if len(rows) >= batch_size:
            _upsert_batch(db_session, rows)
            rows = []

    if rows:
        _upsert_batch(db_session, rows)

    db_session.commit()
    if own_session:
        db_session.close()

    logger.info("Crosswalk match summary: matched=%d rejected=%d total=%d", matched_count, rejected_count, len(reaches))
    _log_validation_report(db_session if not own_session else None, matched_count, rejected_count)
    if rejected_examples:
        logger.info("Example rejected reaches (first %d): %s", len(rejected_examples), rejected_examples)

    return matched_count + rejected_count


def _load_geoglows_reaches(metadata_parquet_path: str | None, attributes_parquet_path: str | None):
    import pandas as pd

    if metadata_parquet_path and Path(metadata_parquet_path).exists():
        meta = pd.read_parquet(metadata_parquet_path)
        logger.info("Loaded GEOGLOWS metadata rows: %d (local=%s)", len(meta), metadata_parquet_path)
    else:
        meta = pd.read_parquet(GEOGLOWS_METADATA_URL)
        logger.info("Loaded GEOGLOWS metadata rows: %d (source=s3)", len(meta))

    if not attributes_parquet_path:
        raise ValueError("attributes_parquet_path is required for DSContArea-based matching")
    attrs_path = Path(attributes_parquet_path)
    if not attrs_path.exists():
        raise FileNotFoundError(f"GEOGLOWS attributes parquet not found: {attributes_parquet_path}")

    attrs = pd.read_parquet(attrs_path)
    logger.info("Loaded GEOGLOWS full attributes rows: %d (%s)", len(attrs), attributes_parquet_path)

    meta_id = _find_column(meta, ["LINKNO", "rivid", "reach_id"])
    attrs_id = _find_column(attrs, ["LINKNO", "rivid", "reach_id"])
    lat_col = _find_column(meta, ["lat", "latitude", "Lat", "LATITUDE"])
    lon_col = _find_column(meta, ["lon", "longitude", "Lon", "LONGITUDE", "lng"])
    area_col = _find_column(attrs, ["DSContArea", "dscontarea"])

    if not all([meta_id, attrs_id, lat_col, lon_col, area_col]):
        raise ValueError(
            f"Required columns missing. metadata(id/lat/lon)={meta_id}/{lat_col}/{lon_col}, attrs(id/area)={attrs_id}/{area_col}"
        )

    meta_df = meta[[meta_id, lat_col, lon_col]].copy()
    attrs_df = attrs[[attrs_id, area_col]].copy()
    meta_df[meta_id] = meta_df[meta_id].astype(str)
    attrs_df[attrs_id] = attrs_df[attrs_id].astype(str)

    joined = meta_df.merge(attrs_df, left_on=meta_id, right_on=attrs_id, how="inner")
    logger.info("Rows after metadata+attributes join on LINKNO: %d", len(joined))

    joined = joined.rename(
        columns={
            meta_id: "reach_id",
            lat_col: "lat",
            lon_col: "lon",
            area_col: "reach_upstream_area_km2",
        }
    )

    valid_coords = joined["lat"].notna() & joined["lon"].notna()
    joined = joined.loc[valid_coords].copy()
    logger.info("Rows with valid coordinates: %d", len(joined))

    joined["reach_upstream_area_km2"] = pd.to_numeric(joined["reach_upstream_area_km2"], errors="coerce")
    valid_area = np.isfinite(joined["reach_upstream_area_km2"]) & (joined["reach_upstream_area_km2"] > 0)
    joined = joined.loc[valid_area].copy()
    logger.info("Rows with valid DSContArea (>0): %d", len(joined))

    joined["lat"] = pd.to_numeric(joined["lat"], errors="coerce")
    joined["lon"] = pd.to_numeric(joined["lon"], errors="coerce")
    joined = joined[np.isfinite(joined["lat"]) & np.isfinite(joined["lon"])].copy()

    return joined[["reach_id", "lat", "lon", "reach_upstream_area_km2"]]


def _load_glofas_candidates(
    *,
    uparea_netcdf_path: str | None,
    threshold_dir: str | None,
    min_river_threshold_cms: float,
) -> np.ndarray:
    import xarray as xr

    if not uparea_netcdf_path:
        raise ValueError("uparea_netcdf_path is required")
    path = Path(uparea_netcdf_path)
    if not path.exists():
        raise FileNotFoundError(f"GloFAS uparea NetCDF not found: {uparea_netcdf_path}")

    ds = xr.open_dataset(str(path))
    if "uparea" not in ds.data_vars:
        raise ValueError(f"Variable 'uparea' not found in {uparea_netcdf_path}")
    uparea_da = ds["uparea"].squeeze(drop=True)

    lat_coord = "latitude" if "latitude" in uparea_da.dims else "lat"
    lon_coord = "longitude" if "longitude" in uparea_da.dims else "lon"
    lats = uparea_da[lat_coord].values
    lons = uparea_da[lon_coord].values
    lon_mesh, lat_mesh = np.meshgrid(lons, lats)

    uparea_m2 = uparea_da.values
    uparea_km2 = uparea_m2 / 1_000_000.0

    river_mask = _load_river_mask_from_thresholds(
        threshold_dir=threshold_dir,
        min_river_cms=min_river_threshold_cms,
        shape=uparea_km2.shape,
    )

    valid_uparea = np.isfinite(uparea_km2) & (uparea_km2 > 0)
    candidate_mask = river_mask & valid_uparea

    ds.close()

    candidate_count = int(candidate_mask.sum())
    logger.info("Valid GloFAS candidate cells (river mask + uparea>0): %d", candidate_count)
    if candidate_count == 0:
        raise ValueError("No valid GloFAS candidate cells found after masking")

    return np.column_stack([lat_mesh[candidate_mask], lon_mesh[candidate_mask], uparea_km2[candidate_mask]])


def _load_river_mask_from_thresholds(*, threshold_dir: str | None, min_river_cms: float, shape: tuple[int, ...]) -> np.ndarray:
    import xarray as xr

    if not threshold_dir:
        logger.info("No threshold_dir provided; river mask defaults to all True")
        return np.ones(shape, dtype=bool)

    nc_dir = Path(threshold_dir)
    pattern = "flood_threshold_glofas_v4_rl_2.0.nc"
    matches = list(nc_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"GloFAS threshold file not found for river mask: {nc_dir / pattern}")

    ds = xr.open_dataset(str(matches[0]))
    data_vars = [v for v in ds.data_vars if v not in {"lat", "lon", "latitude", "longitude"}]
    if not data_vars:
        raise ValueError(f"No threshold variable found in {matches[0]}")
    arr = ds[data_vars[0]].squeeze(drop=True).values
    ds.close()

    if arr.shape != shape:
        raise ValueError(f"River mask shape {arr.shape} does not match uparea shape {shape}")

    river_mask = np.isfinite(arr) & (arr >= min_river_cms)
    logger.info(
        "River mask cells passing rp_2 >= %.2f: %d / %d",
        min_river_cms,
        int(river_mask.sum()),
        int(river_mask.size),
    )
    return river_mask


def _select_best_candidate(
    *,
    reach_lat: float,
    reach_lon: float,
    reach_area_km2: float,
    candidate_distances_rad: np.ndarray,
    candidate_indices: np.ndarray,
    candidates: np.ndarray,
    max_area_ratio: float,
    area_weight: float,
) -> dict | None:
    best: dict | None = None

    for dist_rad, idx in zip(candidate_distances_rad, candidate_indices, strict=False):
        grid_lat, grid_lon, grid_area_km2 = candidates[int(idx)]
        if not np.isfinite(grid_area_km2) or grid_area_km2 <= 0 or reach_area_km2 <= 0:
            continue

        area_ratio = max(reach_area_km2, grid_area_km2) / min(reach_area_km2, grid_area_km2)
        if not np.isfinite(area_ratio) or area_ratio > max_area_ratio:
            continue

        distance_km = float(dist_rad) * 6371.0
        area_penalty = abs(math.log(area_ratio)) * area_weight
        match_score = distance_km + area_penalty

        candidate = {
            "grid_lat": grid_lat,
            "grid_lon": grid_lon,
            "grid_area_km2": grid_area_km2,
            "area_ratio": area_ratio,
            "distance_km": distance_km,
            "match_score": match_score,
        }
        if best is None or candidate["match_score"] < best["match_score"]:
            best = candidate

    return best


def _log_validation_report(db_session, matched_count: int, rejected_count: int) -> None:
    from sqlalchemy import select

    from app.core.database import SessionLocal
    from app.db.models import ReachGridCrosswalk

    own = False
    if db_session is None:
        db_session = SessionLocal()
        own = True

    try:
        rows = db_session.execute(
            select(ReachGridCrosswalk.distance_km, ReachGridCrosswalk.area_ratio, ReachGridCrosswalk.match_score)
            .where(ReachGridCrosswalk.target_provider == "glofas")
            .where(ReachGridCrosswalk.is_valid_match.is_(True))
        ).all()
        if not rows:
            logger.info("No valid crosswalk rows available for validation report")
            return

        dists = np.array([r.distance_km for r in rows if r.distance_km is not None], dtype=np.float64)
        ratios = np.array([r.area_ratio for r in rows if r.area_ratio is not None], dtype=np.float64)
        scores = np.array([r.match_score for r in rows if r.match_score is not None], dtype=np.float64)

        def _pct(arr: np.ndarray, q: int) -> float:
            return float(np.percentile(arr, q)) if arr.size else float("nan")

        total = matched_count + rejected_count
        logger.info(
            "Crosswalk validation report: matched_pct=%.2f rejected_pct=%.2f "
            "distance_km[p50=%.3f,p90=%.3f,p99=%.3f] "
            "area_ratio[p50=%.3f,p90=%.3f,p99=%.3f] "
            "match_score[p50=%.3f,p90=%.3f,p99=%.3f] "
            "area_ratio>2=%d area_ratio>5=%d distance_km>5=%d distance_km>10=%d",
            (matched_count / total * 100.0) if total else 0.0,
            (rejected_count / total * 100.0) if total else 0.0,
            _pct(dists, 50),
            _pct(dists, 90),
            _pct(dists, 99),
            _pct(ratios, 50),
            _pct(ratios, 90),
            _pct(ratios, 99),
            _pct(scores, 50),
            _pct(scores, 90),
            _pct(scores, 99),
            int((ratios > 2).sum()) if ratios.size else 0,
            int((ratios > 5).sum()) if ratios.size else 0,
            int((dists > 5).sum()) if dists.size else 0,
            int((dists > 10).sum()) if dists.size else 0,
        )
    finally:
        if own:
            db_session.close()


def _upsert_batch(db_session, rows: list[dict]) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from app.db.models import ReachGridCrosswalk

    stmt = pg_insert(ReachGridCrosswalk).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_crosswalk_reach_provider",
        set_={
            "grid_lat": stmt.excluded.grid_lat,
            "grid_lon": stmt.excluded.grid_lon,
            "distance_km": stmt.excluded.distance_km,
            "upstream_area_km2": stmt.excluded.upstream_area_km2,
            "reach_upstream_area_km2": stmt.excluded.reach_upstream_area_km2,
            "grid_upstream_area_km2": stmt.excluded.grid_upstream_area_km2,
            "area_ratio": stmt.excluded.area_ratio,
            "match_score": stmt.excluded.match_score,
            "match_method": stmt.excluded.match_method,
            "is_valid_match": stmt.excluded.is_valid_match,
        },
    )
    db_session.execute(stmt)


def _find_column(df, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    lower_map = {c.lower(): c for c in df.columns}
    for col in candidates:
        if col.lower() in lower_map:
            return lower_map[col.lower()]
    return None
