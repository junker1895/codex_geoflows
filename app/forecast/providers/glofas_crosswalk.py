"""Build the reach-grid crosswalk table mapping GeoGloWS reach IDs to GloFAS grid cells.

Uses the GeoGloWS metadata parquet table (which contains reach centroids)
and snaps each centroid to the nearest GloFAS **river** grid cell using a KD-tree.

When a GloFAS threshold NetCDF directory is provided (recommended), the builder
filters the GloFAS grid to only include cells with meaningful discharge
(rp_2 >= min_river_threshold_cms).  This prevents mapping reaches to ocean,
desert, or trivially-small headwater cells that produce near-zero return
periods and cause false severity-6 classifications.

The GeoGloWS metadata upstream area (DSContArea column) is stored in the
crosswalk for downstream quality filtering.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# GeoGloWS metadata table on public S3
GEOGLOWS_METADATA_URL = (
    "https://geoglows-v2.s3.us-west-2.amazonaws.com/tables/package-metadata-table.parquet"
)

# Default minimum rp_2 threshold (m³/s) for a grid cell to be considered a river cell.
# Cells below this have negligible discharge and produce garbage return periods.
_DEFAULT_MIN_RIVER_THRESHOLD_CMS = 1.0


def build_glofas_crosswalk(
    *,
    metadata_parquet_path: str | None = None,
    glofas_threshold_dir: str | None = None,
    min_river_threshold_cms: float = _DEFAULT_MIN_RIVER_THRESHOLD_CMS,
    glofas_grid_resolution: float = 0.05,
    max_snap_distance_km: float = 10.0,
    batch_size: int = 5000,
    db_session=None,
) -> int:
    """Build the crosswalk table mapping GeoGloWS reaches to GloFAS grid cells.

    Parameters
    ----------
    metadata_parquet_path : str | None
        Path to a local copy of the GeoGloWS metadata parquet.
        If None, downloads from S3 (~250 MB).
    glofas_threshold_dir : str | None
        Path to directory containing GloFAS v4 threshold NetCDF files
        (e.g. ``flood_threshold_glofas_v4_rl_2.0.nc``).  When provided,
        the grid is filtered to only include cells with rp_2 above
        *min_river_threshold_cms*, preventing matches to non-river cells.
    min_river_threshold_cms : float
        Minimum rp_2 value (m³/s) for a GloFAS grid cell to be considered
        a valid river cell.  Only used when *glofas_threshold_dir* is given.
    glofas_grid_resolution : float
        GloFAS grid resolution in degrees (0.05 for v4, 0.1 for v3).
    max_snap_distance_km : float
        Maximum snap distance in km. Reaches farther from any grid cell are skipped.
    batch_size : int
        Number of rows to insert per batch.
    db_session : Session | None
        SQLAlchemy session. If None, creates one.

    Returns
    -------
    int
        Number of crosswalk rows inserted.
    """
    import numpy as np
    import pandas as pd

    # ── Load GeoGloWS metadata ──────────────────────────────────────────
    if metadata_parquet_path and Path(metadata_parquet_path).exists():
        logger.info("Loading GeoGloWS metadata from local file: %s", metadata_parquet_path)
        df = pd.read_parquet(metadata_parquet_path)
    else:
        logger.info("Downloading GeoGloWS metadata from S3 (~250 MB)...")
        df = pd.read_parquet(GEOGLOWS_METADATA_URL)

    logger.info("Metadata loaded: %d rows, columns: %s", len(df), list(df.columns))

    # Find required columns
    lat_col = _find_column(df, ["lat", "latitude", "Lat", "LATITUDE", "centroid_lat"])
    lon_col = _find_column(df, ["lon", "longitude", "Lon", "LONGITUDE", "centroid_lon", "lng"])
    id_col = _find_column(df, ["LINKNO", "rivid", "river_id", "reach_id", "RiverID"])

    if not lat_col or not lon_col or not id_col:
        raise ValueError(
            f"Could not find required columns in metadata. "
            f"Available: {list(df.columns)}. "
            f"Found lat={lat_col}, lon={lon_col}, id={id_col}"
        )

    # Find optional upstream area column
    area_col = _find_column(df, [
        "DSContArea", "dscontarea", "TotDASqKm", "totdasqkm",
        "upstream_area_km2", "contrib_area_km2", "drainage_area",
    ])
    if area_col:
        logger.info("Found upstream area column: %s", area_col)
    else:
        logger.info("No upstream area column found in metadata — upstream_area_km2 will be NULL")

    logger.info("Using columns: id=%s, lat=%s, lon=%s, area=%s", id_col, lat_col, lon_col, area_col)

    # Drop rows with missing coordinates
    mask = df[lat_col].notna() & df[lon_col].notna() & df[id_col].notna()
    df = df[mask].copy()
    logger.info("Rows with valid coordinates: %d", len(df))

    reach_ids = df[id_col].astype(str).values
    reach_lats = df[lat_col].astype(float).values
    reach_lons = df[lon_col].astype(float).values
    reach_areas = (
        df[area_col].astype(float).values if area_col else np.full(len(df), np.nan)
    )

    # ── Build GloFAS grid, optionally filtered to river cells ───────────
    grid_points = _build_glofas_grid(
        glofas_grid_resolution,
        glofas_threshold_dir,
        min_river_threshold_cms,
    )

    logger.info("GloFAS grid: %d cells (%.4f° resolution)", len(grid_points), glofas_grid_resolution)

    # ── KD-tree spatial matching ────────────────────────────────────────
    from scipy.spatial import cKDTree

    grid_rad = np.deg2rad(grid_points)
    tree = cKDTree(grid_rad)

    reach_coords_rad = np.deg2rad(np.column_stack([reach_lats, reach_lons]))

    distances_rad, indices = tree.query(reach_coords_rad)
    distances_km = distances_rad * 6371.0
    nearest_grid = grid_points[indices]

    # ── Insert into database ────────────────────────────────────────────
    if db_session is None:
        from app.core.database import SessionLocal
        db_session = SessionLocal()
        own_session = True
    else:
        own_session = False

    count = 0
    skipped_distance = 0
    batch = []

    for i in range(len(reach_ids)):
        dist = distances_km[i]
        if dist > max_snap_distance_km:
            skipped_distance += 1
            continue

        area_val = float(reach_areas[i]) if not np.isnan(reach_areas[i]) else None
        batch.append(
            {
                "reach_id": reach_ids[i],
                "target_provider": "glofas",
                "grid_lat": float(nearest_grid[i, 0]),
                "grid_lon": float(nearest_grid[i, 1]),
                "distance_km": float(dist),
                "upstream_area_km2": area_val,
            }
        )

        if len(batch) >= batch_size:
            _upsert_batch(db_session, batch)
            count += len(batch)
            if count % 100_000 < batch_size:
                logger.info("Inserted %d crosswalk rows so far...", count)
            batch = []

    if batch:
        _upsert_batch(db_session, batch)
        count += len(batch)

    db_session.commit()
    if own_session:
        db_session.close()

    logger.info(
        "Crosswalk build complete: %d rows inserted, %d skipped (distance > %.1f km)",
        count, skipped_distance, max_snap_distance_km,
    )
    return count


def _build_glofas_grid(
    resolution: float,
    threshold_dir: str | None,
    min_river_cms: float,
) -> "np.ndarray":
    """Build GloFAS grid points, optionally filtered to river cells.

    If *threshold_dir* is provided, loads the GloFAS v4 rp_2 threshold
    NetCDF and only includes grid cells where rp_2 >= *min_river_cms*.
    Otherwise, generates the full regular grid.
    """
    import numpy as np

    if threshold_dir:
        return _build_river_grid_from_thresholds(threshold_dir, min_river_cms)

    # Fallback: full regular grid (original behaviour)
    grid_lats = np.arange(-60, 90, resolution)
    grid_lons = np.arange(-180, 180, resolution)
    grid_lat_mesh, grid_lon_mesh = np.meshgrid(grid_lats, grid_lons, indexing="ij")
    return np.column_stack([grid_lat_mesh.ravel(), grid_lon_mesh.ravel()])


def _build_river_grid_from_thresholds(
    threshold_dir: str,
    min_river_cms: float,
) -> "np.ndarray":
    """Load GloFAS v4 rp_2 threshold NetCDF and return only cells with
    discharge >= *min_river_cms* as (lat, lon) array.
    """
    import numpy as np
    import xarray as xr

    nc_dir = Path(threshold_dir)
    pattern = "flood_threshold_glofas_v4_rl_2.0.nc"
    matches = list(nc_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"GloFAS 2-year threshold file not found: {nc_dir / pattern}. "
            f"Download from https://confluence.ecmwf.int/display/CEMS/GloFAS+Flood+Thresholds"
        )

    nc_path = matches[0]
    logger.info("Loading river mask from %s", nc_path)
    ds = xr.open_dataset(str(nc_path))

    # Find the data variable
    data_vars = [v for v in ds.data_vars if v not in {"lat", "lon", "latitude", "longitude"}]
    if not data_vars:
        raise ValueError(f"No data variable found in {nc_path}")
    da = ds[data_vars[0]].squeeze(drop=True)

    lat_coord = "latitude" if "latitude" in da.dims else "lat"
    lon_coord = "longitude" if "longitude" in da.dims else "lon"

    lats = da[lat_coord].values
    lons = da[lon_coord].values

    # Build 2D meshgrid of all lat/lon
    lon_mesh, lat_mesh = np.meshgrid(lons, lats)

    # River mask: cells with rp_2 >= min_river_cms
    values = da.values
    river_mask = np.isfinite(values) & (values >= min_river_cms)

    river_lats = lat_mesh[river_mask]
    river_lons = lon_mesh[river_mask]
    ds.close()

    total_cells = values.size
    river_cells = int(river_mask.sum())
    logger.info(
        "River mask: %d / %d cells have rp_2 >= %.2f m³/s (%.1f%% filtered out)",
        river_cells, total_cells, min_river_cms,
        100.0 * (1.0 - river_cells / total_cells),
    )

    return np.column_stack([river_lats, river_lons])


def _upsert_batch(db_session, rows: list[dict]) -> None:
    """Upsert a batch of crosswalk rows using ON CONFLICT."""
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
        },
    )
    db_session.execute(stmt)


def _find_column(df, candidates: list[str]) -> str | None:
    """Find the first matching column name."""
    for col in candidates:
        if col in df.columns:
            return col
    # Case-insensitive fallback
    lower_map = {c.lower(): c for c in df.columns}
    for col in candidates:
        if col.lower() in lower_map:
            return lower_map[col.lower()]
    return None
