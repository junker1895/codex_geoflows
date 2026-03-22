"""Build the reach-grid crosswalk table mapping GeoGloWS reach IDs to GloFAS grid cells.

Uses the GeoGloWS metadata parquet table (which contains reach centroids)
and snaps each centroid to the nearest GloFAS river grid cell using a KD-tree.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# GeoGloWS metadata table on public S3
GEOGLOWS_METADATA_URL = (
    "https://geoglows-v2.s3.us-west-2.amazonaws.com/tables/package-metadata-table.parquet"
)


def build_glofas_crosswalk(
    *,
    metadata_parquet_path: str | None = None,
    glofas_grid_resolution: float = 0.05,
    max_snap_distance_km: float = 10.0,
    batch_size: int = 50000,
    db_session=None,
) -> int:
    """Build the crosswalk table mapping GeoGloWS reaches to GloFAS grid cells.

    Parameters
    ----------
    metadata_parquet_path : str | None
        Path to a local copy of the GeoGloWS metadata parquet.
        If None, downloads from S3 (~250 MB).
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

    # Load GeoGloWS metadata
    if metadata_parquet_path and Path(metadata_parquet_path).exists():
        logger.info("Loading GeoGloWS metadata from local file: %s", metadata_parquet_path)
        df = pd.read_parquet(metadata_parquet_path)
    else:
        logger.info("Downloading GeoGloWS metadata from S3 (~250 MB)...")
        df = pd.read_parquet(GEOGLOWS_METADATA_URL)

    logger.info("Metadata loaded: %d rows, columns: %s", len(df), list(df.columns))

    # Find lat/lon columns (naming varies across versions)
    lat_col = _find_column(df, ["lat", "latitude", "Lat", "LATITUDE", "centroid_lat"])
    lon_col = _find_column(df, ["lon", "longitude", "Lon", "LONGITUDE", "centroid_lon", "lng"])
    id_col = _find_column(df, ["LINKNO", "rivid", "river_id", "reach_id", "RiverID"])

    if not lat_col or not lon_col or not id_col:
        raise ValueError(
            f"Could not find required columns in metadata. "
            f"Available: {list(df.columns)}. "
            f"Found lat={lat_col}, lon={lon_col}, id={id_col}"
        )

    logger.info("Using columns: id=%s, lat=%s, lon=%s", id_col, lat_col, lon_col)

    # Drop rows with missing coordinates
    mask = df[lat_col].notna() & df[lon_col].notna() & df[id_col].notna()
    df = df[mask].copy()
    logger.info("Rows with valid coordinates: %d", len(df))

    reach_ids = df[id_col].astype(str).values
    reach_lats = df[lat_col].astype(float).values
    reach_lons = df[lon_col].astype(float).values

    # Build GloFAS grid points
    grid_lats = np.arange(-60, 90, glofas_grid_resolution)
    grid_lons = np.arange(-180, 180, glofas_grid_resolution)
    grid_lat_mesh, grid_lon_mesh = np.meshgrid(grid_lats, grid_lons, indexing="ij")
    grid_points = np.column_stack([grid_lat_mesh.ravel(), grid_lon_mesh.ravel()])

    logger.info(
        "Built GloFAS grid: %d cells (%.4f° resolution)", len(grid_points), glofas_grid_resolution
    )

    # Build KD-tree for spatial lookup (convert to radians for haversine)
    from scipy.spatial import cKDTree

    grid_rad = np.deg2rad(grid_points)
    tree = cKDTree(grid_rad)

    reach_coords_rad = np.deg2rad(np.column_stack([reach_lats, reach_lons]))

    # Query nearest grid cell for each reach
    distances_rad, indices = tree.query(reach_coords_rad)

    # Convert radian distance to approximate km (Earth radius ≈ 6371 km)
    distances_km = distances_rad * 6371.0

    # Snap to nearest grid cell coordinates
    nearest_grid = grid_points[indices]

    # Insert into database
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from app.db.models import ReachGridCrosswalk

    if db_session is None:
        from app.core.database import SessionLocal

        db_session = SessionLocal()
        own_session = True
    else:
        own_session = False

    count = 0
    batch = []

    for i in range(len(reach_ids)):
        dist = distances_km[i]
        if dist > max_snap_distance_km:
            continue

        batch.append(
            {
                "reach_id": reach_ids[i],
                "target_provider": "glofas",
                "grid_lat": float(nearest_grid[i, 0]),
                "grid_lon": float(nearest_grid[i, 1]),
                "distance_km": float(dist),
            }
        )

        if len(batch) >= batch_size:
            _upsert_batch(db_session, batch)
            count += len(batch)
            logger.info("Inserted %d crosswalk rows so far...", count)
            batch = []

    if batch:
        _upsert_batch(db_session, batch)
        count += len(batch)

    db_session.commit()
    if own_session:
        db_session.close()

    logger.info("Crosswalk build complete: %d rows inserted", count)
    return count


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
