#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build rivers.pmtiles from GEOGloWS v2 / TDX-Hydro style inputs.
#
# Goals:
#   1. Preserve continuous major rivers at low zoom (avoid sparse "dotty" look)
#   2. Keep progressive detail at higher zooms for medium/minor streams
#   3. Retain reach_id/strmOrder/DSContArea attributes for styling + querying
#
# Usage:
#   scripts/build_rivers_pmtiles.sh <input> [output.pmtiles]
#
# Inputs:
#   - GeoJSON / GeoJSONSeq / FlatGeobuf (local file or /vsicurl URL)
#   - Required properties: strmOrder; optional: DSContArea, reach_id, COMID
# ---------------------------------------------------------------------------
set -euo pipefail

INPUT="${1:-}"
OUTPUT="${2:-rivers.pmtiles}"
TMP_DIR="$(mktemp -d)"
WORK_GEOJSON="$TMP_DIR/rivers.ndjson"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ -z "$INPUT" ]]; then
  echo "Usage: $0 <rivers.geojson|rivers.fgb|/vsicurl/https://...> [output.pmtiles]" >&2
  exit 1
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required dependency: $1" >&2
    exit 1
  fi
}

require_cmd ogr2ogr
require_cmd tippecanoe

echo "Input:  $INPUT"
echo "Output: $OUTPUT"
echo "Temp:   $WORK_GEOJSON"

# Normalize fields to tippecanoe-friendly schema.
# - reach_id is normalized from reach_id|COMID|HYRIV_ID|LINKNO when available
# - strmOrder forced to integer
# - DSContArea forced to real
ogr2ogr \
  -f GeoJSONSeq "$WORK_GEOJSON" "$INPUT" \
  -nlt LINESTRING \
  -lco RS=YES \
  -dialect SQLite \
  -sql "
    SELECT
      CAST(
        COALESCE(
          CAST(reach_id AS TEXT),
          CAST(COMID AS TEXT),
          CAST(HYRIV_ID AS TEXT),
          CAST(LINKNO AS TEXT)
        ) AS TEXT
      ) AS reach_id,
      CAST(strmOrder AS INTEGER) AS strmOrder,
      CAST(DSContArea AS REAL) AS DSContArea,
      geometry
    FROM $(basename "$INPUT" | sed 's/\.[^.]*$//')
    WHERE strmOrder IS NOT NULL
  " >/dev/null 2>&1 || {
    echo "ogr2ogr SQL normalization failed. Falling back to direct conversion..."
    ogr2ogr -f GeoJSONSeq "$WORK_GEOJSON" "$INPUT" -nlt LINESTRING -lco RS=YES >/dev/null
  }

# IMPORTANT flags for river continuity:
#   --no-tile-size-limit / --no-feature-limit: never drop segments in dense tiles
#   --simplification=1 and --simplify-only-low-zooms: gentle simplification
#   --no-line-simplification at z12 (highest generated z) via detail settings
tippecanoe \
  -o "$OUTPUT" \
  --force \
  --name="rivers" \
  --layer="rivers" \
  --minimum-zoom=0 \
  --maximum-zoom=12 \
  --no-feature-limit \
  --no-tile-size-limit \
  --simplification=1 \
  --simplify-only-low-zooms \
  --full-detail=12 \
  --low-detail=10 \
  --minimum-detail=7 \
  --extend-zooms-if-still-dropping \
  --generate-ids \
  --accumulate-attribute=DSContArea:sum \
  --drop-rate=1 \
  --detect-shared-borders \
  --read-parallel \
  "$WORK_GEOJSON"

echo
echo "Done."
echo "Upload example:"
echo "  rclone copyto \"$OUTPUT\" r2:pub-6f1e54035ac14471852f4b7a25bf8354/rivers.pmtiles"
echo
if command -v pmtiles >/dev/null 2>&1; then
  pmtiles show "$OUTPUT"
else
  echo "Install pmtiles CLI to inspect archive metadata."
fi
