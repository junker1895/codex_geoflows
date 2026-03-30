#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build rivers.pmtiles from GEOGloWS v2 / TDX-Hydro style inputs.
#
# Supports two modes:
#   1) Enriched mode (preferred): streams + model table parquet join on LINKNO
#   2) Fallback mode: geometry-only normalization from stream source
#
# Output properties are always normalized to:
#   - reach_id   (string)
#   - strmOrder  (int)
#   - DSContArea (float)
# ---------------------------------------------------------------------------
set -euo pipefail

OUTPUT="rivers.pmtiles"
STREAMS=""
MODEL_TABLE=""
TMP_DIR="$(mktemp -d)"
WORK_GEOJSON="$TMP_DIR/rivers.ndjson"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

usage() {
  cat <<'USAGE'
Usage:
  scripts/build_rivers_pmtiles.sh --streams <path|/vsicurl/url> [options]

Required:
  --streams <path|/vsicurl/url>   Stream geometry source (GeoJSON/GeoJSONSeq/FlatGeobuf/etc)

Optional:
  --model-table <path|/vsicurl/url>  Parquet table with LINKNO,strmOrder,DSContArea (preferred)
  --output <file.pmtiles>            Output PMTiles path (default: rivers.pmtiles)
  -h, --help                         Show this help

Examples:
  scripts/build_rivers_pmtiles.sh \
    --streams /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/geoglows-v2-streams.fgb \
    --model-table /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/tables/v2-model-table.parquet \
    --output rivers.pmtiles

  scripts/build_rivers_pmtiles.sh \
    --streams /path/to/streams.fgb \
    --output rivers.pmtiles
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required dependency: $1" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --streams)
      STREAMS="${2:-}"
      shift 2
      ;;
    --model-table)
      MODEL_TABLE="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$STREAMS" ]]; then
  echo "Error: --streams is required." >&2
  usage >&2
  exit 1
fi

require_cmd ogr2ogr
require_cmd ogrinfo
require_cmd tippecanoe

if [[ -n "$MODEL_TABLE" ]]; then
  require_cmd python3
fi

echo "Streams:     $STREAMS"
if [[ -n "$MODEL_TABLE" ]]; then
  echo "Model table: $MODEL_TABLE"
else
  echo "Model table: (none; geometry-only fallback mode)"
fi
echo "Output:      $OUTPUT"
echo "Temp NDJSON: $WORK_GEOJSON"

if [[ -n "$MODEL_TABLE" ]]; then
  echo "Running enriched join mode..."
  STREAMS="$STREAMS" MODEL_TABLE="$MODEL_TABLE" WORK_GEOJSON="$WORK_GEOJSON" python3 <<'PY'
import json
import math
import os
import subprocess
import sys

import pandas as pd

streams = os.environ["STREAMS"]
model_table = os.environ["MODEL_TABLE"]
out_path = os.environ["WORK_GEOJSON"]

try:
  model_df = pd.read_parquet(model_table, columns=["LINKNO", "strmOrder", "DSContArea"])
except Exception as exc:
  print(f"Failed reading model table parquet: {exc}", file=sys.stderr)
  sys.exit(1)

model_df = model_df.dropna(subset=["LINKNO"]).copy()
model_df["LINKNO"] = pd.to_numeric(model_df["LINKNO"], errors="coerce")
model_df = model_df.dropna(subset=["LINKNO"])
model_df["LINKNO"] = model_df["LINKNO"].astype("int64")

lookup = {}
for row in model_df.itertuples(index=False):
  linkno = int(row.LINKNO)
  so = row.strmOrder
  area = row.DSContArea

  so_val = 1
  if pd.notna(so):
    try:
      so_val = int(so)
    except Exception:
      so_val = 1

  area_val = 0.0
  if pd.notna(area):
    try:
      area_val = float(area)
    except Exception:
      area_val = 0.0

  lookup[linkno] = (so_val, area_val)

proc = subprocess.Popen(
    [
        "ogr2ogr",
        "-f",
        "GeoJSONSeq",
        "-t_srs",
        "EPSG:4326",
        "/vsistdout/",
        streams,
        "-nlt",
        "LINESTRING",
        "-lco",
        "RS=YES",
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

processed = 0
written = 0
missing_ids = 0
missing_attrs = 0

id_fields = ["reach_id", "provider_reach_id", "LINKNO", "COMID", "HYRIV_ID"]

with open(out_path, "w", encoding="utf-8") as out_f:
  assert proc.stdout is not None
  for line in proc.stdout:
    raw = line.strip()
    if not raw:
      continue

    try:
      feat = json.loads(raw)
    except Exception:
      continue

    processed += 1
    props = feat.get("properties") or {}

    candidate = None
    for field in id_fields:
      value = props.get(field)
      if value is not None and str(value).strip() != "":
        candidate = value
        break

    if candidate is None:
      missing_ids += 1
      continue

    linkno = None
    if isinstance(candidate, bool):
      missing_ids += 1
      continue

    if isinstance(candidate, (int, float)):
      if isinstance(candidate, float) and (math.isnan(candidate) or math.isinf(candidate)):
        missing_ids += 1
        continue
      try:
        linkno = int(candidate)
      except Exception:
        linkno = None
    else:
      text = str(candidate).strip()
      try:
        linkno = int(float(text))
      except Exception:
        linkno = None

    if linkno is None:
      missing_ids += 1
      continue

    joined = lookup.get(linkno)

    if joined is not None:
      strm_order = int(joined[0])
      ds_cont_area = float(joined[1])
    else:
      missing_attrs += 1
      src_so = props.get("strmOrder", 1)
      src_area = props.get("DSContArea", 0.0)
      try:
        strm_order = int(src_so)
      except Exception:
        strm_order = 1
      try:
        ds_cont_area = float(src_area)
      except Exception:
        ds_cont_area = 0.0

    feat["properties"] = {
      "reach_id": str(linkno),
      "strmOrder": int(strm_order),
      "DSContArea": float(ds_cont_area),
    }

    out_f.write(json.dumps(feat, separators=(",", ":")) + "\n")
    written += 1

stderr_text = ""
assert proc.stderr is not None
stderr_text = proc.stderr.read()
return_code = proc.wait()
if return_code != 0:
  print("ogr2ogr stream conversion failed:", file=sys.stderr)
  print(stderr_text, file=sys.stderr)
  sys.exit(return_code)

print(f"Processed features: {processed}")
print(f"Written features:   {written}")
print(f"Missing IDs:        {missing_ids}")
print(f"Missing attrs:      {missing_attrs}")

if written == 0:
  print("Hard failure: 0 features written after enrichment.", file=sys.stderr)
  sys.exit(2)
PY
else
  echo "Running geometry-only fallback mode..."

  LAYER_NAME=""
  if LAYER_NAME_RAW="$(ogrinfo -ro -so "$STREAMS" 2>/dev/null | awk -F': ' '/^1: / {print $2; exit}')"; then
    LAYER_NAME="$LAYER_NAME_RAW"
  fi

  if [[ -n "$LAYER_NAME" ]]; then
    echo "Detected layer: $LAYER_NAME"
    ogr2ogr \
      -f GeoJSONSeq "$WORK_GEOJSON" "$STREAMS" \
      -t_srs EPSG:4326 \
      -nlt LINESTRING \
      -lco RS=YES \
      -dialect SQLite \
      -sql "
        SELECT
          CAST(
            COALESCE(
              CAST(reach_id AS TEXT),
              CAST(provider_reach_id AS TEXT),
              CAST(LINKNO AS TEXT),
              CAST(COMID AS TEXT),
              CAST(HYRIV_ID AS TEXT)
            ) AS TEXT
          ) AS reach_id,
          CAST(COALESCE(strmOrder, 1) AS INTEGER) AS strmOrder,
          CAST(COALESCE(DSContArea, 0.0) AS REAL) AS DSContArea,
          geometry
        FROM \"$LAYER_NAME\"
      " >/dev/null 2>&1 || {
        echo "ogr2ogr SQL normalization failed. Falling back to direct conversion..."
        ogr2ogr -f GeoJSONSeq "$WORK_GEOJSON" "$STREAMS" -t_srs EPSG:4326 -nlt LINESTRING -lco RS=YES >/dev/null
      }
  else
    echo "Could not detect layer name with ogrinfo. Falling back to direct conversion..."
    ogr2ogr -f GeoJSONSeq "$WORK_GEOJSON" "$STREAMS" -t_srs EPSG:4326 -nlt LINESTRING -lco RS=YES >/dev/null
  fi

  # Direct conversion fallback may not carry normalized fields consistently.
  # Normalize to reach_id/strmOrder/DSContArea in a second pass.
  WORK_GEOJSON="$WORK_GEOJSON" python3 <<'PY'
import json
import os
import tempfile

src = os.environ["WORK_GEOJSON"]
id_fields = ["reach_id", "provider_reach_id", "LINKNO", "COMID", "HYRIV_ID"]

written = 0
skipped = 0

fd, tmp_path = tempfile.mkstemp(prefix="rivers-normalized-", suffix=".ndjson")
os.close(fd)

with open(src, "r", encoding="utf-8") as in_f, open(tmp_path, "w", encoding="utf-8") as out_f:
    for line in in_f:
        raw = line.strip()
        if not raw:
            continue
        try:
            feat = json.loads(raw)
        except Exception:
            continue

        props = feat.get("properties") or {}
        reach_val = None
        for field in id_fields:
            value = props.get(field)
            if value is not None and str(value).strip() != "":
                reach_val = value
                break

        if reach_val is None:
            skipped += 1
            continue

        try:
            so = int(props.get("strmOrder", 1))
        except Exception:
            so = 1
        try:
            area = float(props.get("DSContArea", 0.0))
        except Exception:
            area = 0.0

        feat["properties"] = {
            "reach_id": str(reach_val),
            "strmOrder": so,
            "DSContArea": area,
        }
        out_f.write(json.dumps(feat, separators=(",", ":")) + "\n")
        written += 1

os.replace(tmp_path, src)
print(f"Fallback normalization written: {written}")
print(f"Fallback normalization skipped (missing ID): {skipped}")
PY
fi

if [[ ! -s "$WORK_GEOJSON" ]]; then
  echo "No output features were produced; aborting." >&2
  exit 1
fi

# Continuity-focused Tippecanoe settings + stable ID from reach_id.
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
  --use-attribute-for-id=reach_id \
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
