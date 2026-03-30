#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build rivers.pmtiles from a GeoJSON / FlatGeobuf river network
#
# Prerequisites:
#   - tippecanoe (https://github.com/felt/tippecanoe)
#   - Input file with properties: strmOrder (int), DSContArea (float)
#
# The key settings preserve river geometry at low zoom so major rivers
# appear as continuous lines rather than fragmented dots.
# ---------------------------------------------------------------------------
set -euo pipefail

INPUT="${1:?Usage: $0 <rivers.geojson|rivers.fgb> [output.pmtiles]}"
OUTPUT="${2:-rivers.pmtiles}"

echo "Building PMTiles from: $INPUT"
echo "Output: $OUTPUT"

tippecanoe \
  -o "$OUTPUT" \
  --name="rivers" \
  --layer="rivers" \
  --minimum-zoom=0 \
  --maximum-zoom=12 \
  --no-feature-limit \
  --no-tile-size-limit \
  --simplification=4 \
  --simplify-only-low-zooms \
  --no-tiny-polygon-reduction \
  -j '{ "*": [
    "all",
    ["has", "strmOrder"],
    ["any",
      [">=", ["get", "strmOrder"], 7],
      ["all", [">=", ["get", "strmOrder"], 4], [">=", "$zoom", 4]],
      ["all", [">=", ["get", "strmOrder"], 2], [">=", "$zoom", 7]],
      [">=", "$zoom", 9]
    ]
  ]}' \
  --force \
  "$INPUT"

echo ""
echo "Done! Upload to R2 with:"
echo "  rclone copyto $OUTPUT r2:pub-6f1e54035ac14471852f4b7a25bf8354/rivers.pmtiles"
echo ""
echo "PMTiles info:"
pmtiles show "$OUTPUT" 2>/dev/null || echo "(install pmtiles CLI for stats)"
