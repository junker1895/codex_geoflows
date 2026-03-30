# Rebuilding `rivers.pmtiles` for continuous low-zoom rivers

This project uses a hybrid strategy so global zooms stay clean while higher zooms reveal GEOGloWS/TDX detail:

| Zoom | What renders |
| --- | --- |
| z0–5 | Natural Earth rivers (continuous lines) + PMTiles major reaches (`strmOrder >= 7`) |
| z6 | Crossover (NE fades out, PMTiles fades in) |
| z5+ | PMTiles medium reaches (`strmOrder 4–6`) with DSContArea gating at lower zooms |
| z8+ | PMTiles minor reaches (`strmOrder < 4`) with DSContArea gating at lower zooms |

## 1) Build PMTiles from GEOGloWS v2 streams

The build script accepts local files or a GDAL `/vsicurl/` URL.

```bash
scripts/build_rivers_pmtiles.sh \
  /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/geoglows-v2-streams.fgb \
  rivers.pmtiles
```

The script:
- normalizes key attributes (`reach_id`, `strmOrder`, `DSContArea`) with `ogr2ogr`,
- uses conservative simplification and no feature/tile dropping in Tippecanoe,
- preserves connectivity better at low zooms than aggressive defaults.

### Notes

- If the SQL normalization path cannot infer a layer name, the script falls back to direct conversion automatically.
- Required tools: `ogr2ogr`, `tippecanoe` (optional: `pmtiles` CLI for inspection).

## 2) Verify archive metadata

```bash
pmtiles show rivers.pmtiles
```

Confirm:
- `minZoom` is `0`
- `maxZoom` is at least `12`
- layer name is `rivers`

## 3) Upload and wire to frontend

```bash
rclone copyto rivers.pmtiles r2:pub-6f1e54035ac14471852f4b7a25bf8354/rivers.pmtiles
```

The frontend already reads:
- PMTiles source: `rivers.pmtiles`
- NE 50m rivers GeoJSON for low zoom continuity

## 4) Visual QA checklist

At each zoom, check:
1. **z0–5:** Amazon, Nile, Murray-Darling, etc. are continuous (NE layer).
2. **z6:** smooth transition NE → PMTiles without abrupt disappear/reappear.
3. **z7–9:** medium streams start appearing as connected lines.
4. **z10+:** minor tributaries fill in progressively.

If you still see sparse dots at low zoom, verify that NE rivers loaded successfully in the browser console and that PMTiles line opacity remains low below z6.

## 5) Test zoom/filter behavior *before* rebuilding PMTiles

You can validate the intended zoom gates without generating tiles:

```bash
python scripts/check_river_zoom_filters.py --min-zoom 0 --max-zoom 12 --step 0.5
```

This prints, per zoom:
- whether NE is active and its expected opacity,
- NE `scalerank` cutoff,
- which PMTiles tiers should render (`major`, `medium`, `minor`).

You can also verify live in the app with a visual overlay:

```text
http://localhost:4173/?debugRivers=1
```

The overlay shows current zoom plus expected NE/PMTiles visibility and filter thresholds.
At transition zooms, DSContArea minimums are intentionally high to reduce visual clutter from too many short reaches.

## 6) QGIS workflow (yes, this helps)

QGIS is useful for preflight checks on source geometry and the `strmOrder` rules:

1. Add GEOGloWS streams (`.fgb` or GeoParquet converted to layer) as a vector layer.
2. Open **Layer Properties → Symbology → Rule-based**.
3. Add these rules:
   - `strmOrder >= 7` (major)
   - `strmOrder >= 4 AND strmOrder < 7` (medium)
   - `strmOrder < 4` (minor)
4. Style each rule with increasing line width for major streams.
5. Zoom to global, continental, and local extents and check continuity of major rivers.
6. Optional: run **Processing → Geometry by expression** and compare `length($geometry)` distributions by `strmOrder` to detect over-fragmented linework before tiling.

QGIS cannot perfectly emulate MapLibre zoom styling, but it is very good for verifying that source geometry and `strmOrder` classes are coherent before running Tippecanoe.
