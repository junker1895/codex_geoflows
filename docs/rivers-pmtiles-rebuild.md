# Rebuilding `rivers.pmtiles` for continuous low-zoom rivers

This project uses a hybrid strategy so global zooms stay clean while higher zooms reveal GEOGloWS/TDX detail:

| Zoom | What renders |
| --- | --- |
| z0–5 | Natural Earth rivers (continuous lines) + PMTiles major reaches (`strmOrder >= 7`) |
| z6 | Crossover (NE fades out, PMTiles fades in) |
| z6+ | PMTiles medium reaches (`strmOrder 4–6`) with DSContArea gating at lower zooms |
| z8+ | PMTiles minor reaches (`strmOrder < 4`) with DSContArea gating at lower zooms |

## 1) Build PMTiles from GEOGloWS v2 streams

Use the enriched mode (recommended) to preserve parity with the proven GEOGloWS model-table join workflow:

```bash
scripts/build_rivers_pmtiles.sh \
  --streams /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/geoglows-v2-streams.fgb \
  --model-table /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/tables/v2-model-table.parquet \
  --output rivers.pmtiles
```

### Why the model-table join is preferred

The `--model-table` path enriches each stream feature by joining on `LINKNO` and guarantees normalized attributes from the authoritative table:
- `reach_id` (string)
- `strmOrder` (int)
- `DSContArea` (float)

Geometry is streamed through GDAL and normalized in EPSG:4326 for Tippecanoe compatibility (matching the older proven workflow expectations).

Reach ID resolution uses this fallback chain from stream features:
1. `reach_id`
2. `provider_reach_id`
3. `LINKNO`
4. `COMID`
5. `HYRIV_ID`

If a numeric reach ID cannot be resolved, the feature is skipped. If `LINKNO` is not present in the model table, the script falls back to stream properties (`strmOrder` default `1`, `DSContArea` default `0.0`).

The script prints processing counts (processed, written, missing IDs, missing attrs) and hard-fails if zero features are written.

## 2) Fallback behavior when model-table is omitted

You can still build geometry-only PMTiles:

```bash
scripts/build_rivers_pmtiles.sh \
  --streams /vsicurl/https://geoglows-v2.s3.us-west-2.amazonaws.com/geoglows-v2-streams.fgb \
  --output rivers.pmtiles
```

In this mode, the script:
- detects the OGR layer name with `ogrinfo`,
- runs SQL normalization where possible,
- falls back to direct conversion if layer detection or SQL normalization fails,
- runs a second normalization pass so output still includes `reach_id`, `strmOrder`, and `DSContArea` even after direct conversion fallback.

## 3) Tippecanoe settings used for continuity

The build keeps anti-patchiness settings tuned for continuous low zoom rivers:
- `--minimum-zoom=0 --maximum-zoom=12`
- `--no-feature-limit`
- `--no-tile-size-limit`
- `--simplification=1 --simplify-only-low-zooms`
- `--read-parallel`
- `--use-attribute-for-id=reach_id` (stable IDs from attributes; no generated IDs)

## 4) Dependencies

Required tools:
- `ogr2ogr`
- `ogrinfo`
- `tippecanoe`
- `python3`
- Python packages: `pandas`, `pyarrow`

Optional:
- `pmtiles` CLI for metadata inspection

## 5) Verify archive metadata

```bash
pmtiles show rivers.pmtiles
```

Confirm:
- `minZoom` is `0`
- `maxZoom` is at least `12`
- layer name is `rivers`

## 6) Upload and wire to frontend

```bash
rclone copyto rivers.pmtiles r2:pub-6f1e54035ac14471852f4b7a25bf8354/rivers.pmtiles
```

The frontend already reads:
- PMTiles source: `rivers.pmtiles`
- NE 50m rivers GeoJSON for low zoom continuity

## 7) Visual QA checklist

At each zoom, check:
1. **z0–5:** Amazon, Nile, Murray-Darling, etc. are continuous (NE layer).
2. **z6:** smooth transition NE → PMTiles without abrupt disappear/reappear.
3. **z7–9:** medium streams start appearing as connected lines.
4. **z10+:** minor tributaries fill in progressively.

If you still see sparse dots at low zoom, verify that NE rivers loaded successfully in the browser console and that PMTiles line opacity remains low below z6.

## 8) Test zoom/filter behavior *before* rebuilding PMTiles

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
