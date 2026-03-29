# Phase 0 Performance Baseline Plan

This document defines the **measurement-only** phase before any viewport filtering or rendering optimizations are implemented.

## Goals

1. Capture repeatable baseline numbers for frontend and backend performance.
2. Identify dominant bottlenecks by interaction type.
3. Define acceptance targets for later implementation phases.

## Scope

Phase 0 does **not** change map data behavior. It only adds instrumentation and a runbook.

## Instrumentation Added

### Frontend

The frontend records lightweight telemetry in `window.__geoflowsPerf`:

- Network call timings and payload bytes.
- Zoom/move/click/provider-switch interaction timings.
- Feature-state write batch counts and durations.
- Browser long-task entries (when supported by the runtime).
- Session snapshot logs via `[perf:snapshot]` in DevTools.

### Backend

Map API logs include:

- Request filter inputs (`bbox`, `limit`, `flagged_only`, `min_severity_score`).
- Result counts.
- Endpoint elapsed time.
- `/forecast/map/severity` payload bytes.

## Baseline Scenarios

Run each scenario at least 3 times and capture p50/p95:

1. Initial map load at default zoom (global view).
2. Zoom from global to continental (e.g. z2 → z5).
3. Pan across dense river regions at medium zoom.
4. Zoom into local basin (e.g. z5 → z8).
5. Click 5 reaches in sequence.
6. Provider switch (GeoGloWS ↔ GloFAS) and repeat zoom/pan.

## Scorecard Template

Record in a table for each scenario:

- API latency p50/p95 (`/runs/latest`, `/map/severity`, `/reaches/*`).
- API payload bytes p50/p95 (`/map/severity`).
- Reaches loaded and cumulative in-memory reach count.
- Feature-state writes per update batch.
- Long-task count and max long-task duration.
- Subjective smoothness notes (stutter/jank observations).

## Exit Criteria for Phase 0

Phase 0 is complete when:

1. Baseline metrics are captured for all scenarios.
2. Top 3 bottlenecks are identified with evidence.
3. Quantitative targets for Phase 1+ are agreed and documented.

## Optional automation script

If backend is running locally (e.g. via `docker compose up`), you can run:

```bash
python scripts/run_phase0_baseline.py --base-url http://localhost:8000 --iterations 3 --out-json phase0-report.json
```

Map-only focused run (skip detail endpoint):

```bash
python scripts/run_phase0_baseline.py --base-url http://localhost:8000 --iterations 5 --timeseries-limit 0 --out-json phase0-map-only.json
```

Viewport-filter endpoint run (exercise `POST /forecast/map/severity/filter`):

```bash
python scripts/run_phase0_baseline.py --base-url http://localhost:8000 --iterations 5 --timeseries-limit 0 --use-filter-endpoint --filter-reach-count 1000 --out-json phase1-after.json
```

This script automates API-side checks for:

- `/forecast/runs/latest`
- `/forecast/map/severity` at the current zoom-tier thresholds
- one `/forecast/reaches/{provider}/{reach_id}` detail request per provider/iteration

And prints p50/p95 summary rows (including per-tier map summaries) for latency, payload bytes, and returned counts.
