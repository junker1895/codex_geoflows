# Frontend Optimization Plan — Implementation Status Snapshot

Date: 2026-04-08

This snapshot compares `docs/frontend-optimization-plan.md` against current frontend implementation in `frontend/src/main.js`.

## Epic A — Gauge relevance + map performance

- **A1. Gauge severity priority model — Started (largely implemented).**
  - Implemented `GAUGE_STATUS_PRIORITY` with high/medium/low tiers and mapped statuses that align with the plan.
  - Zoom-time filtering uses this priority map.

- **A2. Zoom-based gauge visibility policy — Started (implemented).**
  - `GAUGE_ZOOM_VISIBILITY_POLICY` defines low/mid/high zoom bands and allowed priorities.
  - `applyGaugeVisibilityPolicy()` applies filters on map zoom changes.

- **A3. Viewport-scoped gauge loading — Not started.**
  - Gauges are still fetched globally (`where=1=1`) with full geometry and all fields (`outFields=*`), and pagination loads all pages.

- **A4. Low-zoom clustering or thinning — Not started.**
  - Gauges are rendered as a single circle layer with filtering but no clustering/thinning strategy.

## Epic B — Always-on river animation with adaptive quality

- **B1. Zoom-quality animation profiles — Started (partial implementation).**
  - Animation is always on and adapts by zoom via interpolated parameters and a zoom-dependent draw cap.
  - There is no explicit centralized profile table yet.

- **B2. Projected-geometry caching — Not started.**
  - Geometry selection is cached, but projected screen coordinates are recomputed every frame with `map.project`.

- **B3. Frame-budget governor — Not started.**
  - No rolling frame-time budget monitor or auto-degrade/recover loop is present.

- **B4. Animation consistency and stability — Started (partial implementation).**
  - Uses a stable per-path key and retained random offset map to reduce visual reset/flicker across refreshes.
  - No explicit severity-weighted, geographically balanced sampling policy was found.

## Epic C — Unified render contract and layer simplification

- **C1. Render-contract matrix — Not started.**
  - Zoom logic is distributed across multiple constants/functions (`ZOOM_SEVERITY_TIERS`, `GAUGE_ZOOM_VISIBILITY_POLICY`, river tier configs) rather than one unified matrix.

- **C2. Layer overlap reduction — Started (partial implementation).**
  - River rendering is tiered and intentionally split into visible/query/highlight layers.
  - Architecture still contains parallel layer paths (base + query + highlight + canvas animation), so full simplification is still pending.

- **C3. Feature-state efficiency pass — Started (implemented with room to tune).**
  - Feature-state writes are visible-feature scoped, deduplicated, and telemetry-recorded.

## Quick conclusion

Implementation work has clearly started in **A1, A2, B1, B4, C2, and C3**, with **A3, A4, B2, B3, and C1** still outstanding.
