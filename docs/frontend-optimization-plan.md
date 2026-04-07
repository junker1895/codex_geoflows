# Frontend Optimization Implementation Plan (Phases 1–3)

This document translates the agreed architecture strategy into execution-ready tickets.

## Scope

- Focuses on frontend performance and rendering quality.
- Excludes tile-publishing/process changes (former Phase 4 was intentionally removed).
- Preserves current functional behavior (working ingest-backed data) while reducing clutter and lag.

## Product constraints reflected in this plan

1. At far-out zoom extents, gauges should prioritize flood-relevant statuses and avoid low-signal noise (especially Unknown/blue-style clutter).
2. River animation must exist at all zoom extents, but can simplify at low zoom for performance.
3. Layer architecture should be simplified and made predictable without removing required interaction behavior.

---

## Epic A — Gauge relevance + map performance (Phase 1)

### A1. Gauge severity priority model

**Goal:** Classify gauge statuses into priority tiers used by rendering policy.

**Implementation notes**
- Add deterministic status → priority mapping (`high`, `medium`, `low`).
- Suggested defaults:
  - `high`: Major Flood, Moderate Flood, Minor Flood, Action Stage
  - `medium`: Low Flow
  - `low`: No Flooding, Unknown
- Keep color styling as-is; this ticket adds visibility priority behavior only.

**Acceptance criteria**
- Low zoom renders only flood-relevant gauges.
- Unknown/No Flooding no longer dominate global view.
- Popups continue working for visible gauges.

---

### A2. Zoom-based gauge visibility policy

**Goal:** Reveal gauges progressively by zoom band.

**Implementation notes**
- Implement a config-driven zoom policy table:
  - low zoom: `high` only
  - mid zoom: `high + medium`
  - high zoom: `high + medium + low`
- Integrate with existing viewport event handlers to avoid duplicate recomputes.

**Acceptance criteria**
- Gauge density increases predictably as user zooms in.
- Zooming back out quickly declutters.
- Policy can be tuned through constants/config, not logic rewrites.

---

### A3. Viewport-scoped gauge loading

**Goal:** Replace global gauge loading with bounded data acquisition.

**Implementation notes**
- Stop loading all pages globally.
- Request only viewport-relevant gauges (with a small buffer).
- Reduce payload fields to only those needed for styling + popup details.
- Keep refresh timer but scope refresh data to current viewport.

**Acceptance criteria**
- Initial gauge payload size is significantly lower than baseline.
- Pan/zoom in dense regions remains responsive.
- Refreshes do not cause visible freezes.

---

### A4. Low-zoom clustering or thinning

**Goal:** Prevent symbol pileup under public traffic conditions.

**Implementation notes**
- Add clustering at low zoom, or deterministic thinning if clustering is not suitable.
- Ensure smooth transition to individual gauges at higher zoom.

**Acceptance criteria**
- No extreme “point cloud” clutter at low zoom.
- Interaction remains clear (expand cluster or click point behavior).
- Dense-region pan performance improves compared to baseline.

---

## Epic B — Always-on river animation with adaptive quality (Phase 2)

### B1. Zoom-quality animation profiles

**Goal:** Keep animation active at all zooms with adaptive complexity.

**Implementation notes**
- Define profile by zoom band for:
  - max animated paths
  - opacity and width multipliers
  - dash complexity/speed
- Replace static cap logic with profile-driven limits.

**Acceptance criteria**
- Animation remains visible at low, mid, and high zoom.
- Low zoom uses simpler, lighter animation.
- High zoom maintains richer motion detail.

---

### B2. Projected-geometry caching

**Goal:** Reduce per-frame animation CPU cost.

**Implementation notes**
- Cache projected screen-space geometry for animated paths.
- Recompute cache only when camera/viewport changes meaningfully.
- During frames between camera changes, animate offsets over cached geometry.

**Acceptance criteria**
- Reduced CPU usage during steady-state animation.
- Fewer long tasks in dense river extents.
- No visual desync after pan/zoom.

---

### B3. Frame-budget governor

**Goal:** Avoid jank under transient load.

**Implementation notes**
- Track rolling frame-time metrics.
- If budget exceeded, degrade animation quality automatically (fewer paths / simpler effect).
- Recover quality when frame-time budget stabilizes.

**Acceptance criteria**
- p95 frame time in dense views meets agreed target.
- Degradation appears graceful rather than stuttery.
- Behavior is observable via debug/perf logs.

---

### B4. Animation consistency and stability

**Goal:** Make animated subset appear intentional (not random/flickery).

**Implementation notes**
- Stabilize selection strategy across small camera moves.
- Prefer severity-weighted and geographically balanced path sampling at low zoom.
- Reduce popping/flicker when recomputing animated sets.

**Acceptance criteria**
- Animated rivers appear stable during small pans.
- Severe reaches are consistently represented at broader extents.
- Flicker is materially reduced versus baseline.

---

## Epic C — Unified render contract and layer simplification (Phase 3)

### C1. Render-contract matrix

**Goal:** Centralize what renders at each zoom band.

**Implementation notes**
- Create a single configuration matrix controlling:
  - base river layers
  - highlight behavior
  - animation profile
  - gauge priority visibility policy
- Ensure matrix reflects product constraints:
  - animation always on
  - low zoom gauge severity-first

**Acceptance criteria**
- Zoom behavior is predictable and easy to tune.
- Minimal behavior logic is hardcoded in event handlers.
- Team can adjust zoom rules from one config surface.

---

### C2. Layer overlap reduction

**Goal:** Reduce visual glitches from overlapping render paths.

**Implementation notes**
- Audit base tiers, query-only tiers, highlight tiers, and canvas overlay interactions.
- Remove redundant overlap where possible without breaking click/query behavior.
- Keep interaction hit areas usable after simplification.

**Acceptance criteria**
- Fewer active overlapping layers with same feature intent.
- Reduced crossover artifacts and style contention.
- Click/hover behavior remains intact.

---

### C3. Feature-state efficiency pass

**Goal:** Keep highlight updates scalable during pan/zoom.

**Implementation notes**
- Preserve visible-feature-only write approach.
- Tighten duplicate-write guards and batch behavior.
- Validate via existing perf telemetry outputs.

**Acceptance criteria**
- Feature-state write volumes remain bounded under movement.
- No severity/highlight correctness regression.
- Improved interaction smoothness in dense loaded tiles.

---

## Recommended PR sequence

1. **PR-1:** A1 + A2 (policy primitives; low risk)
2. **PR-2:** A3 + A4 (network and symbol density improvements)
3. **PR-3:** B1 + B4 (always-on adaptive animation behavior)
4. **PR-4:** B2 + B3 (core animation perf controls)
5. **PR-5:** C1 + C2 + C3 (architecture cleanup and consolidation)

## Definition of Done

- Low zoom is not clogged by low-signal gauges.
- River animation is present at all zoom extents.
- Pan/zoom responsiveness improves in dense regions.
- Visual glitches from overlapping layer strategies are reduced.
- Improvements are verified against baseline scenarios and telemetry.
