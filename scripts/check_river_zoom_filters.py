#!/usr/bin/env python3
"""Preview river layer visibility/filters by zoom (without rebuilding PMTiles).

This mirrors the frontend strategy in `frontend/src/main.js`:
  - NE fallback at low zoom with scalerank gate
  - PMTiles major/medium/minor by strmOrder + minzoom
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass


NE_PMTILES_CROSSOVER_ZOOM = 6


@dataclass(frozen=True)
class RiverTier:
    name: str
    minzoom: float
    strm_order_rule: str


RIVER_TIERS = [
    RiverTier("rivers-major", 0, "strmOrder >= 7"),
    RiverTier("rivers-medium", 6, "4 <= strmOrder < 7"),
    RiverTier("rivers-minor", 8, "strmOrder < 4"),
]


def interp_linear(zoom: float, stops: list[tuple[float, float]]) -> float:
    if zoom <= stops[0][0]:
        return stops[0][1]
    if zoom >= stops[-1][0]:
        return stops[-1][1]
    for (z0, v0), (z1, v1) in zip(stops, stops[1:]):
        if z0 <= zoom <= z1:
            t = (zoom - z0) / (z1 - z0)
            return v0 + (v1 - v0) * t
    return stops[-1][1]


def ne_scalerank_max(zoom: float) -> float:
    return interp_linear(zoom, [(0, 3), (2, 5), (4, 8), (5, 12)])


def ne_opacity(zoom: float) -> float:
    return interp_linear(
        zoom,
        [
            (0, 0.75),
            (NE_PMTILES_CROSSOVER_ZOOM - 1, 0.7),
            (NE_PMTILES_CROSSOVER_ZOOM, 0.35),
            (NE_PMTILES_CROSSOVER_ZOOM + 0.6, 0),
        ],
    )


def major_opacity(zoom: float) -> float:
    return interp_linear(zoom, [(0, 0.2), (4.5, 0.35), (6, 0.8), (8, 0.9)])


def major_min_area(zoom: float) -> float:
    return interp_linear(zoom, [(0, 120000), (3, 60000), (5, 15000), (6, 0)])


def medium_min_area(zoom: float) -> float:
    return interp_linear(zoom, [(6, 20000), (7, 5000), (8, 1000), (9, 100), (10, 0)])


def minor_min_area(zoom: float) -> float:
    return interp_linear(zoom, [(8, 10000), (9, 2000), (10, 0)])


def visible_tiers(zoom: float) -> list[RiverTier]:
    return [tier for tier in RIVER_TIERS if zoom >= tier.minzoom]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-zoom", type=float, default=0)
    parser.add_argument("--max-zoom", type=float, default=12)
    parser.add_argument("--step", type=float, default=1)
    args = parser.parse_args()

    zoom = args.min_zoom
    print("zoom\tNE_active\tNE_opacity\tNE_scalerank<=\tPMTiles tiers")
    while zoom <= args.max_zoom + 1e-9:
        ne_active = zoom < (NE_PMTILES_CROSSOVER_ZOOM + 1)
        ne_op = ne_opacity(zoom) if ne_active else 0
        ne_rank = ne_scalerank_max(zoom) if ne_active else 0
        tiers = []
        for tier in visible_tiers(zoom):
            if tier.name == "rivers-major":
                tiers.append(
                    f"{tier.name} ({tier.strm_order_rule}, opacity~{major_opacity(zoom):.2f}, DSContArea>={major_min_area(zoom):.0f})"
                )
            elif tier.name == "rivers-medium":
                tiers.append(f"{tier.name} ({tier.strm_order_rule}, DSContArea>={medium_min_area(zoom):.0f})")
            elif tier.name == "rivers-minor":
                tiers.append(f"{tier.name} ({tier.strm_order_rule}, DSContArea>={minor_min_area(zoom):.0f})")
            else:
                tiers.append(f"{tier.name} ({tier.strm_order_rule})")
        print(
            f"{zoom:.1f}\t{str(ne_active).lower()}\t\t{ne_op:.2f}\t\t{ne_rank:.2f}\t\t"
            + (", ".join(tiers) if tiers else "-")
        )
        zoom += args.step


if __name__ == "__main__":
    main()
