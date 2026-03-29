#!/usr/bin/env python3
"""Automate Phase 0 baseline API measurements against a running backend.

Usage example:
  python scripts/run_phase0_baseline.py --base-url http://localhost:8000 --iterations 3
"""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, asdict


DEFAULT_TIERS = [
    {"name": "tier_sev4_limit5000", "min_severity_score": 4, "limit": 5000},
    {"name": "tier_sev3_limit20000", "min_severity_score": 3, "limit": 20000},
    {"name": "tier_sev2_limit50000", "min_severity_score": 2, "limit": 50000},
    {"name": "tier_sev1_unlimited", "min_severity_score": 1, "limit": None},
]


@dataclass
class Sample:
    endpoint: str
    provider: str
    status_code: int
    duration_ms: float
    payload_bytes: int
    count: int | None = None
    meta: dict | None = None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (p / 100.0) * (len(sorted_vals) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return sorted_vals[lo]
    frac = rank - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def do_get(base_url: str, path: str, params: dict[str, str | int]) -> tuple[int, bytes, float]:
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url=url, method="GET")
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            code = resp.getcode()
    except urllib.error.HTTPError as exc:
        body = exc.read()
        code = exc.code
    elapsed_ms = (time.perf_counter() - started) * 1000
    return code, body, elapsed_ms


def run_iteration(base_url: str, provider: str, timeseries_limit: int) -> list[Sample]:
    out: list[Sample] = []
    code, body, dur = do_get(base_url, "/forecast/runs/latest", {"provider": provider})
    if code != 200:
        out.append(
            Sample(
                endpoint="/forecast/runs/latest",
                provider=provider,
                status_code=code,
                duration_ms=dur,
                payload_bytes=len(body),
                meta={"error_body": body[:200].decode("utf-8", errors="replace")},
            )
        )
        return out

    run = json.loads(body.decode("utf-8"))
    run_id = run.get("run_id", "")
    out.append(
        Sample(
            endpoint="/forecast/runs/latest",
            provider=provider,
            status_code=code,
            duration_ms=dur,
            payload_bytes=len(body),
            meta={"run_id": run_id},
        )
    )

    reach_for_detail: str | None = None
    for tier in DEFAULT_TIERS:
        code, body, dur = do_get(
            base_url,
            "/forecast/map/severity",
            {
                "provider": provider,
                "run_id": run_id,
                "min_severity_score": tier["min_severity_score"],
                "limit": tier["limit"],
            },
        )
        count = None
        if code == 200:
            payload = json.loads(body.decode("utf-8"))
            severity = payload.get("severity", {})
            count = len(severity)
            if reach_for_detail is None and severity:
                reach_for_detail = next(iter(severity.keys()))
        out.append(
            Sample(
                endpoint="/forecast/map/severity",
                provider=provider,
                status_code=code,
                duration_ms=dur,
                payload_bytes=len(body),
                count=count,
                meta={"tier": tier["name"], "run_id": run_id},
            )
        )

    if reach_for_detail and timeseries_limit > 0:
        code, body, dur = do_get(
            base_url,
            f"/forecast/reaches/{provider}/{reach_for_detail}",
            {"run_id": run_id, "timeseries_limit": timeseries_limit},
        )
        ts_count = None
        if code == 200:
            payload = json.loads(body.decode("utf-8"))
            ts_count = len(payload.get("timeseries", []))
        out.append(
            Sample(
                endpoint="/forecast/reaches/detail",
                provider=provider,
                status_code=code,
                duration_ms=dur,
                payload_bytes=len(body),
                count=ts_count,
                meta={"run_id": run_id, "reach_id": reach_for_detail},
            )
        )
    return out


def summarize(samples: list[Sample]) -> list[dict]:
    buckets: dict[tuple[str, str, str], list[Sample]] = defaultdict(list)
    for sample in samples:
        tier = ""
        if sample.meta and isinstance(sample.meta, dict):
            tier = str(sample.meta.get("tier") or "")
        key = (sample.provider, sample.endpoint, tier)
        buckets[key].append(sample)

    rows: list[dict] = []
    for (provider, endpoint, tier), vals in sorted(buckets.items()):
        ok_vals = [v for v in vals if v.status_code == 200]
        durations = [v.duration_ms for v in ok_vals]
        payloads = [float(v.payload_bytes) for v in ok_vals]
        counts = [float(v.count) for v in ok_vals if v.count is not None]
        rows.append(
            {
                "provider": provider,
                "endpoint": endpoint,
                "tier": tier or None,
                "samples_total": len(vals),
                "samples_ok": len(ok_vals),
                "latency_ms_p50": round(percentile(durations, 50), 2) if durations else None,
                "latency_ms_p95": round(percentile(durations, 95), 2) if durations else None,
                "payload_bytes_p50": int(round(percentile(payloads, 50))) if payloads else None,
                "payload_bytes_p95": int(round(percentile(payloads, 95))) if payloads else None,
                "count_p50": int(round(percentile(counts, 50))) if counts else None,
                "count_p95": int(round(percentile(counts, 95))) if counts else None,
                "errors": len(vals) - len(ok_vals),
            }
        )
    return rows


def print_summary(summary_rows: list[dict]) -> None:
    if not summary_rows:
        print("No samples collected.")
        return
    headers = [
        "provider",
        "endpoint",
        "tier",
        "samples_ok/total",
        "p50_ms",
        "p95_ms",
        "p50_bytes",
        "p95_bytes",
        "p50_count",
        "p95_count",
        "errors",
    ]
    print(" | ".join(headers))
    print("-" * 110)
    for row in summary_rows:
        print(
            " | ".join(
                [
                    row["provider"],
                    row["endpoint"],
                    str(row["tier"] or "-"),
                    f'{row["samples_ok"]}/{row["samples_total"]}',
                    str(row["latency_ms_p50"]),
                    str(row["latency_ms_p95"]),
                    str(row["payload_bytes_p50"]),
                    str(row["payload_bytes_p95"]),
                    str(row["count_p50"]),
                    str(row["count_p95"]),
                    str(row["errors"]),
                ]
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 0 baseline API checks.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Backend base URL.")
    parser.add_argument(
        "--providers",
        nargs="+",
        default=["geoglows", "glofas"],
        help="Providers to test (default: geoglows glofas).",
    )
    parser.add_argument("--iterations", type=int, default=3, help="How many passes to run.")
    parser.add_argument(
        "--timeseries-limit",
        type=int,
        default=500,
        help="Reach detail timeseries_limit. Set 0 to skip detail calls.",
    )
    parser.add_argument("--out-json", default="", help="Optional path to write full JSON report.")
    args = parser.parse_args()

    all_samples: list[Sample] = []
    print(f"Running Phase 0 baseline checks against {args.base_url}")
    for i in range(1, args.iterations + 1):
        print(f"\n--- Iteration {i}/{args.iterations} ---")
        for provider in args.providers:
            print(f"provider={provider}")
            samples = run_iteration(args.base_url, provider, args.timeseries_limit)
            all_samples.extend(samples)
            for s in samples:
                tier = ""
                if s.meta and s.meta.get("tier"):
                    tier = f" ({s.meta['tier']})"
                print(
                    f"  {s.endpoint}{tier} status={s.status_code} "
                    f"duration_ms={s.duration_ms:.2f} payload_bytes={s.payload_bytes} count={s.count}"
                )

    summary_rows = summarize(all_samples)
    print("\n=== Summary (p50/p95 from successful samples) ===")
    print_summary(summary_rows)

    if args.out_json:
        payload = {
            "generated_at_epoch": time.time(),
            "base_url": args.base_url,
            "providers": args.providers,
            "iterations": args.iterations,
            "summary": summary_rows,
            "samples": [asdict(s) for s in all_samples],
        }
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\nWrote JSON report to {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
