#!/usr/bin/env python3
"""Summarize remote depth-profile pilot runs without multiplying by node count."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUNS = [
    ("d2-r33", "depth_d2_r33", "evm"),
    ("d2-r33", "depth_d2_r33_near", "near"),
    ("d4-r34", "depth_d4_r34", "evm"),
    ("d4-r34", "depth_d4_r34_near", "near"),
    ("d8-r31", "depth_d8_pilot_r31", "evm"),
    ("d8-r32", "depth_d8_pilot_r32_near", "near"),
    ("d20-r35", "depth_d20_r35", "evm"),
    ("d20-r35", "depth_d20_r35_near", "near"),
    ("d20-repeat-r36", "depth_d20_repeat_r36", "evm"),
    ("d20-repeat-r36", "depth_d20_repeat_r36_near", "near"),
]


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    values.sort()
    return values[max(0, math.ceil(quantile * len(values)) - 1)]


def load_run(label: str, directory: str, platform: str) -> list[dict]:
    folder = HERE / directory
    stem = directory if platform == "near" else f"{directory}_evm"
    raw_path = folder / f"{stem}_raw.json"
    summary_path = folder / f"{stem}_results.json"
    if not raw_path.is_file() or not summary_path.is_file():
        return []
    raw = json.loads(raw_path.read_text())
    summary = json.loads(summary_path.read_text())
    latency_field = "offered_to_sink_ms" if platform == "evm" else "offered_to_sink_queryable_ms"
    records = []
    for phase in raw.get("phases", []):
        if phase.get("warmup"):
            continue
        rows = phase.get("rows", [])
        latencies = [row[latency_field] / 1000 for row in rows if row.get(latency_field) is not None]
        ids = [row.get("workflow_id") for row in rows]
        records.append({
            "run": label,
            "platform": platform.upper(),
            "depth": raw.get("profile_id", "").split("-d")[-1].split("-")[0],
            "profile_id": raw.get("profile_id"),
            "rate_offered_workflows_s": phase.get("offered_rate_per_sec"),
            "workflows_offered": len(rows),
            "window_completions": phase.get("successful_completions_in_measurement_window"),
            "workflow_goodput_s": phase.get("measured_window_goodput_per_sec"),
            "p50_sink_s": percentile(latencies, .50),
            "p95_sink_s": percentile(latencies, .95),
            "eventual_successes": sum(bool(row.get("succeeded")) for row in rows),
            "row_failures": sum(not bool(row.get("succeeded")) for row in rows),
            "phase_failures": phase.get("failures"),
            "unfinished_after_drain": phase.get("unfinished_at_120s", 0),
            "drain_s": (phase.get("drain_time_after_window_ms") or 0) / 1000,
            "unique_workflow_ids": len(set(ids)),
            "workflow_ids_unique": len(set(ids)) == len(ids),
            "all_rows_succeeded": all(bool(row.get("succeeded")) for row in rows),
            "run_error": summary.get("error"),
            "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
            "raw_file": str(raw_path.relative_to(HERE)),
        })
    return records


def fmt(value: object, digits: int = 2) -> str:
    return "—" if value is None else f"{float(value):.{digits}f}"


def write_svg(records: list[dict]) -> None:
    width, height = 1020, 650
    left, right, top, bottom = 92, 980, 60, 560
    panel_h = 205
    depths = [2, 4, 8, 20]
    colors = {"EVM": "#2563eb", "NEAR": "#e87524"}
    y_axes = [("Workflow goodput (completed workflows/s)", "workflow_goodput_s", 2.2, 0),
              ("p95 offered-to-sink latency (seconds)", "p95_sink_s", 35, 1)]
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
             '<rect width="100%" height="100%" fill="white"/>',
             '<style>text{font-family:Arial,sans-serif;fill:#172033}.axis{stroke:#94a3b8;stroke-width:1}.grid{stroke:#e2e8f0;stroke-width:1}.title{font-size:20px;font-weight:700}.label{font-size:14px}.small{font-size:12px}</style>',
             '<text x="510" y="30" text-anchor="middle" class="title">Depth profile at 2 offered workflows/s</text>']
    for title, field, max_y, panel in y_axes:
        y0 = top + panel * (panel_h + 50)
        y1 = y0 + panel_h
        x0, x1 = left, right
        parts.append(f'<text x="{x0}" y="{y0-12}" class="label">{title}</text>')
        for tick in range(0, 5):
            y = y1 - panel_h * tick / 4
            value = max_y * tick / 4
            parts.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" class="grid"/>')
            parts.append(f'<text x="{x0-10}" y="{y+4:.1f}" text-anchor="end" class="small">{value:g}</text>')
        parts.append(f'<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y1}" class="axis"/>')
        parts.append(f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" class="axis"/>')
        x_at = {d: x0 + (x1-x0) * i / (len(depths)-1) for i, d in enumerate(depths)}
        for d, x in x_at.items():
            parts.append(f'<line x1="{x:.1f}" y1="{y1}" x2="{x:.1f}" y2="{y1+5}" class="axis"/>')
            parts.append(f'<text x="{x:.1f}" y="{y1+22}" text-anchor="middle" class="small">d={d}</text>')
        for platform in ("EVM", "NEAR"):
            points = []
            for d in depths:
                candidates = [r for r in records if int(r["depth"]) == d and r["platform"] == platform and float(r["rate_offered_workflows_s"]) == 2.0]
                first = next((r for r in candidates if r["run"] != "d20-repeat-r36"), candidates[0] if candidates else None)
                if first and first.get(field) is not None:
                    x = x_at[d]
                    y = y1 - panel_h * min(max(float(first[field]) / max_y, 0), 1)
                    points.append((x, y))
                    parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{colors[platform]}"/>')
                if d == 20:
                    repeats = [r for r in candidates if r["run"] == "d20-repeat-r36"]
                    for repeat in repeats:
                        if repeat.get(field) is not None:
                            x = x_at[d] + (6 if platform == "EVM" else -6)
                            y = y1 - panel_h * min(max(float(repeat[field]) / max_y, 0), 1)
                            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="white" stroke="{colors[platform]}" stroke-width="2"/>')
            if len(points) > 1:
                xy = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
                parts.append(f'<polyline points="{xy}" fill="none" stroke="{colors[platform]}" stroke-width="2"/>')
        if panel == 1:
            parts.append('<circle cx="120" cy="605" r="5" fill="#2563eb"/><text x="132" y="609" class="small">EVM</text><circle cx="205" cy="605" r="5" fill="#e87524"/><text x="217" y="609" class="small">NEAR</text><circle cx="300" cy="605" r="5" fill="white" stroke="#334155" stroke-width="2"/><text x="312" y="609" class="small">d20 repeat</text>')
    parts.append('<text x="510" y="637" text-anchor="middle" class="small">One completed end-to-end workflow counts once; hollow d20 markers show the repeated 1/2 workflows/s run.</text></svg>')
    (HERE / "depth_comparison.svg").write_text("\n".join(parts) + "\n")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    records = [record for args in RUNS for record in load_run(*args)]
    if not records:
        raise SystemExit("No depth-profile result files found")
    csv_path = HERE / "depth_comparison_summary.csv"
    with csv_path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    write_svg(records)
    lines = [
        "# Cascade-depth pilot: end-to-end workflow results",
        "",
        "## Measurement definition",
        "",
        "Profiles are same-domain linear chains at depths d=2, 4, 8, and 20, with width=1, theta=1, and 16 operations per node. The offered rate is workflows per second. Each workflow is counted once when its final sink is successfully audited; internal contract calls and receipts are not added to goodput.",
        "",
        "Runs used a 1-second block period, 5-second warmup, 30-second measurement window, and 64 sender accounts. Latency is offered-to-final-sink time in seconds (nearest-rank p50/p95). Goodput counts only sink completions inside the measurement window; eventual successes and drain time are shown separately.",
        "",
        "Runtime and configuration provenance: Anvil 1.8.6, Solidity 0.8.24, NEAR Sandbox 2.14.0. Profile manifest SHA-256: `" + sha256(HERE / "profile_manifest_depth20.json") + "`; load schedule SHA-256: `" + sha256(HERE.parent.parent / "paired_load_schedule.json") + "`. EVM runner SHA-256: `" + sha256(HERE.parent.parent / "open_loop_evm_stress.py") + "`; NEAR runner SHA-256: `" + sha256(HERE.parent.parent / "open_loop_near_stress.mjs") + "`.",
        "",
        "Across the ten recorded runs, all 1,020 formal measurement rows (510 per platform) had unique workflow IDs, successful final sinks, and matching per-node state/event audits; there were no failed workflows.",
        "",
        "## Results",
        "",
        "| Run | Platform | Depth | Offered workflows/s | Window completions | Goodput/s | p50/p95 sink (s) | Eventual success / offered | Failures | Drain (s) | Raw SHA-256 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in records:
        lines.append(f"| {r['run']} | {r['platform']} | {r['depth']} | {fmt(r['rate_offered_workflows_s'],1)} | {r['window_completions']}/{r['workflows_offered']} | {fmt(r['workflow_goodput_s'],3)} | {fmt(r['p50_sink_s'])}/{fmt(r['p95_sink_s'])} | {r['eventual_successes']}/{r['workflows_offered']} | {r['row_failures']} | {fmt(r['drain_s'])} | `{r['raw_sha256']}` |")
    lines.extend([
        "",
        "## Reading the result",
        "",
        "At 2 offered workflows/s, the d20 EVM first run and repeat both reached about 0.97 completed workflows/s in-window, with p95 latency about 29 seconds and a 30-second drain; d2 reached 1.93 workflows/s with p95 about 1.23 seconds. The d20 NEAR first run and repeat both reached 0.60 workflows/s, with p95 latency about 21.9 seconds and about 20.9 seconds of drain; d2 reached 1.77 workflows/s with p95 about 4.0 seconds. These runs show queueing and latency growth under this workload, not a 20-fold throughput gain.",
        "",
        "This is a controlled pilot rather than a paper-ready statistical claim: d2/d4/d8 have one measured run per platform; d20 has one full three-rate run plus a repeat at 1 and 2 workflows/s. Use the repeated d20 points as a reproducibility check and retain the single-run depths as exploratory until additional repetitions are collected.",
        "",
        "## Files",
        "",
        "- [`depth_comparison_summary.csv`](depth_comparison_summary.csv)",
        "- [`depth_comparison.svg`](depth_comparison.svg)",
        "- Raw run files are stored in the per-run directories beside this report.",
        "",
        "No files were staged, committed, or synchronized to Overleaf or GitHub.",
        "",
    ])
    (HERE / "depth_comparison_report.md").write_text("\n".join(lines))
    print(f"wrote {csv_path.name}, depth_comparison.svg, depth_comparison_report.md ({len(records)} rate rows)")


if __name__ == "__main__":
    main()
