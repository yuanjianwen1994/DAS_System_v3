#!/usr/bin/env python3
"""Create a dependency-free SVG for the d4 RPC-path diagnostic."""
from __future__ import annotations

import html
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WIDTH, HEIGHT = 1720, 980
RUNS = [
    ("EVM per-request poll", "rpc_light_d4_r6", "rpc_light_d4_r6_evm_raw.json", "#d95f02"),
    ("EVM batched receipt poll", "rpc_batch_d4_r8", "rpc_batch_d4_r8_evm_raw.json", "#1f77b4"),
    ("EVM batch + maxTx=1000", "anvil_max1000_d4_r10", "anvil_max1000_d4_r10_evm_raw.json", "#9467bd"),
    ("NEAR Sandbox", "rpc_light_d4_r6", "rpc_light_d4_r6_near_raw.json", "#008b8b"),
]


def esc(value):
    return html.escape(str(value), quote=True)


def svg_text(x, y, value, size=16, anchor="middle", fill="#253238", weight="normal", rotate=None):
    transform = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate else ""
    return (f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="Arial,Helvetica,sans-serif" '
            f'font-size="{size}px" font-weight="{weight}" fill="{fill}"{transform}>{esc(value)}</text>')


def percentile(values, q):
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * q)]


def load_run(label, folder, filename, color):
    data = json.loads((ROOT / folder / filename).read_text())
    output = {"label": label, "color": color, "platform": data["platform"], "points": []}
    for phase in data["phases"]:
        if phase.get("warmup"):
            continue
        rows = phase["rows"]
        latency_key = "offered_to_sink_ms" if data["platform"].startswith("EVM") else "offered_to_sink_queryable_ms"
        latencies = [row[latency_key] for row in rows if row.get(latency_key) is not None]
        if data["platform"].startswith("EVM"):
            completed = phase["successful_completions_in_measurement_window"]
        else:
            completed = phase["successful_completions_in_measurement_window"]
        output["points"].append({
            "offered": float(phase["offered_rate_per_sec"]),
            "goodput": completed / 30.0,
            "p50": percentile(latencies, 0.50) / 1000.0,
            "p95": percentile(latencies, 0.95) / 1000.0,
            "audited": sum(bool(row.get("succeeded")) for row in rows),
            "rows": len(rows),
        })
    return output


def main():
    runs = [load_run(*entry) for entry in RUNS]
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>.grid{stroke:#e2e7e9;stroke-width:1}.axis{stroke:#526168;stroke-width:1.5}.dash{stroke-dasharray:7 5}</style>',
           svg_text(WIDTH / 2, 42, "d4 RPC-path diagnostic: complete-workflow throughput and latency", 28, weight="bold")]
    panels = [
        {"x": 140, "y": 115, "w": 650, "h": 560, "title": "Completed workflow goodput in the 30 s window", "unit": "workflows / second", "kind": "goodput"},
        {"x": 1000, "y": 115, "w": 600, "h": 560, "title": "End-to-end time to a queryable sink", "unit": "seconds (log scale)", "kind": "latency"},
    ]
    for panel in panels:
        x0, y0, w, h = panel["x"], panel["y"], panel["w"], panel["h"]
        svg.append(svg_text(x0 + w / 2, y0 - 26, panel["title"], 20, weight="bold"))
        for offered in (8, 16):
            x = x0 + (offered - 8) / 8 * w
            svg.append(f'<line class="grid" x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y0+h}"/>')
            svg.append(svg_text(x, y0 + h + 26, str(offered), 15))
        if panel["kind"] == "goodput":
            ticks = [0, 2, 4, 6, 8, 10, 12, 14, 16]
            ymap = lambda value: y0 + h - value / 16 * h
            for tick in ticks:
                y = ymap(tick)
                svg.append(f'<line class="grid" x1="{x0}" y1="{y:.1f}" x2="{x0+w}" y2="{y:.1f}"/>')
                svg.append(svg_text(x0 - 16, y + 5, tick, 14, anchor="end"))
        else:
            ticks = [4, 5, 6, 10, 20, 50, 100]
            low, high = 4, 100
            ymap = lambda value: y0 + h - (math.log10(value) - math.log10(low)) / (math.log10(high) - math.log10(low)) * h
            for tick in ticks:
                y = ymap(tick)
                svg.append(f'<line class="grid" x1="{x0}" y1="{y:.1f}" x2="{x0+w}" y2="{y:.1f}"/>')
                svg.append(svg_text(x0 - 16, y + 5, tick, 14, anchor="end"))
        svg.append(f'<line class="axis" x1="{x0}" y1="{y0+h}" x2="{x0+w}" y2="{y0+h}"/>')
        svg.append(f'<line class="axis" x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0+h}"/>')
        svg.append(svg_text(x0 + w / 2, y0 + h + 56, "Offered load (workflows / second)", 15))
        svg.append(svg_text(x0 - 74, y0 + h / 2, panel["unit"], 14, rotate="-90"))
        for run in runs:
            points = sorted(run["points"], key=lambda item: item["offered"])
            xys = [(x0 + (point["offered"] - 8) / 8 * w,
                    ymap(point["goodput"] if panel["kind"] == "goodput" else point["p50"])) for point in points]
            if len(xys) > 1:
                joined = " ".join(f"{x:.1f},{y:.1f}" for x, y in xys)
                svg.append(f'<polyline points="{joined}" fill="none" stroke="{run["color"]}" stroke-width="3"/>')
            for (x, y), point in zip(xys, points):
                value = point["goodput"] if panel["kind"] == "goodput" else point["p50"]
                title = f'{run["label"]}; offered {point["offered"]}/s; value {value:.2f}; audits {point["audited"]}/{point["rows"]}'
                svg.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="7" fill="{run["color"]}" stroke="white" stroke-width="2"><title>{esc(title)}</title></circle>')
                if panel["kind"] == "goodput":
                    svg.append(svg_text(x, y - 12, f'{value:.2f}', 13, fill=run["color"], weight="bold"))
                else:
                    p95y = ymap(point["p95"])
                    svg.append(f'<line class="dash" x1="{x:.1f}" y1="{y:.1f}" x2="{x:.1f}" y2="{p95y:.1f}" stroke="{run["color"]}" stroke-width="2" opacity="0.65"/>')
                    svg.append(f'<circle cx="{x:.1f}" cy="{p95y:.1f}" r="5" fill="white" stroke="{run["color"]}" stroke-width="2"><title>{esc(run["label"])} p95 {point["p95"]:.2f}s</title></circle>')
    legend_y = 755
    for idx, run in enumerate(runs):
        x = 155 + (idx % 2) * 570
        y = legend_y + (idx // 2) * 34
        svg.append(f'<line x1="{x}" y1="{y-5}" x2="{x+45}" y2="{y-5}" stroke="{run["color"]}" stroke-width="3"/>')
        svg.append(svg_text(x + 58, y, run["label"], 16, anchor="start"))
    svg.append(svg_text(WIDTH / 2, 865, "Points are one run per platform and condition; no error bars. All 840 workflow rows per run set (warmup plus measurement) passed post-drain audits.", 15))
    svg.append(svg_text(WIDTH / 2, 894, "Each point counts a completed end-to-end workflow. Results are specific to d4, 16 operations/stage, 1 s blocks, and this host/runtime.", 15))
    svg.append('</svg>')
    output = ROOT / "rpc_diagnostic_d4.svg"
    output.write_text("\n".join(svg) + "\n")
    print(output)


if __name__ == "__main__":
    main()
