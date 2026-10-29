#!/usr/bin/env python3
"""Create a dependency-free, vector SVG of paired workflow goodput and latency."""
from __future__ import annotations

import html
import json
import math
import sys
from pathlib import Path

WIDTH, HEIGHT = 1600, 900
COLORS = {"NEAR": "#087f8c", "EVM": "#d9480f"}
MARKERS = {"low": "circle", "pilot": "triangle", "steady": "square"}
X_TICKS = [0.25, 0.5, 1, 2, 4, 8, 16]


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def load(paths: list[Path]) -> list[dict]:
    rows = []
    for path in paths:
        data = json.loads(path.read_text())
        for row in data["metrics"]:
            row["scenario_group"] = "low" if row["phase"] < 10 else ("pilot" if row["phase"] < 20 else "steady")
            rows.append(row)
    return rows


def log_x(value: float, x0: float, width: float) -> float:
    return x0 + (math.log2(value) + 2) / 6 * width


def log_y(value: float, y0: float, height: float, low: float, high: float) -> float:
    value = max(low, min(high, value))
    return y0 + height - (math.log10(value) - math.log10(low)) / (math.log10(high) - math.log10(low)) * height


def marker(kind: str, x: float, y: float, color: str, label: str = "") -> str:
    if kind == "circle":
        return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{color}" stroke="white" stroke-width="1.5"><title>{esc(label)}</title></circle>'
    if kind == "triangle":
        pts = f"{x:.1f},{y-7:.1f} {x-7:.1f},{y+6:.1f} {x+7:.1f},{y+6:.1f}"
        return f'<polygon points="{pts}" fill="{color}" stroke="white" stroke-width="1.5"><title>{esc(label)}</title></polygon>'
    return f'<rect x="{x-6:.1f}" y="{y-6:.1f}" width="12" height="12" rx="1" fill="{color}" stroke="white" stroke-width="1.5"><title>{esc(label)}</title></rect>'


def text(x: float, y: float, value: str, size: int = 15, anchor: str = "middle", weight: str = "normal", fill: str = "#263238", rotate: str = "") -> str:
    transform = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate else ""
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="Arial,Helvetica,sans-serif" font-size="{size}px" font-weight="{weight}" fill="{fill}"{transform}>{esc(value)}</text>'


def main(paths: list[Path], output: Path) -> None:
    rows = load(paths)
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<style>text{font-variant-numeric:tabular-nums} .grid{stroke:#dfe5e8;stroke-width:1} .axis{stroke:#46545b;stroke-width:1.5}</style>',
           text(WIDTH/2, 40, "Paired load: complete workflow goodput and end-to-end latency", 26, weight="bold")]
    panels = [
        {"x": 110, "y": 100, "w": 630, "h": 560, "title": "Successful workflow goodput", "unit": "workflows / second", "kind": "goodput_per_sec"},
        {"x": 870, "y": 100, "w": 630, "h": 560, "title": "End-to-end latency to sink", "unit": "seconds (log scale)", "kind": "latency"},
    ]
    for panel in panels:
        x0, y0, w, h = panel["x"], panel["y"], panel["w"], panel["h"]
        svg.append(text(x0+w/2, y0-25, panel["title"], 20, weight="bold"))
        for tick in X_TICKS:
            x = log_x(tick, x0, w)
            svg.append(f'<line class="grid" x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y0+h}"/>')
            svg.append(text(x, y0+h+25, str(tick), 14))
        if panel["kind"] == "goodput_per_sec":
            ticks = [0, 2, 4, 6, 8, 10, 12, 14, 16]
            ymap = lambda v: y0+h-(v/16)*h
            for tick in ticks:
                y = ymap(tick)
                svg.append(f'<line class="grid" x1="{x0}" y1="{y:.1f}" x2="{x0+w}" y2="{y:.1f}"/>')
                svg.append(text(x0-14, y+5, str(tick), 13, anchor="end"))
        else:
            ticks = [0.5, 1, 2, 5, 10, 20, 50, 100]
            ymap = lambda v: log_y(v, y0, h, 0.3, 100)
            for tick in ticks:
                y = ymap(tick)
                svg.append(f'<line class="grid" x1="{x0}" y1="{y:.1f}" x2="{x0+w}" y2="{y:.1f}"/>')
                svg.append(text(x0-14, y+5, str(tick), 13, anchor="end"))
        svg.append(f'<line class="axis" x1="{x0}" y1="{y0+h}" x2="{x0+w}" y2="{y0+h}"/>')
        svg.append(f'<line class="axis" x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0+h}"/>')
        svg.append(text(x0+w/2, y0+h+54, "Offered load (workflows / second, log2 scale)", 15))
        svg.append(text(x0-68, y0+h/2, panel["unit"], 14, rotate="-90"))

        for platform in ("NEAR", "EVM"):
            for group in ("low", "pilot", "steady"):
                subset = sorted((r for r in rows if r["platform"] == platform and r["scenario_group"] == group), key=lambda r: r["offered_rate_per_sec"])
                if not subset:
                    continue
                color = COLORS[platform]
                for metric_name, dash, opacity in (("goodput_per_sec", "", 1), ("latency_p50_ms", "", 1), ("latency_p95_ms", "6 5", .62)):
                    if panel["kind"] == "goodput_per_sec":
                        if metric_name != "goodput_per_sec":
                            continue
                        coords = [(log_x(r["offered_rate_per_sec"], x0, w), ymap(r["goodput_per_sec"])) for r in subset]
                    else:
                        if metric_name == "goodput_per_sec":
                            continue
                        factor = 1000
                        coords = [(log_x(r["offered_rate_per_sec"], x0, w), ymap(r[metric_name]/factor)) for r in subset if r[metric_name] is not None]
                    if len(coords) > 1:
                        points = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
                        svg.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.6" stroke-dasharray="{dash}" opacity="{opacity}"/>')
                    for point, row in zip(coords, subset):
                        offered = row["offered_rate_per_sec"]
                        label = f"{platform}, {group}, offered {offered}/s"
                        if panel["kind"] == "goodput_per_sec":
                            label += ", window goodput {:.2f}/s".format(row["goodput_per_sec"])
                        else:
                            label += ", p50 {:.0f}ms, p95 {:.0f}ms".format(row["latency_p50_ms"], row["latency_p95_ms"])
                        svg.append(marker(MARKERS[group], point[0], point[1], color, label))

    # Compact legends: color is platform; marker is measurement schedule; line is latency percentile.
    legend_y = 715
    svg.append(text(150, legend_y, "Platform", 15, anchor="start", weight="bold"))
    for i, platform in enumerate(("NEAR", "EVM")):
        x = 235 + i*150
        svg.append(f'<line x1="{x}" y1="{legend_y-5}" x2="{x+34}" y2="{legend_y-5}" stroke="{COLORS[platform]}" stroke-width="3"/>')
        svg.append(text(x+44, legend_y, platform, 14, anchor="start"))
    svg.append(text(600, legend_y, "Schedule", 15, anchor="start", weight="bold"))
    legend_items = [("low", "20s window"), ("pilot", "10s window"), ("steady", "30s window")]
    for i, (group, label) in enumerate(legend_items):
        x = 700 + i*175
        svg.append(marker(MARKERS[group], x, legend_y-5, "#455a64", label))
        svg.append(text(x+15, legend_y, label, 14, anchor="start"))
    svg.append(f'<line x1="1250" y1="{legend_y-5}" x2="1285" y2="{legend_y-5}" stroke="#455a64" stroke-width="2.4"/>')
    svg.append(text(1295, legend_y, "p50", 14, anchor="start"))
    svg.append(f'<line x1="1370" y1="{legend_y-5}" x2="1405" y2="{legend_y-5}" stroke="#455a64" stroke-width="2.4" stroke-dasharray="6 5"/>')
    svg.append(text(1415, legend_y, "p95", 14, anchor="start"))
    svg.append(text(WIDTH/2, 770, "Each point counts one completed four-stage workflow; each stage runs 16 operations. Local NEAR Sandbox 2.14.0 and Anvil 1.8.6, 1 s blocks; one run per point.", 14))
    svg.append(text(WIDTH/2, 798, "High-rate windows show only completions inside the measurement window; all submitted workflows were later drained successfully. Results do not support linear extrapolation by cascade depth.", 14))
    svg.append('</svg>')
    output.write_text("\n".join(svg) + "\n")
    print(output)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise SystemExit("usage: plot_paired_load_svg.py AUDIT_JSON [AUDIT_JSON ...] OUTPUT.svg")
    main([Path(x) for x in sys.argv[1:-1]], Path(sys.argv[-1]))
