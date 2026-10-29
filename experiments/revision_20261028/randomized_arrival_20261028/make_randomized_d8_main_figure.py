#!/usr/bin/env python3
"""Build the d8 latency figure from audited seed 31 and repaired seed 32 values."""
import html
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent
W, H = 1380, 760
colors = {"seed 31 · exclusive (all-phase)": "#2764ae", "seed 32 · repaired NEAR / existing EVM (all-phase)": "#b45f06"}
sources = {
    "seed 31 · exclusive (all-phase)": {
        "EVM": ROOT / "formal_seed20261031/evm/d8_poisson_seed20261031_evm_evm_raw.json",
        "NEAR": ROOT / "formal_seed20261031/near_corrected/d8_poisson_seed20261031_near_near_raw.json",
    },
    "seed 32 · repaired NEAR / existing EVM (all-phase)": {
        "EVM": ROOT / "formal_seed20261032/results/evm/d8_poisson_seed20261032_evm_evm_raw.json",
        "NEAR": ROOT / "formal_seed20261032/auditfix_replay/full_replay_results/results/near/d8_poisson_seed20261032_near_auditfix_full_near_raw.json",
    },
}

def percentile(values, q):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]

data, phase_counts, goodput_counts = {}, {}, {}
for label, platform_paths in sources.items():
    for platform, path in platform_paths.items():
        raw = json.loads(path.read_text())
        for phase in raw["phases"]:
            if phase.get("warmup"):
                continue
            rate = float(phase["offered_rate_per_sec"])
            key = "offered_to_sink_ms" if platform == "EVM" else "offered_to_sink_queryable_ms"
            success_key = "succeeded" if platform == "EVM" else "execution_succeeded"
            values = [row[key] for row in phase["rows"]
                      if row.get(success_key) is True
                      and row.get("states_match_receipt_events") is True
                      and row.get(key) is not None]
            data.setdefault(rate, {}).setdefault(label, {})[platform] = (
                percentile(values, .50) / 1000,
                percentile(values, .95) / 1000,
            )
            phase_counts.setdefault(label, {}).setdefault(rate, {})[platform] = len(values)
            if label.startswith("seed 32"):
                goodput_counts.setdefault(rate, {})[platform] = int(phase.get("successful_completions_in_measurement_window", 0))
svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
       '<rect width="100%" height="100%" fill="white"/>']

def text(x, y, value, size=12, anchor="start", color="#222", weight="normal"):
    svg.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="Arial,sans-serif" font-size="{size}" font-weight="{weight}" fill="{color}">{html.escape(str(value))}</text>')

text(W / 2, 31, "Eight-stage workflow: absolute end-to-end sink latency", 21, "middle", weight="bold")
text(W / 2, 54, "All-phase fully audited successes, including warmup and drain · circles p50, triangles p95 · seconds", 12, "middle", "#555")
for panel, rate in enumerate((0.5, 1.0)):
    x0 = 35 + panel * 655
    left, right, top, bottom, xmax = x0 + 105, x0 + 625, 133, 388, 12
    svg.append(f'<rect x="{x0}" y="91" width="625" height="318" fill="white" stroke="#aaa"/>')
    text(x0 + 312, 114, f"λ = {rate:g} workflow/s", 15, "middle", weight="bold")
    for tick in range(0, 13, 2):
        x = left + tick / xmax * (right - left)
        svg.append(f'<path d="M{x},{top} V{bottom}" stroke="#e6e6e6"/>')
        text(x, bottom + 18, tick, 10, "middle", "#555")
    text((left + right) / 2, bottom + 38, "Latency (seconds)", 11, "middle", "#555")
    rows = (("EVM", "p50", 190), ("EVM", "p95", 230), ("NEAR", "p50", 310), ("NEAR", "p95", 350))
    for platform, percentile, y in rows:
        text(left - 12, y + 4, f"{platform} {percentile}", 11, "end", "#333")
        for idx, label in enumerate(colors):
            value = data[rate][label][platform][0 if percentile == "p50" else 1]
            yy = y + (idx - 0.5) * 14
            xx = left + value / xmax * (right - left)
            color = colors[label]
            if percentile == "p50":
                svg.append(f'<circle cx="{xx:.2f}" cy="{yy:.2f}" r="5.5" fill="{color}"/>')
            else:
                svg.append(f'<path d="M{xx:.2f},{yy-6:.2f} L{xx+6:.2f},{yy+5:.2f} L{xx-6:.2f},{yy+5:.2f} Z" fill="{color}"/>')
            text(xx + 7, yy - 4, f"{value:.2f}", 9, color=color)

for idx, (label, color) in enumerate(colors.items()):
    x = 175 + idx * 520
    svg.append(f'<circle cx="{x}" cy="438" r="6" fill="{color}"/>')
    text(x + 14, 442, label, 12)

text(40, 486, "Audit denominators and cohort definitions", 14, weight="bold")
text(40, 510, "All displayed p50/p95 use all fully audited successes in each full phase, including warmup and drain completions; they are not observation-window-only percentiles.", 12)
text(40, 532, f"Latency n per platform, λ=.5 / 1.0: seed 31 {phase_counts[next(iter(colors))][.5]['EVM']} / {phase_counts[next(iter(colors))][1.0]['EVM']}; seed 32 {phase_counts[list(colors)[1]][.5]['EVM']} / {phase_counts[list(colors)[1]][1.0]['EVM']}.", 12)
text(40, 554, f"Goodput uses successful sink completions within the 780 s observation window: seed 32 EVM {goodput_counts[.5]['EVM']} / {goodput_counts[1.0]['EVM']}, NEAR {goodput_counts[.5]['NEAR']} / {goodput_counts[1.0]['NEAR']} (λ=.5 / 1.0).", 12)
text(40, 576, "Seed 32 repaired replay: NEAR 385/385 and 817/817 audited; 0 failures, 0 pending. EVM is the existing same-schedule baseline; only NEAR was replayed.", 12)
text(40, 598, "Historical seed 32 NEAR before repair: 353 audited + 10 failure labels + 22 pending (λ=.5); 767 + 16 + 34 (λ=1). Kept separate. Seed 30 overlapped unrelated NEAR work.", 12)
text(40, 620, "Workload: 8 stateful contracts, 7 dependent edges, 16 operations per stage; 30 s preheat + 780 s observation. The local runtime latency gap is not a DAS causal estimate.", 12)
text(40, 642, "Do not extrapolate d8 to a 20-contract cascade: depth, receipt/block progression, overlap, contention, and critical path may change.", 12, weight="bold")
text(40, 676, "Seed 31/32 are not three fresh same-condition repeats · NEAR 2.13.4 / protocol 86 · cleanup_status=forced_after_timeout", 11, color="#555")
svg.append("</svg>")
(ROOT / "randomized_d8_main_figure.svg").write_text("\n".join(svg) + "\n")
print(ROOT / "randomized_d8_main_figure.svg")
