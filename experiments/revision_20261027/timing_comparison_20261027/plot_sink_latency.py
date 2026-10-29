"""Plot retained local-sandbox sink-query timings; no workloads are started."""
import csv
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/das-timing-chart-mpl-cache")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
with (ROOT / "timing_summary.csv").open() as f:
    rows = list(csv.DictReader(f))
plt.rcParams["svg.fonttype"] = "none"
fig, ax = plt.subplots(figsize=(7.8, 4.8))
colors = {"NEAR Sandbox": "#D76B1E", "EVM / Anvil": "#1574B3"}
offsets = {"NEAR Sandbox": -0.08, "EVM / Anvil": 0.08}
for platform in ("NEAR Sandbox", "EVM / Anvil"):
    selected = [r for r in rows if r["platform"] == platform]
    depths = [int(r["depth"]) for r in selected]
    medians = [float(r["median_ms"]) / 1000 for r in selected]
    lows = [float(r["min_ms"]) / 1000 for r in selected]
    highs = [float(r["max_ms"]) / 1000 for r in selected]
    lower = [m - lo for m, lo in zip(medians, lows)]
    upper = [hi - m for m, hi in zip(medians, highs)]
    ax.errorbar([d + offsets[platform] for d in depths], medians, yerr=[lower, upper],
        fmt="o-", linewidth=2.3, markersize=7, capsize=4, color=colors[platform], label=platform)
ax.set(title="Sink-state query latency in local sandboxes", xlabel="Serial DAG depth (independent contracts)",
    ylabel="Client call start → sink first queryable (s)", xticks=[2, 4, 8])
ax.text(.01, -.24, "Median and min–max, n=6 successful sessions per point; nominal 1 s block interval.\n"
    "Sandbox results only. Execution/commit failure semantics differ across platforms.",
    transform=ax.transAxes, fontsize=9, color="#444444")
ax.grid(axis="y", alpha=.2)
ax.spines[["top", "right"]].set_visible(False)
ax.legend(frameon=False)
fig.tight_layout()
fig.savefig(ROOT / "sink_latency_comparison.png", dpi=180, facecolor="white", bbox_inches="tight")
fig.savefig(ROOT / "sink_latency_comparison.svg", facecolor="white", bbox_inches="tight")
