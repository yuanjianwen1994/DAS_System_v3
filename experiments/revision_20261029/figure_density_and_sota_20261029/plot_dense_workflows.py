#!/usr/bin/env python3
"""Replot existing audited observations; no blockchain or workload execution."""
import csv
import hashlib
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/das-paper-figure-mpl")
sys.path.insert(0, "/private/tmp/das-root-plot-libs")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

OUT = Path(__file__).resolve().parent
EXPERIMENTS = OUT.parent.parent
SOURCE = EXPERIMENTS / "revision_20261028/figure_validation_chain_20261028"
COLORS = {"evm": "#BE8200", "near": "#2586B2"}
plt.rcParams.update({"font.family": "Times New Roman", "font.size": 9,
    "axes.labelsize": 9, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "legend.fontsize": 7.4, "axes.linewidth": .65,
    "axes.spines.top": False, "axes.spines.right": False,
    "pdf.fonttype": 42, "svg.fonttype": "none"})

def ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def main():
    records, hashes = [], {}
    for seed in (20261033, 20261034):
        pair_hashes = []
        for vm in ("evm", "near"):
            path = SOURCE / f"results/seed{seed}/{vm}/d8_lambda1_seed{seed}_{vm}_{vm}_raw.json"
            blob = path.read_bytes()
            hashes[str(path.relative_to(EXPERIMENTS))] = hashlib.sha256(blob).hexdigest()
            raw = json.loads(blob)
            pair_hashes.append(raw["schedule_sha256"])
            phase = raw["phases"][0]
            start = ts(phase["observation_start_at"])
            latency_key = "offered_to_sink_ms" if vm == "evm" else "offered_to_sink_queryable_ms"
            for row in phase["rows"]:
                assert row["succeeded"] and row["states_match_receipt_events"]
                if row["warmup"] is not False:
                    continue
                elapsed = (ts(row["planned_at"]) - start).total_seconds()
                assert -.025 <= elapsed < 360.025
                records.append({"seed": seed, "runtime": vm,
                    "workflow_id": row["workflow_id"],
                    "scheduled_observation_seconds": elapsed,
                    "offered_observation_seconds": (ts(row["offered_at"]) - start).total_seconds(),
                    "completion_latency_seconds": row[latency_key]/1000})
        assert pair_hashes[0] == pair_hashes[1]
    assert len(records) == 1376
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.95), gridspec_kw={"width_ratios": [1.25, 1, 1]})
    fig.subplots_adjust(left=.065, right=.985, top=.82, bottom=.25, wspace=.40)
    for seed, marker in [(20261033, "o"), (20261034, "^")]:
        for vm in ("evm", "near"):
            r = [x for x in records if x["seed"] == seed and x["runtime"] == vm]
            x = [a["scheduled_observation_seconds"] for a in r]
            y = [a["completion_latency_seconds"] for a in r]
            for ax in (axes[0], axes[1 if vm == "evm" else 2]):
                ax.scatter(x, y, s=7, marker=marker, c=COLORS[vm], alpha=.48,
                           edgecolors="none", rasterized=False)
    for ax in axes:
        ax.set_xlim(0, 360)
        ax.set_xticks([0, 120, 240, 360])
        ax.set_xlabel("Scheduled time (s)")
        ax.set_ylabel("Workflow latency (s)")
        ax.grid(color="#DADADA", linewidth=.4, alpha=.8)
        ax.set_axisbelow(True)
    axes[0].set_ylim(0, 11)
    axes[0].set_yticks([0, 2, 4, 6, 8, 10])
    axes[1].set_ylim(0, 2.6)
    axes[1].set_yticks([0, .5, 1, 1.5, 2, 2.5])
    near_values = [r["completion_latency_seconds"] for r in records if r["runtime"] == "near"]
    lo, hi = math.floor(min(near_values)*5)/5, math.ceil(max(near_values)*5)/5
    axes[2].set_ylim(lo-.04, hi+.04)
    axes[2].set_yticks([round(lo+i*.4, 1) for i in range(int(round((hi-lo)/.4))+1)])
    for ax, title in zip(axes, ["(a) Full latency scale", "(b) Local EVM: zoom", "(c) NEAR: zoom"]):
        ax.text(.5, -.34, title, transform=ax.transAxes, ha="center", va="top", fontsize=9)
    handles = []
    for vm, label in [("evm", "EVM"), ("near", "NEAR")]:
        for seed, marker, n in [(33, "o", 363), (34, "^", 325)]:
            handles.append(Line2D([], [], linestyle="none", marker=marker,
                color=COLORS[vm], markersize=4, label=f"{label} R{seed} (n={n})"))
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(.52, 1.0),
               ncol=4, frameon=False, columnspacing=1.0, handletextpad=.35)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(OUT / f"dense_workflow_observations.{ext}", dpi=240)
    plt.close(fig)
    with (OUT / "plotted_workflows.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]))
        writer.writeheader(); writer.writerows(records)
    manifest = {"status": "replot_of_existing_audited_observations",
        "unique_workflows_per_runtime": 688, "total_unique_plotted_records": len(records),
        "independent_seed_pairs": 2,
        "cohort": "Observation-window offers, including their post-window drain completions",
        "x_axis": "Actual archived planned_at minus observation_start_at, no jitter or resampling",
        "measurement": "EVM offered-to-sink; NEAR offered-to-sink-queryable",
        "zoom_note": "Panels b/c repeat the same measurements on explicitly different y ranges; they add no new samples",
        "raw_data_modified": False, "source_sha256": hashes}
    (OUT / "source_manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    print(json.dumps({"records": len(records), "independent_pairs": 2,
                      "near_zoom_seconds": [lo, hi]}))

if __name__ == "__main__":
    main()
