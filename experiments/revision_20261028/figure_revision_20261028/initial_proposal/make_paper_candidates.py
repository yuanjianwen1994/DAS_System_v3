#!/usr/bin/env python3
"""Paper-size figure proposals from immutable archived measurements.

This performs lightweight local analysis/plotting, never an experiment.
All original figure PDFs and manuscript sources remain unchanged.
"""
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import sys

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/das-paper-figure-mpl")
if Path("/private/tmp/das-root-plot-libs").exists():
    sys.path.insert(0, "/private/tmp/das-root-plot-libs")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle, Patch
import numpy as np

OUT = Path(__file__).resolve().parent
REV = OUT.parent
RANDOM = REV / "randomized_arrival_20261028"
TOPO = REV / "controller2_topology_20261028"
BURST = REV / "controller2_burst_20261028"
SOURCES = {
    ("R31", "EVM"): RANDOM / "formal_seed20261031/evm/d8_poisson_seed20261031_evm_evm_raw.json",
    ("R31", "NEAR"): RANDOM / "formal_seed20261031/near_corrected/d8_poisson_seed20261031_near_near_raw.json",
    ("R32", "EVM"): RANDOM / "formal_seed20261032/results/evm/d8_poisson_seed20261032_evm_evm_raw.json",
    ("R32", "NEAR"): RANDOM / "formal_seed20261032/auditfix_replay/full_replay_results/verified_archive_contents/results/near/d8_poisson_seed20261032_near_auditfix_full_near_raw.json",
}
C = {"EVM": "#BE8200", "NEAR": "#2586B2", "same": "#78659B"}
plt.rcParams.update({
    "font.family": "Times New Roman", "mathtext.fontset": "stix",
    "font.size": 9, "axes.labelsize": 9.5, "xtick.labelsize": 8.5,
    "ytick.labelsize": 8.5, "legend.fontsize": 8,
    "axes.linewidth": .65, "lines.linewidth": 1.25,
    "xtick.major.width": .6, "ytick.major.width": .6,
    "xtick.major.size": 3, "ytick.major.size": 3,
    "pdf.fonttype": 42, "ps.fonttype": 42, "svg.fonttype": "none",
    "savefig.facecolor": "white", "axes.spines.top": False,
    "axes.spines.right": False,
})


def q(values, quantile):
    a = sorted(values)
    return a[max(0, math.ceil(quantile * len(a)) - 1)]


def read_chain():
    data, exports, hashes = {}, [], {}
    for (run, runtime), path in SOURCES.items():
        raw_bytes = path.read_bytes()
        hashes[str(path.relative_to(REV))] = hashlib.sha256(raw_bytes).hexdigest()
        raw = json.loads(raw_bytes)
        for phase in raw["phases"]:
            if phase.get("warmup"):
                continue
            rate = float(phase["offered_rate_per_sec"])
            key = "offered_to_sink_ms" if runtime == "EVM" else "offered_to_sink_queryable_ms"
            success_key = "succeeded" if runtime == "EVM" else "execution_succeeded"
            rows = phase["rows"]
            assert all(r.get(success_key) and r.get("states_match_receipt_events") for r in rows), (run, runtime, rate)
            values = [float(row[key]) / 1000 for row in rows]
            assert len(values) == int(phase["offered"])
            data[run, runtime, rate] = {
                "values": values, "p50": q(values, .5), "p95": q(values, .95),
                "goodput": float(phase["measured_window_goodput_per_sec"]),
                "n": len(values), "window_n": int(phase["successful_completions_in_measurement_window"]),
            }
            for row, latency in zip(rows, values):
                exports.append({"run": run, "runtime": runtime, "target_rate": rate,
                                "workflow_id": row["workflow_id"], "latency_seconds": latency,
                                "cohort": "all_phase_audited_including_warmup_and_drain"})
    with (OUT / "chain_samples_used.csv").open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(exports[0]))
        writer.writeheader(); writer.writerows(exports)
    return data, hashes


def style(ax, grid="y"):
    ax.grid(axis=grid, color="#DADADA", linewidth=.45, alpha=.8)
    ax.set_axisbelow(True)


def panel_label(fig, x, text):
    fig.text(x, .035, text, ha="center", va="bottom", fontsize=9.5)


def save(fig, stem):
    fig.savefig(OUT / (stem + ".pdf"))
    fig.savefig(OUT / (stem + ".svg"))
    fig.savefig(OUT / (stem + ".png"), dpi=220)
    plt.close(fig)


def overall(data):
    fig, axes = plt.subplots(1, 2, figsize=(7.16, 2.78), gridspec_kw={"width_ratios": [1.5, 1]})
    fig.subplots_adjust(left=.105, right=.985, bottom=.25, top=.84, wspace=.34)
    groups = [("R31", .5), ("R32", .5), ("R31", 1.), ("R32", 1.)]
    y = [3, 2, 1, 0]
    for yy, (run, rate) in zip(y, groups):
        for runtime, dy, marker in [("EVM", -.12, "o"), ("NEAR", .12, "s")]:
            d = data[run, runtime, rate]
            axes[0].plot([d["p50"], d["p95"]], [yy+dy]*2, color=C[runtime], lw=2.0)
            axes[0].plot(d["p50"], yy+dy, marker, color=C[runtime], ms=4)
            axes[0].plot(d["p95"], yy+dy, "|", color=C[runtime], ms=8, mew=1.2)
            axes[1].plot(d["goodput"], yy+dy, marker, color=C[runtime], ms=4)
        axes[1].plot([rate, rate], [yy-.34, yy+.34], color="#888888", lw=.8, ls=":")
    axes[0].set_yticks(y, [r"$\lambda$=0.5, " + run if rate == .5 else r"$\lambda$=1.0, " + run for run, rate in groups])
    axes[1].set_yticks(y, [])
    axes[0].set_xlim(0, 11); axes[0].set_xticks([0, 2, 4, 6, 8, 10])
    axes[1].set_xlim(0, 1.13); axes[1].set_xticks([0, .5, 1.])
    axes[0].set_xlabel("Workflow completion latency (s)")
    axes[1].set_xlabel("Completed workflows / s")
    for ax in axes:
        ax.set_ylim(-.5, 3.5); style(ax, "x")
    fig.legend(handles=[Line2D([], [], marker="o", color=C["EVM"], label="Local EVM composition"),
                        Line2D([], [], marker="s", color=C["NEAR"], label="NEAR cross-contract")],
               loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(.55, 1.01))
    axes[0].text(.02, 1.02, "Point: median; endpoint: P95", transform=axes[0].transAxes, fontsize=8)
    axes[1].text(.02, 1.02, "Dotted marks: target arrival rate", transform=axes[1].transAxes, fontsize=8)
    panel_label(fig, .34, "(a) Latency by run and offered load")
    panel_label(fig, .81, "(b) Observed completion rate")
    save(fig, "candidate_fig4_measured_outcomes")


def distribution(data):
    fig, axes = plt.subplots(1, 2, figsize=(7.16, 2.93), gridspec_kw={"width_ratios": [1.12, 1]})
    fig.subplots_adjust(left=.085, right=.985, bottom=.25, top=.84, wspace=.30)
    for runtime in ["EVM", "NEAR"]:
        for run, linestyle in [("R31", "-"), ("R32", "--")]:
            a = np.sort(data[run, runtime, 1.]["values"])
            axes[0].step(a, np.arange(1, len(a)+1)/len(a), where="post", color=C[runtime], ls=linestyle, lw=1.2)
    axes[0].set_xlim(0, 11); axes[0].set_ylim(0, 1.02)
    axes[0].set_xticks([0, 2, 4, 6, 8, 10]); axes[0].set_yticks([0, .25, .5, .75, 1.])
    axes[0].set_xlabel("Workflow completion latency (s)"); axes[0].set_ylabel("Empirical cumulative probability")
    style(axes[0], "both")
    axes[0].text(.45, .62, "R31: solid\nR32: dashed", transform=axes[0].transAxes, va="top", fontsize=8)
    burst_rows = list(csv.DictReader((BURST / "formal_seed_summary.csv").open()))
    routes = [("EVM independent Stage", C["EVM"]), ("NEAR independent contracts", C["NEAR"]),
              ("NEAR same receiver control", C["same"])]
    actual_routes = sorted({r["route"] for r in burst_rows})
    # Preserve the exact source label instead of guessing a control spelling.
    routes[2] = (next(r for r in actual_routes if "same" in r.lower()), C["same"])
    for route, color in routes:
        for seed, marker in zip(sorted({r["seed"] for r in burst_rows}), ["o", "s", "^"]):
            pair = [next(r for r in burst_rows if r["seed"] == seed and r["route"] == route and r["scenario"] == scenario)
                    for scenario in ["poisson_d8", "burst_d8"]]
            axes[1].plot([0, 1], [float(r["p50_offer_to_sink_ms"])/1000 for r in pair], color=color,
                         marker=marker, ms=3.5, lw=.85, alpha=.9)
    axes[1].set_xlim(-.2, 1.2); axes[1].set_xticks([0, 1], ["Random arrivals", "Periodic bursts"])
    axes[1].set_ylim(0, 11); axes[1].set_yticks([0, 2, 4, 6, 8, 10]); axes[1].set_ylabel("Median workflow latency (s)")
    style(axes[1])
    axes[1].text(.03, 1.02, "Three seeds; exploratory parallel runs", transform=axes[1].transAxes, fontsize=8)
    fig.legend(handles=[Line2D([], [], color=C["EVM"], label="Local EVM composition"),
                        Line2D([], [], color=C["NEAR"], label="NEAR cross-contract"),
                        Line2D([], [], color=C["same"], label="NEAR same receiver")],
               ncol=3, frameon=False, loc="upper center", bbox_to_anchor=(.54, 1.005), columnspacing=1.0)
    panel_label(fig, .29, "(a) Random arrivals: full latency distributions")
    panel_label(fig, .785, "(b) Sensitivity to burst arrivals")
    save(fig, "candidate_fig5_distribution_robustness")


def topology():
    summary = json.loads((TOPO / "results/pilot/routeC_summary.json").read_text())
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.95), gridspec_kw={"width_ratios": [1, 1, 1.12]})
    fig.subplots_adjust(left=.075, right=.99, bottom=.27, top=.88, wspace=.45)
    rows = summary["latency_per_repeat"]
    for repeat, marker in zip([1, 2, 3], ["o", "s", "^"]):
        vals = [next(r["near_receipt_tree_ms"] for r in rows if r["platform"] == "NEAR" and r["repeat"] == repeat and r["topology"] == t)/1000
                for t in ["chain", "fanout6_join"]]
        axes[0].plot([0, 1], vals, color=C["NEAR"], lw=.85, marker=marker, ms=3.5)
    axes[0].set_xticks([0, 1], ["Chain", "Fan-out /\njoin"]); axes[0].set_xlim(-.25, 1.25)
    axes[0].set_ylim(0, 1.5); axes[0].set_ylabel("Receipt-tree completion time (s)")
    axes[0].text(.5, 1.02, "NEAR native block setting", transform=axes[0].transAxes, ha="center", fontsize=8)
    style(axes[0])
    sm = summary["native_cost_summaries_median_min_max"]
    # Cost changes are within one VM; never divide NEAR gas by EVM gas.
    ratio_specs = [("NEAR gas", "NEAR_fanout6_join_workflow_native_gas_tgas", "NEAR_chain_workflow_native_gas_tgas", C["NEAR"]),
                   ("NEAR receipts", "NEAR_fanout6_join_workflow_receipts", "NEAR_chain_workflow_receipts", C["NEAR"]),
                   ("EVM gas", "EVM_fanout6_join_workflow_native_gas", "EVM_chain_workflow_native_gas", C["EVM"])]
    values = [100*(sm[a]["median"]/sm[b]["median"]-1) for _, a, b, _ in ratio_specs]
    axes[1].barh([2, 1, 0], values, height=.5, color=[r[3] for r in ratio_specs], edgecolor="#333333", lw=.5)
    axes[1].set_yticks([2, 1, 0], ["NEAR gas", "NEAR\nreceipts", "EVM gas"])
    axes[1].set_xlim(0, 100); axes[1].set_xticks([0, 50, 100]); axes[1].set_xlabel("Increase over chain (%)")
    for yy, value in zip([2, 1, 0], values):
        axes[1].text(value+2, yy, f"{value:.1f}%", va="center", fontsize=8)
    style(axes[1], "x")
    ax = axes[2]
    persisted = {0, 1, 2, 3, 5, 6}
    for row in [0, 1]:
        for node in range(8):
            fill = C["NEAR"] if row == 1 and node in persisted else "white"
            ax.add_patch(Rectangle((node, row), .88, .70, facecolor=fill, edgecolor="#333333", lw=.6))
            if row == 1 and node == 4:
                ax.text(node+.44, row+.35, "×", ha="center", va="center", fontsize=12)
    ax.set_xticks(np.arange(8)+.44, [str(i) for i in range(8)])
    ax.set_yticks([.35, 1.35], ["EVM", "NEAR"])
    ax.set_xlim(-.05, 7.95); ax.set_ylim(-.95, 2.05)
    ax.set_xlabel("Node ID (node 7 is the sink)"); ax.tick_params(length=0)
    for spine in ax.spines.values(): spine.set_visible(False)
    ax.text(.5, 1.02, "After failure at node 4", transform=ax.transAxes, ha="center", fontsize=8)
    ax.legend(handles=[Patch(facecolor=C["NEAR"], edgecolor="#333333", label="State retained"),
                       Patch(facecolor="white", edgecolor="#333333", label="No retained state")],
              frameon=False, loc="lower center", bbox_to_anchor=(.5, .02), fontsize=7.5, handlelength=1.)
    panel_label(fig, .205, "(a) Parallelism reduces waiting")
    panel_label(fig, .52, "(b) Additional execution cost")
    panel_label(fig, .845, "(c) State after a failed workflow")
    save(fig, "candidate_fig6_topology_failure")


if __name__ == "__main__":
    data, hashes = read_chain()
    overall(data); distribution(data); topology()
    for p in [BURST / "formal_seed_summary.csv", TOPO / "results/pilot/routeC_summary.json"]:
        hashes[str(p.relative_to(REV))] = hashlib.sha256(p.read_bytes()).hexdigest()
    (OUT / "source_manifest.json").write_text(json.dumps({
        "status": "paper_figure_proposals_from_existing_data",
        "cohort": "chain percentiles/ECDF: all audited phase rows including preheat/drain; rate: observation window",
        "run_boundary": "R31 paired exclusive; R32 old complete EVM + repaired NEAR replay; not two new simultaneous pairs",
        "burst_boundary": "different experiment, fixed-count conditional Poisson, three seeds, parallel exploratory",
        "topology_boundary": "NEAR native approx125ms blocks; EVM automine; within-runtime comparisons only",
        "raw_data_modified": False,
        "source_sha256": hashes,
        "chain_statistics": {"|".join(map(str,k)): {x:y for x,y in v.items() if x!="values"} for k,v in data.items()},
    }, indent=2)+"\n")
    print("Created 3 paper-size PDF/SVG/PNG proposals; original manuscript and figures untouched.")
