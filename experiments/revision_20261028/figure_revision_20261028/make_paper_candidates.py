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
FRESH = REV / "figure_validation_chain_20261028"
TOPO = REV / "controller2_topology_20261028"
TOPO_FRESH = REV / "figure_validation_topology_20261028"
BURST = REV / "controller2_burst_20261028"
SOURCES = {
    (f"R{seed % 100}", vm.upper()): FRESH / f"results/seed{seed}/{vm}/d8_lambda1_seed{seed}_{vm}_{vm}_raw.json"
    for seed in (20261033, 20261034) for vm in ("evm", "near")
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
            all_rows = phase["rows"]
            assert all(r.get(success_key) and r.get("states_match_receipt_events") for r in all_rows), (run, runtime, rate)
            assert len(all_rows) == int(phase["offered"])
            # Select observation-window OFFERS, retain their outcomes through drain.
            rows = [r for r in all_rows if r.get("warmup") is False]
            values = [float(row[key]) / 1000 for row in rows]
            data[run, runtime, rate] = {
                "values": values, "p50": q(values, .5), "p95": q(values, .95),
                "goodput": float(phase["measured_window_goodput_per_sec"]),
                "n": len(values), "n_allphase": len(all_rows),
                "window_n": int(phase["successful_completions_in_measurement_window"]),
                "realized_offer_rate": len(rows) / float(phase["observation_duration_sec"]),
            }
            for row, latency in zip(rows, values):
                exports.append({"run": run, "runtime": runtime, "target_rate": rate,
                                "workflow_id": row["workflow_id"], "latency_seconds": latency,
                                "cohort": "observation_window_offers_followed_through_drain"})
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
    groups = [("R33", 1.), ("R34", 1.)]
    y = [1, 0]
    for yy, (run, rate) in zip(y, groups):
        for runtime, dy, marker in [("EVM", -.12, "o"), ("NEAR", .12, "s")]:
            d = data[run, runtime, rate]
            axes[0].plot([d["p50"], d["p95"]], [yy+dy]*2, color=C[runtime], lw=2.0)
            axes[0].plot(d["p50"], yy+dy, marker, color=C[runtime], ms=4)
            axes[0].plot(d["p95"], yy+dy, "|", color=C[runtime], ms=8, mew=1.2)
            axes[1].plot(d["goodput"], yy+dy, marker, color=C[runtime], ms=4)
        offered = data[run, "EVM", rate]["realized_offer_rate"]
        axes[1].plot([offered, offered], [yy-.34, yy+.34], color="#777777", lw=.9, ls=":")
    axes[0].set_yticks(y, [run + f" (n={data[run, 'EVM', rate]['n']})" for run, rate in groups])
    axes[1].set_yticks(y, [])
    axes[0].set_xlim(0, 11); axes[0].set_xticks([0, 2, 4, 6, 8, 10])
    axes[1].set_xlim(0, 1.13); axes[1].set_xticks([0, .5, 1.])
    axes[0].set_xlabel("Workflow completion latency (s)")
    axes[1].set_xlabel("Completed workflows / s")
    for ax in axes:
        ax.set_ylim(-.6, 1.6); style(ax, "x")
    fig.legend(handles=[Line2D([], [], marker="o", color=C["EVM"], label="Local EVM composition"),
                        Line2D([], [], marker="s", color=C["NEAR"], label="NEAR cross-contract")],
               loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(.55, 1.01))
    axes[0].text(.02, 1.02, "Point: median; endpoint: P95", transform=axes[0].transAxes, fontsize=8)
    axes[1].text(.02, 1.02, "Dotted marks: realized arrival rate", transform=axes[1].transAxes, fontsize=8)
    panel_label(fig, .34, "(a) Latency in two fresh paired repeats")
    panel_label(fig, .81, "(b) Observed completion rate")
    save(fig, "candidate_fig4_measured_outcomes")


def distribution(data):
    fig, axes = plt.subplots(1, 2, figsize=(7.16, 2.93), gridspec_kw={"width_ratios": [1.12, 1]})
    fig.subplots_adjust(left=.085, right=.985, bottom=.25, top=.84, wspace=.30)
    for runtime in ["EVM", "NEAR"]:
        for run, linestyle in [("R33", "-"), ("R34", "--")]:
            a = np.sort(data[run, runtime, 1.]["values"])
            axes[0].step(a, np.arange(1, len(a)+1)/len(a), where="post", color=C[runtime], ls=linestyle, lw=1.2)
    axes[0].set_xlim(0, 11); axes[0].set_ylim(0, 1.02)
    axes[0].set_xticks([0, 2, 4, 6, 8, 10]); axes[0].set_yticks([0, .25, .5, .75, 1.])
    axes[0].set_xlabel("Workflow completion latency (s)"); axes[0].set_ylabel("Empirical cumulative probability")
    style(axes[0], "both")
    axes[0].text(.45, .62, "R33: solid (n=363)\nR34: dashed (n=325)", transform=axes[0].transAxes, va="top", fontsize=8)
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


def read_fresh_topology():
    paths = {
        (part, vm): next((TOPO_FRESH / part / "results/pilot").glob(vm + "*raw.json"))
        for part in ("topology5", "failure3") for vm in ("near", "evm")
    }
    raw = {k: json.loads(p.read_text()) for k, p in paths.items()}
    assert len(raw["topology5", "near"]["repeats"]) == 5
    assert len(raw["topology5", "evm"]["completed_profile_repeats"]) == 10
    rows, costs, failures = [], [], []
    for rep in raw["topology5", "near"]["repeats"]:
        assert rep["runtime"]["rpc_version"] == "2.13.4"
        assert rep["runtime"]["protocol_version"] == 86
        for profile in rep["profiles"]:
            shape = profile["profile"]["shape"]
            session = profile["sessions"][0]
            run = session["multi_contract"]["runs"][0]
            assert run["succeeded"] and not run["failed"]
            assert all(x["all_nodes_present"] and x["nodes_match_same_receiver"] for x in session["same_nodes_and_checksum"])
            rows.append({"platform": "NEAR", "repeat": rep["repeat"], "topology": shape,
                         "near_receipt_tree_ms": run["client_wall_ms_to_receipt_tree"],
                         "near_sink_poll_ms": run["sink_first_queryable_ms"],
                         "same_receiver_tree_ms": session["same_receiver_best_control"]["run"]["client_wall_ms_to_receipt_tree"]})
            costs.append({"platform": "NEAR", "repeat": rep["repeat"], "topology": shape,
                          "workflow_native_gas_tgas": int(run["gas_burnt"]) / 1e12,
                          "workflow_receipts": run["receipt_count"]})
    for rep in raw["topology5", "evm"]["completed_profile_repeats"]:
        session = rep["independent_stage_sessions"][0]
        run = session["router_execution"]
        assert session["session_state_valid"] and session["checksums_match_near_and_stage_inputs_valid"]
        assert run["status"] == 1 and run["external_stage_call_frame_count"] == 8
        shape = rep["profile"]["shape"]
        rows.append({"platform": "EVM", "repeat": rep["repeat"], "topology": shape,
                     "evm_receipt_ms": run["client_wall_ms_to_receipt"]})
        costs.append({"platform": "EVM", "repeat": rep["repeat"], "topology": shape,
                      "workflow_native_gas": run["gas_used"]})
    for rep in raw["failure3", "near"]["repeats"]:
        probe = rep["profiles"][0]["sessions"][0]["injected_failure"]
        kept = [x["node_id"] for x in probe["multi_contract_stored_nodes"] if x["result"]]
        assert kept == [0, 1, 2, 3, 5, 6] and probe["verified"] and not probe["multi_contract_completed_sinks"]
        failures.append({"platform": "NEAR", "repeat": rep["repeat"], "retained_nodes": kept})
    for rep in raw["failure3", "evm"]["completed_profile_repeats"]:
        probe = rep["independent_stage_sessions"][0]["failure_probe"]
        assert probe["status"] == 0 and not any(x["result_exists"] for x in probe["stage_state_after_revert"])
        failures.append({"platform": "EVM", "repeat": rep["repeat"], "retained_nodes": []})
    assert len(failures) == 6
    def summarise(values):
        return {"median": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}
    sm = {}
    for vm, fields in [("NEAR", ["workflow_native_gas_tgas", "workflow_receipts"]), ("EVM", ["workflow_native_gas"])]:
        for shape in ("chain", "fanout6_join"):
            for field in fields:
                sm[f"{vm}_{shape}_{field}"] = summarise([r[field] for r in costs if r["platform"] == vm and r["topology"] == shape])
    hashes = {str(p.relative_to(REV)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths.values()}
    summary = {"batch": "new_topology5_failure3_only", "latency_per_repeat": rows,
               "native_cost_summaries_median_min_max": sm, "failure_per_repeat": failures,
               "source_sha256": hashes}
    (OUT / "fresh_topology_root_metrics.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def topology(summary):
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.95), gridspec_kw={"width_ratios": [1, 1, 1.12]})
    fig.subplots_adjust(left=.075, right=.99, bottom=.27, top=.88, wspace=.45)
    rows = summary["latency_per_repeat"]
    for repeat, marker in zip([1, 2, 3, 4, 5], ["o", "s", "^", "D", "v"]):
        vals = [next(r["near_receipt_tree_ms"] for r in rows if r["platform"] == "NEAR" and r["repeat"] == repeat and r["topology"] == t)/1000
                for t in ["chain", "fanout6_join"]]
        axes[0].plot([0, 1], vals, color=C["NEAR"], lw=.85, marker=marker, ms=3.5)
        same = [next(r["same_receiver_tree_ms"] for r in rows if r["platform"] == "NEAR" and r["repeat"] == repeat and r["topology"] == t)/1000
                for t in ["chain", "fanout6_join"]]
        axes[0].plot([0, 1], same, color=C["same"], lw=.85, marker=marker, ms=3.5)
    axes[0].set_xticks([0, 1], ["Chain", "Fan-out /\njoin"]); axes[0].set_xlim(-.25, 1.25)
    axes[0].set_ylim(0, 1.5); axes[0].set_ylabel("Receipt-tree completion time (s)")
    axes[0].text(.5, 1.02, "NEAR; five fresh repeats", transform=axes[0].transAxes, ha="center", fontsize=8)
    axes[0].legend(handles=[Line2D([], [], color=C["NEAR"], label="Independent contracts"),
                            Line2D([], [], color=C["same"], label="Same receiver")],
                   loc="upper right", frameon=False, fontsize=6.7, handlelength=1.1)
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
    ax.text(.5, 1.02, "Failure at node 4; 3/3 repeats", transform=ax.transAxes, ha="center", fontsize=8)
    ax.legend(handles=[Patch(facecolor=C["NEAR"], edgecolor="#333333", label="State retained"),
                       Patch(facecolor="white", edgecolor="#333333", label="No retained state")],
              frameon=False, loc="lower center", bbox_to_anchor=(.5, .02), fontsize=7.5, handlelength=1.)
    panel_label(fig, .205, "(a) Parallelism reduces waiting")
    panel_label(fig, .52, "(b) Additional execution cost")
    panel_label(fig, .845, "(c) State after a failed workflow")
    save(fig, "candidate_fig6_topology_failure")


if __name__ == "__main__":
    data, hashes = read_chain()
    fresh_topology = read_fresh_topology()
    overall(data); distribution(data); topology(fresh_topology)
    for p in [BURST / "formal_seed_summary.csv"]:
        hashes[str(p.relative_to(REV))] = hashlib.sha256(p.read_bytes()).hexdigest()
    hashes.update(fresh_topology["source_sha256"])
    for p in [TOPO_FRESH / "chain_fresh_supplement" / f"seed{seed}" / "launch_manifest.txt" for seed in (20261033, 20261034)]:
        hashes[str(p.relative_to(REV))] = hashlib.sha256(p.read_bytes()).hexdigest()
    (OUT / "source_manifest.json").write_text(json.dumps({
        "status": "paper_figure_proposals_from_existing_data",
        "cohort": "chain percentiles/ECDF: observation-window offers followed through drain; rate: successful completions within 360s observation window",
        "percentile_rule": "nearest rank ceil(q*n), applied consistently to all plotted chain values",
        "run_boundary": "R33 and R34 each fresh EVM+NEAR paired runs, lambda1/s, 30s preheat+360s observation, fixed1s blocks; historical R31/R32 figures retained under initial_proposal",
        "independent_audit": "../figure_validation_topology_20261028/chain_fresh_independent_audit.md",
        "provenance_limitations": [
            "Formal seed33/34 original launch manifests recovered under figure_validation_topology_20261028/chain_fresh_supplement; record NEAR2.13.4/protocol86/binaryhash and EVMrunnerhash, independently of original81-file archive.",
            "Anvil executable binary hash was not captured for these formal chain runs; do not replace with a later measurement.",
            "Both NEAR teardowns forced_after_timeout; no historical post-finish child-process probe archived.",
            "Separate current-time PID probe found all8knownPIDs absent; cannot establish their finish-time exit.",
            "Finalization includes audit gate wait and must not be called teardown time.",
        ],
        "burst_boundary": "different experiment, fixed-count conditional Poisson, three seeds, parallel exploratory",
        "topology_boundary": "new five topology repeats and three failure probes only, old3+1 separate; NEAR native approx125ms blocks; EVM automine; within-runtime comparisons only",
        "raw_data_modified": False,
        "source_sha256": hashes,
        "chain_statistics": {"|".join(map(str,k)): {x:y for x,y in v.items() if x!="values"} for k,v in data.items()},
    }, indent=2)+"\n")
    print("Created 3 paper-size PDF/SVG/PNG proposals; original manuscript and figures untouched.")
