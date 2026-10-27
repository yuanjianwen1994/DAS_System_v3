#!/usr/bin/env python3
"""Create publication-ready DAG topology, runtime, and scaling figures."""
import json
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path

plot_libs = os.environ.get("DAG_PLOT_LIBS")
if plot_libs:
    sys.path.insert(0, plot_libs)
os.environ.setdefault("MPLCONFIGDIR", str(Path(os.environ.get("TMPDIR", "/tmp")) / "das-dag-mpl-cache"))
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

HERE = Path(__file__).resolve().parent
NEAR_PATH = HERE.parent / "near_runtime_20261027" / "near_dag_full_results.json"
EVM_PATH = HERE / "das_composition_full_results.json"
EVM_DISTRIBUTED_PATH = HERE / "das_distributed_composition_results.json"
MANIFEST_PATH = HERE / "profile_manifest.json"
OUT = HERE

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "figure.titlesize": 15,
    "savefig.bbox": "tight",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

COLORS = {
    "ink": "#24334A",
    "near": "#3976AF",
    "compute": "#3976AF",
    "callback": "#E49B36",
    "refund": "#98A4B3",
    "entry": "#66A182",
    "exit": "#A877B4",
    "grid": "#DCE3EA",
    "surface": "#F4F7FA",
    "accent": "#D6634E",
}


def save(fig, stem):
    fig.savefig(OUT / f"{stem}.png", dpi=240)
    fig.savefig(OUT / f"{stem}.pdf")
    plt.close(fig)


def draw_graph(ax, title, nodes, edges, positions, levels):
    ax.set_title(title, loc="left", fontweight="bold", color=COLORS["ink"], pad=10)
    for source, target in edges:
        x1, y1 = positions[source]
        x2, y2 = positions[target]
        rad = max(-0.16, min(0.16, (y2 - y1) * 0.25))
        ax.add_patch(FancyArrowPatch((x1 + 0.09, y1), (x2 - 0.09, y2), arrowstyle="-|>",
                                     mutation_scale=11, linewidth=1.2, color="#8392A3",
                                     connectionstyle=f"arc3,rad={rad}", zorder=1))
    for node in nodes:
        x, y = positions[node]
        box = FancyBboxPatch((x - 0.09, y - 0.07), 0.18, 0.14,
                             boxstyle="round,pad=0.015,rounding_size=0.025",
                             linewidth=1.0, edgecolor=COLORS["near"], facecolor="#EAF2FA", zorder=2)
        ax.add_patch(box)
        ax.text(x, y, f"v{node}", ha="center", va="center", fontsize=8, color=COLORS["ink"], zorder=3)
    ax.text(0.02, 0.02, f"{len(nodes)} nodes  |  {len(edges)} edges", transform=ax.transAxes,
            color="#596A7D", fontsize=9)
    ax.set_xlim(-0.23, 1.23)
    ax.set_ylim(0.02, 1.02)
    ax.axis("off")


def graph_shapes():
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.2))
    chain_nodes = list(range(4))
    chain_edges = [(i, i + 1) for i in range(3)]
    chain_pos = {i: (0.12 + i * 0.25, 0.5) for i in chain_nodes}
    draw_graph(axes[0], "Chain (d=4)", chain_nodes, chain_edges, chain_pos, None)

    fork_layers = [[0], [1, 2, 3, 4], [5, 6, 7, 8], [9]]
    fork_edges = [(0, i) for i in range(1, 5)]
    fork_edges += [(i, j) for i in range(1, 5) for j in range(5, 9)]
    fork_edges += [(i, 9) for i in range(5, 9)]
    fork_pos = {}
    for layer, values in enumerate(fork_layers):
        for index, node in enumerate(values):
            fork_pos[node] = (0.12 + layer * 0.28, 0.5 if len(values) == 1 else 0.84 - index * 0.22)
    draw_graph(axes[1], "Fork-join (d=4, w=4)", list(range(10)), fork_edges, fork_pos, fork_layers)

    mixed_layers = [[0], [1, 2, 3, 4], [5, 6, 7, 8], [9]]
    mixed_edges = [(0, i) for i in range(1, 5)]
    mixed_edges += [(1, 5), (2, 5), (2, 6), (3, 6), (3, 7), (4, 7), (1, 8), (4, 8)]
    mixed_edges += [(i, 9) for i in range(5, 9)]
    mixed_pos = {}
    for layer, values in enumerate(mixed_layers):
        for index, node in enumerate(values):
            mixed_pos[node] = (0.12 + layer * 0.28, 0.5 if len(values) == 1 else 0.84 - index * 0.22)
    draw_graph(axes[2], "Mixed (d=4, w=4)", list(range(10)), mixed_edges, mixed_pos, mixed_layers)
    fig.suptitle("Parameterized stateful DAG shapes", x=0.06, ha="left", fontweight="bold", color=COLORS["ink"])
    fig.text(0.06, 0.015, "Each node consumes stored predecessor checksums and applies the same 16-step state update.", color="#596A7D", fontsize=9)
    fig.tight_layout(rect=(0.02, 0.06, 0.99, 0.91))
    save(fig, "dag_shapes")


def load_data():
    return (json.loads(NEAR_PATH.read_text()), json.loads(EVM_PATH.read_text()),
            json.loads(EVM_DISTRIBUTED_PATH.read_text()), json.loads(MANIFEST_PATH.read_text()))


def group_near(near):
    result = defaultdict(list)
    for repeat in near["completed_repeats"]:
        for profile in repeat["profiles"]:
            for session in profile["sessions"]:
                for run in session["multi_contract"]["runs"]:
                    result[profile["profile"]["profile_id"]].append(run)
    return result


def group_evm(evm):
    result = defaultdict(list)
    for repeat in evm["completed_repeats"]:
        for profile in repeat["profiles"]:
            result[profile["profile"]["profile_id"]].extend(profile["local_type_ii_session_mode"])
    return result


def group_distributed(evm):
    result = defaultdict(list)
    for repeat in evm["completed_repeats"]:
        for profile in repeat["profiles"]:
            result[profile["profile"]["profile_id"]].append(profile)
    return result


def mean_sd(values):
    return statistics.mean(values), (statistics.stdev(values) if len(values) > 1 else 0.0)


def execution_comparison(near, evm, distributed, manifest):
    ng, dg = group_near(near), group_distributed(distributed)
    profiles = {x["profile_id"]: x for x in manifest["profiles"]}
    depths = [2, 4, 8]
    ids = [next(pid for pid, p in profiles.items() if p["shape"] == "chain" and p["dependency_depth_d"] == d and p["workflows_per_session_theta"] == 4) for d in depths]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.2))
    x = list(range(len(depths)))

    lag_means, lag_sds = [], []
    for pid in ids:
        profile = profiles[pid]
        sinks = [i for i, children in enumerate(profile["successors"]) if not children]
        m, sd = mean_sd([max(e["block_height"] for e in r["log_events"] if e["type"] == "node_execution" and e["node_id"] in sinks) - r["tx_block_height"] for r in ng[pid]])
        lag_means.append(m); lag_sds.append(sd)
    axes[0, 0].bar(x, lag_means, yerr=lag_sds, color=COLORS["near"], capsize=4, width=0.6)
    axes[0, 0].set_xticks(x, [f"d={d}" for d in depths])
    axes[0, 0].set_ylabel("Start transaction → DAG sink (blocks)")
    axes[0, 0].set_title("NEAR multi-contract sink completion", loc="left", fontweight="bold")
    axes[0, 0].grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    axes[0, 0].set_axisbelow(True)
    for i, y in enumerate(lag_means): axes[0, 0].text(i, y + 0.16, f"{y:.1f}", ha="center", fontsize=9)

    width = 0.19
    for offset, (field, label, color) in enumerate([
        ("entry_proxy", "Entry proxy", COLORS["entry"]),
        ("router_execution", "Router execution tx", COLORS["near"]),
        ("exit_proxy", "Exit proxy", COLORS["exit"]),
    ]):
        vals = [mean_sd([s[field]["status"] and 1 for row in dg[pid] for s in row["independent_stage_sessions"]])[0] for pid in ids]
        bars = axes[0, 1].bar([z + (offset - 1) * width for z in x], vals, width, label=label, color=color)
        axes[0, 1].bar_label(bars, fmt="%d", padding=2, fontsize=8)
    axes[0, 1].set_xticks(x, [f"d={d}" for d in depths])
    axes[0, 1].set_ylim(0, 1.35)
    axes[0, 1].set_ylabel("EVM transactions per session")
    axes[0, 1].set_title("Independent-stage EVM path", loc="left", fontweight="bold")
    axes[0, 1].legend(frameon=False, fontsize=8, ncol=1, loc="upper left")
    axes[0, 1].grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    axes[0, 1].set_axisbelow(True)
    call_axis = axes[0, 1].twinx()
    call_counts = [mean_sd([s["router_execution"]["external_stage_call_frame_count"] for row in dg[pid] for s in row["independent_stage_sessions"]])[0] for pid in ids]
    call_axis.plot(x, call_counts, color=COLORS["accent"], marker="D", linewidth=1.8, label="External Stage CALLs")
    call_axis.set_ylabel("External Stage CALL frames per session", color=COLORS["accent"])
    call_axis.tick_params(axis="y", labelcolor=COLORS["accent"])
    call_axis.set_ylim(0, max(call_counts) * 1.25)
    for i, y in enumerate(call_counts):
        call_axis.text(i, y + max(call_counts) * 0.035, f"{int(y)}", color=COLORS["accent"], ha="center", fontsize=8)

    selected = [
        ("dag-02-chain-d2-w1-theta4-nu1", "Chain d2"),
        ("dag-05-chain-d4-w1-theta4-nu1", "Chain d4"),
        ("dag-08-chain-d8-w1-theta4-nu1", "Chain d8"),
        ("dag-11-fork_join-d4-w4-theta4-nu1", "Fork-join w4"),
    ]
    xs = list(range(len(selected)))
    node_calls = [1 + profiles[pid]["edge_count"] for pid, _ in selected]
    callbacks = [sum(len(z) > 1 for z in profiles[pid]["successors"]) for pid, _ in selected]
    refunds = [profiles[pid]["edge_count"] + callbacks[i] for i, (pid, _) in enumerate(selected)]
    axes[1, 0].bar(xs, node_calls, color=COLORS["compute"], label="Node FunctionCalls")
    axes[1, 0].bar(xs, callbacks, bottom=node_calls, color=COLORS["callback"], label="Fanout callbacks")
    bottoms = [node_calls[i] + callbacks[i] for i in range(len(xs))]
    axes[1, 0].bar(xs, refunds, bottom=bottoms, color=COLORS["refund"], label="System refund Transfers")
    for i, total in enumerate([node_calls[j] + callbacks[j] + refunds[j] for j in range(len(xs))]):
        axes[1, 0].text(i, total + 0.8, str(total), ha="center", fontsize=9)
    axes[1, 0].set_xticks(xs, [name for _, name in selected])
    axes[1, 0].set_ylabel("Receipt outcomes per workflow")
    axes[1, 0].set_title("NEAR outcome-tree classification", loc="left", fontweight="bold")
    axes[1, 0].legend(frameon=False, fontsize=8)
    axes[1, 0].grid(axis="y", color=COLORS["grid"], linewidth=0.8)
    axes[1, 0].set_axisbelow(True)

    axes[1, 1].axis("off")
    axes[1, 1].set_title("Interpretation", loc="left", fontweight="bold")
    text = (
        "The measured NEAR path adds asynchronous contract-call dependencies as d grows.\n\n"
        "Each EVM session has one Router execution transaction and θ × node-count external CALLs to independent Stage contracts.\n\n"
        "The earlier monolithic EVM runner is retained separately as a fused control.\n\n"
        "For the width-4 DAGs, the sink completes at +4 blocks; the full callback/refund tree drains at +6.\n\n"
        "Entry and exit bars are local storage-commitment proxies, not bridge proofs or measured migration.\n\n"
        "NEAR blocks and EVM transaction counts are separate units; no cross-runtime time ratio is inferred."
    )
    axes[1, 1].text(0.02, 0.91, text, va="top", ha="left", color=COLORS["ink"], fontsize=9.2, linespacing=1.38,
                    bbox={"boxstyle": "round,pad=0.7", "facecolor": COLORS["surface"], "edgecolor": COLORS["grid"]})
    fig.suptitle("Dependency completion and native execution units", x=0.06, ha="left", fontweight="bold", color=COLORS["ink"])
    fig.tight_layout(rect=(0.02, 0.02, 0.99, 0.93))
    save(fig, "dag_execution_comparison")


def parameter_trends(near, evm, distributed, manifest):
    ng, eg, dg = group_near(near), group_evm(evm), group_distributed(distributed)
    profiles = {x["profile_id"]: x for x in manifest["profiles"]}
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.8))
    colors = {1: "#3976AF", 4: "#E49B36", 8: "#4C9A78"}

    for theta in (1, 4, 8):
        xs, ys, es = [], [], []
        for depth in (2, 4, 8):
            pid = next(pid for pid, p in profiles.items() if p["shape"] == "chain" and p["dependency_depth_d"] == depth and p["workflows_per_session_theta"] == theta)
            sinks = [i for i, children in enumerate(profiles[pid]["successors"]) if not children]
            values = [max(e["block_height"] for e in r["log_events"] if e["type"] == "node_execution" and e["node_id"] in sinks) - r["tx_block_height"] for r in ng[pid]]
            xs.append(depth); ys.append(statistics.mean(values)); es.append(statistics.stdev(values) if len(values) > 1 else 0)
        axes[0].errorbar(xs, ys, yerr=es, marker="o", linewidth=2, capsize=3, color=colors[theta], label=f"θ={theta}")
    drain_xs, drain_ys = [], []
    for depth in (2, 4, 8):
        pid = next(pid for pid, p in profiles.items() if p["shape"] == "chain" and p["dependency_depth_d"] == depth and p["workflows_per_session_theta"] == 4)
        drain_xs.append(depth)
        drain_ys.append(statistics.mean([r["completion_lag_blocks"] for r in ng[pid]]))
    axes[0].plot(drain_xs, drain_ys, marker="x", linestyle="--", linewidth=1.5, color=COLORS["ink"], label="Full receipt-tree drain")
    axes[0].set_xlabel("Dependency depth d")
    axes[0].set_ylabel("NEAR block-height span")
    axes[0].set_title("DAG sink vs receipt-tree drain", loc="left", fontweight="bold")
    axes[0].legend(frameon=False)
    axes[0].grid(color=COLORS["grid"])
    axes[0].set_axisbelow(True)

    theta_values = [1, 4, 8]
    multi, same = [], []
    for theta in theta_values:
        pid = next(pid for pid, p in profiles.items() if p["shape"] == "chain" and p["dependency_depth_d"] == 4 and p["workflows_per_session_theta"] == theta)
        mvals, svals = [], []
        for repeat in near["completed_repeats"]:
            row = next(r for r in repeat["profiles"] if r["profile"]["profile_id"] == pid)
            for session in row["sessions"]:
                mvals.append(sum(int(x["gas_burnt"]) for x in session["multi_contract"]["runs"]) / 1e12)
                svals.append(int(session["same_receiver_best_control"]["run"]["gas_burnt"]) / 1e12)
        multi.append(mean_sd(mvals)); same.append(mean_sd(svals))
    xx = list(range(3)); ww = 0.34
    axes[1].bar([z - ww/2 for z in xx], [x[0] for x in multi], ww, yerr=[x[1] for x in multi], capsize=3, color=COLORS["near"], label="Multi-contract")
    axes[1].bar([z + ww/2 for z in xx], [x[0] for x in same], ww, yerr=[x[1] for x in same], capsize=3, color="#80B7D8", label="Same-receiver θ-batch")
    axes[1].set_xticks(xx, [f"θ={t}" for t in theta_values])
    axes[1].set_xlabel("Workflows per session")
    axes[1].set_ylabel("NEAR gas per session (Tgas)")
    axes[1].set_title("NEAR native execution cost", loc="left", fontweight="bold")
    axes[1].legend(frameon=False, fontsize=8)
    axes[1].grid(axis="y", color=COLORS["grid"])
    axes[1].set_axisbelow(True)

    fused_totals, stage_totals, stage_setups = [], [], []
    for theta in theta_values:
        pid = next(pid for pid, p in profiles.items() if p["shape"] == "chain" and p["dependency_depth_d"] == 4 and p["workflows_per_session_theta"] == theta)
        fused_sessions = [s for repeat in evm["completed_repeats"] for row in repeat["profiles"] if row["profile"]["profile_id"] == pid for s in row["local_type_ii_session_mode"]]
        stage_rows = dg[pid]
        stage_sessions = [s for row in stage_rows for s in row["independent_stage_sessions"]]
        fused_totals.append(mean_sd([s["complete_session_gas"] for s in fused_sessions]))
        stage_totals.append(mean_sd([s["complete_session_gas_excluding_setup"] for s in stage_sessions]))
        stage_setups.append(mean_sd([r["deployment_and_registration_gas_per_profile"] for r in stage_rows]))
    bw = 0.32
    axes[2].bar([z - bw/2 for z in xx], [x[0] for x in fused_totals], bw,
                yerr=[x[1] for x in fused_totals], capsize=3, color="#72849A", label="Fused-control session")
    axes[2].bar([z + bw/2 for z in xx], [x[0] for x in stage_totals], bw,
                yerr=[x[1] for x in stage_totals], capsize=3, color=COLORS["near"], label="Independent-stage session")
    setup_axis = axes[2].twinx()
    setup_vals = [x[0] for x in stage_setups]
    setup_axis.plot(xx, setup_vals, color=COLORS["accent"], marker="D", linestyle="--", label="Stage deployment + registration")
    setup_axis.set_ylabel("Setup gas per profile", color=COLORS["accent"])
    setup_axis.tick_params(axis="y", labelcolor=COLORS["accent"])
    setup_axis.set_ylim(0, max(setup_vals) * 1.2)
    axes[2].set_xticks(xx, [f"θ={t}" for t in theta_values])
    axes[2].set_xlabel("Workflows per session")
    axes[2].set_ylabel("EVM gas per session (native gas units)")
    axes[2].set_title("EVM gas: fused vs independent stages", loc="left", fontweight="bold")
    axes[2].legend(frameon=False, fontsize=7, loc="upper left")
    setup_axis.legend(frameon=False, fontsize=7, loc="upper right")
    axes[2].grid(axis="y", color=COLORS["grid"])
    axes[2].set_axisbelow(True)

    fig.suptitle("Depth and session-size trends (chain workloads)", x=0.04, ha="left", fontweight="bold", color=COLORS["ink"])
    fig.text(0.04, 0.005, "Panels use native units independently. Error bars show standard deviation across six sessions (three fresh repeats × two sessions).", color="#596A7D", fontsize=8.5)
    fig.tight_layout(rect=(0.02, 0.06, 0.99, 0.9))
    save(fig, "dag_parameter_trends")


def independent_stage_gas(evm, distributed):
    rows = group_distributed(distributed)
    fused = group_evm(evm)
    selected = [
        ("dag-01-chain-d2-w1-theta1-nu1", "Chain d2 θ1"),
        ("dag-05-chain-d4-w1-theta4-nu1", "Chain d4 θ4"),
        ("dag-08-chain-d8-w1-theta4-nu1", "Chain d8 θ4"),
        ("dag-11-fork_join-d4-w4-theta4-nu1", "Fork-join w4 θ4"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.2))
    x = list(range(len(selected)))
    width = 0.34
    fused_total, stage_total, setup = [], [], []
    stage_exec, stage_frame = [], []
    for pid, _ in selected:
        fs = fused[pid]
        dr = rows[pid]
        ds = [s for row in dr for s in row["independent_stage_sessions"]]
        fused_total.append(mean_sd([s["complete_session_gas"] for s in fs]))
        stage_total.append(mean_sd([s["complete_session_gas_excluding_setup"] for s in ds]))
        setup.append(mean_sd([r["deployment_and_registration_gas_per_profile"] for r in dr]))
        stage_exec.append(mean_sd([s["router_execution"]["gas_used"] for s in ds]))
        stage_frame.append(mean_sd([s["router_execution"]["external_stage_call_gas_used_sum"] for s in ds]))
    axes[0].bar([z-width/2 for z in x], [v[0] for v in fused_total], width, color="#72849A", label="Fused-control full session")
    axes[0].bar([z+width/2 for z in x], [v[0] for v in stage_total], width, color=COLORS["near"], label="Independent-stage full session")
    setup_axis = axes[0].twinx()
    setup_vals = [v[0] for v in setup]
    setup_axis.plot(x, setup_vals, color=COLORS["accent"], marker="D", linestyle="--", label="Deployment + registration per profile")
    axes[0].set_xticks(x, [label for _, label in selected], rotation=15, ha="right")
    axes[0].set_ylabel("EVM gas per session (entry + execution + exit)")
    axes[0].set_title("Session gas; setup shown separately", loc="left", fontweight="bold")
    axes[0].grid(axis="y", color=COLORS["grid"]); axes[0].set_axisbelow(True)
    setup_axis.set_ylabel("Setup gas per profile", color=COLORS["accent"])
    setup_axis.tick_params(axis="y", labelcolor=COLORS["accent"])
    handles1, labels1 = axes[0].get_legend_handles_labels()
    handles2, labels2 = setup_axis.get_legend_handles_labels()
    axes[0].legend(handles1+handles2, labels1+labels2, frameon=False, fontsize=7.5, loc="upper left")

    for offset, (values, label, color) in enumerate([
        (stage_exec, "Router execution transaction gas", COLORS["near"]),
        (stage_frame, "Sum of Stage CALL-frame gas", COLORS["callback"]),
    ]):
        bars = axes[1].bar([z+(offset-0.5)*0.36 for z in x], [v[0] for v in values], 0.36,
                           color=color, label=label)
        axes[1].bar_label(bars, labels=[f"{int(v[0]):,}" for v in values], padding=2, fontsize=7, rotation=90)
    axes[1].set_xticks(x, [label for _, label in selected], rotation=15, ha="right")
    axes[1].set_ylabel("EVM gas (native units)")
    axes[1].set_title("One Router tx with external Stage CALLs", loc="left", fontweight="bold")
    axes[1].legend(frameon=False, fontsize=8)
    axes[1].grid(axis="y", color=COLORS["grid"]); axes[1].set_axisbelow(True)
    fig.suptitle("Independent Stage contracts versus fused EVM control", x=0.04, ha="left", fontweight="bold", color=COLORS["ink"])
    fig.text(0.04, 0.005, "Stage-frame gas is nested within router transaction gas and is not added to session totals. Deployment and registration are setup costs per profile.", color="#596A7D", fontsize=8.5)
    fig.tight_layout(rect=(0.02, 0.06, 0.99, 0.9))
    save(fig, "dag_evm_composition_controls")


def main():
    near, evm, distributed, manifest = load_data()
    graph_shapes()
    execution_comparison(near, evm, distributed, manifest)
    parameter_trends(near, evm, distributed, manifest)
    independent_stage_gas(evm, distributed)
    print("Created DAG topology, execution, trend, and EVM-control PNG/PDF figures.")


if __name__ == "__main__":
    main()
