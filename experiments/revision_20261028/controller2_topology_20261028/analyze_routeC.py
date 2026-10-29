#!/usr/bin/env python3
"""Descriptive analysis and figures for the Route C remote experiment outputs."""
from __future__ import annotations

import csv
import json
import statistics
from collections import Counter
from datetime import datetime, timezone
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results" / "pilot"
OUT = RESULTS
PROFILES = {
    "chain": "topology-chain-d8-8n-pilot",
    "fanout6_join": "topology-fanout6-join-8n-pilot",
}
LABELS = {"chain": "8节点链", "fanout6_join": "根→6分支→汇合"}


def load(name: str) -> dict:
    return json.loads((RESULTS / name).read_text())


def near_profile(repeat: dict, profile_id: str) -> dict:
    return next(p for p in repeat["profiles"] if p["profile"]["profile_id"] == profile_id)


def evm_profile(repeat: dict, profile_id: str) -> dict:
    return next(p for p in repeat["profiles"] if p["profile"]["profile_id"] == profile_id)


def median_range(values: list[float]) -> dict:
    return {
        "n": len(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "values": values,
    }


def near_block_intervals(run: dict) -> list[float]:
    by_height: dict[int, int] = {}
    for row in run["records"]:
        by_height[int(row["block_height"])] = int(row["block_timestamp_nanosec"])
    heights = sorted(by_height)
    intervals = []
    for left, right in zip(heights, heights[1:]):
        gap = right - left
        intervals.append((by_height[right] - by_height[left]) / 1e6 / gap)
    return intervals


def xy(cx: float, cy: float) -> str:
    return f"{cx:.1f},{cy:.1f}"


def latency_svg(rows: list[dict]) -> str:
    width, height = 1160, 485
    panel_w, panel_h = 350, 370
    left0, top = 40, 74
    specs = [
        ("NEAR：sink 首次成功查询（ms）", "near_sink_poll_ms", 1400, "first successful view poll", "NEAR"),
        ("NEAR：完整 receipt tree（ms）", "near_receipt_tree_ms", 1400, "callRaw resolved", "NEAR"),
        ("EVM：执行交易 receipt（ms）", "evm_receipt_ms", 150, "Anvil automine, local RPC", "EVM"),
    ]
    colors = {"chain": "#176b87", "fanout6_join": "#b55b18"}
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">']
    svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    svg.append('<style>text{font-family:Arial,"Noto Sans CJK SC",sans-serif;fill:#16212b}.title{font-size:19px;font-weight:700}.subtitle{font-size:12px;fill:#475569}.axis{font-size:12px}.tick{font-size:11px;fill:#475569}.note{font-size:11px;fill:#475569}.median{stroke-width:3}</style>')
    svg.append(f'<text x="{width/2}" y="26" text-anchor="middle" class="title">Route C：拓扑延迟（每点一次独立 runtime，n=3）</text>')
    svg.append(f'<text x="{width/2}" y="47" text-anchor="middle" class="subtitle">仅在各平台面板内比较拓扑；NEAR 首次成功查询以 50 ms 轮询观测，EVM sink 查询发生在交易 receipt 后</text>')
    for panel_idx, (title, key, max_y, sub, platform) in enumerate(specs):
        x0 = left0 + panel_idx * 375
        plot_left, plot_top = x0 + 50, top + 32
        plot_w, plot_h = 270, panel_h - 80
        svg.append(f'<text x="{x0+panel_w/2}" y="{top+10}" text-anchor="middle" class="axis" font-weight="700">{escape(title)}</text>')
        svg.append(f'<text x="{x0+panel_w/2}" y="{top+27}" text-anchor="middle" class="note">{escape(sub)}</text>')
        for tick in range(0, max_y + 1, max(1, max_y // 4)):
            y = plot_top + plot_h - (tick / max_y) * plot_h
            svg.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_left+plot_w}" y2="{y:.1f}" stroke="#d9e1e8"/>')
            svg.append(f'<text x="{plot_left-8}" y="{y+4:.1f}" text-anchor="end" class="tick">{tick}</text>')
        svg.append(f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{plot_top+plot_h}" stroke="#334155"/>')
        svg.append(f'<line x1="{plot_left}" y1="{plot_top+plot_h}" x2="{plot_left+plot_w}" y2="{plot_top+plot_h}" stroke="#334155"/>')
        for cat_i, topology in enumerate(("chain", "fanout6_join")):
            cx = plot_left + plot_w * (0.29 if cat_i == 0 else 0.73)
            vals = [r[key] for r in rows if r["platform"] == platform and r["topology"] == topology]
            med = statistics.median(vals)
            my = plot_top + plot_h - (med / max_y) * plot_h
            svg.append(f'<line x1="{cx-24}" y1="{my:.1f}" x2="{cx+24}" y2="{my:.1f}" stroke="{colors[topology]}" class="median"/>')
            for rep_idx, val in enumerate(vals):
                dx = (rep_idx - 1) * 11
                py = plot_top + plot_h - (val / max_y) * plot_h
                svg.append(f'<circle cx="{cx+dx:.1f}" cy="{py:.1f}" r="5.5" fill="{colors[topology]}" stroke="#fff" stroke-width="1.2"><title>{escape(LABELS[topology])} R{rep_idx+1}: {val:.2f} ms</title></circle>')
            svg.append(f'<text x="{cx}" y="{plot_top+plot_h+22}" text-anchor="middle" class="axis">{escape(LABELS[topology])}</text>')
            svg.append(f'<text x="{cx}" y="{my-8:.1f}" text-anchor="middle" class="tick">中位数 {med:.1f}</text>')
    svg.append('<circle cx="375" cy="455" r="5" fill="#176b87"/><text x="387" y="459" class="note">8节点链</text><circle cx="485" cy="455" r="5" fill="#b55b18"/><text x="497" y="459" class="note">根→6分支→汇合</text>')
    svg.append('</svg>')
    return "\n".join(svg)


def failure_svg(near_failure: dict, evm_failure: dict) -> str:
    width, height = 1040, 420
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">']
    svg.append('<rect width="100%" height="100%" fill="#fff"/>')
    svg.append('<style>text{font-family:Arial,"Noto Sans CJK SC",sans-serif;fill:#16212b}.title{font-size:19px;font-weight:700}.panel{font-size:15px;font-weight:700}.node{stroke:#334155;stroke-width:1.5}.label{font-size:12px}.small{font-size:11px;fill:#475569}.edge{stroke:#64748b;stroke-width:1.4;fill:none;marker-end:url(#arrow)}</style>')
    svg.append('<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#64748b"/></marker></defs>')
    svg.append(f'<text x="{width/2}" y="28" text-anchor="middle" class="title">中间分支 node 4 失败：异步部分提交与同步交易回滚</text>')
    panels = [(35, "NEAR Promise：失败分支不撤销已完成的独立调用", "near"), (535, "EVM CALL：失败使整个执行交易回滚", "evm")]
    ys = {0: 118, 1: 216, 2: 216, 3: 216, 4: 216, 5: 216, 6: 216, 7: 320}
    near_persisted = sum(1 for row in near_failure["multi_contract_stored_nodes"] if row["result"])
    near_total = len(near_failure["multi_contract_stored_nodes"])
    for x0, title, kind in panels:
        svg.append(f'<text x="{x0+235}" y="62" text-anchor="middle" class="panel">{escape(title)}</text>')
        xs = {0:x0+235, 1:x0+62, 2:x0+132, 3:x0+202, 4:x0+272, 5:x0+342, 6:x0+412, 7:x0+235}
        for child in range(1,7):
            svg.append(f'<line x1="{xs[0]}" y1="{ys[0]+19}" x2="{xs[child]}" y2="{ys[child]-19}" class="edge"/>')
            svg.append(f'<line x1="{xs[child]}" y1="{ys[child]+19}" x2="{xs[7]}" y2="{ys[7]-19}" class="edge"/>')
        for node in range(8):
            if kind == "near":
                fill = "#d9f2e6" if node in (0,1,2,3,5,6) else "#ffd6d6" if node == 4 else "#e5e7eb"
                text = "部分状态" if node in (0,1,2,3,5,6) else "失败" if node == 4 else "无 sink"
            else:
                fill = "#ffd6d6" if node == 4 else "#e5e7eb"
                text = "回滚" if node in (0,1,2,3) else "失败" if node == 4 else "未调用"
                if node == 7: text = "无状态"
            svg.append(f'<circle cx="{xs[node]}" cy="{ys[node]}" r="19" fill="{fill}" class="node"/>')
            svg.append(f'<text x="{xs[node]}" y="{ys[node]+4}" text-anchor="middle" class="label">{node}</text>')
            svg.append(f'<text x="{xs[node]}" y="{ys[node]+34}" text-anchor="middle" class="small">{escape(text)}</text>')
    svg.append(f'<text x="{width/2}" y="386" text-anchor="middle" class="small">NEAR：{near_persisted}/{near_total} 节点有持久结果，sink 未生成；EVM：失败交易 status=0、5 个 Stage CALL frame，所有合约结果回滚。未实现或执行任何业务补偿。</text>')
    svg.append('</svg>')
    return "\n".join(svg)


def main() -> None:
    near = load("near_topology_formal_results.json")
    near_raw = load("near_topology_formal_raw.json")
    evm = load("evm_topology_formal_results.json")
    evm_raw = load("evm_topology_formal_raw.json")
    near_fail = load("near_topology_failure_results.json")
    evm_fail = load("evm_topology_failure_results.json")

    latency_rows: list[dict] = []
    gas_receipt_rows: list[dict] = []
    block_rows: list[dict] = []
    receipt_classes: dict[str, Counter] = {key: Counter() for key in PROFILES}
    all_state_audited = True

    for rep in near["completed_repeats"]:
        for topology, profile_id in PROFILES.items():
            p = near_profile(rep, profile_id)
            session = p["sessions"][0]
            run = session["multi_contract"]["runs"][0]
            ctrl = session["same_receiver_best_control"]["run"]
            latency_rows.append({
                "platform": "NEAR", "repeat": rep["repeat"], "topology": topology,
                "near_sink_poll_ms": run["sink_first_queryable_ms"],
                "near_receipt_tree_ms": run["client_wall_ms_to_receipt_tree"],
                "sink_poll_before_tree": bool(run["sink_first_queryable_before_receipt_tree"]),
            })
            gas_receipt_rows.append({
                "platform": "NEAR", "repeat": rep["repeat"], "topology": topology,
                "workflow_native_gas_tgas": int(run["gas_burnt"]) / 1e12,
                "workflow_receipts": int(run["receipt_count"]),
                "workflow_completion_lag_blocks": int(run["completion_lag_blocks"]),
                "same_receiver_native_gas_tgas": int(ctrl["gas_burnt"]) / 1e12,
                "same_receiver_receipts": int(ctrl["receipt_count"]),
                "same_receiver_tree_ms": float(ctrl["client_wall_ms_to_receipt_tree"]),
                "same_receiver_view_after_tree_ms": float(ctrl["sink_observed_in_post_receipt_tree_view_ms"]),
            })
            for receipt in run["records"][1:]:
                receipt_classes[topology][receipt.get("receipt_class", "unknown")] += 1
            for ms in near_block_intervals(run):
                block_rows.append({"platform": "NEAR", "repeat": rep["repeat"], "topology": topology, "block_interval_ms": ms})
            state_rows = session["same_nodes_and_checksum"]
            all_state_audited = all_state_audited and all(x["all_nodes_present"] and x["nodes_match_same_receiver"] for x in state_rows)

    for rep in evm["completed_repeats"]:
        for topology, profile_id in PROFILES.items():
            p = evm_profile(rep, profile_id)
            session = p["independent_stage_sessions"][0]
            execution = session["router_execution"]
            latency_rows.append({
                "platform": "EVM", "repeat": rep["repeat"], "topology": topology,
                "evm_receipt_ms": float(execution["client_wall_ms_to_receipt"]),
                "evm_sink_post_receipt_ms": float(execution["sink_view_query_after_receipt"]["observed_after_receipt_elapsed_ms"]),
            })
            gas_receipt_rows.append({
                "platform": "EVM", "repeat": rep["repeat"], "topology": topology,
                "workflow_native_gas": int(session["local_execution_gas"]),
                "workflow_count": len(session["theta_workflows"]),
                "complete_session_gas_excluding_setup": int(session["complete_session_gas_excluding_setup"]),
                "setup_gas_per_profile": int(session["stage_setup"]["deployment_and_registration_gas"]),
                "external_stage_calls": int(execution["external_stage_call_frame_count"]),
                "edge_input_events": int(execution["edge_input_event_count"]),
            })
            all_state_audited = all_state_audited and bool(session["checksums_match_near_and_stage_inputs_valid"] and session["session_state_valid"])

    for key in PROFILES:
        block_vals = [r["block_interval_ms"] for r in block_rows if r["topology"] == key]
        # Every interval comes from consecutive block heights within a workflow's recorded receipt tree.
        assert block_vals

    # Build per-run and per-seed derived summaries.
    metric_summaries = {}
    for platform, key, field in [
        ("NEAR", "chain", "near_sink_poll_ms"), ("NEAR", "fanout6_join", "near_sink_poll_ms"),
        ("NEAR", "chain", "near_receipt_tree_ms"), ("NEAR", "fanout6_join", "near_receipt_tree_ms"),
        ("EVM", "chain", "evm_receipt_ms"), ("EVM", "fanout6_join", "evm_receipt_ms"),
    ]:
        vals = [r[field] for r in latency_rows if r["platform"] == platform and r["topology"] == key]
        metric_summaries[f"{platform}_{key}_{field}"] = median_range(vals)

    gas_summaries = {}
    for platform in ("NEAR", "EVM"):
        for key in PROFILES:
            for field in ("workflow_native_gas_tgas", "workflow_receipts", "same_receiver_native_gas_tgas", "same_receiver_receipts") if platform == "NEAR" else ("workflow_native_gas", "complete_session_gas_excluding_setup", "setup_gas_per_profile", "external_stage_calls", "edge_input_events"):
                vals = [r[field] for r in gas_receipt_rows if r["platform"] == platform and r["topology"] == key]
                gas_summaries[f"{platform}_{key}_{field}"] = median_range(vals)

    interval_summaries = {}
    for key in PROFILES:
        vals = [r["block_interval_ms"] for r in block_rows if r["topology"] == key]
        interval_summaries[key] = median_range(vals)

    # Failure control state evidence.
    nf = near_fail["completed_repeats"][0]["profiles"][0]["sessions"][0]["injected_failure"]
    ef = evm_fail["completed_repeats"][0]["profiles"][0]["independent_stage_sessions"][0]["failure_probe"]
    near_present = [row["node_id"] for row in nf["multi_contract_stored_nodes"] if row["result"]]
    near_missing = [row["node_id"] for row in nf["multi_contract_stored_nodes"] if not row["result"]]
    failure_summary = {
        "near": {
            "verified": bool(nf["verified"]), "failed_node": int(nf["fail_at_node"]),
            "persisted_nodes": near_present, "missing_nodes": near_missing,
            "persisted_node_count": len(near_present), "total_node_count": len(nf["multi_contract_stored_nodes"]),
            "missing_node_count": len(near_missing),
            "failed_receipt_observed": bool(nf["failed_receipt_observed"]),
            "multi_contract_receipt_count": int(nf["multi_contract"]["receipt_count"]),
            "multi_contract_gas_burnt_tgas": int(nf["multi_contract"]["gas_burnt"]) / 1e12,
            "full_receipt_tree_wall_ms": nf["multi_contract"].get("client_wall_ms_to_receipt_tree"),
            "same_receiver_failed": bool(nf["same_receiver"].get("failed")),
            "business_compensation_transactions": 0,
            "compensation_status": "no compensator implemented or invoked; recovery time not measured",
        },
        "evm": {
            "failed_node": int(ef["fail_node"]), "tx_status": int(ef["status"]),
            "failure_tx_gas_used": int(ef["gas_used"]), "client_wall_ms_to_failure_receipt": float(ef["client_wall_ms_to_receipt"]),
            "stage_call_frames_before_revert": len(ef["external_stage_call_frames"]),
            "all_contract_state_rolled_back": bool(ef["all_contract_state_rolled_back"]),
            "cleanup_tx_status": int(ef["cleanup"]["status"]), "cleanup_tx_gas_used": int(ef["cleanup"]["gas_used"]),
            "business_compensation_transactions": 0,
            "compensation_status": "abortSession is session cleanup, not business compensation; recovery time not measured",
        },
    }
    assert near_present == [0, 1, 2, 3, 5, 6]
    assert near_missing == [4, 7]
    assert nf["verified"] and nf["failed_receipt_observed"]
    assert ef["status"] == 0 and ef["all_contract_state_rolled_back"]
    assert all_state_audited

    near_runtime_rows = []
    for rep in near_raw["repeats"]:
        near_runtime_rows.append({
            "repeat": rep["repeat"], "sandbox_start_requested_at": rep.get("sandbox_start_requested_at"),
            "sandbox_ready_at": rep.get("sandbox_ready_at"), "sandbox_tear_down_requested_at": rep.get("sandbox_tear_down_requested_at"),
            "sandbox_tear_down_completed_at": rep.get("sandbox_tear_down_completed_at"),
            "near_version": rep.get("runtime", {}).get("rpc_version"), "protocol_version": rep.get("runtime", {}).get("protocol_version"),
        })
    evm_runtime_rows = evm.get("anvil_runs", [])

    csv_rows = [{"scenario_type": "topology_success", **row} for row in latency_rows + gas_receipt_rows]
    csv_rows.extend([
        {"scenario_type": "failure_control", "platform": "NEAR", "repeat": 1, "topology": "fanout6_join",
         "failed_node": failure_summary["near"]["failed_node"], "persisted_node_count": failure_summary["near"]["persisted_node_count"],
         "total_node_count": failure_summary["near"]["total_node_count"],
         "persisted_node_ids": ";".join(map(str, failure_summary["near"]["persisted_nodes"])),
         "missing_node_ids": ";".join(map(str, failure_summary["near"]["missing_nodes"])),
         "failure_receipt_observed": failure_summary["near"]["failed_receipt_observed"],
         "workflow_receipts": failure_summary["near"]["multi_contract_receipt_count"],
         "workflow_native_gas_tgas": failure_summary["near"]["multi_contract_gas_burnt_tgas"],
         "business_compensation_transactions": failure_summary["near"]["business_compensation_transactions"]},
        {"scenario_type": "failure_control", "platform": "EVM", "repeat": 1, "topology": "fanout6_join",
         "failed_node": failure_summary["evm"]["failed_node"], "tx_status": failure_summary["evm"]["tx_status"],
         "workflow_native_gas": failure_summary["evm"]["failure_tx_gas_used"],
         "external_stage_calls": failure_summary["evm"]["stage_call_frames_before_revert"],
         "all_contract_state_rolled_back": failure_summary["evm"]["all_contract_state_rolled_back"],
         "cleanup_tx_status": failure_summary["evm"]["cleanup_tx_status"],
         "cleanup_tx_gas_used": failure_summary["evm"]["cleanup_tx_gas_used"],
         "business_compensation_transactions": failure_summary["evm"]["business_compensation_transactions"]},
    ])
    with (OUT / "routeC_per_repeat_metrics.csv").open("w", newline="") as f:
        cols = sorted({k for row in csv_rows for k in row})
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(csv_rows)

    summary = {
        "experiment_id": "routeC_topology_failure_20261028",
        "topology_design": {
            "chain": {"nodes": 8, "edges": 7, "operations_per_node": 16},
            "fanout6_join": {"nodes": 8, "edges": 12, "operations_per_node": 16},
            "causal_scope": "node count and operations matched; edge count differs, so this is not a pure depth-only comparison",
        },
        "latency_per_repeat": latency_rows,
        "latency_summaries_median_min_max": metric_summaries,
        "native_cost_per_repeat": gas_receipt_rows,
        "native_cost_summaries_median_min_max": gas_summaries,
        "near_receipt_class_counts_by_topology": {k: dict(v) for k, v in receipt_classes.items()},
        "near_observed_workflow_block_intervals": interval_summaries,
        "near_sandbox_runtime_windows": near_runtime_rows,
        "evm_anvil_runtime_windows": evm_runtime_rows,
        "near_block_configuration": near["completed_repeats"][0]["runtime"].get("genesis_config"),
        "evm_mining_configuration": evm.get("mining_configuration"),
        "failure_control": failure_summary,
        "state_audit_all_passed": all_state_audited,
        "derived_data_self_check_passed": True,
        "independent_audit_status": "not completed",
        "formal_exit_codes": {"near": 0, "evm": 0},
        "resource_monitor": {
            "formal": load("formal_resource_summary.json"),
            "failure": load("failure_resource_summary.json"),
        },
        "remote_archive_sha256": "981160465ccdc3327bdeea9bbbf4985aaf3b38d6f0bc496bd9944a80302ae365",
        "limitations": [
            "n=3 fresh runtime repetitions per platform and topology; descriptive only, no inferential test",
            "NEAR sink timing is the first successful view poll at 50 ms intervals, not the exact instant state became queryable",
            "NEAR vs EVM wall times are not architecture-only; NEAR Sandbox block cadence and Anvil automine differ",
            "the shared remote container had no hard CPU or memory quota; CPU affinity is soft binding, not isolation",
            "the chain and fanout-join workloads differ by five dependency edges",
        ],
    }
    (OUT / "routeC_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
    (OUT / "routeC_topology_latency.svg").write_text(latency_svg(latency_rows), encoding="utf-8")
    (OUT / "routeC_failure_outcome.svg").write_text(failure_svg(nf, ef), encoding="utf-8")

    def s(key: str) -> dict:
        return metric_summaries[key]

    def med(key: str, digits: int = 2) -> str:
        m = s(key)
        return f"{m['median']:.{digits}f}（{m['min']:.{digits}f}–{m['max']:.{digits}f}）"

    chain_sink = s("NEAR_chain_near_sink_poll_ms")["median"]
    fork_sink = s("NEAR_fanout6_join_near_sink_poll_ms")["median"]
    chain_tree = s("NEAR_chain_near_receipt_tree_ms")["median"]
    fork_tree = s("NEAR_fanout6_join_near_receipt_tree_ms")["median"]
    chain_evm = s("EVM_chain_evm_receipt_ms")["median"]
    fork_evm = s("EVM_fanout6_join_evm_receipt_ms")["median"]
    chain_gas = gas_summaries["NEAR_chain_workflow_native_gas_tgas"]["median"]
    fork_gas = gas_summaries["NEAR_fanout6_join_workflow_native_gas_tgas"]["median"]
    chain_rcpt = gas_summaries["NEAR_chain_workflow_receipts"]["median"]
    fork_rcpt = gas_summaries["NEAR_fanout6_join_workflow_receipts"]["median"]
    evm_chain_gas = gas_summaries["EVM_chain_workflow_native_gas"]["median"]
    evm_fork_gas = gas_summaries["EVM_fanout6_join_workflow_native_gas"]["median"]
    formal_resource = summary["resource_monitor"]["formal"]
    failure_resource = summary["resource_monitor"]["failure"]
    start = near["started_at"]
    finish = near["finished_at"]
    def fmt_gas(key: str) -> str:
        metric = gas_summaries[key]
        return f"{metric['median']:.4f}（{metric['min']:.4f}–{metric['max']:.4f}）"

    near_chain_gas_text = fmt_gas("NEAR_chain_workflow_native_gas_tgas")
    near_fork_gas_text = fmt_gas("NEAR_fanout6_join_workflow_native_gas_tgas")
    near_windows_md = "\n".join(
        f"| R{r['repeat']} | {r['sandbox_start_requested_at']} | {r['sandbox_ready_at']} | {r['sandbox_tear_down_requested_at']} | {r['sandbox_tear_down_completed_at']} |"
        for r in near_runtime_rows
    )
    evm_windows_md = "\n".join(
        f"| R{r['repeat']} | {r['start_requested_at']} | {r['rpc_ready_at']} | {r['stop_requested_at']} | {r['stopped_at']} |"
        for r in evm_runtime_rows
    )
    report = f'''## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: run
- Origin Date: {datetime.now(timezone.utc).date().isoformat()}
- Verification Status: UNVERIFIED（本地派生数据自检通过；独立审计尚未完成）
- Version Label: exp_result_v1

## Experiment Result

- **ID**: routeC_topology_failure_20261028
- **Type**: generic
- **Status**: completed
- **Command**: 远端 `run_formal_remote.sh`（NEAR、EVM 各 3 个 fresh runtime；之后独立运行 `run_failure_control_remote.sh`）
- **Working Directory**: `{ROOT}`；远端目录 `/home/starshell/das_controller2_topology_20261028`
- **Duration**: 正式拓扑段 85 秒；另含独立失败控制段
- **Exit Code**: 正式 NEAR 0，正式 EVM 0；失败控制 NEAR 0，EVM 0

### 实验设计与运行边界

每个工作流均为 8 个有状态节点、每节点 16 次操作。链为 7 条依赖边；根→6 分支→汇合为 12 条边，故两者不是纯深度因果对照。每个平台、每种拓扑均运行 3 次独立 Sandbox/Anvil，workflow 状态和 checksum 审计均通过；结果按三个重复逐点呈现，样本量只支持描述性比较。本轮使用实际 Sandbox/Anvil 原生设置，独立成组，不与此前固定 1 秒区块条件下的结果合并。

NEAR 实际版本为 2.13.4、protocol 86，三次均使用同一 binary SHA-256 `{near['completed_repeats'][0]['runtime']['sandbox_binary_sha256']}`，单验证者、单 shard。Sandbox 未显式覆盖 block interval；工作流 receipt 所在连续 block 高度的 timestamp 间隔为链中位数 {interval_summaries['chain']['median']:.2f} ms（{interval_summaries['chain']['min']:.2f}–{interval_summaries['chain']['max']:.2f}，n={interval_summaries['chain']['n']} 个间隔），汇合拓扑中位数 {interval_summaries['fanout6_join']['median']:.2f} ms（{interval_summaries['fanout6_join']['min']:.2f}–{interval_summaries['fanout6_join']['max']:.2f}，n={interval_summaries['fanout6_join']['n']}）。这些间隔来自工作流内逐个连续 block 高度，非整个 Sandbox 空闲期的全时段采样。

EVM 使用 Anvil 1.8.6、automine 默认开启，未设置 `--block-time`；每次 fresh Anvil 的启动、RPC ready、停止请求和停止时间见 `routeC_summary.json`。Anvil 区块 timestamp 是整数秒的逻辑时间，不能视为实际 RPC wall time。远端容器有 20 个可用 CPU、无硬 CPU/内存 quota；运行软绑定 CPU 16–19。正式段资源采样 {formal_resource['samples']} 次，任务进程树峰值采样 RSS {formal_resource['task_tree_peak_sampled_rss_kb']/1024:.1f} MiB、CPU 峰值 {formal_resource['task_tree_peak_sampled_cpu_pct_of_one_core']:.1f}% of one core；独立失败段采样 {failure_resource['samples']} 次，RSS 峰值 {failure_resource['task_tree_peak_sampled_rss_kb']/1024:.1f} MiB、CPU 峰值 {failure_resource['task_tree_peak_sampled_cpu_pct_of_one_core']:.1f}% of one core。容器内存和其他进程共享，RSS 可能重复计算共享映射，结果不代表独占资源。

运行边界时间均为 UTC：

| NEAR repeat | Sandbox 请求启动 | RPC ready | teardown 请求 | teardown 完成 |
|---|---|---|---|---|
{near_windows_md}

| EVM repeat | Anvil 请求启动 | RPC ready | stop 请求 | stop 完成 |
|---|---|---|---|---|
{evm_windows_md}

### 拓扑延迟与原生执行成本

| 平台/指标 | 8 节点链，中位数（范围） | 根→6 分支→汇合，中位数（范围） |
|---|---:|---:|
| NEAR sink 首次成功 view 查询（ms） | {med('NEAR_chain_near_sink_poll_ms')} | {med('NEAR_fanout6_join_near_sink_poll_ms')} |
| NEAR 完整 receipt tree 返回（ms） | {med('NEAR_chain_near_receipt_tree_ms')} | {med('NEAR_fanout6_join_near_receipt_tree_ms')} |
| NEAR receipt tree 高度跨度（blocks） | {statistics.median([r['workflow_completion_lag_blocks'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='chain']):.0f} | {statistics.median([r['workflow_completion_lag_blocks'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='fanout6_join']):.0f} |
| NEAR 原生 gas（Tgas/workflow） | {near_chain_gas_text} | {near_fork_gas_text} |
| NEAR receipt 数（不含 originating tx） | {chain_rcpt:.0f} | {fork_rcpt:.0f} |
| EVM execution tx receipt（ms） | {med('EVM_chain_evm_receipt_ms')} | {med('EVM_fanout6_join_evm_receipt_ms')} |
| EVM 原生 gas（gas/workflow） | {evm_chain_gas:,} | {evm_fork_gas:,} |
| EVM 完整 session gas（entry+execute+exit，不含部署） | {gas_summaries['EVM_chain_complete_session_gas_excluding_setup']['median']:,} | {gas_summaries['EVM_fanout6_join_complete_session_gas_excluding_setup']['median']:,} |
| EVM 部署与注册 gas（每拓扑、每 Anvil） | {gas_summaries['EVM_chain_setup_gas_per_profile']['median']:,} | {gas_summaries['EVM_fanout6_join_setup_gas_per_profile']['median']:,} |
| EVM 真实 Stage 外部 CALL / edge events | 8 / 7 | 8 / 12 |

NEAR 汇合拓扑的 sink 首次成功轮询中位数较链快 {((chain_sink-fork_sink)/chain_sink*100):.1f}%，完整 receipt tree 中位数较链快 {((chain_tree-fork_tree)/chain_tree*100):.1f}%；同时 native gas 增加 {((fork_gas/chain_gas-1)*100):.1f}%，receipt 数增加 {((fork_rcpt/chain_rcpt-1)*100):.1f}%。EVM 两种拓扑的交易 wall time 接近（汇合中位数相对链 {((fork_evm/chain_evm-1)*100):+.2f}%），汇合拓扑 native gas 增加 {((evm_fork_gas/evm_chain_gas-1)*100):.1f}%。两个 VM 的 gas 单位和时间设置不同，跨平台绝对秒数及 gas 不直接作架构优劣比较。

NEAR 三次重复的 receipt 分类完全一致：链为 `start` 1、`receive_input` 7、`transfer_or_refund` 7；汇合为 `start` 1、`receive_input` 12、`on_fanout_complete` 1、`transfer_or_refund` 13。receipt 总数不是依赖轮数。sink 指标是每 50 ms 查询一次后首次读到值的时间；汇合拓扑三次均在完整 receipt tree 返回前读到 sink，链的三次均未早于 tree 返回被观测到。因此 sink 观测是轮询上界，不等于精确可见时刻。

NEAR 同 receiver 控制每个拓扑每次均为 1 个 receipt；完整 tree wall time 中位数约 {statistics.median([r['same_receiver_tree_ms'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='chain']):.1f} ms（链）和 {statistics.median([r['same_receiver_tree_ms'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='fanout6_join']):.1f} ms（汇合），native gas 中位数分别 {statistics.median([r['same_receiver_native_gas_tgas'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='chain']):.4f} 与 {statistics.median([r['same_receiver_native_gas_tgas'] for r in gas_receipt_rows if r['platform']=='NEAR' and r['topology']=='fanout6_join']):.4f} Tgas。此控制把节点状态操作合并在同一接收合约，作用是边界参照，不与独立 Stage 拓扑混为同一执行模型。

### 中间分支失败控制

在汇合拓扑 node 4 注入失败。NEAR 完整 receipt tree 观察到失败 receipt（25 个 receipt、34.4938 Tgas）；节点 0、1、2、3、5、6 保留状态，节点 4 失败且 sink 7 不存在（6/8 节点有状态）。这表明独立异步调用可留下部分结果，不据此称为协议漏洞。当前实现没有业务补偿器，实际执行的补偿交易数为 0，恢复时间未测量。

EVM 中间 Stage CALL 失败，执行交易 receipt status 为 {failure_summary['evm']['tx_status']}，故障交易消耗 {failure_summary['evm']['failure_tx_gas_used']:,} gas；已有 {failure_summary['evm']['stage_call_frames_before_revert']} 个 Stage CALL frame，所有合约状态在交易回滚后均不存在。这反映 EVM 单笔交易的回滚边界，不是 DAS 独占的原子性。随后执行 1 笔 `abortSession` 会话清理交易（status {failure_summary['evm']['cleanup_tx_status']}，{failure_summary['evm']['cleanup_tx_gas_used']:,} gas），它不是业务补偿。两端均没有实现或执行业务补偿，不能声称已恢复。

### 图表与输出文件

- `routeC_topology_latency.svg`：三次逐点延迟；各平台单独面板，避免把运行时区块节奏差异误读成架构效应。
- `routeC_failure_outcome.svg`：NEAR 部分持久状态与 EVM 整笔回滚的节点级示意。
- `routeC_per_repeat_metrics.csv`：每个重复的 latency、native gas、receipt/CALL 数。
- `routeC_summary.json`：完整派生统计、运行窗口、区块配置、失败状态、资源边界。
- 原始结果：`near_topology_formal_raw.json`、`evm_topology_formal_raw.json`、`near_topology_failure_raw.json`、`evm_topology_failure_raw.json`；归档 SHA-256 `{summary['remote_archive_sha256']}`。

### 优势、代价与边界

- **优势**：在本次 NEAR Sandbox 条件下，分支汇合更早产生 sink 且完整 receipt tree 时间更短；运行状态与 checksum 审计通过。
- **代价**：分支汇合以更多依赖边、receipt 和 native gas 换取较低完成时间；EVM 交易时间没有观察到同样的收益。
- **边界**：仅 3 次 fresh runtime、受控单工作流；链和汇合边数不同、共享容器且 NEAR/EVM 出块设定不同，结果不支持跨平台架构因果或生产环境吞吐结论。

### 异常与审计说明

正式拓扑和失败控制退出码均为 0，成功工作流状态审计全部通过；失败控制中的 node 4 failure 是预期注入。此前的若干远端准备失败（工具缺项、错误目标路径、进程预检未命中）保留在 `near_*failure*` 与 startup 日志中，均发生在对应工作负载启动前，不计作工作流失败。
'''
    (ROOT / "formal_report.md").write_text(report, encoding="utf-8")
    print("Wrote Route C summary, CSV, two SVG figures, and formal_report.md")


if __name__ == "__main__":
    main()
