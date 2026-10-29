#!/usr/bin/env python3
"""Summarize the retained NEAR Sandbox and local EVM raw DAG results."""
import csv
import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
NEAR = HERE.parent / "near_runtime_20261027" / "near_dag_full_results.json"
EVM = HERE / "das_composition_full_results.json"
EVM_DISTRIBUTED = HERE / "das_distributed_composition_results.json"


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def main():
    near = json.loads(NEAR.read_text())
    evm = json.loads(EVM.read_text())
    distributed = json.loads(EVM_DISTRIBUTED.read_text())
    grouped = defaultdict(lambda: {"near_runs": [], "near_same": [], "near_faults": [], "evm": []})
    distributed_grouped = defaultdict(lambda: {"rows": [], "sessions": []})

    for repeat in near["completed_repeats"]:
        for row in repeat["profiles"]:
            pid = row["profile"]["profile_id"]
            acc = grouped[pid]
            for session in row["sessions"]:
                multi = session["multi_contract"]
                for run in multi["runs"]:
                    acc["near_runs"].append(run)
                same = session["same_receiver_best_control"]
                acc["near_same"].append(same)
                fault = session["injected_failure"]
                acc["near_faults"].append(fault)

    for repeat in evm["completed_repeats"]:
        for row in repeat["profiles"]:
            grouped[row["profile"]["profile_id"]]["evm"].extend(row["local_type_ii_session_mode"])

    for repeat in distributed["completed_repeats"]:
        for row in repeat["profiles"]:
            acc = distributed_grouped[row["profile"]["profile_id"]]
            acc["rows"].append(row)
            acc["sessions"].extend(row["independent_stage_sessions"])

    profiles = {p["profile_id"]: p for p in json.loads((HERE / "profile_manifest.json").read_text())["profiles"]}
    output = []
    for pid, acc in grouped.items():
        p = profiles[pid]
        runs = acc["near_runs"]
        same = acc["near_same"]
        faults = acc["near_faults"]
        evms = acc["evm"]
        distributed_rows = distributed_grouped[pid]["rows"]
        distributed_sessions = distributed_grouped[pid]["sessions"]
        fanout_nodes = sum(len(children) > 1 for children in p["successors"])
        function_call_nodes = 1 + p["edge_count"]
        refund_transfers = p["edge_count"] + fanout_nodes
        expected_near_receipts = function_call_nodes + fanout_nodes + refund_transfers
        all_near_decompositions_match = all(
            run["receipt_count"] == expected_near_receipts
            for rep in near["completed_repeats"]
            for row in rep["profiles"] if row["profile"]["profile_id"] == pid
            for session in row["sessions"]
            for run in session["multi_contract"]["runs"]
        )
        def sink_lag(run):
            if "dag_sink_completion_lag_blocks" in run:
                return run["dag_sink_completion_lag_blocks"]
            sinks = [i for i, children in enumerate(p["successors"]) if not children]
            event_heights = [e["block_height"] for e in run["log_events"] if e["type"] == "node_execution" and e["node_id"] in sinks]
            return max(event_heights) - run["tx_block_height"]
        output.append({
            "profile_id": pid,
            "shape": p["shape"],
            "depth_d": p["dependency_depth_d"],
            "width_w": p["parallel_width_w"],
            "theta": p["workflows_per_session_theta"],
            "nu": p["concurrent_workflows_nu"],
            "nodes": p["node_count"],
            "edges": p["edge_count"],
            "operations_per_node": p["operations_per_node"],
            "near_workflow_successes": sum(bool(x["succeeded"]) and not x["failed"] for x in runs),
            "near_workflow_count": len(runs),
            "near_mean_receipts_per_workflow": mean([x["receipt_count"] for x in runs]),
            "near_node_function_call_receipts_per_workflow": function_call_nodes,
            "near_fanout_barrier_callback_receipts_per_workflow": fanout_nodes,
            "near_system_refund_transfer_receipts_per_workflow": refund_transfers,
            "near_receipt_decomposition_verified": all_near_decompositions_match,
            "near_mean_sink_completion_lag_blocks": mean([sink_lag(x) for x in runs]),
            "near_mean_receipt_tree_drain_lag_blocks": mean([x["completion_lag_blocks"] for x in runs]),
            "near_mean_gas_burnt_per_workflow": mean([int(x["gas_burnt"]) for x in runs]),
            "near_same_receiver_mean_receipts_per_theta_session": mean([x["receipt_count_for_theta_session"] for x in same]),
            "near_same_receiver_mean_gas_burnt_per_theta_session": mean([int(x["run"]["gas_burnt"]) for x in same]),
            "near_fault_receipts_observed": sum(bool(x["failed_receipt_observed"]) for x in faults),
            "near_fault_trials": len(faults),
            "near_fault_workflows_incomplete": sum(bool(x["multi_contract_unfinished"]) for x in faults),
            "near_fault_sink_completed": sum(bool(x["multi_contract_completed_sinks"]) for x in faults),
            "near_checksum_parity_trials": sum(
                all(x["all_nodes_present"] and x["nodes_match_same_receiver"] for x in session["same_nodes_and_checksum"])
                for rep in near["completed_repeats"]
                for row in rep["profiles"] if row["profile"]["profile_id"] == pid
                for session in row["sessions"]
            ),
            "evm_fused_control_checksum_parity_sessions": sum(bool(x["checksums_match_near_same_receiver"]) for x in evms),
            "evm_fused_control_session_count": len(evms),
            "evm_fused_control_mean_entry_gas": mean([x["migration_in"]["gas_used"] for x in evms]),
            "evm_fused_control_mean_execution_gas": mean([x["workflow_execution"]["gas_used"] for x in evms]),
            "evm_fused_control_mean_exit_gas": mean([x["migration_out"]["gas_used"] for x in evms]),
            "evm_fused_control_mean_session_gas": mean([x["complete_session_gas"] for x in evms]),
            "evm_fused_control_fault_rollbacks": sum(bool(x["failure_probe"]["unfinished_and_rolled_back"]) for x in evms),
            "evm_fused_control_fault_trials": len(evms),
            "evm_independent_stage_session_count": len(distributed_sessions),
            "evm_independent_stage_setup_gas_per_profile": mean([x["deployment_and_registration_gas_per_profile"] for x in distributed_rows]),
            "evm_independent_stage_mean_entry_proxy_gas": mean([x["entry_proxy"]["gas_used"] for x in distributed_sessions]),
            "evm_independent_stage_mean_router_execution_gas": mean([x["local_execution_gas"] for x in distributed_sessions]),
            "evm_independent_stage_mean_external_stage_frame_gas_sum": mean([x["router_execution"]["external_stage_call_gas_used_sum"] for x in distributed_sessions]),
            "evm_independent_stage_mean_exit_proxy_gas": mean([x["exit_proxy"]["gas_used"] for x in distributed_sessions]),
            "evm_independent_stage_mean_session_gas_excluding_setup": mean([x["complete_session_gas_excluding_setup"] for x in distributed_sessions]),
            "evm_independent_stage_mean_session_gas_including_unamortized_setup": mean([
                x["complete_session_gas_excluding_setup"] + next(r["deployment_and_registration_gas_per_profile"] for r in distributed_rows if x in r["independent_stage_sessions"])
                for x in distributed_sessions
            ]),
            "evm_independent_stage_mean_execution_transactions": mean([x["router_execution"]["transaction_count"] for x in distributed_sessions]),
            "evm_independent_stage_mean_external_stage_calls": mean([x["router_execution"]["external_stage_call_frame_count"] for x in distributed_sessions]),
            "evm_independent_stage_mean_node_execution_events": mean([x["router_execution"]["node_event_count"] for x in distributed_sessions]),
            "evm_independent_stage_mean_parent_input_events": mean([x["router_execution"]["edge_input_event_count"] for x in distributed_sessions]),
            "evm_independent_stage_checksum_and_input_valid_sessions": sum(bool(x["checksums_match_near_and_stage_inputs_valid"]) for x in distributed_sessions),
            "evm_independent_stage_rollback_trials": sum(bool(x["failure_probe"]["all_contract_state_rolled_back"]) for x in distributed_sessions),
            "evm_independent_stage_fault_trials": len(distributed_sessions),
        })

    output.sort(key=lambda x: x["profile_id"])
    summary = {
        "experiment": "parameterized stateful DAG runtime comparison",
        "near_raw_source": str(NEAR),
        "evm_raw_source": str(EVM),
        "near_repeats": len(near["completed_repeats"]),
        "near_profile_repeats": sum(len(r["profiles"]) for r in near["completed_repeats"]),
        "evm_repeats": len(evm["completed_repeats"]),
        "evm_profile_repeats": sum(len(r["profiles"]) for r in evm["completed_repeats"]),
        "evm_independent_stage_repeats": len(distributed["completed_repeats"]),
        "evm_independent_stage_profile_repeats": sum(len(r["profiles"]) for r in distributed["completed_repeats"]),
        "evm_independent_stage_execution_transactions": sum(len(row["independent_stage_sessions"]) for rep in distributed["completed_repeats"] for row in rep["profiles"]),
        "evm_independent_stage_external_call_frames": sum(session["router_execution"]["external_stage_call_frame_count"] for rep in distributed["completed_repeats"] for row in rep["profiles"] for session in row["independent_stage_sessions"]),
        "evm_independent_stage_node_execution_events": sum(session["router_execution"]["node_event_count"] for rep in distributed["completed_repeats"] for row in rep["profiles"] for session in row["independent_stage_sessions"]),
        "evm_independent_stage_parent_input_events": sum(session["router_execution"]["edge_input_event_count"] for rep in distributed["completed_repeats"] for row in rep["profiles"] for session in row["independent_stage_sessions"]),
        "evm_independent_stage_all_contract_state_rollbacks": all(session["failure_probe"]["all_contract_state_rolled_back"] for rep in distributed["completed_repeats"] for row in rep["profiles"] for session in row["independent_stage_sessions"]),
        "checksum_parity_all_profiles": all(x["near_checksum_parity_trials"] == 6 and x["evm_fused_control_checksum_parity_sessions"] == 6 and x["evm_independent_stage_checksum_and_input_valid_sessions"] == 6 for x in output),
        "profiles": output,
    }
    (HERE / "dag_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (HERE / "dag_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    print(json.dumps({k: v for k, v in summary.items() if k != "profiles"}, indent=2))


if __name__ == "__main__":
    main()
