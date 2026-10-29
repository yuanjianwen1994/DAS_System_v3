#!/usr/bin/env python3
"""Audit receipt-outcome counts against NEAR receipt action payloads."""
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
NEAR = HERE.parent / "near_runtime_20261027" / "near_dag_full_results.json"
DIAGNOSTIC = HERE.parent / "near_runtime_20261027" / "near_receipt_diagnostic_results.json"


def main():
    full = json.loads(NEAR.read_text())
    diagnostic = json.loads(DIAGNOSTIC.read_text())
    profiles_out = []
    full_by_id = {}
    for repeat in full["completed_repeats"]:
        for profile in repeat["profiles"]:
            full_by_id.setdefault(profile["profile"]["profile_id"], []).append(profile)

    actual_diag = {}
    for repeat in diagnostic["completed_repeats"]:
        for profile in repeat["profiles"]:
            pid = profile["profile"]["profile_id"]
            counters = Counter()
            sample_rows = []
            for session in profile["sessions"]:
                for run in session["multi_contract"]["runs"]:
                    for record in run["records"][1:]:
                        detail = record.get("receipt_details", {})
                        if detail.get("receipt", {}).get("Action"):
                            action = detail["receipt"]["Action"]
                            for item in action.get("actions", []):
                                if "FunctionCall" in item:
                                    name = item["FunctionCall"].get("method_name", "unknown")
                                    counters[f"function_call:{name}"] += 1
                                elif "Transfer" in item:
                                    kind = "system_refund_transfer" if detail.get("predecessor_id") == "system" else "other_transfer"
                                    counters[kind] += 1
                                else:
                                    counters["other_action"] += 1
                            sample_rows.append({
                                "receipt_id": record["outcome_id"],
                                "predecessor_id": detail.get("predecessor_id"),
                                "receiver_id": detail.get("receiver_id"),
                                "method": next((x["FunctionCall"].get("method_name") for x in action.get("actions", []) if "FunctionCall" in x), None),
                                "system_refund": detail.get("predecessor_id") == "system" and any("Transfer" in x for x in action.get("actions", [])),
                            })
            actual_diag[pid] = {"action_counts": dict(sorted(counters.items())), "sample_receipts": sample_rows}

    overall_valid = True
    for pid, runs in full_by_id.items():
        first_profile = runs[0]["profile"]
        edges = first_profile["edge_count"]
        fanouts = sum(len(children) > 1 for children in first_profile["successors"])
        node_function_calls = 1 + edges
        fanout_callbacks = fanouts
        refund_transfers = edges + fanouts
        expected = node_function_calls + fanout_callbacks + refund_transfers
        measured = []
        execution_event_checks = []
        join_node_checks = []
        join_nodes = {node for node, parents in enumerate(first_profile["parents"]) if len(parents) > 1}
        join_audit = {node: {"workflow_executions": 0, "single_node_execution": 0, "complete_parent_edges": 0, "complete_stored_parent_values": 0, "checksum_matches_control": 0} for node in join_nodes}
        for rep_profile in runs:
            for session in rep_profile["sessions"]:
                for run in session["multi_contract"]["runs"]:
                    measured.append(run["receipt_count"])
                    nodes = Counter(e["node_id"] for e in run["log_events"] if e["type"] == "node_execution")
                    edges_seen = [e for e in run["log_events"] if e["type"] == "edge_receipt"]
                    callbacks = [r for r in run["records"] if any(log.startswith("DAG_JOIN|") for log in r["logs"])]
                    execution_event_checks.append(
                        nodes == Counter({i: 1 for i in range(first_profile["node_count"])})
                        and len(edges_seen) == edges
                        and len(callbacks) == fanouts
                    )
                    for node in join_nodes:
                        join_audit[node]["workflow_executions"] += 1
                        join_audit[node]["single_node_execution"] += int(nodes[node] == 1)
                        received = sum(e["node_id"] == node for e in edges_seen)
                        join_audit[node]["complete_parent_edges"] += int(received == len(first_profile["parents"][node]))
                for flow in session["same_nodes_and_checksum"]:
                    for node, expected_parents in enumerate(first_profile["parents"]):
                        evidence = flow["nodes"][node]["result"]
                        join_node_checks.append(
                            evidence is not None
                            and evidence["input_count"] == len(expected_parents)
                            and evidence["operation_count"] == first_profile["operations_per_node"]
                            and flow["nodes_match_same_receiver"]
                        )
                        if node in join_nodes:
                            join_audit[node]["complete_stored_parent_values"] += int(evidence is not None and evidence["input_count"] == len(expected_parents))
                            join_audit[node]["checksum_matches_control"] += int(flow["nodes_match_same_receiver"] and evidence is not None)
        decomposition_ok = all(x == expected for x in measured)
        records_ok = all(execution_event_checks)
        overall_valid = overall_valid and decomposition_ok and records_ok
        profiles_out.append({
            "profile_id": pid,
            "node_count": first_profile["node_count"],
            "edge_count": edges,
            "fanout_producer_count": fanouts,
            "per_workflow_receipt_outcomes": {
                "function_call_node_entries": node_function_calls,
                "function_call_fanout_barrier_callbacks": fanout_callbacks,
                "system_transfer_refunds": refund_transfers,
                "total_receipt_outcomes": expected,
            },
            "measured_receipt_outcome_counts": sorted(set(measured)),
            "receipt_outcome_decomposition_verified_all_repeats": decomposition_ok,
            "one_execution_per_node_all_edges_once_and_callbacks_once": records_ok,
            "all_multi_parent_join_node_inputs_and_outputs_verified": all(join_node_checks),
            "multi_parent_node_execution_checks": len(join_node_checks),
            "multi_parent_join_nodes": [
                {"node_id": node, "expected_parent_count": len(first_profile["parents"][node]), **counts,
                 "verified_once_after_all_parent_values": all(value == counts["workflow_executions"] for key, value in counts.items() if key != "workflow_executions")}
                for node, counts in sorted(join_audit.items())
            ],
            "action_payload_diagnostic": actual_diag.get(pid),
        })

    result = {
        "receipt_count_definition": "receipts_outcome entries returned for each originating start transaction; the originating transaction itself is recorded separately",
        "receipt_type_audit": "EXPERIMENTAL_receipt Action payloads inspected for a representative chain and a width-4 fork-join. The function call methods are start, receive_input, and on_fanout_complete. Every inspected Transfer has predecessor_id=system and is treated as a refund transfer. New/deploy/init actions are outside measured start calls.",
        "diagnostic_profile_repeats": 1,
        "diagnostic_session_override": 1,
        "all_profiles_decomposition_verified": overall_valid,
        "profiles": profiles_out,
    }
    (HERE / "receipt_classification_audit.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"all_profiles_decomposition_verified": overall_valid, "profiles": len(profiles_out), "diagnostic_profiles": list(actual_diag)}, indent=2))


if __name__ == "__main__":
    main()
