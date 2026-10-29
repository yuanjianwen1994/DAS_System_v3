"""Audit retained d2/d4/d8 timing rows for identity and state attribution."""
import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
near = json.loads((ROOT / "near_results.json").read_text())
evm = json.loads((ROOT / "evm_results.json").read_text())
assert len(near["completed_repeats"]) == len(evm["completed_repeats"]) == 3

near_rows, evm_rows = {}, {}
details = []

for repeat in near["completed_repeats"]:
    for profile in repeat["profiles"]:
        profile_id = profile["profile"]["profile_id"]
        depth = profile["profile"]["node_count"]
        assert depth in (2, 4, 8) and len(profile["sessions"]) == 2
        for session in profile["sessions"]:
            sid = str(session["session_id"])
            assert len(session["theta_workflows"]) == 1
            flow = session["theta_workflows"][0]
            wid = str(flow["workflow_id"])
            assert str(flow["session_id"]) == sid
            runs = session["multi_contract"]["runs"]
            assert len(runs) == 1
            run = runs[0]
            assert run["succeeded"] and not run["failed"]
            assert run["workflow"] == flow
            vis = run["sink_visibility"]
            assert vis["first_sink_queryable_at"] and vis["api_to_sink_queryable_ms"] > 0

            states = session["same_nodes_and_checksum"]
            assert len(states) == 1 and states[0]["all_nodes_present"] and states[0]["nodes_match_same_receiver"]
            state_row = states[0]
            assert str(state_row["workflow"]["session_id"]) == sid
            assert str(state_row["workflow"]["workflow_id"]) == wid
            state_by_node = {int(x["node_id"]): x["result"] for x in state_row["nodes"]}
            assert set(state_by_node) == set(range(depth))
            events = {}
            for record in run["records"]:
                if record["status_kind"].startswith("Failure") or record.get("failure"):
                    raise AssertionError(f"NEAR failed record: {repeat['repeat']} {profile_id} {sid}")
                for log in record.get("logs", []):
                    if log.startswith("DAG_NODE|"):
                        _, event_sid, event_wid, node, checksum, ops = log.split("|")
                        assert event_sid == sid and event_wid == wid
                        node = int(node)
                        assert node not in events
                        events[node] = (checksum, int(ops), record["outcome_id"])
            assert set(events) == set(range(depth))
            for node, state in state_by_node.items():
                checksum, ops, _ = events[node]
                assert str(state["session_id"]) == sid and int(state["node_id"]) == node
                assert str(state["checksum"]) == checksum
                assert int(state["input_count"]) == (0 if node == 0 else 1)
                assert int(state["operation_count"]) == ops == 16
            key = (repeat["repeat"], profile_id, sid, wid)
            assert key not in near_rows
            near_rows[key] = {"depth": depth, "latency_ms": float(vis["api_to_sink_queryable_ms"]),
                              "node_checksums": [events[i][0] for i in range(depth)],
                              "tx_block_height": run.get("tx_block_height"),
                              "completion_block_height": run.get("completion_block_height")}

for repeat in evm["completed_repeats"]:
    for profile in repeat["profiles"]:
        profile_id = profile["profile"]["profile_id"]
        depth = profile["profile"]["node_count"]
        assert depth in (2, 4, 8) and len(profile["independent_stage_sessions"]) == 2
        for session in profile["independent_stage_sessions"]:
            sid = str(session["session_id"])
            assert len(session["theta_workflows"]) == 1
            wid = str(session["theta_workflows"][0])
            assert session["session_state_valid"] and session["checksums_match_near_and_stage_inputs_valid"]
            run = session["router_execution"]
            assert int(run["status"]) == 1 and int(run["transaction_count"]) == 1
            assert int(run["node_event_count"]) == depth
            assert int(run["external_stage_call_frame_count"]) == depth
            assert all(frame.get("error") is None for frame in run["external_stage_call_frames"])
            vis = run["sink_visibility"]
            assert vis["first_sink_queryable_at"] and vis["api_to_sink_queryable_ms"] > 0
            events = sorted(run["node_events"], key=lambda x: int(x["node_id"]))
            assert [int(x["node_id"]) for x in events] == list(range(depth))
            assert all(str(x["session_id"]) == sid and str(x["workflow_id"]) == wid for x in events)
            key = (repeat["repeat"], profile_id, sid, wid)
            assert key not in evm_rows
            evm_rows[key] = {"depth": depth, "latency_ms": float(vis["api_to_sink_queryable_ms"]),
                             "node_checksums": [str(x["checksum"]) for x in events],
                             "router_tx_hash": run["tx_hash"], "block_number": run["block_number"]}

assert len(near_rows) == len(evm_rows) == 18
assert near_rows.keys() == evm_rows.keys(), "NEAR and EVM rows do not share session/workflow identities"
for key in near_rows:
    assert near_rows[key]["node_checksums"] == evm_rows[key]["node_checksums"], f"node checksum mismatch: {key}"

summaries = []
for platform, table in (("NEAR Sandbox", near_rows), ("EVM / Anvil", evm_rows)):
    for depth in (2, 4, 8):
        vals = [x["latency_ms"] for x in table.values() if x["depth"] == depth]
        assert len(vals) == 6
        summaries.append({"platform": platform, "depth": depth, "n": len(vals),
                          "median_ms": statistics.median(vals), "min_ms": min(vals), "max_ms": max(vals),
                          "mean_ms": statistics.mean(vals), "sample_sd_ms": statistics.stdev(vals)})

csv_rows = []
for key in sorted(near_rows):
    rep, profile_id, sid, wid = key
    csv_rows.append({"repeat": rep, "profile_id": profile_id, "session_id": sid, "workflow_id": wid,
                     "depth": near_rows[key]["depth"], "near_sink_ms": near_rows[key]["latency_ms"],
                     "evm_sink_ms": evm_rows[key]["latency_ms"],
                     "near_start_height": near_rows[key]["tx_block_height"],
                     "near_complete_height": near_rows[key]["completion_block_height"],
                     "evm_router_tx_hash": evm_rows[key]["router_tx_hash"],
                     "evm_router_block": evm_rows[key]["block_number"]})

out = {"status": "all assertions passed", "paired_rows": len(csv_rows),
       "checks": ["unique repeat/profile/session/workflow keys", "same workflow identities on both platforms",
                  "successful sink observations", "NEAR per-node logs match stored session/node/checksum/input/operation state",
                  "EVM router status and independent external Stage CALL count", "cross-platform per-node checksums match"],
       "summary": summaries, "paired_sessions": csv_rows}
(ROOT / "existing_timing_identity_audit.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps({"status": out["status"], "paired_rows": len(csv_rows), "summary": summaries}, indent=2))
