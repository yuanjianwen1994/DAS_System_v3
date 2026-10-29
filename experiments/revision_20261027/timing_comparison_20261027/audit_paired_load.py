#!/usr/bin/env python3
"""Audit paired NEAR/EVM open-loop load data and emit compact metrics."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path


def nearest_rank(values: list[float], q: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def main(folder: Path) -> None:
    schedule = json.loads((folder.parent.parent / "paired_load_schedule.json").read_text())
    expected_sha = hashlib.sha256((folder.parent.parent / "paired_load_schedule.json").read_bytes()).hexdigest()
    data = {
        name: json.loads((folder / f"{folder.name}_{name}_raw.json").read_text())
        for name in ("near", "evm")
    }
    for name, raw in data.items():
        if raw.get("schedule_sha256") != expected_sha:
            raise ValueError(f"{name}: schedule hash mismatch")
    audit = {"schedule_sha256": expected_sha, "checks": {}, "metrics": []}
    near_phases = {(x["phase"], x["warmup"]): x for x in data["near"]["phases"]}
    evm_phases = {(x["phase"], x["warmup"]): x for x in data["evm"]["phases"]}
    if near_phases.keys() != evm_phases.keys():
        raise ValueError("NEAR and EVM phase keys differ")
    all_ids: dict[str, set[str]] = {"near": set(), "evm": set()}
    pair_count = 0
    for key in sorted(near_phases):
        n, e = near_phases[key], evm_phases[key]
        if (n["offered_rate_per_sec"], n["offered"], n["warmup"]) != (e["offered_rate_per_sec"], e["offered"], e["warmup"]):
            raise ValueError(f"phase {key}: offered schedule differs")
        nr, er = n["rows"], e["rows"]
        if len(nr) != n["offered"] or len(er) != e["offered"]:
            raise ValueError(f"phase {key}: row count differs from offered count")
        nmap = {str(row["workflow_id"]): row for row in nr}
        emap = {str(row["workflow_id"]): row for row in er}
        if len(nmap) != len(nr) or len(emap) != len(er) or nmap.keys() != emap.keys():
            raise ValueError(f"phase {key}: duplicate or unpaired workflow IDs")
        for wid in nmap:
            a, b = nmap[wid], emap[wid]
            if str(a["session_id"]) != str(b["session_id"]):
                raise ValueError(f"workflow {wid}: session IDs differ")
            if a.get("seed") is not None and b.get("seed") is not None and str(a["seed"]) != str(b["seed"]):
                raise ValueError(f"workflow {wid}: deterministic seeds differ")
            if a["succeeded"] != b["succeeded"]:
                raise ValueError(f"workflow {wid}: success outcomes differ")
            if not a["pre_submit_sink_state"] is None:
                raise ValueError(f"NEAR workflow {wid}: sink was not null before submit")
            if not b.get("pre_submit_sink_check_passed") or bool(b["pre_submit_sink_state"][0]):
                raise ValueError(f"EVM workflow {wid}: pre-submit sink check failed")
            if not a.get("states_match_receipt_events") or not b.get("states_match_receipt_events"):
                raise ValueError(f"workflow {wid}: node state/event audit failed")
            if not a.get("transaction_hash_matches_outcome") or not b.get("transaction_identity_nonce_match"):
                raise ValueError(f"workflow {wid}: transaction identity audit failed")
            if len(a.get("node_events", [])) != 4 or len(b.get("node_events", [])) != 4:
                raise ValueError(f"workflow {wid}: expected four node events")
            if a["succeeded"]:
                if len(a.get("receipt_statuses", [])) != 7 or any(x.get("failure") for x in a["receipt_statuses"]):
                    raise ValueError(f"NEAR workflow {wid}: receipt tree incomplete or failed")
                if b.get("receipt_status") != 1:
                    raise ValueError(f"EVM workflow {wid}: successful row lacks status-1 receipt")
            pair_count += 1
            all_ids["near"].add(wid)
            all_ids["evm"].add(wid)
        if n["successful_sink_completions"] != len([r for r in nr if r["succeeded"]]):
            raise ValueError(f"NEAR phase {key}: summary success count mismatch")
        if e["successful_sink_completions"] != len([r for r in er if r["succeeded"]]):
            raise ValueError(f"EVM phase {key}: summary success count mismatch")
        if not n["drained"] or not e["drained"]:
            raise ValueError(f"phase {key}: not fully drained")
        if key[1]:
            continue
        for platform, phase, rows, latency_field in (
            ("NEAR", n, nr, "offered_to_sink_queryable_ms"),
            ("EVM", e, er, "offered_to_sink_ms"),
        ):
            latencies = [r[latency_field] for r in rows if r["succeeded"] and r.get(latency_field) is not None]
            metric = {
                "platform": platform,
                "phase": key[0],
                "offered_rate_per_sec": phase["offered_rate_per_sec"],
                "offered": phase["offered"],
                "successes": phase["successful_sink_completions"],
                "successes_in_window": phase["successful_completions_in_measurement_window"],
                "failures": phase["failures"],
                "goodput_per_sec": phase["measured_window_goodput_per_sec"],
                "latency_p50_ms": nearest_rank(latencies, 0.5),
                "latency_p95_ms": nearest_rank(latencies, 0.95),
                "in_flight_at_window_end": phase["in_flight_at_measurement_window_end"],
                "drain_time_after_window_ms": phase["drain_time_after_window_ms"],
                "queue_high_water": phase.get("client_queue_depth_high_water", phase.get("client_task_queue_depth_high_water")),
                "inflight_high_water": phase.get("client_inflight_high_water"),
                "mean_client_queue_delay_ms": phase.get("mean_client_queue_delay_ms"),
            }
            audit["metrics"].append(metric)
    if all_ids["near"] != all_ids["evm"]:
        raise ValueError("platform workflow ID sets differ")
    audit["checks"] = {
        "paired_phases": len(near_phases),
        "paired_rows_including_warmups": pair_count,
        "unique_workflow_ids_near": len(all_ids["near"]),
        "unique_workflow_ids_evm": len(all_ids["evm"]),
        "all_pre_submit_sinks_empty": True,
        "all_successful_node_state_event_audits_pass": True,
        "all_transaction_identity_audits_pass": True,
        "all_successful_near_receipt_trees_have_7_receipts": True,
        "all_phases_drained": True,
    }
    (folder / "paired_load_audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    with (folder / "paired_load_metrics.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(audit["metrics"][0]))
        writer.writeheader()
        writer.writerows(audit["metrics"])
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
