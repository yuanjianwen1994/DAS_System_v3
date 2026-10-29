#!/usr/bin/env python3
"""Offline evidence audit for the exact seed20261032 repaired NEAR replay."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--schedule", type=Path, required=True)
    ap.add_argument("--runner", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    ap.add_argument("--gate", type=Path)
    a = ap.parse_args()

    raw = json.loads(a.raw.read_text())
    schedule = json.loads(a.schedule.read_text())
    cfg = schedule["scenarios"]["poisson_d8"]
    expected_schedule_sha = sha(a.schedule)
    expected_runner_sha = sha(a.runner)
    problems = []
    if int(schedule.get("seed", -1)) != 20261032:
        problems.append("schedule seed is not 20261032")
    if cfg.get("arrival_rule") != "paired_exponential":
        problems.append("schedule arrival rule is not paired_exponential")
    if raw.get("schedule_sha256") != expected_schedule_sha:
        problems.append("raw schedule checksum mismatch")
    if raw.get("runner_sha256") != expected_runner_sha:
        problems.append("raw runner checksum mismatch")
    if raw.get("pipeline_mode") is False or raw.get("diagnostic_light") is True:
        problems.append("raw config is not the full pipeline audit run")

    phase_reports = []
    total = Counter()
    ledger = []
    expected_rates = [float(x) for x in cfg["rates_per_sec"]]
    phases = raw.get("phases") or []
    if len(phases) != len(expected_rates):
        problems.append(f"phase count {len(phases)} != {len(expected_rates)}")

    for index, rate in enumerate(expected_rates):
        candidates = [p for p in phases if int(p.get("rate_index", -1)) == index and not p.get("warmup")]
        if len(candidates) != 1:
            problems.append(f"rate index {index}: expected exactly one measurement phase")
            continue
        phase = candidates[0]
        key = next(k for k in cfg["arrival_offsets_sec_by_rate"] if float(k) == rate)
        expected_offsets = cfg["arrival_offsets_sec_by_rate"][key]
        expected_count = len(expected_offsets)
        rows = phase.get("rows") or []
        trace = phase.get("trace_validation") or {}
        if float(phase.get("offered_rate_per_sec", -1)) != rate:
            problems.append(f"phase {index}: offered rate mismatch")
        if int(phase.get("offered", -1)) != expected_count or len(rows) != expected_count:
            problems.append(f"phase {index}: offered/row count does not match {expected_count}")
        if phase.get("planned_arrival_offsets_sec") != expected_offsets:
            problems.append(f"phase {index}: planned offsets differ from seed schedule")
        if trace.get("passed") is not True or trace.get("exact_offsets_passed_to_scheduler") is not True or trace.get("row_schedule_timestamps_match_offsets") is not True:
            problems.append(f"phase {index}: arrival trace checkpoint did not pass")
        if float(phase.get("observation_duration_sec", -1)) != 780:
            problems.append(f"phase {index}: observation duration is not 780 seconds")

        row_classes = Counter()
        phase_ledger = phase.get("rpc_send_tx_request_ledger") or []
        ledger.extend(phase_ledger)
        ledger_hashes = {x.get("transaction_hash") for x in phase_ledger if x.get("transaction_hash")}
        for row in rows:
            pending = bool(row.get("audit_pending")) or row.get("audit_status") in {"pending", "audit_pending", "incomplete"}
            complete = row.get("audit_status") == "complete" and not pending
            succeeded = row.get("execution_succeeded") is True
            confirmed_failure = complete and row.get("execution_succeeded") is False
            if pending:
                classification = "audit_pending"
            elif complete and succeeded:
                classification = "audited_success"
            elif confirmed_failure:
                classification = "confirmed_execution_failure"
            else:
                classification = "unclassified"
            row_classes[classification] += 1
            if classification in {"audit_pending", "unclassified"}:
                problems.append(f"phase {index} workflow {row.get('workflow_id')}: {classification}")
            if complete and (not row.get("transaction_hash") or row.get("transaction_hash") not in ledger_hashes):
                problems.append(f"phase {index} workflow {row.get('workflow_id')}: no matching send_tx hash record")
            if succeeded:
                states = row.get("all_stage_states") or []
                if len(states) != 8:
                    problems.append(f"phase {index} workflow {row.get('workflow_id')}: expected 8 stage states")
                if row.get("states_match_receipt_events") is not True:
                    problems.append(f"phase {index} workflow {row.get('workflow_id')}: receipt event states mismatch")
                if row.get("sink_state_matches_workflow") is not True:
                    problems.append(f"phase {index} workflow {row.get('workflow_id')}: sink state mismatch")
                if row.get("receipt_resolution_status") != "completed" or row.get("sink_resolution_status") != "queryable":
                    problems.append(f"phase {index} workflow {row.get('workflow_id')}: success lacks both receipt and sink resolution")

        if phase.get("audit_pending_count", 0) != row_classes["audit_pending"]:
            problems.append(f"phase {index}: pending count disagrees with row classification")
        if phase.get("audit_status") not in {"complete", "completed"}:
            problems.append(f"phase {index}: phase audit_status={phase.get('audit_status')}")
        total.update(row_classes)
        phase_reports.append({
            "phase": index,
            "rate": rate,
            "expected_rows": expected_count,
            "actual_rows": len(rows),
            "trace_validation": trace,
            "audit_status": phase.get("audit_status"),
            "row_classification": dict(row_classes),
            "send_tx_requests": len(phase_ledger),
            "send_tx_http_errors": len(phase.get("rpc_http_error_ledger") or []),
            "same_payload_retries": sum(int(x.get("same_payload_retry_count") or 0) for x in phase_ledger),
        })

    cleanup_status = raw.get("cleanup_status")
    if cleanup_status == "forced_after_timeout":
        decision = raw.get("offline_audit_decision") or {}
        if decision.get("audit_status") != "passed" or decision.get("allow_sandbox_teardown") is not True:
            problems.append("forced cleanup is not backed by a passing offline gate decision")
    elif cleanup_status not in {"awaiting_offline_audit", "preserved_for_supplemental_audit", "complete"}:
        problems.append(f"runner cleanup status is unrecognized: {cleanup_status}")

    report = {
        "audit": "pass" if not problems else "incomplete_or_failed",
        "seed": schedule.get("seed"),
        "schedule_sha256": expected_schedule_sha,
        "runner_sha256": expected_runner_sha,
        "near_platform": raw.get("platform"),
        "near_cleanup_status_at_audit": raw.get("cleanup_status"),
        "offline_audit_decision": raw.get("offline_audit_decision"),
        "phases": phase_reports,
        "totals": dict(total),
        "send_tx_requests": len(ledger),
        "send_tx_http_errors": sum(len(p.get("rpc_http_error_ledger") or []) for p in phases),
        "same_payload_retries": sum(int(x.get("same_payload_retry_count") or 0) for x in ledger),
        "problems": problems,
    }
    a.report.parent.mkdir(parents=True, exist_ok=True)
    a.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if not problems and a.gate:
        a.gate.parent.mkdir(parents=True, exist_ok=True)
        a.gate.write_text(json.dumps({"audit_status": "passed", "allow_sandbox_teardown": True,
                                      "report_sha256": sha(a.report)}, indent=2) + "\n")
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
