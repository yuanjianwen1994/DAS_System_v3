#!/usr/bin/env python3
"""Independent audit for the two authorized d8, lambda=1 validation repeats."""
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
    ap.add_argument("--platform", choices=("evm", "near"), required=True)
    ap.add_argument("--report", type=Path, required=True)
    ap.add_argument("--gate", type=Path)
    a = ap.parse_args()
    raw, schedule = json.loads(a.raw.read_text()), json.loads(a.schedule.read_text())
    scenario_name = next(iter(schedule["scenarios"]))
    cfg = schedule["scenarios"][scenario_name]
    sched_hash, runner_hash = sha(a.schedule), sha(a.runner)
    problems, totals, phase_reports = [], Counter(), []
    seed = int(schedule.get("seed", -1))
    if cfg.get("arrival_rule") != "paired_exponential":
        problems.append("schedule arrival rule is not paired_exponential")
    if schedule.get("rates_per_sec") != [1.0] and schedule.get("rates_per_sec") != [1]:
        problems.append("schedule does not contain only lambda=1")
    if float(schedule.get("block_period_sec", -1)) != 1:
        problems.append("schedule block period is not 1 second")
    if raw.get("schedule_sha256") != sched_hash:
        problems.append("raw schedule checksum mismatch")
    if a.platform == "near" and raw.get("runner_sha256") != runner_hash:
        problems.append("raw runner checksum mismatch")
    if raw.get("pipeline_mode") is False or raw.get("diagnostic_light") is True:
        problems.append("raw configuration is not the full audit run")
    if a.platform == "evm" and float((raw.get("runtime") or {}).get("block_time_sec", -1)) != 1:
        problems.append("EVM runtime block time is not 1 second")
    if a.platform == "near" and float(raw.get("block_period_sec", -1)) != 1:
        problems.append("NEAR raw block period is not 1 second")
    if a.platform == "near" and raw.get("cleanup_status") == "forced_after_timeout":
        decision = raw.get("offline_audit_decision") or {}
        if decision.get("audit_status") != "passed" or decision.get("allow_sandbox_teardown") is not True:
            problems.append("forced cleanup lacks a passed offline audit gate")
    if a.platform == "near" and raw.get("cleanup_status") not in {"awaiting_offline_audit", "complete", "forced_after_timeout", "preserved_for_supplemental_audit"}:
        problems.append(f"unrecognized NEAR cleanup status: {raw.get('cleanup_status')}")
    rates = [float(x) for x in cfg["rates_per_sec"]]
    phases = raw.get("phases") or []
    if len(phases) != len(rates):
        problems.append(f"phase count {len(phases)} != {len(rates)}")

    ledger_hashes = set()
    for phase_index, rate in enumerate(rates):
        candidates = [p for p in phases if int(p.get("rate_index", -1)) == phase_index and not p.get("warmup")]
        if a.platform == "evm" and len(phases) == 1:
            candidates = phases
        if len(candidates) != 1:
            problems.append(f"phase {phase_index}: expected exactly one measurement phase")
            continue
        phase = candidates[0]
        offset_key = next(k for k in cfg["arrival_offsets_sec_by_rate"] if float(k) == rate)
        offsets = cfg["arrival_offsets_sec_by_rate"][offset_key]
        rows = phase.get("rows") or []
        planned = phase.get("planned_arrival_offsets_sec", phase.get("planned_arrival_offsets_sec"))
        if planned is not None and planned != offsets:
            problems.append(f"phase {phase_index}: planned offsets differ from schedule")
        if float(phase.get("offered_rate_per_sec", -1)) != rate:
            problems.append(f"phase {phase_index}: offered rate mismatch")
        if int(phase.get("offered", -1)) != len(offsets) or len(rows) != len(offsets):
            problems.append(f"phase {phase_index}: offered/rows do not match planned count {len(offsets)}")
        if abs(float(phase.get("observation_duration_sec", -1)) - 360) > 1e-9:
            problems.append(f"phase {phase_index}: observation duration is not 360 seconds")
        trace = phase.get("trace_validation") or {}
        if a.platform == "near" and not (trace.get("passed") is True and trace.get("exact_offsets_passed_to_scheduler") is True and trace.get("row_schedule_timestamps_match_offsets") is True):
            problems.append(f"phase {phase_index}: NEAR arrival trace checkpoint did not pass")

        warmup = float(schedule.get("warmup_sec", 30))
        obs = float(schedule.get("measurement_sec", 360))
        phase_offset = float(cfg.get("phase_random_offset_sec", schedule.get("phase_random_offset_sec", 0)))
        obs_offsets = [x for x in offsets if phase_offset + warmup <= float(x) < phase_offset + warmup + obs]
        actual_obs_rows = [r for r in rows if r.get("warmup") is False]
        classes = Counter()
        observation_classes = Counter()
        phase_ledger = phase.get("rpc_send_tx_request_ledger") or []
        http_errors = phase.get("rpc_http_error_ledger") or []
        ledger_hashes.update(x.get("transaction_hash") for x in phase_ledger if x.get("transaction_hash"))
        for row in rows:
            if a.platform == "near":
                pending = bool(row.get("audit_pending")) or row.get("audit_status") in {"pending", "audit_pending", "incomplete"}
                complete = row.get("audit_status") == "complete" and not pending
                success = row.get("execution_succeeded") is True
                if pending:
                    label = "audit_pending"
                elif complete and success:
                    label = "audited_success"
                elif complete and row.get("execution_succeeded") is False:
                    label = "confirmed_execution_failure"
                else:
                    label = "unclassified"
                if label in {"audit_pending", "unclassified"}:
                    problems.append(f"workflow {row.get('workflow_id')}: {label}")
                if complete and (not row.get("transaction_hash") or not row.get("rpc_send_tx_requests_linked_by_transaction_hash")):
                    problems.append(f"workflow {row.get('workflow_id')}: missing send_tx request linkage")
                if success:
                    states = row.get("all_stage_states") or []
                    if len(states) != 8 or [int(x.get("node_id", -1)) for x in states if isinstance(x, dict)] != list(range(8)):
                        problems.append(f"workflow {row.get('workflow_id')}: expected ordered states for 8 stages")
                    if row.get("states_match_receipt_events") is not True or row.get("sink_state_matches_workflow") is not True:
                        problems.append(f"workflow {row.get('workflow_id')}: receipt event or sink state mismatch")
                    if row.get("receipt_resolution_status") != "completed" or row.get("sink_resolution_status") != "queryable":
                        problems.append(f"workflow {row.get('workflow_id')}: receipt/sink resolution incomplete")
            else:
                success = row.get("succeeded") is True
                receipt = row.get("receipt_status") == 1
                classified = receipt or row.get("receipt_status") == 0 or row.get("failure")
                label = "audited_success" if success else ("confirmed_execution_failure" if classified else "unclassified")
                if label == "unclassified":
                    problems.append(f"workflow {row.get('workflow_id')}: unclassified EVM result")
                if success:
                    if row.get("pre_submit_sink_check_passed") is not True or not row.get("sink_first_queryable_at"):
                        problems.append(f"workflow {row.get('workflow_id')}: sink completion lacks evidence")
                    if row.get("transaction_identity_nonce_match") is not True or row.get("transaction_hash_matches_outcome") is not True:
                        problems.append(f"workflow {row.get('workflow_id')}: transaction identity/nonce mismatch")
                    if len(row.get("all_stage_states") or []) != 8 or row.get("states_match_receipt_events") is not True:
                        problems.append(f"workflow {row.get('workflow_id')}: stage state/receipt event audit failed")
                    events = row.get("node_events") or []
                    if sorted(int(x.get("node_id", -1)) for x in events) != list(range(8)):
                        problems.append(f"workflow {row.get('workflow_id')}: expected all 8 parent stage events")
            classes[label] += 1
            if row.get("warmup") is False:
                observation_classes[label] += 1
        if a.platform == "near" and int(phase.get("audit_pending_count", 0)) != classes["audit_pending"]:
            problems.append(f"phase {phase_index}: pending count disagrees with row classifications")
        totals.update(classes)
        latency_key = "offered_to_sink_queryable_ms" if a.platform == "near" else "offered_to_sink_ms"
        latencies = sorted(float(r[latency_key]) for r in rows if r.get("succeeded") is True and r.get(latency_key) is not None)
        obs_latencies = sorted(float(r[latency_key]) for r in actual_obs_rows if r.get("succeeded") is True and r.get(latency_key) is not None)
        def percentile(values, q):
            if not values:
                return None
            return values[max(0, min(len(values) - 1, int((len(values) - 1) * q + 0.5)))]
        rpc = phase.get("rpc_load_path_by_method") or {}
        trace_summary = ({
            "passed": trace.get("passed"),
            "exact_offsets_passed_to_scheduler": trace.get("exact_offsets_passed_to_scheduler"),
            "row_schedule_timestamps_match_offsets": trace.get("row_schedule_timestamps_match_offsets"),
            "expected_count": trace.get("expected_count"), "recorded_count": trace.get("recorded_count"),
            "max_row_offset_deviation_sec": trace.get("max_row_offset_deviation_sec"),
        } if trace else {})
        phase_reports.append({
            "phase": phase_index, "rate": rate, "planned_rows": len(offsets), "actual_rows": len(rows),
            "observation_window_planned_arrivals": len(obs_offsets), "observation_window_actual_arrivals": len(actual_obs_rows),
            "observation_window_audited_successes": sum(1 for r in actual_obs_rows if r.get("execution_succeeded", r.get("succeeded")) is True),
            "observation_window_audit_pending": sum(1 for r in actual_obs_rows if r.get("audit_pending") is True or r.get("audit_status") in {"pending", "audit_pending", "incomplete"}),
            "observation_window_row_classification": dict(observation_classes),
            "observation_duration_sec": phase.get("observation_duration_sec"),
            "row_classification": dict(classes), "trace_validation": trace_summary,
            "http_send_errors": sum(len(p.get("rpc_http_error_ledger") or []) for p in phases),
            "send_tx_requests": len(phase_ledger),
            "send_tx_http_attempts": len(phase_ledger),
            "send_tx_http_failures": sum(1 for x in phase_ledger if x.get("http_ok") is False or int(x.get("http_status") or 0) >= 400),
            "send_tx_retries": sum(int(x.get("same_payload_retry_count") or 0) for x in phase_ledger),
            "audit_pending_count": phase.get("audit_pending_count", sum(1 for r in rows if r.get("audit_pending") is True)),
            "receipt_count": sum(int(r.get("receipt_count") or 0) for r in rows) if a.platform == "near" else sum(len(r.get("node_events") or []) for r in rows),
            "native_gas_used": sum(int(r.get("gas_used") or 0) for r in rows) if a.platform == "evm" else None,
            "rpc_http_calls_all_methods": sum(int(v.get("calls", 0)) for v in rpc.values() if isinstance(v, dict)),
            "rpc_http_errors_all_methods": sum(int(v.get("errors", 0)) for v in rpc.values() if isinstance(v, dict)),
            "receipt_poll_pending_at_phase_end": phase.get("receipt_poll_pending_at_phase_end"),
            "successful_workflows_per_sec": phase.get("measured_window_goodput_per_sec"),
            "workflow_latency_ms": {"p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95)},
            "observation_workflow_latency_ms": {"p50": percentile(obs_latencies, 0.50), "p95": percentile(obs_latencies, 0.95)},
            "successful_workflow_count_for_latency": len(latencies)
        })

    report = {
        "audit": "pass" if not problems else "incomplete_or_failed", "platform": a.platform, "seed": seed,
        "scenario": scenario_name, "schedule_sha256": sched_hash, "runner_sha256": runner_hash,
        "cleanup_status": raw.get("cleanup_status"), "phases": phase_reports,
        "totals": dict(totals), "problems": problems,
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
