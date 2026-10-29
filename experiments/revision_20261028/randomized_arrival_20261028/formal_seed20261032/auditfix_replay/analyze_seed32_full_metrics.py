#!/usr/bin/env python3
"""Summarize already archived seed32 EVM and audited NEAR full-run evidence."""
import argparse
import csv
import json
import math
import re
from pathlib import Path


def nums(values):
    return [float(x) for x in values if x is not None and x != "" and math.isfinite(float(x))]


def pct(values, q):
    xs = sorted(nums(values))
    if not xs:
        return None
    index = max(0, min(len(xs) - 1, math.ceil(q * len(xs)) - 1))
    return xs[index]


def stat(values):
    xs = nums(values)
    return {"n": len(xs), "mean": sum(xs) / len(xs) if xs else None,
            "p50": pct(xs, .50), "p95": pct(xs, .95), "max": max(xs) if xs else None}


def phase_metrics(phase, platform):
    rows = phase.get("rows") or []
    successes = [r for r in rows if r.get("succeeded") is True] if platform == "evm" else [r for r in rows if r.get("execution_succeeded") is True]
    if platform == "evm":
        latencies = [r.get("offered_to_sink_ms") for r in successes]
        gas = [r.get("gas_used") for r in successes]
        receipt_counts = [1 if r.get("receipt_status") is not None else None for r in successes]
        wait_ms = [r.get("submit_to_sink_ms") for r in successes]
        stage_ok = sum(r.get("states_match_receipt_events") is True for r in successes)
        failure_count = int(phase.get("failures") or 0)
        receipt_sample = {"transaction_receipt_status_present": sum(x == 1 for x in receipt_counts),
                          "transaction_receipts": len(successes)}
    else:
        latencies = [r.get("offered_to_sink_queryable_ms") for r in successes]
        wait_ms = [r.get("call_to_receipt_tree_ms") for r in successes]
        gas = []
        receipt_counts = [r.get("receipt_count") for r in successes]
        stage_ok = sum(r.get("states_match_receipt_events") is True for r in successes)
        failure_count = int(phase.get("failures") or 0)
        receipt_sample = {"workflow_receipt_count": stat(receipt_counts),
                          "receipt_status_entries": sum(len(r.get("receipt_statuses") or []) for r in successes)}
    blocks = phase.get("block_usage_samples") or []
    native_block_gas = {
        "sample_count": len(blocks),
        "gas_used": stat([b.get("gas_used") for b in blocks]),
        "gas_limit": stat([b.get("gas_limit") for b in blocks]),
        "utilization_pct": stat([float(b["utilization"]) * (100 if float(b["utilization"]) <= 1 else 1) for b in blocks if b.get("utilization") is not None]),
    }
    return {
        "rate": phase.get("offered_rate_per_sec"), "offered": phase.get("offered"),
        "completed": phase.get("completed"), "successful_workflows": len(successes),
        "failures": failure_count,
        "audit_pending": phase.get("audit_pending_count", 0) if platform == "near" else 0,
        "measured_goodput_workflows_per_sec": phase.get("measured_window_goodput_per_sec"),
        "measured_successful_workflows": phase.get("successful_completions_in_measurement_window"),
        "offered_to_sink_ms": stat(latencies),
        "submit_to_sink_ms": stat(wait_ms) if platform == "evm" else None,
        "call_to_receipt_tree_ms": stat(wait_ms) if platform == "near" else None,
        "stage_state_receipt_agreement": {"successes": stage_ok, "successful_rows": len(successes)},
        "workflow_receipt_metrics": receipt_sample,
        "evm_transaction_gas_used": stat(gas) if platform == "evm" else None,
        "near_block_gas_samples": native_block_gas if platform == "near" else None,
    }


def process_samples(path):
    if not path or not Path(path).exists():
        return {"samples": 0}
    current = None
    groups = {}
    for line in Path(path).read_text(errors="replace").splitlines():
        if line.startswith("SAMPLE "):
            current = line.partition(" ")[2]
            groups[current] = {"cpu": [], "rss": []}
            continue
        if current is None:
            continue
        parts = line.split(None, 7)
        if len(parts) < 7 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        try:
            cpu, rss = float(parts[2]), float(parts[4])
        except ValueError:
            continue
        groups[current]["cpu"].append(cpu)
        groups[current]["rss"].append(rss)
    cpu_sum, rss_sum = [], []
    for g in groups.values():
        if g["cpu"]:
            cpu_sum.append(sum(g["cpu"]))
            rss_sum.append(sum(g["rss"]))
    return {"samples": len(cpu_sum), "combined_cpu_pct": stat(cpu_sum),
            "combined_rss_kb": stat(rss_sum)}


def csv_process_samples(path):
    p = Path(path)
    if not p.exists():
        return {"samples": 0}
    by_time = {}
    with p.open(newline="") as f:
        for row in csv.DictReader(f):
            try:
                by_time.setdefault(row["timestamp"], {"cpu": [], "rss": []})["cpu"].append(float(row["cpu_pct"]))
                by_time[row["timestamp"]]["rss"].append(float(row["rss_kb"]))
            except (KeyError, ValueError):
                pass
    cpu = [sum(x["cpu"]) for x in by_time.values() if x["cpu"]]
    rss = [sum(x["rss"]) for x in by_time.values() if x["rss"]]
    return {"samples": len(cpu), "combined_cpu_pct": stat(cpu), "combined_rss_kb": stat(rss)}


def host_vmstat(path, near=False):
    p = Path(path)
    if not p.exists():
        return {"samples": 0}
    ids, iowaits = [], []
    header = None
    for line in p.read_text(errors="replace").splitlines():
        if near:
            parts = line.split()
            if "id" in parts and "wa" in parts:
                header = parts
                continue
            if header and len(parts) == len(header) and all(x.lstrip("-+").isdigit() for x in parts):
                ids.append(float(parts[header.index("id")]))
                iowaits.append(float(parts[header.index("wa")]))
        else:
            # Original EVM monitor stores timestamp, then raw vmstat fields.
            raw = line.split(",", 1)[-1].split()
            if len(raw) >= 17 and all(x.lstrip("-+").isdigit() for x in raw[-5:]):
                ids.append(float(raw[-3]))
                iowaits.append(float(raw[-2]))
    busy = [100 - x for x in ids]
    return {"samples": len(ids), "host_cpu_busy_pct": stat(busy), "host_cpu_iowait_pct": stat(iowaits)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--near-raw", type=Path, required=True)
    ap.add_argument("--evm-raw", type=Path, required=True)
    ap.add_argument("--near-process", type=Path, required=True)
    ap.add_argument("--near-vmstat", type=Path, required=True)
    ap.add_argument("--evm-process", type=Path, required=True)
    ap.add_argument("--evm-vmstat", type=Path, required=True)
    ap.add_argument("--output-json", type=Path, required=True)
    ap.add_argument("--output-md", type=Path, required=True)
    a = ap.parse_args()
    near, evm = json.loads(a.near_raw.read_text()), json.loads(a.evm_raw.read_text())
    evm_phases, near_phases = [], []
    for rate in (0.5, 1.0):
        e = next(p for p in evm["phases"] if float(p.get("offered_rate_per_sec")) == rate and not p.get("warmup"))
        n = next(p for p in near["phases"] if float(p.get("offered_rate_per_sec")) == rate and not p.get("warmup"))
        evm_phases.append(phase_metrics(e, "evm")); near_phases.append(phase_metrics(n, "near"))
    output = {
        "seed": 20261032,
        "schedule_sha256": near.get("schedule_sha256"),
        "scope": "same seed/schedule, existing complete EVM baseline and repaired complete NEAR replay",
        "latency_percentile_scope": "all fully audited successful workflow rows in the full phase, including warmup rows and completions observed during drain",
        "goodput_scope": "successful sink completions observed within the 780-second measurement window, divided by 780 seconds",
        "evm": {"phases": evm_phases, "process_cpu_memory": csv_process_samples(a.evm_process), "host_vmstat": host_vmstat(a.evm_vmstat)},
        "near": {"phases": near_phases, "process_resources": process_samples(a.near_process), "host_vmstat": host_vmstat(a.near_vmstat, near=True)},
    }
    a.output_json.parent.mkdir(parents=True, exist_ok=True)
    a.output_json.write_text(json.dumps(output, indent=2) + "\n")
    lines = ["# Seed 20261032 full replay: platform-native metrics", "",
             "The EVM side uses its already completed same-schedule baseline. The NEAR side uses the repaired full replay. These are descriptive system-specific measurements; gas accounting and receipt execution models differ by platform.", "",
             "Latency p50/p95 use all fully audited successes in the full phase, including warmup rows and completions observed during drain; the sample count is recorded as `offered_to_sink_ms.n` in the JSON. Measured-window goodput counts successful sink completions observed within the 780-second measurement window, divided by 780 seconds. These use different cohorts.", "",
             "| Platform | λ | Offered | Successful workflows | Failures | Measured-window goodput (workflows/s) | p50 full-phase audited success (ms) | p95 full-phase audited success (ms) |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label, phases in (("EVM", evm_phases), ("NEAR", near_phases)):
        for p in phases:
            lat = p["offered_to_sink_ms"]
            lines.append(f"| {label} | {float(p['rate']):g} | {p['offered']} | {p['successful_workflows']} | {p['failures']} | {p['measured_goodput_workflows_per_sec']} | {lat['p50']} | {lat['p95']} |")
    lines += ["", "## Native resource evidence", "",
              f"- EVM per-workflow transaction gas: reported as mean/p50/p95/max in JSON; process CPU and RSS are summarized from the original process monitor.",
              f"- NEAR native block gas: phase block-sample `gas_used`, `gas_limit`, and utilization distributions are in JSON; per-workflow receipt counts and receipt-tree timing are also included.",
              f"- NEAR process tree CPU: mean/p50/p95/max combined `%CPU` and RSS from {output['near']['process_resources']['samples']} time samples; host CPU busy and I/O wait are summarized separately.",
              f"- Full NEAR receipt/state audit: {sum(p['stage_state_receipt_agreement']['successful_rows'] for p in near_phases)} successful rows with stage/receipt-event agreement; see the audit report.", "",
              "Latency, native gas, receipt counts, and CPU are reported separately. NEAR block-gas samples are not attributed to individual workflows.", ""]
    a.output_md.parent.mkdir(parents=True, exist_ok=True)
    a.output_md.write_text("\n".join(lines))
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
