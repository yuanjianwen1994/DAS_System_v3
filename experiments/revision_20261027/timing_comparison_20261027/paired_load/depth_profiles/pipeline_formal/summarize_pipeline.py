#!/usr/bin/env python3
"""Summarize continuous-pipeline runs and draw latency in seconds."""
import csv, json, math, re, statistics
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent

def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None

def percentile(values, p):
    values = sorted(values)
    if not values: return None
    return values[max(0, math.ceil(p * len(values)) - 1)]

records = []
all_bins = []
for path in sorted(ROOT.glob("**/*_raw.json")):
    if any(part.endswith("_stalled") for part in path.parts): continue
    try: data = json.loads(path.read_text())
    except Exception: continue
    for phase in data.get("phases", []):
        if not phase.get("pipeline"): continue
        start = stamp(phase.get("observation_start_at"))
        end = stamp(phase.get("measurement_window_end_at"))
        rows = phase.get("rows", [])
        block_times = {b.get("block_hash"): int(b["timestamp_nanosec"]) / 1e6 for b in phase.get("block_usage_samples", [])
                       if b.get("block_hash") and b.get("timestamp_nanosec")}
        def accepted_at(row):
            if data.get("platform") == "NEAR Sandbox":
                value = block_times.get(row.get("transaction_block_hash"))
                return value / 1000 if value is not None else None
            value = stamp(row.get("tx_hash_returned_at"))
            return value
        accepts = [r for r in rows if (v := accepted_at(r)) is not None and start is not None and end is not None and start <= v <= end]
        window = [r for r in rows if r.get("execution_succeeded", r.get("succeeded")) and start is not None and end is not None
                  and r.get("sink_first_queryable_at") and start <= stamp(r["sink_first_queryable_at"]) <= end]
        latencies = []
        for row in window:
            value = row.get("offered_to_sink_ms", row.get("offered_to_sink_queryable_ms"))
            if value is not None: latencies.append(float(value) / 1000)
        run_dir = path.parent.name
        primary_repeat = bool(re.fullmatch(r"d(?:8|20)_r[123]_(?!.*recovered).+", run_dir))
        exit_file = path.parent / "exit.code"
        cleanup_note_file = path.parent / "cleanup_note.txt"
        runner_exit_code = exit_file.read_text().strip() if exit_file.exists() else None
        cleanup_note = cleanup_note_file.read_text().strip() if cleanup_note_file.exists() else None
        cpu_values = []
        resource_file = path.parent / "resources.csv"
        if resource_file.exists():
            try:
                with resource_file.open() as rf:
                    cpu_values = [float(x["cpu_busy_pct"]) for x in csv.DictReader(rf) if x.get("cpu_busy_pct")]
            except Exception: pass
        block_samples = []
        for b in phase.get("block_usage_samples", []):
            ts = (int(b["timestamp_nanosec"]) / 1e9 if b.get("timestamp_nanosec") else None)
            if ts is None and b.get("timestamp") is not None: ts = float(b["timestamp"])
            if start is not None and end is not None and ts is not None and start <= ts <= end: block_samples.append(b)
        rpc_data = phase.get("rpc_load_path_by_method", {})
        rpc_calls = sum(int(v.get("calls", 0)) for k, v in rpc_data.items() if not k.endswith("_batch"))
        rpc_errors = sum(int(v.get("errors", 0)) for k, v in rpc_data.items() if not k.endswith("_batch"))
        block_gas = [int(b.get("gas_used", 0)) for b in block_samples if b.get("gas_used") is not None]
        records.append({
            "run_dir": run_dir, "platform": data.get("platform", "?"),
            "primary_repeat": primary_repeat,
            "runner_exit_code": runner_exit_code, "cleanup_status": data.get("cleanup_status"), "cleanup_note": cleanup_note,
            "route": data.get("route", "independent_stage"), "profile": data.get("profile_id", "?"),
            "depth": int(data.get("profile_id", "d0").split("-d")[-1].split("-")[0]) if "-d" in data.get("profile_id", "") else 0,
            "repeat": data.get("repeat", ""), "offered_rate_per_sec": phase.get("offered_rate_per_sec"),
            "observation_sec": phase.get("observation_duration_sec"), "window_completions": len(window),
            "goodput_per_sec": phase.get("measured_window_goodput_per_sec"),
            "accepted_in_window": len(accepts), "accepted_rate_per_sec": len(accepts) / max(0.001, phase.get("observation_duration_sec", 60)),
            "p50_sec": percentile(latencies, .50), "p95_sec": percentile(latencies, .95),
            "audited_successes_after_drain": sum(bool(r.get("succeeded")) and not bool(r.get("audit_pending")) for r in rows),
            "failures_after_drain": sum(not bool(r.get("succeeded")) and not bool(r.get("audit_pending")) for r in rows),
            "audit_pending_count": sum(bool(r.get("audit_pending")) for r in rows) + sum(bool(b.get("audit_pending")) for b in phase.get("block_usage_samples", [])),
            "rows": len(rows), "queue_end": (phase.get("time_bins_10s") or [{}])[-1].get("queue_at_bin_end"),
            "blocks_in_window": len(block_samples),
            "mean_block_utilization": (sum(float(b.get("utilization") or 0) for b in block_samples) / len(block_samples)) if block_samples else None,
            "mean_block_gas_used": (sum(block_gas) / len(block_gas)) if block_gas else None,
            "peak_block_gas_used": max(block_gas) if block_gas else None,
            "rpc_calls": rpc_calls, "rpc_errors": rpc_errors,
            "peak_cpu_busy_pct": max(cpu_values) if cpu_values else None,
            "raw_file": str(path.relative_to(ROOT)),
        })
        bin_width = 10
        bins = []
        observation_sec = float(phase.get("observation_duration_sec", 60))
        for lo in range(0, int(math.ceil(observation_sec / bin_width)) * bin_width, bin_width):
            hi = min(observation_sec, lo + bin_width); wlo, whi = start + lo, min(end, start + hi)
            offered = sum(1 for r in rows if r.get("offered_at") and wlo <= stamp(r["offered_at"]) < whi)
            accepted = sum(1 for r in rows if (v := accepted_at(r)) is not None and wlo <= v < whi)
            completed = [r for r in window if r.get("sink_first_queryable_at") and wlo <= stamp(r["sink_first_queryable_at"]) < whi]
            accepted_cum = sum(1 for r in rows if (v := accepted_at(r)) is not None and v <= whi)
            completed_cum = sum(1 for r in rows if r.get("execution_succeeded", r.get("succeeded")) and r.get("sink_first_queryable_at") and stamp(r["sink_first_queryable_at"]) <= whi)
            bin_lat = [float(r.get("offered_to_sink_ms", r.get("offered_to_sink_queryable_ms"))) / 1000
                       for r in completed if r.get("offered_to_sink_ms", r.get("offered_to_sink_queryable_ms")) is not None]
            bins.append({"run_dir": run_dir, "platform": data.get("platform", "?"), "route": data.get("route", "independent_stage"),
                "primary_repeat": primary_repeat,
                "depth": records[-1]["depth"], "rate": phase.get("offered_rate_per_sec"), "bin_start_sec": lo, "bin_end_sec": hi,
                "offered": offered, "accepted_chain_or_mempool": accepted, "completed": len(completed),
                "queue_at_bin_end": max(0, accepted_cum - completed_cum), "p50_sec": percentile(bin_lat, .5), "p95_sec": percentile(bin_lat, .95)})
        phase["recomputed_time_bins_actual_acceptance"] = bins
        phase["actual_acceptance_count_in_window"] = len(accepts)
        phase["actual_accept_rate_per_sec"] = len(accepts) / max(0.001, phase.get("observation_duration_sec", 60))
        if bins: records[-1]["queue_end"] = bins[-1]["queue_at_bin_end"]
        all_bins.extend(bins)

all_records = records
supplemental_records = [r for r in all_records if not r["primary_repeat"]]
records = [r for r in all_records if r["primary_repeat"]]
supplemental_bins = [b for b in all_bins if not b["primary_repeat"]]
all_bins = [b for b in all_bins if b["primary_repeat"]]

columns = ["run_dir", "primary_repeat", "platform", "route", "profile", "depth", "repeat", "offered_rate_per_sec", "observation_sec",
           "window_completions", "goodput_per_sec", "accepted_in_window", "accepted_rate_per_sec", "p50_sec", "p95_sec", "audited_successes_after_drain", "failures_after_drain", "audit_pending_count", "runner_exit_code", "cleanup_status", "cleanup_note", "rows", "queue_end", "blocks_in_window", "mean_block_utilization", "mean_block_gas_used", "peak_block_gas_used", "rpc_calls", "rpc_errors", "peak_cpu_busy_pct", "raw_file"]
with (ROOT / "pipeline_summary.csv").open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=columns); w.writeheader(); w.writerows(records)
with (ROOT / "pipeline_summary_supplemental.csv").open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=columns); w.writeheader(); w.writerows(supplemental_records)
with (ROOT / "pipeline_time_bins.csv").open("w", newline="") as f:
    cols = ["run_dir", "primary_repeat", "platform", "route", "depth", "rate", "bin_start_sec", "bin_end_sec", "offered", "accepted_chain_or_mempool", "completed", "queue_at_bin_end", "p50_sec", "p95_sec"]
    w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
    w.writerows(all_bins)
with (ROOT / "pipeline_time_bins_supplemental.csv").open("w", newline="") as f:
    cols = ["run_dir", "primary_repeat", "platform", "route", "depth", "rate", "bin_start_sec", "bin_end_sec", "offered", "accepted_chain_or_mempool", "completed", "queue_at_bin_end", "p50_sec", "p95_sec"]
    w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
    w.writerows(supplemental_bins)

with (ROOT / "pipeline_report.md").open("w") as f:
    f.write("# Continuous cascade pipeline measurements\n\n")
    f.write("Primary statistics include only the pre-registered r1/r2/r3 run directories. Validation (`s1`) and recovered repeats are listed separately and excluded from the primary median and main figure.\n\n")
    f.write("Latency is in seconds from workflow offer to first queryable final sink. Window completions include any workflow whose sink became queryable during the fixed observation window, including workflows offered during preheat. State and receipt audits run after drain.\n\n")
    f.write("Accepted-rate timestamps use each runtime's observable acceptance boundary: EVM transaction hash returned by the local node; NEAR transaction outcome's included block timestamp. This platform-specific boundary is retained explicitly in `pipeline_time_bins.csv`.\n\n")
    f.write("| Run | Platform | Route | Depth | Offered/s | Accepted/s | Window completions | Goodput/s | p50 (s) | p95 (s) | Audit passed | Failures | Audit pending | Queue at window end |\n|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for r in records:
        v = lambda k: "" if r[k] is None else (f"{r[k]:.3f}" if isinstance(r[k], float) else str(r[k]))
        f.write(f"| {r['run_dir']} | {r['platform']} | {r['route']} | {r['depth']} | {v('offered_rate_per_sec')} | {v('accepted_rate_per_sec')} | {r['window_completions']} | {v('goodput_per_sec')} | {v('p50_sec')} | {v('p95_sec')} | {r['audited_successes_after_drain']}/{r['rows']} | {r['failures_after_drain']} | {r['audit_pending_count']} | {r['queue_end']} |\n")
    f.write("\n## Reading cascade depth\n\nEach workflow invokes one node operation at each cascade level. Moving from d8 to d20 therefore changes per-workflow work by 20/8 = 2.5x. Reported goodput counts completed workflows per second; it is not multiplied by cascade depth. Report workflow latency and per-node resource use alongside it, and do not read depth growth itself as a throughput speedup.\n")
    # Keep the interpretation tied to the matched rate and replicate-level medians.
    matched = {}
    for r in records:
        if r["platform"] != "NEAR Sandbox" or float(r["offered_rate_per_sec"] or 0) != 1.0:
            continue
        if r["route"] == "same_receiver":
            if "domain_control" not in r["run_dir"]: continue
            group = "same_receiver"
        elif r["route"] in ("independent_stage", "multi_contract"): group = "cross_contract"
        else: continue
        matched.setdefault((group, r["depth"]), []).append(r)
    if all((g, d) in matched for g in ("same_receiver", "cross_contract") for d in (8, 20)):
        f.write("\n## Matched NEAR route controls at 1 workflow/s\n\nValues below are medians across available repeats. `cross_contract` groups the independent-stage and multi-contract labels used by the runner versions; these rows all execute the per-level cascade across contracts. This is a route comparison, not a pure contract-count intervention.\n\n")
        f.write("| Route | Depth | Repeats | Median goodput (workflows/s) | Median offer-to-sink p95 (s) |\n|---|---:|---:|---:|---:|\n")
        for group in ("same_receiver", "cross_contract"):
            for depth in (8, 20):
                rs = matched[(group, depth)]
                f.write(f"| {group} | {depth} | {len(rs)} | {statistics.median(float(x['goodput_per_sec']) for x in rs):.3f} | {statistics.median(float(x['p95_sec']) for x in rs):.3f} |\n")
        f.write("\nAt this offered rate, the same-receiver d8 and d20 controls have nearly identical p95 latency, while the cross-contract d20 median is about 2.3x the cross-contract d8 median. Goodput remains about 1 workflow/s in both. The much larger cross-contract versus same-receiver latency reflects the route/deployment change in this setup; it does not show that adding cascade levels multiplies throughput loss one-for-one. Do not present this as a universal platform law.\n")
    f.write("\n## Resource and RPC context\n\nBlock gas values use each platform's native gas unit. Block samples are restricted to the observation window; host CPU is the peak across 1-second resource samples for the run. RPC totals are runner-observed request counts.\n\n")
    f.write("| Run | Blocks sampled | Mean block utilization | Mean block gas | Peak block gas | RPC calls | RPC errors | Peak host CPU (%) |\n|---|---:|---:|---:|---:|---:|---:|---:|\n")
    for r in records:
        v = lambda k: "" if r[k] is None else (f"{r[k]:.3f}" if isinstance(r[k], float) else str(r[k]))
        f.write(f"| {r['run_dir']} | {r['blocks_in_window']} | {v('mean_block_utilization')} | {v('mean_block_gas_used')} | {v('peak_block_gas_used')} | {r['rpc_calls']} | {r['rpc_errors']} | {v('peak_cpu_busy_pct')} |\n")
    near_d20_cross = [r for r in records if r["platform"] == "NEAR Sandbox" and r["depth"] == 20 and r["route"] in ("independent_stage", "multi_contract")]
    if near_d20_cross:
        f.write("\n### Interpreting NEAR `send_tx` RPC errors\n\nThe proxy marks an observation as an error when the upstream HTTP response is non-2xx or fetch throws; it does not retain response status/body or correlate attempts with a final response. In the registered NEAR d20 cross-contract runs, each 135-workflow repeat recorded 405 `send_tx` observations, of which 270 were marked errors (90 during the 45-workflow phase and 180 during the 90-workflow phase). All 45/45 and 90/90 workflows nevertheless completed with zero audited workflow failures and zero audit-pending rows. The installed near-api-js provider (`@near-js/providers` 1.0.3) retries HTTP 500/503/408 and fetch errors, up to 12 attempts; the runner uses a single custom `JsonRpcProvider`, not a multi-endpoint failover provider. This makes intermediate same-endpoint SDK retries the likely explanation, rather than 270 failed business operations. Because the proxy did not retain status codes, the exact mix cannot be verified per request. Report these as retried-or-unclassified transport observations, separate from workflow failures.\n")
    f.write("\nThe short-window depth pilot remains separate and is not pooled with these continuous-pipeline runs.\n")
    f.write("\n## Retained high-load short-window diagnostics\n\nThe earlier 30-second pilot is retained as a separate queue/censoring diagnostic. At 2 offered workflows/s, EVM d20 completed 29/60 in-window (0.967/s, p95 28.95 s) versus d2 58/60 (1.933/s, p95 1.23 s); both d20 repeats required about 30 s to drain. NEAR d20 completed 18/60 (0.600/s, p95 21.88 s) versus d2 53/60 (1.767/s, p95 4.02 s), then required about 21 s to drain. The NEAR d20 window is left-censored by its roughly 22-second route latency; these figures are not steady-state capacity estimates. See `../depth_comparison_report.md` for the full pilot rows and hashes.\n")
    cleanup_by_run = {}
    for r in records:
        if r.get("cleanup_note") or r.get("cleanup_status") not in (None, "complete"):
            cleanup_by_run[r["run_dir"]] = (r["cleanup_note"], r["runner_exit_code"], r["cleanup_status"])
    cleanup_notes = [(name, *values) for name, values in cleanup_by_run.items()]
    if cleanup_notes:
        f.write("\n## Cleanup status\n\n")
        for run_name, note, code, status in cleanup_notes:
            detail = note or "Runner recorded a forced timeout cleanup after saving all measurement JSON."
            f.write(f"`{run_name}`: cleanup status `{status}`, runner exit code `{code}`. {detail} Cleanup is separate from workflow and audit-pending counts.\n\n")

    if supplemental_records:
        f.write("\n## Supplemental validation and recovery runs (excluded from primary statistics)\n\n")
        f.write("| Run | Platform | Route | Rate (workflow/s) | Goodput/s | p95 (s) | Audited / rows | Failures | Audit pending | Cleanup |\n|---|---|---|---:|---:|---:|---:|---:|---:|---|\n")
        for r in supplemental_records:
            f.write(f"| {r['run_dir']} | {r['platform']} | {r['route']} | {r['offered_rate_per_sec']} | {r['goodput_per_sec']} | {r['p95_sec']} | {r['audited_successes_after_drain']}/{r['rows']} | {r['failures_after_drain']} | {r['audit_pending_count']} | {r['cleanup_status'] or 'not_recorded'} |\n")

if records:
    W,H=1120,700; left,right,top,bottom=105,1080,95,590
    max_y=25.0
    depths=(8,20)
    def xval(d): return left + depths.index(d)*(right-left)
    def yval(v): return bottom - v/max_y*(bottom-top)
    series=[
        ("EVM / Anvil", "fused_single_contract", "EVM fused single contract", "#1d4ed8", "square"),
        ("EVM / Anvil", "independent_stage", "EVM independent stages", "#60a5fa", "circle"),
        ("NEAR Sandbox", "same_receiver", "NEAR same receiver", "#c2410c", "diamond"),
        ("NEAR Sandbox", "cross_contract", "NEAR cross-contract stages", "#f97316", "triangle"),
    ]
    grouped={}
    for r in records:
        if float(r["offered_rate_per_sec"] or 0) != 1.0: continue
        if r["platform"] == "NEAR Sandbox" and r["route"] in ("independent_stage", "multi_contract"):
            route="cross_contract"
        else: route=r["route"]
        grouped.setdefault((r["platform"], route, r["depth"]), []).append(r["p95_sec"])
    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">', '<rect width="100%" height="100%" fill="white"/>', '<text x="560" y="35" text-anchor="middle" font-family="sans-serif" font-size="22">Offer-to-final-sink latency at 1 workflow/s</text>', '<text x="560" y="62" text-anchor="middle" font-family="sans-serif" font-size="15">Primary runs only: median p95 across r1/r2/r3; error bars show min–max</text>']
    for tick in range(0,26,5):
        y=yval(tick); svg.append(f'<line x1="{left}" y1="{y}" x2="{right}" y2="{y}" stroke="#ddd"/><text x="{left-14}" y="{y+5}" text-anchor="end" font-family="sans-serif" font-size="14">{tick}</text>')
    svg += [f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#333"/>', f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#333"/>']
    for d in depths: svg.append(f'<text x="{xval(d)}" y="{bottom+28}" text-anchor="middle" font-family="sans-serif" font-size="17">d{d}</text>')
    offsets=(-27,-9,9,27)
    for i,(platform,route,label,color,shape) in enumerate(series):
        points=[]
        for depth in depths:
            vals=grouped.get((platform,route,depth),[])
            if not vals: continue
            vals=sorted(float(v) for v in vals if v is not None)
            if not vals: continue
            med=statistics.median(vals); x=xval(depth)+offsets[i]; y=yval(med); points.append((x,y))
            low,high=yval(vals[-1]),yval(vals[0])
            svg.append(f'<line x1="{x}" y1="{low}" x2="{x}" y2="{high}" stroke="{color}" stroke-width="2"/><line x1="{x-5}" y1="{low}" x2="{x+5}" y2="{low}" stroke="{color}"/><line x1="{x-5}" y1="{high}" x2="{x+5}" y2="{high}" stroke="{color}"/>')
            if shape=="square": svg.append(f'<rect x="{x-5}" y="{y-5}" width="10" height="10" fill="{color}"/>')
            elif shape=="diamond": svg.append(f'<path d="M{x} {y-6} L{x+6} {y} L{x} {y+6} L{x-6} {y} Z" fill="{color}"/>')
            elif shape=="triangle": svg.append(f'<path d="M{x} {y-6} L{x+6} {y+5} L{x-6} {y+5} Z" fill="{color}"/>')
            else: svg.append(f'<circle cx="{x}" cy="{y}" r="5" fill="{color}"/>')
            label_y=y-10 if i%2==0 else y+19
            svg.append(f'<text x="{x+8}" y="{label_y}" font-family="sans-serif" font-size="13" fill="{color}">{med:.2f}s (n={len(vals)})</text>')
        if len(points)==2:
            x1,y1=points[0]; x2,y2=points[1]
            svg.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2"/>')
        lx=130+(i%2)*470; ly=640+(i//2)*25
        svg.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+28}" y2="{ly}" stroke="{color}" stroke-width="3"/><text x="{lx+36}" y="{ly+5}" font-family="sans-serif" font-size="14">{label}</text>')
    svg.append(f'<text x="32" y="340" transform="rotate(-90 32 340)" text-anchor="middle" font-family="sans-serif" font-size="16">Median offer-to-sink p95 (seconds)</text>')
    svg.append('</svg>')
    (ROOT / "pipeline_latency_seconds.svg").write_text("\n".join(svg))
print(f"summarized {len(records)} pipeline phases")
