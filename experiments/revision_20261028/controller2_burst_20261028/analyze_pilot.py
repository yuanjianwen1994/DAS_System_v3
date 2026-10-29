#!/usr/bin/env python3
"""Summarize the paired burst pilot without altering or smoothing raw samples."""
import csv
from datetime import datetime
import json
import math
import statistics
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results" / "pilot_seed20261028"
PLATFORMS = [
    ("EVM independent Stage", RESULTS / "evm", "evm"),
    ("NEAR independent contracts", RESULTS / "near_independent", "near"),
    ("NEAR same-receiver control", RESULTS / "near_same_receiver", "near"),
]
SCENARIOS = {0: "poisson_d8", 10: "burst_d8"}
COLORS = {"EVM independent Stage": "#2563eb", "NEAR independent contracts": "#dc2626",
          "NEAR same-receiver control": "#d97706"}


def percentile(values, p):
    xs = sorted(values)
    return xs[max(0, math.ceil(p * len(xs)) - 1)] if xs else None


def load_raw(directory, suffix):
    files = sorted(directory.glob(f"*_{suffix}_raw.json"))
    if not files:
        raise FileNotFoundError(f"no {suffix} raw file under {directory}")
    return json.loads(files[-1].read_text()), files[-1]


def is_warmup_row(row, phase, platform):
    if platform != "NEAR same-receiver control":
        return bool(row.get("warmup", False))
    if bool(row.get("warmup", False)):
        return True
    planned = row.get("planned_at")
    phase_start = phase.get("phase_started_at")
    preheat_sec = phase.get("preheat_sec")
    if planned and phase_start and preheat_sec is not None:
        boundary_ms = datetime.fromisoformat(phase_start.replace("Z", "+00:00")).timestamp() * 1000 + float(preheat_sec) * 1000
        # Near control's planned timestamp may round a boundary event a few ms early;
        # 50 ms matches the sink polling granularity and retains the scheduled boundary offer.
        return datetime.fromisoformat(planned.replace("Z", "+00:00")).timestamp() * 1000 < boundary_ms - 50
    return False


def svg_text(x, y, value, size=13, anchor="middle", color="#222"):
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="Arial,sans-serif" font-size="{size}" fill="{color}">{escape(str(value))}</text>'


def write_latency_svg(samples):
    width, height, panel_w, plot_h = 1160, 500, 470, 330
    lefts, top = (90, 630), 75
    xmax = 12.0
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
             '<rect width="100%" height="100%" fill="white"/>',
             svg_text(width/2, 30, "d8 offer-to-sink latency ECDF", 19),
             svg_text(width/2, 51, "One seed; each curve uses measured-window audited workflows only", 12, color="#555")]
    for left, scenario, title in zip(lefts, ("poisson_d8", "burst_d8"), ("Poisson-conditioned arrivals", "10 s periodic bursts")):
        parts.append(svg_text(left + panel_w/2, 70, title, 15))
        for tick in range(0, 13, 3):
            x = left + tick/xmax*panel_w
            parts.append(f'<line x1="{x}" y1="{top}" x2="{x}" y2="{top+plot_h}" stroke="#e5e7eb"/>')
            parts.append(svg_text(x, top+plot_h+22, tick, 11))
        for tick in (0, .25, .5, .75, 1):
            y = top + (1-tick)*plot_h
            parts.append(f'<line x1="{left}" y1="{y}" x2="{left+panel_w}" y2="{y}" stroke="#e5e7eb"/>')
            parts.append(svg_text(left-12, y+4, f"{tick:.2g}", 10, "end", "#555"))
        parts.append(f'<rect x="{left}" y="{top}" width="{panel_w}" height="{plot_h}" fill="none" stroke="#444"/>')
        for label, _, _ in PLATFORMS:
            xs = sorted(float(s["offered_to_sink_ms"])/1000 for s in samples
                        if s["platform"] == label and s["scenario"] == scenario and not s["warmup"] and s["succeeded"] and not s["audit_pending"])
            if not xs:
                continue
            coords = []
            prev_y = top + plot_h
            for i, value in enumerate(xs):
                x = left + min(xmax, value)/xmax*panel_w
                y = top + (1-(i+1)/len(xs))*plot_h
                if i == 0:
                    coords.append(f"M{x:.1f},{prev_y:.1f}")
                else:
                    coords.append(f"L{x:.1f},{prev_y:.1f}")
                coords.append(f"L{x:.1f},{y:.1f}")
                prev_y = y
            parts.append(f'<path d="{" ".join(coords)}" fill="none" stroke="{COLORS[label]}" stroke-width="2.2"/>')
        parts.append(svg_text(left+panel_w/2, top+plot_h+48, "Offer-to-sink queryable (seconds)", 12))
    legend_x, legend_y = 130, 482
    for i, (label, color) in enumerate(COLORS.items()):
        x = legend_x+i*330
        parts.append(f'<line x1="{x}" y1="{legend_y-4}" x2="{x+25}" y2="{legend_y-4}" stroke="{color}" stroke-width="3"/>')
        parts.append(svg_text(x+32, legend_y, label, 11, "start"))
    parts.append('</svg>')
    (ROOT / "latency_ecdf.svg").write_text("\n".join(parts))


def write_queue_svg(loaded):
    width, height, left, top, plot_w, plot_h = 1000, 460, 90, 70, 820, 300
    all_y = []
    routes = []
    for label, raw in loaded.items():
        phase = next((p for p in raw.get("phases", []) if int(p.get("phase", -1)) == 10), None)
        if not phase:
            continue
        bins = phase.get("time_bins_sec", [])
        poisson = next((p for p in raw.get("phases", []) if int(p.get("phase", -1)) == 0), {})
        base = percentile([int(b.get("queue_at_bin_end", 0)) for b in poisson.get("time_bins_sec", [])], .5) or 0
        points = [(float(b.get("bin_end_sec", i+1)), max(0, int(b.get("queue_at_bin_end", 0))-int(base))) for i,b in enumerate(bins)]
        routes.append((label, points))
        all_y.extend(y for _,y in points)
    ymax=max(1,max(all_y,default=1))
    parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           svg_text(width/2,30,"Burst queue above same-route Poisson median",18),
           svg_text(width/2,50,"1 s bin proxy: submitted/accepted-minus-sink-completed; normal in-flight work is baseline",11,color="#555")]
    for tick in range(0,21,5):
        x=left+tick/20*plot_w
        parts.append(f'<line x1="{x}" y1="{top}" x2="{x}" y2="{top+plot_h}" stroke="#e5e7eb"/>')
        parts.append(svg_text(x,top+plot_h+22,tick,11))
    for tick in range(ymax+1):
        y=top+plot_h-tick/ymax*plot_h
        parts.append(f'<line x1="{left}" y1="{y}" x2="{left+plot_w}" y2="{y}" stroke="#e5e7eb"/>')
        parts.append(svg_text(left-10,y+4,tick,10,"end","#555"))
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="#444"/>')
    for label, points in routes:
        path=[]; last_y=0
        for i,(xv,yv) in enumerate(points):
            x=left+min(20,xv)/20*plot_w; y=top+plot_h-yv/ymax*plot_h
            if i==0: path.append(f"M{x:.1f},{y:.1f}")
            else: path.append(f"L{x:.1f},{last_y:.1f} L{x:.1f},{y:.1f}")
            last_y=y
        parts.append(f'<path d="{" ".join(path)}" fill="none" stroke="{COLORS[label]}" stroke-width="2.2"/>')
    parts.append(svg_text(left+plot_w/2,top+plot_h+48,"Seconds from measurement-window start",12))
    for i,(label,color) in enumerate(COLORS.items()):
        x=120+i*300
        parts.append(f'<line x1="{x}" y1="430" x2="{x+24}" y2="430" stroke="{color}" stroke-width="3"/>')
        parts.append(svg_text(x+30,434,label,11,"start"))
    parts.append('</svg>')
    (ROOT / "burst_queue_recovery.svg").write_text("\n".join(parts))


def latency(row):
    for key in ("offered_to_sink_ms", "offered_to_sink_queryable_ms"):
        if row.get(key) is not None:
            return float(row[key])
    return None


def main():
    summaries, samples, loaded = [], [], {}
    schedule_hashes, trace_hashes = set(), {}
    for label, directory, suffix in PLATFORMS:
        raw, source = load_raw(directory, suffix)
        loaded[label] = raw
        schedule_hashes.add(raw.get("schedule_sha256"))
        for phase in raw.get("phases", []):
            scenario = SCENARIOS.get(int(phase.get("phase", -1)))
            if scenario is None:
                continue
            trace_hashes.setdefault(scenario, set()).add(phase.get("arrival_trace_sha256"))
            rows = phase.get("rows", [])
            measured = []
            for row in rows:
                value = latency(row)
                if value is None:
                    continue
                succeeded = bool(row.get("succeeded", row.get("execution_succeeded", False)))
                is_warmup = is_warmup_row(row, phase, label)
                if not is_warmup and succeeded and not row.get("audit_pending", False):
                    measured.append(value)
                samples.append({
                    "platform": label, "scenario": scenario,
                    "workflow_id": row.get("workflow_id"), "offered_at": row.get("offered_at"),
                    "call_issued_at": row.get("call_issued_at", row.get("tx_submit_started_at")),
                    "sink_queryable_at": row.get("sink_first_queryable_at"),
                    "offered_to_sink_ms": value,
                    "warmup": is_warmup, "succeeded": succeeded, "audit_pending": bool(row.get("audit_pending", False)),
                    "failure": row.get("failure"),
                    "generator_schedule_lag_ms": row.get("generator_schedule_lag_ms"),
                    "client_queue_delay_ms": row.get("client_queue_delay_ms"),
                })
            bins = phase.get("time_bins_sec", phase.get("time_bins_10s", []))
            queue_peak = max((int(b.get("queue_at_bin_end", 0)) for b in bins), default=0)
            offer_peak = max((int(b.get("offered", 0)) for b in bins), default=0)
            rpc = phase.get("rpc_load_path_by_method", {})
            rpc_errors = sum(int(v.get("errors", 0)) for v in rpc.values() if isinstance(v, dict))
            block_samples = phase.get("block_usage_samples", [])
            block_utilization = [float(b["utilization"]) for b in block_samples if b.get("utilization") is not None]
            recovery_delays = []
            if scenario == "burst_d8":
                phase_offset = float(phase.get("phase_random_offset_sec", 0))
                preheat = float(phase.get("preheat_sec", 0))
                burst_duration = 2.0
                cycle = 10.0
                poisson_phase = next((p for p in raw.get("phases", []) if int(p.get("phase", -1)) == 0), {})
                poisson_bins = poisson_phase.get("time_bins_sec", [])
                poisson_queue = [int(b.get("queue_at_bin_end", 0)) for b in poisson_bins]
                baseline_queue = percentile(poisson_queue, 0.50)
                for cycle_start in range(0, 30, 10):
                    final_global = phase_offset + cycle_start + burst_duration - 1 / 5
                    relative = final_global - (phase_offset + preheat)
                    if 0 <= relative < float(phase.get("observation_duration_sec", 0)):
                        next_burst_start = cycle_start + cycle - preheat
                        next_burst_start = next_burst_start if next_burst_start < float(phase.get("observation_duration_sec", 0)) else None
                        later = [b for b in bins if float(b.get("bin_end_sec", 0)) >= relative]
                        recovered = next((b for b in later if int(b.get("queue_at_bin_end", 0)) <= baseline_queue), None)
                        returned_at = None if recovered is None else float(recovered["bin_end_sec"])
                        recovery_delays.append({
                            "last_offer_sec": round(relative, 3),
                            "poisson_median_queue_baseline": baseline_queue,
                            "returned_to_baseline_sec": returned_at,
                            "recovery_delay_ms": None if returned_at is None else round(max(0.0, returned_at-relative)*1000, 1),
                            "recovered_before_next_burst": None if returned_at is None or next_burst_start is None else returned_at < next_burst_start,
                        })
            baseline_queue = None
            if scenario == "poisson_d8":
                baseline_queue = percentile([int(b.get("queue_at_bin_end", 0)) for b in bins], 0.50)
            else:
                poisson_phase = next((p for p in raw.get("phases", []) if int(p.get("phase", -1)) == 0), {})
                baseline_queue = percentile([int(b.get("queue_at_bin_end", 0)) for b in poisson_phase.get("time_bins_sec", [])], 0.50)
            summaries.append({
                "platform": label, "scenario": scenario,
                "schedule_sha256": raw.get("schedule_sha256"),
                "offered": phase.get("offered", len(rows)),
                "measurement_offered": sum(not is_warmup_row(r, phase, label) for r in rows),
                "workflow_rows": len(rows), "audited_success_n": len(measured),
                "p50_offer_to_sink_ms": percentile(measured, 0.50),
                "p95_offer_to_sink_ms": percentile(measured, 0.95) if len(measured) >= 20 else None,
                "p99_offer_to_sink_ms": percentile(measured, 0.99) if len(measured) >= 100 else None,
                "max_offer_to_sink_ms": max(measured) if measured else None,
                "window_goodput_per_sec": phase.get("measured_window_goodput_per_sec",
                                                    phase.get("successful_completions_in_measurement_window", 0) /
                                                    max(0.001, float(phase.get("observation_duration_sec", 1)))),
                "window_submission_or_accept_rate_per_sec": phase.get("measured_window_accept_rate_per_sec",
                    phase.get("offered_call_rate_in_measurement_window",
                              phase.get("submission_attempts_in_measurement_window", 0) /
                              max(0.001, float(phase.get("observation_duration_sec", 1))))),
                "submission_rate_semantics": "EVM tx-hash-returned" if "EVM" in label else "NEAR call-issued",
                "queue_high_water": phase.get("client_queue_depth_high_water",
                                                phase.get("client_task_queue_depth_high_water",
                                                          phase.get("client_inflight_high_water"))),
                "queue_at_bin_end_peak": queue_peak,
                "poisson_median_queue_baseline": baseline_queue,
                "queue_peak_above_poisson_baseline": max(0, queue_peak - int(baseline_queue or 0)),
                "max_offers_in_1s_bin": offer_peak,
                "mean_generator_schedule_lag_ms": phase.get("mean_generator_schedule_lag_ms"),
                "mean_client_queue_delay_ms": phase.get("mean_client_queue_delay_ms"),
                "rpc_load_errors": rpc_errors,
                "block_samples": len(block_samples),
                "max_block_gas_utilization": max(block_utilization) if block_utilization else None,
                "receipt_sampling_error_bound_ms": phase.get("receipt_sampling_error_bound_ms", phase.get("sink_poll_interval_ms")),
                "burst_recovery_to_poisson_baseline": json.dumps(recovery_delays),
                "failures": phase.get("failures", 0),
                "audit_pending": phase.get("audit_pending_count", phase.get("audit_pending", 0)),
                "drain_time_after_window_ms": phase.get("drain_time_after_window_ms"),
                "source_raw": str(source.relative_to(ROOT)),
            })
    if len(schedule_hashes) != 1 or None in schedule_hashes:
        raise RuntimeError(f"paired schedule hash mismatch across routes: {schedule_hashes}")
    if not summaries:
        raise RuntimeError("no completed scenario phases found in raw outputs")
    if any(len(values) != 1 or None in values for values in trace_hashes.values()):
        raise RuntimeError(f"paired arrival trace mismatch: {trace_hashes}")

    with (ROOT / "pilot_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    with (ROOT / "pilot_workflow_samples.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(samples[0]))
        writer.writeheader()
        writer.writerows(samples)

    write_latency_svg(samples)
    write_queue_svg(loaded)

    lines = ["# d8 突发到达 Pilot 结果", "",
             "本报告由远端原始 JSON 生成。Pilot 只有一个 seed，每轨迹 30 个 workflow，其中测量窗约 20 个；P95 仅作探索性描述，P99 不报告。队列曲线是已提交/调用发出数减 sink 完成数的 1 秒分箱代理值。",
             "", "## 汇总", "", "| 平台路线 | 到达 | 有效审计样本 | P50 s | P95 s | 峰值队列 | 窗口 goodput/s | 失败 | 审计待定 |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in summaries:
        p50 = "—" if r["p50_offer_to_sink_ms"] is None else f'{r["p50_offer_to_sink_ms"]/1000:.3f}'
        p95 = "—" if r["p95_offer_to_sink_ms"] is None else f'{r["p95_offer_to_sink_ms"]/1000:.3f}'
        lines.append(f'| {r["platform"]} | {r["scenario"]} | {r["audited_success_n"]} | {p50} | {p95} | {r["queue_at_bin_end_peak"]} | {float(r["window_goodput_per_sec"] or 0):.3f} | {r["failures"]} | {r["audit_pending"]} |')
    lines += ["", "## 图与数据", "", "- `latency_ecdf.svg`：逐 workflow offer-to-sink ECDF。", "- `burst_queue_recovery.svg`：减去 Poisson 中位队列后的 1 秒队列代理值。", "- `pilot_summary.csv`：路线/场景汇总。", "- `pilot_workflow_samples.csv`：逐 workflow 样本与审计状态。", "- `results/pilot_seed20261028/`：未改写的远端原始结果、日志与资源记录。", ""]
    (ROOT / "pilot_report.md").write_text("\n".join(lines))
    print("wrote pilot_report.md, pilot_summary.csv, pilot_workflow_samples.csv, latency_ecdf.svg, burst_queue_recovery.svg")


if __name__ == "__main__":
    main()
