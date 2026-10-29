#!/usr/bin/env python3
"""Validate and summarize the paired d8 exponential-arrival pilot."""
import csv
import hashlib
import html
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PILOT = ROOT / "pilot_d8_seed20261028"
SCHEDULE_PATH = PILOT / "poisson_d8_pilot_seed20261028.json"
SCHEDULE_TEXT = SCHEDULE_PATH.read_text()
SCHEDULE = json.loads(SCHEDULE_TEXT)
EV = json.loads(next((PILOT / "evm").glob("*_raw.json")).read_text())
NE = json.loads(next((PILOT / "near_corrected").glob("*_raw.json")).read_text())
EV_SUM = json.loads(next((PILOT / "evm").glob("*_results.json")).read_text())
NE_SUM = json.loads(next((PILOT / "near_corrected").glob("*_results.json")).read_text())

assert hashlib.sha256(SCHEDULE_TEXT.encode()).hexdigest() == EV["schedule_sha256"] == NE["schedule_sha256"]
assert (PILOT / "evm" / "exit_code.txt").read_text().strip() == "0"
assert (PILOT / "near_corrected" / "exit_code.txt").read_text().strip() == "0"


def rank(values, q):
    values = sorted(values)
    return values[max(0, math.ceil(q * len(values)) - 1)] if values else None


def latency(row, platform, kind):
    if platform == "EVM":
        key = "offered_to_sink_ms" if kind == "offer" else "submit_to_sink_ms"
    else:
        key = "offered_to_sink_queryable_ms" if kind == "offer" else "call_to_sink_queryable_ms"
    value = row.get(key)
    return float(value) if value is not None else None


samples = []
summary = []
trace_checks = []
for ephase, nphase, esum, nsum in zip(EV["phases"], NE["phases"], EV_SUM["phases"], NE_SUM["phases"]):
    rate = float(ephase["offered_rate_per_sec"])
    scenario = SCHEDULE["scenarios"]["poisson_d8"]
    offsets = [float(x) for x in scenario["arrival_offsets_sec_by_rate"][str(rate)]]
    expected_ids = {str(810_050_000_000 + int(ephase["phase"]) * 100_000_000 + i + 1) for i in range(len(offsets))}
    erows = {str(r["workflow_id"]): r for r in ephase["rows"]}
    nrows = {str(r["workflow_id"]): r for r in nphase["rows"]}
    assert set(erows) == set(nrows) == expected_ids, f"workflow ID mismatch at lambda={rate}"
    assert [float(x) for x in ephase["planned_arrival_offsets_sec"]] == offsets
    assert [float(x) for x in nphase["planned_arrival_offsets_sec"]] == offsets
    phase_offset = float(scenario["phase_random_offset_sec"])
    trace = f"{rate:g}|{SCHEDULE['seed']}|{phase_offset:.12f}|" + ",".join(f"{x:.9f}" for x in offsets)
    digest = hashlib.sha256(trace.encode()).hexdigest()
    assert nphase["arrival_trace_sha256"] == digest
    trace_checks.append({"rate_workflows_per_sec": rate, "schedule_sha256": EV["schedule_sha256"],
                        "arrival_trace_sha256": digest, "arrival_count": len(offsets),
                        "phase_random_offset_sec": phase_offset, "offsets_match_both_platforms": True,
                        "workflow_id_sets_match": True})

    for platform, phase, phase_summary, rowmap in (("EVM", ephase, esum, erows), ("NEAR", nphase, nsum, nrows)):
        order = sorted(rowmap, key=lambda wid: int(wid))
        rows = [rowmap[wid] for wid in order]
        by_id = {wid: i for i, wid in enumerate(order)}
        start = phase["observation_start_at"]
        end = phase["measurement_window_end_at"]
        observed = [r for r in rows if not r.get("warmup")]
        succeeded_in_window = [r for r in observed if r.get("succeeded") and start <= r.get("sink_first_queryable_at", "") <= end]
        for i, (wid, row) in enumerate(zip(order, rows), 1):
            samples.append({
                "seed": SCHEDULE["seed"], "rate_workflows_per_sec": rate, "platform": platform,
                "arrival_order": i, "workflow_id": wid, "warmup": row.get("warmup"),
                "planned_at": row.get("planned_at"), "offered_at": row.get("offered_at"),
                "sink_first_queryable_at": row.get("sink_first_queryable_at"),
                "offer_to_sink_ms": latency(row, platform, "offer"),
                "submit_to_sink_ms": latency(row, platform, "submit"),
                "generator_schedule_lag_ms": row.get("generator_schedule_lag_ms"),
                "sink_poll_detection_bracket_ms": row.get("sink_poll_detection_bracket_ms"),
                "succeeded": row.get("succeeded"), "failure": row.get("failure"),
                "audit_pending": row.get("audit_pending"),
            })

        for metric in ("offer", "submit"):
            all_measurement = [latency(r, platform, metric) for r in observed]
            all_measurement = [x for x in all_measurement if x is not None]
            in_window = [latency(r, platform, metric) for r in succeeded_in_window]
            in_window = [x for x in in_window if x is not None]
            summary.append({
                "seed": SCHEDULE["seed"], "rate_workflows_per_sec": rate, "platform": platform,
                "metric": "offer_to_sink" if metric == "offer" else "submit_to_sink",
                "all_offered": len(rows), "warmup_offered": sum(bool(r.get("warmup")) for r in rows),
                "measured_offered": len(observed), "completed_in_measurement_window_n": len(in_window),
                "all_measured_p50_ms": rank(all_measurement, .50), "all_measured_p95_ms": rank(all_measurement, .95),
                "all_measured_max_ms": max(all_measurement) if all_measurement else None,
                "window_p50_ms": rank(in_window, .50), "window_p95_ms": rank(in_window, .95),
                "window_p95_sample_n": len(in_window),
                "successful": sum(bool(r.get("succeeded")) for r in rows),
                "failures": sum(bool(r.get("failure")) for r in rows),
                "audit_pending": sum(bool(r.get("audit_pending")) for r in rows),
                "trace_cv": phase.get("planned_interarrival_cv"),
                "generator_lag_p95_ms": rank([float(r["generator_schedule_lag_ms"]) for r in rows if r.get("generator_schedule_lag_ms") is not None], .95),
                "sink_sampling_bound_or_max_bracket_ms": phase.get("receipt_sampling_error_bound_ms") if platform == "EVM" else max((r.get("sink_poll_detection_bracket_ms") or 0 for r in rows), default=None),
                "phase_start_block": phase_summary.get("phase_start_block", (phase_summary.get("block_usage_samples") or [{}])[0].get("height", (phase_summary.get("block_usage_samples") or [{}])[0].get("block_number"))),
                "phase_end_block": phase_summary.get("phase_end_block", (phase_summary.get("block_usage_samples") or [{}])[-1].get("height", (phase_summary.get("block_usage_samples") or [{}])[-1].get("block_number"))),
                "phase_block_samples": len(phase_summary.get("block_usage_samples") or []),
            })


def write_csv(path, rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


write_csv(ROOT / "randomized_pilot_workflow_samples.csv", samples)
write_csv(ROOT / "randomized_pilot_summary.csv", summary)
write_csv(ROOT / "randomized_pilot_trace_validation.csv", trace_checks)


def txt(x, y, text, size=13, anchor="middle", fill="#222", weight="normal"):
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}">{html.escape(str(text))}</text>'


colors = {"EVM": "#2764ae", "NEAR": "#e07a24"}
markers = {"EVM": "circle", "NEAR": "square"}


def point(x, y, platform):
    if platform == "EVM":
        return f'<circle cx="{x:.2f}" cy="{y:.2f}" r="3" fill="{colors[platform]}" fill-opacity=".76"/>'
    return f'<rect x="{x-2.5:.2f}" y="{y-2.5:.2f}" width="5" height="5" fill="{colors[platform]}" fill-opacity=".76"/>'


def chart_scatter(zoom=False):
    W, H = 1180, 510
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">', '<rect width="100%" height="100%" fill="white"/>']
    svg += [txt(W/2, 28, "Randomized d8: per-workflow offer-to-sink samples", 20, weight="bold"),
            txt(W/2, 50, "Seed 20261028 · matched exponential arrivals · no added jitter" + (" · zoom 0–2 s" if zoom else " · full scale 0–12 s"), 12, fill="#555")]
    left, top, pw, ph, gap = 82, 78, 500, 355, 36
    for j, rate in enumerate((0.5, 1.0)):
        x0 = left + j * (pw + gap)
        subset = [r for r in samples if r["rate_workflows_per_sec"] == rate and not r["warmup"]]
        vals = [r["offer_to_sink_ms"] / 1000 for r in subset if r["offer_to_sink_ms"] is not None]
        ymin, ymax = (0, 2 if zoom else 12)
        xL, xR, yT, yB = x0 + 50, x0 + pw - 15, top + 28, top + ph - 38
        svg.append(f'<rect x="{x0}" y="{top}" width="{pw}" height="{ph}" fill="white" stroke="#aaa"/>')
        svg.append(txt(x0 + pw/2, top + 17, f"λ = {rate:g} workflow/s", 13, weight="bold"))
        for k in range(5):
            yv = ymin + (ymax-ymin)*k/4
            yy = yB - (yv-ymin)/(ymax-ymin)*(yB-yT)
            svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>')
            svg.append(txt(xL-8, yy+4, f"{yv:.1f}", 10, "end", "#555"))
        for r in subset:
            value = r["offer_to_sink_ms"]
            if value is None: continue
            yv = value/1000
            if yv > ymax: continue
            xx = xL + (r["arrival_order"]-1)/max(1, len(offsets_for(rate))-1)*(xR-xL)
            yy = yB - (yv-ymin)/(ymax-ymin)*(yB-yT)
            svg.append(point(xx, yy, r["platform"]))
        svg.append(txt((xL+xR)/2, yB+25, "workflow arrival order", 11, fill="#555"))
        svg.append(txt(x0+16, (yT+yB)/2, "seconds", 10, fill="#555"))
    for i, platform in enumerate(("EVM", "NEAR")):
        x = 500 + i*150
        svg.append(point(x, 474, platform)); svg.append(txt(x+15, 478, platform, 12, "start"))
    svg.append('</svg>')
    name = "randomized_pilot_scatter_zoom.svg" if zoom else "randomized_pilot_scatter_full.svg"
    (ROOT/name).write_text("\n".join(svg))


def offsets_for(rate):
    return SCHEDULE["scenarios"]["poisson_d8"]["arrival_offsets_sec_by_rate"][str(rate)]


def chart_ecdf(zoom=False):
    W, H = 1180, 510
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">', '<rect width="100%" height="100%" fill="white"/>']
    svg += [txt(W/2, 28, "Randomized d8: offer-to-sink latency ECDF", 20, weight="bold"),
            txt(W/2, 50, "Non-warmup offers · seed 20261028 · no synthetic samples" + (" · zoom 0–2 s" if zoom else " · full scale 0–12 s"), 12, fill="#555")]
    left, top, pw, ph, gap = 82, 78, 500, 355, 36
    for j, rate in enumerate((0.5, 1.0)):
        x0 = left + j * (pw + gap); xmin, xmax = 0, (2 if zoom else 12)
        xL, xR, yT, yB = x0 + 50, x0 + pw - 15, top + 28, top + ph - 38
        svg.append(f'<rect x="{x0}" y="{top}" width="{pw}" height="{ph}" fill="white" stroke="#aaa"/>')
        svg.append(txt(x0 + pw/2, top + 17, f"λ = {rate:g} workflow/s", 13, weight="bold"))
        for k in range(5):
            p = k/4; yy = yB - p*(yB-yT); xv = xmin+(xmax-xmin)*k/4; xx=xL+(xv-xmin)/(xmax-xmin)*(xR-xL)
            svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>')
            svg.append(txt(xL-8, yy+4, f"{p:.2f}", 10, "end", "#555"))
            svg.append(txt(xx, yB+18, f"{xv:.1f}", 10, fill="#555"))
        for platform in ("EVM", "NEAR"):
            vals = sorted(r["offer_to_sink_ms"]/1000 for r in samples if r["rate_workflows_per_sec"] == rate and r["platform"] == platform and not r["warmup"] and r["offer_to_sink_ms"] is not None)
            vals = [v for v in vals if v <= xmax]
            if not vals: continue
            pts=[]
            denominator=sum(1 for r in samples if r["rate_workflows_per_sec"]==rate and r["platform"]==platform and not r["warmup"])
            for i,v in enumerate(vals,1): pts.append((xL+(v-xmin)/(xmax-xmin)*(xR-xL), yB-(i/denominator)*(yB-yT)))
            path=f'M{pts[0][0]:.2f},{pts[0][1]:.2f}'
            for a,b in zip(pts,pts[1:]): path+=f' H{b[0]:.2f} V{b[1]:.2f}'
            path+=f' H{xR}'
            svg.append(f'<path d="{path}" fill="none" stroke="{colors[platform]}" stroke-width="2"/>')
        svg.append(txt((xL+xR)/2, yB+35, "latency (s)", 11, fill="#555"))
    for i, platform in enumerate(("EVM", "NEAR")):
        x=500+i*150; svg.append(f'<path d="M{x-8},474 H{x+8}" stroke="{colors[platform]}" stroke-width="2"/>'); svg.append(txt(x+15,478,platform,12,"start"))
    svg.append('</svg>')
    name = "randomized_pilot_ecdf_zoom.svg" if zoom else "randomized_pilot_ecdf_full.svg"
    (ROOT/name).write_text("\n".join(svg))


for zoom in (False, True):
    chart_scatter(zoom)
    chart_ecdf(zoom)

print(f"validated {len(trace_checks)} shared randomized traces and wrote {len(samples)} per-workflow samples")
