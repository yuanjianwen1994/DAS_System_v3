#!/usr/bin/env python3
"""Analyze per-seed burst-formal outputs without pooling seeds as iid workflows."""
import csv
import json
import math
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORMAL = ROOT / "results" / "formal"
ROUTES = {
    "EVM independent Stage": ("evm", "evm"),
    "NEAR independent contracts": ("near_independent", "near"),
    "NEAR same-receiver control": ("near_same_receiver", "near"),
}
SCENARIOS = {0: "poisson_d8", 10: "burst_d8"}


def pct(xs, p):
    s = sorted(xs)
    return s[max(0, math.ceil(p * len(s)) - 1)] if s else None


def latency(row):
    for key in ("offered_to_sink_ms", "offered_to_sink_queryable_ms"):
        if row.get(key) is not None:
            return float(row[key])
    return None


def read_raw(directory, suffix):
    matches = sorted(directory.glob(f"*_{suffix}_raw.json"))
    if not matches:
        raise FileNotFoundError(f"missing {suffix} raw output in {directory}")
    return json.loads(matches[-1].read_text()), matches[-1]


def main():
    summaries, samples = [], []
    schedule_hashes = {}
    trace_hashes = {}
    seeds = sorted(p.name.removeprefix("seed_") for p in FORMAL.glob("seed_*"))
    if len(seeds) < 3:
        raise RuntimeError(f"expected at least three formal seeds, found {seeds}")

    raw_by_seed_route = {}
    for seed in seeds:
        for route, (folder, suffix) in ROUTES.items():
            raw, source = read_raw(FORMAL / f"seed_{seed}" / folder, suffix)
            raw_by_seed_route[(seed, route)] = raw
            schedule_hashes.setdefault(seed, set()).add(raw.get("schedule_sha256"))
            for phase in raw.get("phases", []):
                scenario = SCENARIOS.get(int(phase.get("phase", -1)))
                if scenario:
                    trace_hashes.setdefault((seed, scenario), set()).add(phase.get("arrival_trace_sha256"))
                    rows = phase.get("rows", [])
                    measured = [r for r in rows if not r.get("warmup", False)]
                    valid = [r for r in measured if r.get("succeeded", r.get("execution_succeeded", False)) and
                             not r.get("audit_pending", False) and latency(r) is not None]
                    values = [latency(r) for r in valid]
                    bins = phase.get("time_bins_sec", [])
                    q = [int(b.get("queue_at_bin_end", 0)) for b in bins]
                    poisson_phase = next((p for p in raw.get("phases", []) if int(p.get("phase", -1)) == 0), {})
                    poisson_bins = poisson_phase.get("time_bins_sec", [])
                    baseline = pct([int(b.get("queue_at_bin_end", 0)) for b in poisson_bins], .5)
                    recovery = []
                    if scenario == "burst_d8":
                        obs = float(phase.get("observation_duration_sec", 0))
                        poisson_by_end = {float(b.get("bin_end_sec", i + 1)): int(b.get("queue_at_bin_end", 0))
                                          for i, b in enumerate(poisson_bins)}
                        # Arrival traces run continuously through warmup and observation. In
                        # measurement-relative time the six observed bursts start at 0..50 s.
                        for start in range(0, int(obs), 10):
                            last_offer = start + 1.8
                            if last_offer >= obs:
                                continue
                            next_start = start + 10 if start + 10 < obs else None
                            ordered_bins = sorted(bins, key=lambda b: float(b.get("bin_end_sec", 0)))
                            returned = None
                            for i, b in enumerate(ordered_bins):
                                end = float(b.get("bin_end_sec", 0))
                                if end < last_offer or i + 1 >= len(ordered_bins):
                                    continue
                                following = ordered_bins[i + 1]
                                end2 = float(following.get("bin_end_sec", 0))
                                q1 = int(b.get("queue_at_bin_end", 0))
                                q2 = int(following.get("queue_at_bin_end", 0))
                                if (end2 <= end + 1.01 and end in poisson_by_end and end2 in poisson_by_end and
                                        q1 <= poisson_by_end[end] and q2 <= poisson_by_end[end2]):
                                    returned = end
                                    break
                            recovery.append({
                                "last_offer_sec": round(last_offer, 1),
                                "returned_to_poisson_baseline_sec": returned,
                                "recovery_delay_sec": None if returned is None else round(max(0, returned-last_offer)),
                                "sampling_resolution_sec": 1,
                                "sampling_uncertainty_sec": "about +/-1",
                                "before_next_burst": None if returned is None or next_start is None else returned < next_start,
                            })
                    rpc = phase.get("rpc_load_path_by_method", {})
                    rpc_errors = sum(int(v.get("errors", 0)) for v in rpc.values() if isinstance(v, dict))
                    blocks = phase.get("block_usage_samples", [])
                    utilization = [float(x["utilization"]) for x in blocks if x.get("utilization") is not None]
                    summaries.append({
                        "seed": seed, "route": route, "scenario": scenario,
                        "schedule_sha256": raw.get("schedule_sha256"),
                        "arrival_trace_sha256": phase.get("arrival_trace_sha256"),
                        "offered_total_including_warmup": phase.get("offered", len(rows)),
                        "measurement_offered_n": len(measured), "audited_success_n": len(values),
                        "p50_offer_to_sink_ms": pct(values, .5),
                        "p95_offer_to_sink_ms": pct(values, .95) if len(values) >= 20 else None,
                        "p99_offer_to_sink_ms": pct(values, .99) if len(values) >= 100 else None,
                        "max_offer_to_sink_ms": max(values) if values else None,
                        "measurement_goodput_per_sec": phase.get("measured_window_goodput_per_sec",
                            phase.get("successful_completions_in_measurement_window", 0) / max(.001, float(phase.get("observation_duration_sec", 1)))),
                        "submission_or_accept_rate_per_sec": phase.get("measured_window_accept_rate_per_sec",
                            phase.get("offered_call_rate_in_measurement_window", 0)),
                        "submission_metric_semantics": "EVM tx-hash-returned" if "EVM" in route else "NEAR call-issued",
                        "queue_at_bin_end_median": statistics.median(q) if q else None,
                        "queue_at_bin_end_peak": max(q) if q else None,
                        "poisson_median_queue_baseline": baseline,
                        "peak_queue_above_baseline": None if not q else max(0, max(q)-int(baseline or 0)),
                        "queue_vs_time_matched_poisson_peak": max((int(b.get("queue_at_bin_end", 0)) -
                            poisson_by_end.get(float(b.get("bin_end_sec", 0)), int(b.get("queue_at_bin_end", 0)))
                            for b in bins), default=None) if scenario == "burst_d8" else None,
                        "burst_recovery_to_baseline": json.dumps(recovery),
                        "burst_cycle_recovery_median_sec": statistics.median([x["recovery_delay_sec"] for x in recovery
                            if x["recovery_delay_sec"] is not None]) if any(x["recovery_delay_sec"] is not None for x in recovery) else None,
                        "burst_cycle_recovery_range_sec": (f"{min(x['recovery_delay_sec'] for x in recovery if x['recovery_delay_sec'] is not None)}–"
                            f"{max(x['recovery_delay_sec'] for x in recovery if x['recovery_delay_sec'] is not None)}")
                            if any(x["recovery_delay_sec"] is not None for x in recovery) else None,
                        "burst_cycles_n": len(recovery),
                        "burst_cycles_recovered_n": sum(x["recovery_delay_sec"] is not None for x in recovery),
                        "burst_cycles_before_next_n": sum(x["before_next_burst"] is True for x in recovery),
                        "burst_cycles_with_next_n": sum(x["before_next_burst"] is not None for x in recovery),
                        "mean_schedule_error_ms": phase.get("mean_generator_schedule_lag_ms"),
                        "mean_client_queue_delay_ms": phase.get("mean_client_queue_delay_ms"),
                        "client_inflight_high_water": phase.get("client_inflight_high_water"),
                        "rpc_load_errors": rpc_errors,
                        "block_samples": len(blocks),
                        "max_block_gas_utilization": max(utilization) if utilization else None,
                        "receipt_sampling_error_ms": phase.get("receipt_sampling_error_bound_ms", phase.get("sink_poll_interval_ms")),
                        "failures": phase.get("failures", 0),
                        "audit_pending": phase.get("audit_pending_count", phase.get("audit_pending", 0)),
                        "drain_time_after_window_ms": phase.get("drain_time_after_window_ms"),
                        "source_raw": str(source.relative_to(ROOT)),
                    })
                    for row in rows:
                        samples.append({
                            "seed": seed, "route": route, "scenario": scenario,
                            "workflow_id": row.get("workflow_id"), "warmup": bool(row.get("warmup", False)),
                            "offered_at": row.get("offered_at"),
                            "offered_to_sink_ms": latency(row),
                            "succeeded": bool(row.get("succeeded", row.get("execution_succeeded", False))),
                            "audit_pending": bool(row.get("audit_pending", False)),
                            "failure": row.get("failure"),
                            "generator_schedule_error_ms": row.get("generator_schedule_lag_ms"),
                        })

    if any(len(v) != 1 or None in v for v in schedule_hashes.values()):
        raise RuntimeError(f"schedule hash mismatch by seed: {schedule_hashes}")
    if any(len(v) != 1 or None in v for v in trace_hashes.values()):
        raise RuntimeError(f"arrival trace mismatch by seed/scenario: {trace_hashes}")

    for name, rows in (("formal_seed_summary.csv", summaries), ("formal_workflow_samples.csv", samples)):
        with (ROOT / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    route_order = list(ROUTES)
    seed_colors = {seed: ("#2563eb", "#f97316", "#16a34a")[i % 3] for i, seed in enumerate(seeds)}
    by_key = {(r["seed"], r["route"], r["scenario"]): r for r in summaries}
    lines = ['<svg xmlns="http://www.w3.org/2000/svg" width="1120" height="500" viewBox="0 0 1120 500">',
             '<rect width="100%" height="100%" fill="white"/>',
             '<style>text{font-family:Arial,sans-serif;fill:#222} .small{font-size:11px} .tick{font-size:12px} .title{font-size:15px;font-weight:600} .grid{stroke:#d9dee7;stroke-width:1} .axis{stroke:#444;stroke-width:1}</style>',
             '<text x="560" y="28" text-anchor="middle" font-size="18" font-weight="600">Burst arrival: latency effect and return to baseline</text>']
    route_labels = ["EVM Stage", "NEAR independent", "NEAR same receiver"]

    # Left panel: paired per-seed burst/Poisson ratios; P95 is screening-only.
    ratios = []
    for seed in seeds:
        for route in route_order:
            p = by_key.get((seed, route, "poisson_d8"), {})
            b = by_key.get((seed, route, "burst_d8"), {})
            for metric in ("p50_offer_to_sink_ms", "p95_offer_to_sink_ms"):
                if p.get(metric) and b.get(metric):
                    ratios.append(float(b[metric]) / float(p[metric]))
    ymax = max(3, math.ceil(max(ratios, default=1) * 1.15))
    lx0, lx1, top, bottom = 75, 610, 82, 385
    lines += ['<text x="342" y="58" text-anchor="middle" class="title">Burst / fixed-count Poisson latency ratio</text>',
              '<text x="342" y="75" text-anchor="middle" class="small">Each point is one seed; P95 is exploratory (about 60 samples/condition)</text>']
    for t in range(0, ymax + 1):
        y = bottom - (bottom-top) * t / ymax
        lines.append(f'<line x1="{lx0}" y1="{y:.1f}" x2="{lx1}" y2="{y:.1f}" class="grid"/>')
        lines.append(f'<text x="{lx0-9}" y="{y+4:.1f}" text-anchor="end" class="tick">{t}</text>')
    y1 = bottom - (bottom-top) / ymax
    lines.append(f'<line x1="{lx0}" y1="{y1:.1f}" x2="{lx1}" y2="{y1:.1f}" stroke="#333" stroke-dasharray="5,4"/>')
    lines += [f'<line x1="{lx0}" y1="{top}" x2="{lx0}" y2="{bottom}" class="axis"/>',
              f'<line x1="{lx0}" y1="{bottom}" x2="{lx1}" y2="{bottom}" class="axis"/>',
              f'<text x="20" y="{(top+bottom)/2}" transform="rotate(-90 20 {(top+bottom)/2})" text-anchor="middle" class="tick">ratio (&gt;1 = slower under burst)</text>']
    centers = [165, 340, 515]
    for ri, route in enumerate(route_order):
        lines.append(f'<text x="{centers[ri]}" y="{bottom+22}" text-anchor="middle" class="small">{route_labels[ri]}</text>')
        for si, seed in enumerate(seeds):
            p = by_key.get((seed, route, "poisson_d8"), {})
            b = by_key.get((seed, route, "burst_d8"), {})
            jitter = (si - (len(seeds)-1)/2) * 7
            for metric, marker, shift in (("p50_offer_to_sink_ms", "circle", -17),
                                          ("p95_offer_to_sink_ms", "triangle", 17)):
                if not p.get(metric) or not b.get(metric):
                    continue
                val = float(b[metric]) / float(p[metric])
                x = centers[ri] + shift + jitter
                y = bottom - (bottom-top) * min(ymax, val) / ymax
                if marker == "circle":
                    lines.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{seed_colors[seed]}" stroke="#222" stroke-width=".7"/>')
                else:
                    lines.append(f'<path d="M{x:.1f},{y-6:.1f} l5.5,10 h-11 z" fill="{seed_colors[seed]}" stroke="#222" stroke-width=".7"/>')
    lines += ['<circle cx="210" cy="431" r="5" fill="#555"/><text x="221" y="435" class="small">P50</text>',
              '<path d="M278,425 l5.5,10 h-11 z" fill="#555"/><text x="290" y="435" class="small">P95 (screening only)</text>']

    # Right panel: per-seed median recovery across burst cycles, with 1-second bin error.
    rx0, rx1, rtop, rbottom = 690, 1080, 82, 385
    max_recovery = max([float(r["burst_cycle_recovery_median_sec"]) for r in summaries
                        if r["scenario"] == "burst_d8" and r["burst_cycle_recovery_median_sec"] is not None] + [4])
    rymax = max(4, math.ceil(max_recovery + 1))
    lines += ['<text x="885" y="58" text-anchor="middle" class="title">Recovery to same-time Poisson queue</text>',
              '<text x="885" y="75" text-anchor="middle" class="small">Two consecutive 1-second bins at/below baseline; error bars ±1 s</text>']
    for t in range(0, rymax + 1):
        y = rbottom - (rbottom-rtop) * t / rymax
        lines.append(f'<line x1="{rx0}" y1="{y:.1f}" x2="{rx1}" y2="{y:.1f}" class="grid"/>')
        lines.append(f'<text x="{rx0-8}" y="{y+4:.1f}" text-anchor="end" class="tick">{t}</text>')
    lines += [f'<line x1="{rx0}" y1="{rtop}" x2="{rx0}" y2="{rbottom}" class="axis"/>',
              f'<line x1="{rx0}" y1="{rbottom}" x2="{rx1}" y2="{rbottom}" class="axis"/>',
              f'<text x="650" y="{(rtop+rbottom)/2}" transform="rotate(-90 650 {(rtop+rbottom)/2})" text-anchor="middle" class="tick">seconds after burst tail</text>']
    rcenters = [760, 885, 1010]
    for ri, route in enumerate(route_order):
        lines.append(f'<text x="{rcenters[ri]}" y="{rbottom+22}" text-anchor="middle" class="small">{route_labels[ri]}</text>')
        for si, seed in enumerate(seeds):
            row = by_key.get((seed, route, "burst_d8"), {})
            val = row.get("burst_cycle_recovery_median_sec")
            if val is None:
                continue
            x = rcenters[ri] + (si - (len(seeds)-1)/2) * 14
            y = rbottom - (rbottom-rtop) * min(rymax, float(val)) / rymax
            ylo = rbottom - (rbottom-rtop) * max(0, float(val)-1) / rymax
            yhi = rbottom - (rbottom-rtop) * min(rymax, float(val)+1) / rymax
            lines.append(f'<line x1="{x}" y1="{ylo:.1f}" x2="{x}" y2="{yhi:.1f}" stroke="{seed_colors[seed]}" stroke-width="1.5"/>')
            lines.append(f'<circle cx="{x}" cy="{y:.1f}" r="5" fill="{seed_colors[seed]}" stroke="#222" stroke-width=".7"/>')
    for si, seed in enumerate(seeds):
        x = 742 + si*112
        lines.append(f'<circle cx="{x}" cy="458" r="5" fill="{seed_colors[seed]}"/><text x="{x+9}" y="462" class="small">seed {seed}</text>')
    lines.append('</svg>')
    (ROOT / "formal_burst_boundary.svg").write_text("\n".join(lines))

    route_result_lines = ["| 路线 | 突发/Poisson P50比值，中位数（范围） | 探索性P95比值，中位数（范围） | 下一次突发前恢复的周期数 |",
                          "|---|---:|---:|---:|"]
    for route in ROUTES:
        p50_ratios, p95_ratios = [], []
        before_next = with_next = 0
        for seed in seeds:
            p = next(r for r in summaries if r["seed"] == seed and r["route"] == route and r["scenario"] == "poisson_d8")
            b = next(r for r in summaries if r["seed"] == seed and r["route"] == route and r["scenario"] == "burst_d8")
            if p["p50_offer_to_sink_ms"] and b["p50_offer_to_sink_ms"]:
                p50_ratios.append(float(b["p50_offer_to_sink_ms"]) / float(p["p50_offer_to_sink_ms"]))
            if p["p95_offer_to_sink_ms"] and b["p95_offer_to_sink_ms"]:
                p95_ratios.append(float(b["p95_offer_to_sink_ms"]) / float(p["p95_offer_to_sink_ms"]))
            before_next += int(b["burst_cycles_before_next_n"] or 0)
            with_next += int(b["burst_cycles_with_next_n"] or 0)
        p50_label = f"{statistics.median(p50_ratios):.2f} ({min(p50_ratios):.2f}–{max(p50_ratios):.2f})" if p50_ratios else "n/a"
        p95_label = f"{statistics.median(p95_ratios):.2f} ({min(p95_ratios):.2f}–{max(p95_ratios):.2f})" if p95_ratios else "n/a"
        route_name = {"EVM independent Stage": "EVM独立Stage", "NEAR independent contracts": "NEAR独立合约",
                      "NEAR same-receiver control": "NEAR同接收器控制"}[route]
        route_result_lines.append(f"| {route_name} | {p50_label} | {p95_label} | {before_next}/{with_next} |")
    audit_failures = sum(int(r["failures"] or 0) for r in summaries)
    audit_pending = sum(int(r["audit_pending"] or 0) for r in summaries)
    report = [
        "# d8 突发到达多种子结果", "",
        "## Material Passport", "",
        "- ID：controller2_burst_d8_formal_20261028", "- 类型：实验结果", "- 状态：3 seed筛选完成；受并行资源限制，属于探索性结果。",
        "- 执行：远端 `/home/starshell/das_controller2_burst_20261028/run_formal_remote.sh`；seed 20261103 EVM补跑使用 `run_seed3_evm_retry_remote.sh`。",
        "- 输出：本目录的 `formal_burst_boundary.svg`、`formal_seed_summary.csv`、`formal_workflow_samples.csv` 与 `results/formal/` 原始记录。",
        "- 异常：seed3 EVM首次在下载Solidity编译器时遇到SSL错误，未产生负载；使用已有0.8.24缓存重跑成功。原批处理在最后一个路线结束后有收尾shell错误；各路线退出码与原始数据已记录，最终资源快照在核对进程结束后补记，原日志保留。", "",
        f"正式重复数：{len(seeds)} 个独立 seeds。每 seed、每场景测量窗约 60 个 workflow；P95 逐 seed 报告，P99 仅当该 seed 有至少 100 个有效测量样本时输出。Poisson 轨迹是在固定总数条件下生成的齐次 Poisson 次序统计量，不能解释为无条件 Poisson 计数过程。",
        "", "## 筛选结果", "", *route_result_lines, "",
        f"九个路线/seed数据单元的工作流失败数合计为 {audit_failures}，审计待定合计为 {audit_pending}。EVM seed 20261103 的首次启动失败发生在Solidity编译器缓存下载阶段，没有产生实验负载；随后用同版本缓存重跑成功。",
        "",
        "EVM独立Stage路线在三个seed中都出现更高的burst中位时延；两条NEAR路线的P50/P95与Poisson接近，但NEAR独立合约路线只有11/15个存在后续burst的周期在下一次burst前回到同秒Poisson基线，seed3出现连续周期叠加后的延后恢复。NEAR同接收器控制为15/15周期在下一次burst前恢复。故结果支持EVM路线对突发更敏感，并显示NEAR独立路线可能积累跨周期队列；它不构成通用DAS吞吐结论。P95每条件约54–64个样本，仅是筛选指标。",
        "",
        "不同 seeds 是重复单位；summary 保留每个 seed/路线/场景的P50/P95、queue、完成率和审计结果。正文不把跨 seeds 的 workflow 简单视作 iid 样本。NEAR接受率是调用发出率代理；EVM接受率是tx hash返回率。队列恢复逐观测秒与同路线、同elapsed-time的Poisson队列对比；需连续两个采样点不高于该匹配基线才记作恢复。每轮burst与周期统计按1秒采样精度报告，时间误差约±1秒，不给单次采样报伪精确小数。",
        "",
        "资源边界：driver与节点使用4个CPU软绑定；实验与控制器1在同一远端容器内有重叠运行，未实现硬隔离。将这批结果视为并行探索，不能与独占条件结果混合，也不能据此声称资源独占下的精确性能。",
        "", "## 图与数据", "", "- `formal_burst_boundary.svg`：左侧展示每 seed 的配对 burst/Poisson P50 与探索性 P95 比值；右侧展示按同秒 Poisson 队列基线恢复的每 seed 周期中位数，误差线为1秒采样分辨率。", "- `formal_seed_summary.csv`：每 seed/路线/场景结果与周期恢复计数。", "- `formal_workflow_samples.csv`：逐 workflow 数据。", "- `results/formal/`：远端原始结果、日志、资源样本和复现输入。", ""
    ]
    (ROOT / "formal_report.md").write_text("\n".join(report))
    print(f"Analyzed {len(seeds)} seeds across {len(ROUTES)} routes; schedule hashes and paired traces match.")


if __name__ == "__main__":
    main()
