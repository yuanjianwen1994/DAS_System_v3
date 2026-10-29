#!/usr/bin/env python3
"""Summarize the three independent remote 1-second d8 session-DAG pilot repeats."""
import csv, json, statistics
from pathlib import Path

ROOT=Path(__file__).resolve().parent
FILES=[ROOT/f"formal_1s_r{i}/session_dag_raw.json" for i in (1,2,3)]
datasets=[json.loads(p.read_text()) for p in FILES]
if any(len(d.get("runs",[])) != 8 for d in datasets):
    raise SystemExit("each repeat must contain all 8 arm/theta cells")
hash_sets=[d.get("source_hashes") for d in datasets]
if any(h != hash_sets[0] for h in hash_sets[1:]):
    raise SystemExit("source hashes differ across repeats")
runs=[r for d in datasets for r in d["runs"]]
keys=sorted({(r["arm"],r["theta"]) for r in runs},key=lambda x:(x[0],x[1]))
summary=[]
for arm,theta in keys:
    rows=[r for r in runs if r["arm"]==arm and r["theta"]==theta]
    migrated=arm=="migrated_dag"
    def med(field): return statistics.median(float(r.get(field,0)) for r in rows)
    summary.append({
        "arm":arm,"theta":theta,"depth":8,"repeats":len(rows),
        "completed":sum(r["status"]=="completed" for r in rows),
        "failed":sum(bool(r["failed"]) for r in rows),"incomplete":sum(bool(r["incomplete"]) for r in rows),
        "median_internal_dag_seconds":med("internal_business_seconds"),
        "median_migration_in_seconds":med("entry_seconds"),"median_migration_out_seconds":med("exit_seconds"),
        "median_full_journey_seconds":med("journey_seconds"),
        "median_full_journey_per_workflow_seconds":med("journey_seconds")/theta,
        "median_dag_execution_tx_gas":statistics.median(int(r["internal_tx_gas"]) for r in rows),
        "median_dag_gas_per_workflow":statistics.median(int(r["internal_tx_gas"]) for r in rows)/theta,
        "min_full_journey_seconds":min(float(r["journey_seconds"]) for r in rows),
        "max_full_journey_seconds":max(float(r["journey_seconds"]) for r in rows),
        "receipt_bindings_passed":sum(int(r["receipt_bindings_passed"]) for r in rows),
        "receipt_bindings_expected":sum(int(r["receipt_bindings_expected"]) for r in rows),
        "node_results_audited":sum(len(r["node_audits"]) for r in rows),
        "predecessor_bindings_passed":sum(int(x["bound"]) for r in rows for x in r["node_audits"] if x["predecessor_value"] is not None),
        "terminal_accounting_all_pass":all(bool(r["terminal_accounting_valid"]) for r in rows),
    })

with (ROOT/"session_dag_migration_summary.csv").open("w",newline="") as f:
    writer=csv.DictWriter(f,fieldnames=list(summary[0])); writer.writeheader(); writer.writerows(summary)

lines=["# 实际 d8 Stage DAG + session 迁移控制 pilot 汇总", "",
"三次独立远端重复，均为 1 秒 Anvil 出块设置；这是受控 pilot，不是公链时延或原生跨 VM 端到端结果。每轮使用新 Anvil 与新部署账本。两个逻辑域在同一 EVM 上，迁移由本地可信 ECDSA 测试签名者授权。θ 个独立 workflow 可同一交易预提交；每个 workflow 内部的 8 个 Stage 按前驱 checksum 依赖顺序执行。", "",
"每个 session 按 8θ+1 注资，每个 Stage 扣 1 个余额单位，最终保留 1。迁移组必须完成迁入 receipt settlement 才能进入 session；必须执行完全部 θ 个 workflow、审计所有 DAG sink 并关闭 session 后才能迁出最终余额。", "",
"| Arm | θ workflows/session | Repeats complete | Internal DAG median (s) | DAG tx gas median | Entry median (s) | Exit median (s) | Full journey median (s) | Journey / workflow (s) | Receipt binding checks | DAG nodes audited | Predecessor bindings | Terminal accounting | Fail / incomplete |",
"|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|"]
for r in summary:
    lines.append(f"| {r['arm']} | {r['theta']} | {r['completed']}/{r['repeats']} | {r['median_internal_dag_seconds']:.3f} | {r['median_dag_execution_tx_gas']:,} | {r['median_migration_in_seconds']:.3f} | {r['median_migration_out_seconds']:.3f} | {r['median_full_journey_seconds']:.3f} | {r['median_full_journey_per_workflow_seconds']:.3f} | {r['receipt_bindings_passed']}/{r['receipt_bindings_expected']} | {r['node_results_audited']} | {r['predecessor_bindings_passed']} | {'pass' if r['terminal_accounting_all_pass'] else 'FAIL'} | {r['failed']} / {r['incomplete']} |")

same=[r for r in summary if r["arm"]=="same_domain_dag"]
mig=[r for r in summary if r["arm"]=="migrated_dag"]
lines += ["", "## 读数", "",
f"24/24 sessions completed, failed=0, incomplete=0. Migrated sessions passed {sum(r['receipt_bindings_passed'] for r in mig)}/{sum(r['receipt_bindings_expected'] for r in mig)} receipt-envelope/settlement bindings. The 3 repeats audited {sum(len(r['node_audits']) for r in runs)} Stage results and {sum(int(x['bound']) for r in runs for x in r['node_audits'] if x['predecessor_value'] is not None)} stored predecessor edges; terminal balance conservation passed for every session.", "",
"The direct same-domain best-case and migrated control both execute the same d8 chain inside one DAG transaction per session. At this 1-second local block setting, internal DAG phase remains about one block interval as θ grows, because θ independent workflows are submitted together. The DAG transaction gas rises from about 1.117M at θ=1 to 7.740M at θ=8 (about 6.93×), even though wall-clock time is near the one-second block-inclusion floor. The migration route adds entry and exit receipt transactions; its full journey therefore stays near seven one-second blocks while the direct route stays near four. Per-workflow journey time falls with θ through amortization of this fixed receipt overhead. This same-EVM control gives the destination no locality or execution advantage, so it cannot identify a migration break-even point.", "",
"Do not interpret θ=8 as an 8× performance gain: θ counts independent d8 workflows sharing a session, not a throughput speedup. This result does not test cross-contract execution across VMs or a native bridge. The separate NEAR internal-DAG results must remain a separate runtime arm; these values must not be concatenated into a synthetic native end-to-end result.", "",
"## Reproducibility", "",
"Source SHA-256 values are recorded in each raw JSON. Runtime/compiler versions and chain IDs are retained per repeat. `formal_1s_r1/` through `formal_1s_r3/` contain the unmodified per-session raw records, including transaction IDs, receipt IDs/envelopes, checksums, predecessor values, balances, and timing fields.", ""]
(ROOT/"session_dag_migration_report.md").write_text("\n".join(lines))
print(f"wrote summary for {len(summary)} arm/theta cells from {len(runs)} sessions")

# d20 is a separate one-repeat pilot. Keep it out of the d8 three-repeat summaries.
d20_path=ROOT/"pilot_d20_raw.json"
if d20_path.exists():
    d20=json.loads(d20_path.read_text())
    d20_rows=d20["runs"]
    d20_summary=[]
    for arm,theta in sorted({(r["arm"],r["theta"]) for r in d20_rows},key=lambda x:(x[0],x[1])):
        rs=[r for r in d20_rows if r["arm"]==arm and r["theta"]==theta]
        r=rs[0]
        d20_summary.append({
            "arm":arm,"theta":theta,"depth":20,"repeat":d20["repeat"],
            "status":r["status"],"internal_dag_seconds":r.get("internal_business_seconds"),
            "entry_seconds":r.get("entry_seconds"),"exit_seconds":r.get("exit_seconds"),
            "full_journey_seconds":r.get("journey_seconds"),"dag_execution_tx_gas":r.get("internal_tx_gas"),
            "dag_nodes_audited":len(r.get("node_audits",[])),"predecessor_bindings_passed":sum(int(x["bound"]) for x in r.get("node_audits",[]) if x["predecessor_value"] is not None),
            "receipt_bindings_passed":r.get("receipt_bindings_passed"),"receipt_bindings_expected":r.get("receipt_bindings_expected"),
            "terminal_accounting_valid":r.get("terminal_accounting_valid"),"failed":r.get("failed"),"incomplete":r.get("incomplete"),
        })
    with (ROOT/"d20_pilot_summary.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(d20_summary[0]));w.writeheader();w.writerows(d20_summary)
    lines=(ROOT/"session_dag_migration_report.md").read_text().splitlines()
    lines += ["", "## Supplementary d20 pilot — separate from the d8 three-repeat matrix", "",
        "One fresh-Anvil repeat at θ=1/2/4/8; descriptive pilot only and not pooled with the d8 results.", "",
        "| Arm | θ workflows/session | Internal DAG (s) | DAG tx gas | Full journey (s) | Stage results audited | Predecessor bindings | Receipt bindings | Final accounting | Failed / incomplete |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|"]
    for r in d20_summary:
        lines.append(f"| {r['arm']} | {r['theta']} | {r['internal_dag_seconds']:.3f} | {r['dag_execution_tx_gas']:,} | {r['full_journey_seconds']:.3f} | {r['dag_nodes_audited']} | {r['predecessor_bindings_passed']} | {r['receipt_bindings_passed']}/{r['receipt_bindings_expected']} | {'pass' if r['terminal_accounting_valid'] else 'FAIL'} | {int(r['failed'])} / {int(r['incomplete'])} |")
    lines += ["", "### Matched-depth descriptive comparison", "",
        "| θ | d8 same-domain journey median (s) | d20 same-domain (s, 1 run) | d20/d8 same-domain DAG gas | d8 migrated journey median (s) | d20 migrated (s, 1 run) | d20/d8 migrated DAG gas |",
        "|---:|---:|---:|---:|---:|---:|---:|"]
    for theta in (1,2,4,8):
        d8same=next(r for r in summary if r["arm"]=="same_domain_dag" and r["theta"]==theta)
        d8mig=next(r for r in summary if r["arm"]=="migrated_dag" and r["theta"]==theta)
        d20same=next(r for r in d20_summary if r["arm"]=="same_domain_dag" and r["theta"]==theta)
        d20mig=next(r for r in d20_summary if r["arm"]=="migrated_dag" and r["theta"]==theta)
        lines.append(f"| {theta} | {d8same['median_full_journey_seconds']:.3f} | {d20same['full_journey_seconds']:.3f} | {d20same['dag_execution_tx_gas']/d8same['median_dag_execution_tx_gas']:.2f}× | {d8mig['median_full_journey_seconds']:.3f} | {d20mig['full_journey_seconds']:.3f} | {d20mig['dag_execution_tx_gas']/d8mig['median_dag_execution_tx_gas']:.2f}× |")
    all_d20_ok=all(r["status"]=="completed" and r["terminal_accounting_valid"] and not r["failed"] and not r["incomplete"] for r in d20_rows)
    lines += ["",f"The d20 pilot completed {sum(r['status']=='completed' for r in d20_rows)}/{len(d20_rows)} sessions; {sum(len(r['node_audits']) for r in d20_rows)} Stage results and {sum(int(x['bound']) for r in d20_rows for x in r['node_audits'] if x['predecessor_value'] is not None)} predecessor edges were audited; all receipt binding and final-accounting checks passed: {'yes' if all_d20_ok else 'no'}.", "",
        "At d20, θ=8 represents 160 dependent Stage executions in the session, not a 20× speedup. For θ=1→8, DAG execution gas rises from 2.851M to 20.566M (7.21×), and the measured full session grows from about 4.03 to 6.05 seconds in the same-domain arm and 6.98 to 9.03 seconds in the migrated arm. At matched θ, moving from d8 to d20 increases Stage count by 2.5× and DAG transaction gas by about 2.55× at θ=1 and 2.66× at θ=8. The full-session times do not increase by 20× in this controlled EVM run. Since d20 has only one repeat, treat this as a pilot observation rather than a general scaling law.", "",
        "The d20 row values are one repeat each, not medians. They quantify a same-EVM trusted-attestor composition. They do not measure a native cross-VM migration or establish production scalability.", "",
        "Raw input: `pilot_d20_raw.json`; supplemental rows: `d20_pilot_summary.csv`.", ""]
    (ROOT/"session_dag_migration_report.md").write_text("\n".join(lines))
    print(f"wrote separate d20 one-repeat supplement with {len(d20_rows)} sessions")
