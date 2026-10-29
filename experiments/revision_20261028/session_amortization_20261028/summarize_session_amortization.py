#!/usr/bin/env python3
"""Summarize the remote same-EVM session amortization experiment."""
import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORMAL = ROOT / "formal_12s_r1_r3" / "session_amortization_raw.json"
PILOT = ROOT / "pilot_1s_r1" / "session_amortization_raw.json"


def median(rows, key):
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return statistics.median(values) if values else None


formal = json.loads(FORMAL.read_text())
pilot = json.loads(PILOT.read_text())
records = formal["sessions"]
groups = []
for arm in ("same_domain", "migrated_unbatched", "migrated_batch"):
    for theta in (1, 2, 4, 8):
        rows = [r for r in records if r["arm"] == arm and int(r["theta"]) == theta]
        completed = [r for r in rows if r["status"] == "completed"]
        bindings = sum(1 for r in completed for binding in r["receipt_bindings"] if binding["binding_verified"])
        expected_bindings = sum(len(r["receipt_bindings"]) for r in completed)
        group = {
            "arm": arm, "theta": theta, "planned_repeats": len(rows),
            "completed": len(completed), "failed": sum(r["status"] == "failed" for r in rows),
            "incomplete": sum(bool(r["incomplete"]) for r in rows),
            "median_internal_business_seconds": median(completed, "internal_business_seconds"),
            "min_internal_business_seconds": min((r["internal_business_seconds"] for r in completed), default=None),
            "max_internal_business_seconds": max((r["internal_business_seconds"] for r in completed), default=None),
            "median_migration_in_seconds": median(completed, "entry_seconds"),
            "median_migration_out_seconds": median(completed, "exit_seconds"),
            "median_end_to_end_seconds": median(completed, "end_to_end_seconds"),
            "median_end_to_end_per_operation_seconds": median(completed, "end_to_end_seconds") / theta if completed else None,
            "min_end_to_end_seconds": min((r["end_to_end_seconds"] for r in completed), default=None),
            "max_end_to_end_seconds": max((r["end_to_end_seconds"] for r in completed), default=None),
            "receipt_bindings_passed": bindings, "receipt_bindings_expected": expected_bindings,
            "all_terminal_balances_valid": all(
                r.get("source_final_balance") == 1 and r.get("destination_final_balance") == 0
                and r.get("batch_executor_final_balance") == 0 and r.get("pending_final_balance") == 0
                and r.get("business_spent") == theta and r.get("funded") == theta + 1
                and r.get("conservation_invariant") is True for r in completed
            ) and bool(completed),
        }
        groups.append(group)

columns = list(groups[0]) if groups else []
with (ROOT / "session_amortization_formal_summary.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(groups)

with (ROOT / "session_amortization_sessions.csv").open("w", newline="") as f:
    columns = ["repeat", "theta", "arm", "status", "failure", "incomplete", "internal_business_seconds",
               "entry_seconds", "exit_seconds", "end_to_end_seconds", "source_final_balance",
               "destination_final_balance", "batch_executor_final_balance", "pending_final_balance",
               "business_spent", "funded", "conservation_invariant", "receipt_binding_count"]
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    for r in records:
        writer.writerow({**{k: r.get(k) for k in columns if k != "receipt_binding_count"},
                         "receipt_binding_count": sum(bool(b.get("binding_verified")) for b in r["receipt_bindings"])})

with (ROOT / "session_amortization_report.md").open("w") as f:
    f.write("# θ session migration amortization — same-EVM control\n\n")
    f.write("The primary results below use three fresh-Anvil repeats at a 12-second block interval. The unchanged repository `MigrationHarness.sol` runs on one EVM with two logical domain labels and a trusted local ECDSA attestor. This is a state-machine/mechanism control, not a cross-VM bridge or a production attestation path. `same_domain` is the no-migration best-case control; both migrated arms move in, execute θ dependent balance spends, and move the remainder back out. The batch arm compresses the θ spends into one helper-contract transaction.\n\n")
    f.write("| Arm | θ | Completed / planned | Internal business median (s) | Entry median (s) | Exit median (s) | Full journey median (s) | Full journey / operation (s) | Receipt binding | Final accounting | Failures | Incomplete |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|\n")
    for r in groups:
        f.write(f"| {r['arm']} | {r['theta']} | {r['completed']}/{r['planned_repeats']} | {r['median_internal_business_seconds']:.3f} | {r['median_migration_in_seconds']:.3f} | {r['median_migration_out_seconds']:.3f} | {r['median_end_to_end_seconds']:.3f} | {r['median_end_to_end_per_operation_seconds']:.3f} | {r['receipt_bindings_passed']}/{r['receipt_bindings_expected']} | {'pass' if r['all_terminal_balances_valid'] else 'FAIL'} | {r['failed']} | {r['incomplete']} |\n")
    f.write("\n## Interpretation\n\nAcross θ=1,2,4,8, the same-domain full journey is about one 12-second block. Both migrated paths take about five block intervals: source-side migration-in start and settlement, one business-execution block, then migration-out start and settlement. Their absolute full-journey time therefore does not fall as θ grows in this same-EVM model. The fixed migration overhead per business action declines: the migrated full journey is about 60/θ seconds per action (approximately 60, 30, 15, and 7.5 seconds), while same-domain is about 12/θ (12, 6, 3, and 1.5 seconds). No break-even appears because this controlled VM gives destination execution no speed or locality advantage. This is an expected mechanism control, not evidence against a cross-domain benefit under a separately justified execution environment.\n\n")
    f.write("At θ=8, submitting eight individual dependent calls and batching them each occupies one interval in this Anvil configuration; the measured internal phase is therefore near one block interval in both migrated arms. This does not establish production batching equivalence.\n\n")
    f.write("## Pilot (separate; excluded from formal medians)\n\nThe 1-second pilot completed all 12 sessions (three arms × four θ values) with no failed/incomplete sessions and valid final accounting. It is an operational pilot only; its times are not pooled with the 12-second formal matrix.\n\n")
    f.write("| Arm | θ | Internal (s) | Full journey (s) | Binding checks |\n|---|---:|---:|---:|---:|\n")
    for r in pilot["sessions"]:
        f.write(f"| {r['arm']} | {r['theta']} | {r.get('internal_business_seconds', float('nan')):.3f} | {r.get('end_to_end_seconds', float('nan')):.3f} | {sum(bool(b.get('binding_verified')) for b in r['receipt_bindings'])}/{len(r['receipt_bindings'])} |\n")
    f.write("\n## Reproducibility and safeguards\n\nThe raw JSON contains per-session timings, gas totals, source and destination domains, both receipt IDs/envelopes/nonces, transaction gas, terminal balance values, status/failure fields, source hashes, Anvil/solc/Web3 versions, and deployment addresses. Each formal repeat uses a fresh Anvil process, ledger, and batch executor. Migration arms must end with source spendable balance 1, both destination ledgers 0, pending 0, spent θ, funded θ+1, and a passing conservation invariant. Every migrated session verifies both pending receipt envelopes, event payload binding, settled flags, and settlement events.\n")


def svg_chart():
    width, height = 1120, 700
    left, right, top, bottom = 100, 1070, 95, 555
    ymax = 70
    x_positions = {1: 160, 2: 450, 4: 740, 8: 1030}
    y = lambda value: bottom - float(value) / ymax * (bottom - top)
    specs = [
        ("same_domain", "end_to_end_seconds", "Same domain — full session", "#1d4ed8", "solid"),
        ("migrated_unbatched", "internal_business_seconds", "Unbatched — business phase", "#ea580c", "dash"),
        ("migrated_unbatched", "end_to_end_seconds", "Unbatched — full in/out journey", "#ea580c", "solid"),
        ("migrated_batch", "internal_business_seconds", "Batch — business phase", "#15803d", "dash"),
        ("migrated_batch", "end_to_end_seconds", "Batch — full in/out journey", "#15803d", "solid"),
    ]
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
           '<rect width="100%" height="100%" fill="white"/>',
           '<text x="560" y="34" text-anchor="middle" font-family="sans-serif" font-size="22">Session timing at a 12-second block interval</text>',
           '<text x="560" y="62" text-anchor="middle" font-family="sans-serif" font-size="15">Median across three fresh-Anvil repeats; same-EVM logical-domain mechanism control</text>']
    for tick in range(0, 71, 10):
        yy = y(tick)
        svg.append(f'<line x1="{left}" y1="{yy}" x2="{right}" y2="{yy}" stroke="#ddd"/><text x="{left-14}" y="{yy+5}" text-anchor="end" font-family="sans-serif" font-size="14">{tick}</text>')
    svg += [f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#333"/>', f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#333"/>']
    for theta, xx in x_positions.items():
        svg.append(f'<text x="{xx}" y="{bottom+26}" text-anchor="middle" font-family="sans-serif" font-size="16">θ={theta}</text>')
    for idx, (arm, metric, label, color, style) in enumerate(specs):
        points = []
        for theta in x_positions:
            values = [r[metric] for r in records if r["arm"] == arm and r["theta"] == theta and r["status"] == "completed"]
            med = statistics.median(values)
            xx, yy = x_positions[theta], y(med)
            points.append((xx, yy))
            lo, hi = y(max(values)), y(min(values))
            dash = ' stroke-dasharray="7 5"' if style == "dash" else ""
            svg.append(f'<line x1="{xx}" y1="{lo}" x2="{xx}" y2="{hi}" stroke="{color}" stroke-width="2"/>')
            svg.append(f'<circle cx="{xx}" cy="{yy}" r="5" fill="{color}"/>')
        line_dash = ' stroke-dasharray="7 5"' if style == "dash" else ""
        for (x1, y1), (x2, y2) in zip(points, points[1:]):
            svg.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2.5"{line_dash}/>')
        lx = 125 + (idx % 3) * 330
        ly = 610 + (idx // 3) * 28
        svg.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+32}" y2="{ly}" stroke="{color}" stroke-width="3"{line_dash}/><text x="{lx+40}" y="{ly+5}" font-family="sans-serif" font-size="13">{label}</text>')
    svg.append(f'<text x="33" y="325" transform="rotate(-90 33 325)" text-anchor="middle" font-family="sans-serif" font-size="16">Seconds per session</text>')
    svg.append('</svg>')
    (ROOT / "session_amortization_seconds.svg").write_text("\n".join(svg))


svg_chart()
print(f"wrote {len(groups)} formal groups, {len(records)} formal sessions")
