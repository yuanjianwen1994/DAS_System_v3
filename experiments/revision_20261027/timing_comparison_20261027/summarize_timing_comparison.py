"""Validate retained remote timing results and derive session/summary CSV files."""
import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def near_observations():
    data = json.loads((ROOT / "near_results.json").read_text())
    assert not data.get("error") and len(data["completed_repeats"]) == 3
    rows = []
    for repeat in data["completed_repeats"]:
        assert len(repeat["profiles"]) == 3
        for profile in repeat["profiles"]:
            depth = profile["profile"]["node_count"]
            assert depth in (2, 4, 8) and len(profile["sessions"]) == 2
            for index, session in enumerate(profile["sessions"], 1):
                run = session["multi_contract"]["runs"][0]
                assert run["succeeded"] and not run["failed"]
                assert all(x["all_nodes_present"] and x["nodes_match_same_receiver"] for x in session["same_nodes_and_checksum"])
                timing = run["sink_visibility"]
                assert timing and timing["first_sink_queryable_at"] and timing["api_to_sink_queryable_ms"] > 0
                rows.append({"platform": "NEAR Sandbox", "repeat": repeat["repeat"], "depth": depth,
                    "session_index": index, "session_id": session["session_id"], "latency_ms": timing["api_to_sink_queryable_ms"]})
    return rows


def evm_observations():
    data = json.loads((ROOT / "evm_results.json").read_text())
    assert not data.get("error") and len(data["completed_repeats"]) == 3
    rows = []
    for repeat in data["completed_repeats"]:
        assert len(repeat["profiles"]) == 3
        for profile in repeat["profiles"]:
            depth = profile["profile"]["node_count"]
            assert depth in (2, 4, 8) and len(profile["independent_stage_sessions"]) == 2
            for index, session in enumerate(profile["independent_stage_sessions"], 1):
                run = session["router_execution"]
                assert run["status"] == 1 and session["checksums_match_near_and_stage_inputs_valid"]
                timing = run["sink_visibility"]
                assert timing and timing["first_sink_queryable_at"] and timing["api_to_sink_queryable_ms"] > 0
                rows.append({"platform": "EVM / Anvil", "repeat": repeat["repeat"], "depth": depth,
                    "session_index": index, "session_id": session["session_id"], "latency_ms": timing["api_to_sink_queryable_ms"]})
    return rows


rows = near_observations() + evm_observations()
assert len(rows) == 36
for platform in ("NEAR Sandbox", "EVM / Anvil"):
    for depth in (2, 4, 8):
        assert sum(r["platform"] == platform and r["depth"] == depth for r in rows) == 6

with (ROOT / "per_session_timing.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["platform", "repeat", "depth", "session_index", "session_id", "latency_ms"])
    writer.writeheader()
    writer.writerows(rows)

summary = []
for platform in ("NEAR Sandbox", "EVM / Anvil"):
    for depth in (2, 4, 8):
        values = [r["latency_ms"] for r in rows if r["platform"] == platform and r["depth"] == depth]
        summary.append({"platform": platform, "depth": depth, "n_successful_sessions": len(values),
            "mean_ms": statistics.mean(values), "median_ms": statistics.median(values),
            "sample_sd_ms": statistics.stdev(values), "min_ms": min(values), "max_ms": max(values)})
with (ROOT / "timing_summary.csv").open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(summary[0]))
    writer.writeheader()
    writer.writerows(summary)
print(json.dumps(summary, indent=2))
