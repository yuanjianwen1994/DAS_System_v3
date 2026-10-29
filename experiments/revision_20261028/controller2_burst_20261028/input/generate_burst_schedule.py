#!/usr/bin/env python3
"""Create equal-count paired Poisson-conditioned and periodic burst trajectories."""
import argparse
import hashlib
import json
import random
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--warmup-sec", type=int, default=10)
    ap.add_argument("--measurement-sec", type=int, default=20)
    ap.add_argument("--cycle-sec", type=int, default=10)
    ap.add_argument("--burst-duration-sec", type=int, default=2)
    ap.add_argument("--burst-rate", type=int, default=5)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    if args.measurement_sec % args.cycle_sec or args.warmup_sec % args.cycle_sec:
        raise SystemExit("pilot duration must contain whole burst cycles")
    if args.burst_duration_sec * args.burst_rate != args.cycle_sec:
        raise SystemExit("burst settings must offer one workflow/s on average over each cycle")

    phase_rng = random.Random(args.seed ^ 0x5EEDC0DE)
    phase = phase_rng.random()
    rng = random.Random(args.seed)
    duration = args.warmup_sec + args.measurement_sec
    count = duration  # Mean offered load is 1 workflow/s in both paired traces.
    poisson = sorted(phase + rng.random() * duration for _ in range(count))
    burst = []
    for cycle_start in range(0, duration, args.cycle_sec):
        for i in range(args.burst_duration_sec * args.burst_rate):
            burst.append(phase + cycle_start + i / args.burst_rate)

    common = {
        "scenario_index": 0,
        "rates_per_sec": [1.0],
        "warmup_sec": args.warmup_sec,
        "measurement_sec": args.measurement_sec,
        "phase_random_offset_sec": phase,
        "arrival_seed": args.seed,
    }
    data = {
        "schema_version": 1,
        "experiment": "paired-d8-poisson-conditioned-vs-periodic-burst-pilot",
        "profile_id": "dag-07-chain-d8-w1-theta1-nu1",
        "node_count": 8,
        "operations_per_node": 16,
        "seed": args.seed,
        "block_period_sec": 1.0,
        "warmup_sec": args.warmup_sec,
        "measurement_sec": args.measurement_sec,
        "phase_random_offset_sec": phase,
        "rates_per_sec": [1.0],
        "resource_policy": {
            "sender_accounts_per_platform": 64,
            "per_sender_submission_serialization": True,
            "stop_when": "fixed schedule ends; drain all offered workflows; preserve failures, pending rows, tails, and block spans",
        },
        "scenarios": {
            "poisson_d8": {
                **common,
                "arrival_rule": "homogeneous_poisson_conditioned_on_equal_fixed_count",
                "arrival_offsets_sec_by_rate": {"1.0": poisson},
            },
            "burst_d8": {
                **common,
                "arrival_rule": f"periodic_{args.cycle_sec}s_cycle_{args.burst_duration_sec}s_at_{args.burst_rate}/s_burst",
                "arrival_offsets_sec_by_rate": {"1.0": burst},
            },
        },
    }
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    data["schedule_content_sha256"] = hashlib.sha256(canonical).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "seed": args.seed, "phase_offset_sec": phase,
                      "duration_sec": duration, "count_each": count,
                      "poisson_first_last": [poisson[0], poisson[-1]],
                      "burst_first_last": [burst[0], burst[-1]],
                      "burst_count": len(burst)}, indent=2))


if __name__ == "__main__":
    main()
