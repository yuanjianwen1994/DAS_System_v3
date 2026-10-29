#!/usr/bin/env python3
"""Generate topology-first, business-domain-neutral NEAR/DAS workflow profiles."""
from __future__ import annotations

import json
from pathlib import Path


def graph_for(kind: str, depth: int, width: int) -> tuple[list[list[int]], list[list[int]]]:
    if kind == "chain":
        parents = [[]] + [[node - 1] for node in range(1, depth)]
    elif kind in {"fork_join", "mixed"}:
        if depth < 3 or width < 2:
            raise ValueError("fork/join graphs require depth >= 3 and width >= 2")
        layers = [[0]]
        next_node = 1
        for _ in range(depth - 2):
            layers.append(list(range(next_node, next_node + width)))
            next_node += width
        layers.append([next_node])
        parents = [[] for _ in range(next_node + 1)]
        for previous, current in zip(layers, layers[1:]):
            for i, child in enumerate(current):
                if kind == "fork_join" or len(previous) == 1 or len(current) == 1:
                    parents[child] = list(previous)
                else:
                    parents[child] = sorted({previous[i % len(previous)], previous[(i + 1) % len(previous)]})
    else:
        raise ValueError(f"unknown DAG shape: {kind}")

    successors = [[] for _ in parents]
    for child, parent_ids in enumerate(parents):
        for parent in parent_ids:
            successors[parent].append(child)
    return parents, successors


def main() -> None:
    sessions_per_profile = 2
    fresh_repeats = 3
    operations_per_node = 16
    specs: list[tuple[str, int, int, int, int]] = []

    # Primary two-factor screen: dependency depth d x workflows per session theta.
    for depth in (2, 4, 8):
        for theta in (1, 4, 8):
            specs.append(("chain", depth, 1, theta, 1))

    # Boundary/shape probes: width w changes while d and theta are held fixed.
    for kind in ("fork_join", "mixed"):
        for width in (2, 4):
            specs.append((kind, 4, width, 4, 1))

    # Concurrency boundary: same d/theta as the primary reference, larger nu.
    specs.append(("chain", 4, 1, 4, 4))

    profiles = []
    for index, (kind, depth, width, theta, concurrency) in enumerate(specs, 1):
        parents, successors = graph_for(kind, depth, width)
        edge_count = sum(map(len, parents))
        profiles.append({
            "profile_id": f"dag-{index:02d}-{kind}-d{depth}-w{width}-theta{theta}-nu{concurrency}",
            "shape": kind,
            "dependency_depth_d": depth,
            "parallel_width_w": width,
            "cross_execution_domain_edge_fraction_p": 0.0,
            "cross_contract_edge_fraction_near_multicontract": 1.0,
            "concurrent_workflows_nu": concurrency,
            "workflows_per_session_theta": theta,
            "sessions_per_profile": sessions_per_profile,
            "fresh_sandbox_repeats": fresh_repeats,
            "operations_per_node": operations_per_node,
            "node_count": len(parents),
            "edge_count": edge_count,
            "parents": parents,
            "successors": successors,
            "execution_domain_by_node": [0] * len(parents),
        })

    manifest = {
        "experiment": "parameterized stateful multi-contract DAG workflows",
        "schema_version": 1,
        "runtime_scope": "nearcore Sandbox; single shard; cross-contract edges are not cross-execution-domain edges",
        "repeat_unit": "each replicate starts a fresh nearcore Sandbox process and chain",
        "timing_unit": "receipt dependency and finalized block-height spans; wall time is diagnostic only",
        "business_domain_terms": [],
        "profiles": profiles,
    }
    out = Path(__file__).with_name("profile_manifest.json")
    out.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {len(profiles)} profiles to {out}")


if __name__ == "__main__":
    main()
