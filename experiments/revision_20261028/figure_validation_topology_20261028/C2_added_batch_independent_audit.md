## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-10-28
- Verification Status: ANALYZED
- Version Label: topology_batch_validation_v1

## Validation Report

- **Source**: completed C2 topology/failure batch readback (`figure_validation_topology_20261028`)
- **Overall Confidence**: CAUTION
- **Scope**: local independent inspection of archived manifests, raw JSON, logs, hashes, and per-run state; this is not an independent rerun.
- **Batch window recorded**: started_utc=2026-10-28T12:10:17Z to finished_utc=2026-10-28T12:13:31Z; batch exit marker: `0`.
- **Primary outputs**: `topology5_per_repeat_audit.csv` (20 rows: 5 repeats × 2 shapes × 2 platforms) and `failure3_per_repeat_audit.csv` (6 rows: 3 repeats × 2 platforms).

### Sample counts and integrity

- Topology batch: NEAR raw/result records contain 5 fresh Sandbox repeats, each with both declared shapes; EVM raw/result records contain 10 profile runs paired over 5 distinct Anvil-ready timestamps. Failure raw/result records each contain 3 repeats per platform. All four platform runner exit markers are `topology5_near=0, topology5_evm=0, failure3_near=0, failure3_evm=0`. The single batch exit marker is also zero.
- Failure batch: 3 fresh NEAR Sandbox and 3 fresh Anvil EVM repeats, each configured for `fanout6_join` and failure at node 4.
- C2 readback manifest: 36/36 entries re-hash correctly; the topology formal-input list re-hashes 8/8. The failure bundle has no separate input-hash list; its preflight record has 9 `OK` checks. Topology preflight has 9 `OK` checks.
- Supplemental C1 provenance bundle: 22/22 listed files match `supplement_sha256.txt`.
- The four old baseline raw files (NEAR/EVM topology n=3; NEAR/EVM failure n=1) match their referenced SHA-256 values (4/4); they remain a separate cohort and are not pooled with the new 5/3 batch.

### Configuration and runtime evidence

- Both topology profiles use 8 stateful nodes and 16 operations per node. Chain: depth 8, width 1, 7 edges. Fan-out/join: depth 3, width 6, 12 edges. This contrasts graph topologies, not depth alone.
- NEAR raw records report Sandbox 2.13.4, protocol 86, and binary SHA-256 `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2`. Each topology repeat has a distinct Sandbox chain ID. No explicit NEAR block-interval override is recorded.
- EVM raw records report Anvil 1.8.6 and binary SHA-256 `3c28263c7989c78b70b56090a97832263b118201dd038a8662b77bd49a252cd5`; automine is enabled and no block-time override is recorded. Each topology repeat uses one fresh Anvil across its two shapes. Latencies below are local receipt wall times under automine, not periodic-block confirmation latency.
- Runtime values above come from run raw records. Batch manifests/logs and hashes establish archive integrity; neither establishes independent replication.

### Parent edges and completed state

- Manifest parent-edge sets were compared against every NEAR `edge_receipt` log and EVM `edge_input_event`. Match: 20/20 topology profile-runs. Each profile-run also records 8 node executions; sink node 7 exposes 1 input in chain and 6 in fan-out/join. EVM workflow records confirm stage storage, input values, operation counts, and NEAR checksum agreement; corresponding per-row checks are in the CSV.
- NEAR normal workflow `succeeded/failed` flags are successful for all 10 profile observations. These are sandbox workflow outcomes, not production behavior.
- NEAR receipt-tree client wall time (median [range], ms): chain 1249.762 [1246.030, 1253.332]; fan-out/join 751.919 [747.394, 753.727]. Sink first-queryable time (median [range], ms): chain 1253.888 [1252.030, 1298.379]; fan-out/join 627.260 [626.538, 630.473]. In fan-out/join, the sink is queryable before the entire receipt tree drains; do not substitute sink visibility for tree completion.
- NEAR native gas burnt (median Tgas): chain 22.202 [21.994, 22.206]; fan-out/join 37.419 [37.234, 37.422]. These are NEAR-native units.
- EVM receipt wall time (median [range], ms): chain 111.804 [111.584, 113.427]; fan-out/join 112.958 [112.268, 113.470]. Router gas (median): chain 1124775 [1124775, 1124775]; fan-out/join 1430761 [1430761, 1430761]. Complete-session gas excluding setup (median): chain 1249892 [1249892, 1249892]; fan-out/join 1555878 [1555854, 1555878]. Do not compare raw NEAR gas and EVM gas across virtual machines.

### Failure behavior

- NEAR: all 3 repeats record a failed descendant receipt at node 4, an unfinished multi-contract workflow, and no completed sink; 6 of 8 stage results (nodes 0,1,2,3,5,6) persist while nodes 4 and 7 are absent. The outer dispatch field remains `succeeded=true, failed=false`; this is not a business-workflow success. See each repeat's row in `failure3_per_repeat_audit.csv`.
- EVM: all 3 failure transactions at node 4 have status 0 and consume 558,052 gas; all eight stage result flags are false; router remains active with expected_workflows=1 but executed_workflows=0, workflow_checksum=0, and operation_count=0 after revert. Any status-1 cleanup transaction belongs to harness teardown, not business compensation. No compensation/recovery implementation or recovery time is demonstrated by these results.

### Warnings and fallacy scan

- Topology is confounded by both depth/width and edge count (7 versus 12), so latency/cost differences cannot be attributed to depth alone.
- Five successful repeats per topology and three failure repeats are descriptive samples; they do not establish broad platform-level distributions or production-chain performance.
- EVM automine receipt time and NEAR receipt-tree time measure different runtime behavior; compare within-platform and report observables explicitly.
- Failure outcomes are about this local single-validator runtime and harness semantics. They do not establish system-wide recovery, compensation, or cross-domain rollback policy.
- Old 3/1 baseline and new 5/3 batch are distinct cohorts and remain separate.

Fallacy scan coverage: 11/11 statistical/methodological classes checked for relevance: multiple testing (not a multi-hypothesis test), pseudoreplication (repeat/runtime unit preserved), selection/survivorship (all archived repeats counted; no rerun-based exclusion), Simpson's/ecological aggregation (platform/shape strata kept separate), base-rate neglect (no population rates claimed), regression to mean (no before/after causal interpretation), post-hoc subgrouping (only predeclared shapes/failure node reported), measurement mismatch (timing definitions separated), correlation/causation (no causal depth claim), and reverse causality/omitted confounding (edge count/width/config recognized). No inferential p-values or confidence intervals are claimed.

### Reproducibility

- **Method**: not run (read-only archive audit; prior execution is only inspected)
- **Verdict**: CANNOT_VERIFY
- **Reason**: recorded hashes and successful exits support integrity and completion of the archived batch, but this audit did not independently rerun the experiments.

### Claims supported by this archive

Within the recorded local runtimes, all 5 declared topology repeats per platform completed the chain and fan-out/join graphs with parent edges and sink states matching their manifests; the observed median completion, sink visibility, and gas summaries are descriptive. Under the recorded node-4 injection, NEAR leaves 6/8 partial stage states while EVM transaction reversion leaves none. Do not extend this to production-chain latency/capacity or compensation behavior.
