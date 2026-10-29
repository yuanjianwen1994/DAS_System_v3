# Parameterized Stateful DAG Experiment

Run date: 2026-10-27. Status: complete. Each profile has three fresh NEAR Sandbox repeats, three fused-control Anvil repeats, and three independent-stage Anvil repeats. The EVM runs were executed on a remote Linux x86_64 host (Anvil 1.8.6; solc 0.8.24).

## Question and design

The experiment measures stateful contract workflows as generic dependency graphs. Each node reads the stored values of its predecessor nodes, performs the same configured deterministic operation, writes a checksum, and passes that value to its successors. Every compared route receives the same workflow IDs, inputs, dependency graph, and operation count per node. The checksums match at every node.

The profile matrix varies dependency depth `d`, workflows per session `theta`, graph width `w`, graph shape, and one workflow-concurrency boundary `nu`. It includes chain, fork-join, and mixed DAGs. Each profile has two sessions per fresh chain, three fresh chains per runtime and EVM implementation, and 16 stateful compute iterations per node. The full parameter table is in [profile_manifest.json](profile_manifest.json); the computed per-profile metrics are in [dag_summary.csv](dag_summary.csv) and [dag_summary.json](dag_summary.json).

The graph families are shown in [dag_shapes.png](dag_shapes.png). The execution comparison and parameter trends are shown in [dag_execution_comparison.png](dag_execution_comparison.png) and [dag_parameter_trends.png](dag_parameter_trends.png). The two EVM composition controls are shown in [dag_evm_composition_controls.png](dag_evm_composition_controls.png). NEAR's multi-contract route uses actual Promise dependencies. Two EVM implementations are measured: a fused single-contract control that runs the topologically ordered graph in one local execution transaction, and an independent-stage implementation in which a Router calls a separate Stage contract for every node through external EVM `CALL`s. The best NEAR same-receiver control batches the same `theta` workflows into one legal call and computes the same node outputs.

For a node with multiple predecessors, each `receive_input` action checks that the sender is a declared parent, rejects duplicate parent edges, checks the session and seed, and stores that parent's value. Only when the number of distinct stored parents reaches the configured parent count does the node compute; any later duplicate is rejected because a result already exists. Across all 42 profile repeats, every node emitted exactly one execution event per workflow, each graph edge arrived once, every node's stored `input_count` equaled its declared parent count, and every node checksum matched the same-receiver control. In the width-4 fork-join, the inner join nodes and final sink each read all four parent values and ran once per workflow. The `DAG_JOIN` log is a producer-side Promise fanout barrier callback after that producer's child calls complete; it is not the DAG's multi-parent join-node execution.

## Main observations

- All 354 NEAR multi-contract workflows completed, and all 84 NEAR same-receiver sessions matched every expected node checksum. All 84 fused-control EVM sessions and all 84 independent-stage EVM sessions matched NEAR's same-receiver node checksums; every independent-stage session also passed stored-input validation.
- NEAR's same-receiver control used one execution receipt per `theta`-workflow session across all 84 sessions. The multi-contract workflow receipt-outcome count rose with depth and graph fanout; it counts child execution outcomes and refund transfers, while the initiating transaction is tracked separately. Two block-height measures are kept separate: sink completion (last terminal-node execution minus the originating transaction block) tracks completion of the DAG itself; full receipt-tree drain (last outcome, including callbacks and refunds) can happen later. Neither is converted to wall-clock time.
- `receipt_count` is specifically the number of `receipts_outcome` records for the measured `start` transaction; the originating transaction is recorded separately. Receipt action payloads were fetched with `EXPERIMENTAL_receipt` for a representative depth-4 chain and width-4 fork-join. The observed action receipts were function calls (`start`, `receive_input`, `on_fanout_complete`) and `Transfer` actions with `predecessor_id=system` (refund transfers). Contract deployment and `new` initialization happened before the measured `start` call and are not included. There are no synthetic initialization calls hidden in the reported 3/7/15 or 59 outcome counts.
- In a chain, depth `d` has `d` node function calls, `d-1` system refund transfers, and no fanout callback. Thus d2/d4/d8 produce 3/7/15 receipt outcomes. In the width-4 fork-join, one start plus 24 edge-input calls yields 25 node calls, five branching producers emit five fanout barrier callbacks, and the 24 outgoing calls plus five callbacks yield 29 system refund transfers: 59 total. Across every profile and repeat, this decomposition exactly matched the measured outcome count. Refunds are a real part of the returned execution tree but are not DAG computation or dependency rounds.
- The chain depth sweep measured NEAR completion spans of 2, 4, and 8 block heights for d2, d4, and d8. Each EVM implementation executes a session in one state-changing transaction, with entry and exit storage-proxy transactions measured separately. The independent-stage transaction makes one external `CALL` per DAG node; it does not add one transaction per stage. This supports the scoped claim that the tested independent multi-contract workflow incurs additional asynchronous dependency confirmation as depth grows, while local composition keeps graph execution in one transaction. Block heights and EVM transactions are shown in separate panels because they are not equal units or elapsed-time comparisons.
- In the 84 NEAR fault trials, each trial produced a failed receipt; all workflows remained incomplete at their sink node. The root async call may still report success when a descendant receipt fails, so the analysis classifies the failure from the full receipt tree and terminal node state.
- All 84 EVM fault probes reverted their partial writes; a separate cleanup transaction closed the fault session. This is an atomic single-runtime result and does not imply atomicity across independent NEAR receipts.

| Profile | Nodes / edges | NEAR receipt outcomes per workflow | NEAR sink span / full tree drain, blocks | NEAR same-receiver gas per theta session, mean | Fused single-contract EVM control: full session gas, mean |
|---|---:|---:|---:|---:|---:|
| Chain d2, theta1 | 2 / 1 | 3 = 2 calls + 1 refund | 2.0 / 2.0 | 2.10 Tgas | 293,829 |
| Chain d4, theta4 | 4 / 3 | 7 = 4 calls + 3 refunds | 4.0 / 4.0 | 4.54 Tgas | 1,007,636 |
| Chain d8, theta8 | 8 / 7 | 15 = 8 calls + 7 refunds | 8.0 / 8.0 | 11.81 Tgas | 3,164,505 |
| Fork-join d4, w2, theta4 | 6 / 8 | 23 = 9 calls + 3 callbacks + 11 refunds | 4.0 / 6.0 | 5.64 Tgas | 1,376,807 |
| Fork-join d4, w4, theta4 | 10 / 24 | 59 = 25 calls + 5 callbacks + 29 refunds | 4.0 / 6.0 | 7.85 Tgas | 2,406,974 |
| Mixed d4, w2, theta4 | 6 / 8 | 23 = 9 calls + 3 callbacks + 11 refunds | 4.0 / 6.0 | 5.59 Tgas | 1,376,811 |
| Mixed d4, w4, theta4 | 10 / 16 | 43 = 17 calls + 5 callbacks + 21 refunds | 4.0 / 6.0 | 7.84 Tgas | 2,252,388 |
| Chain d4, theta4, nu4 | 4 / 3 | 7 = 4 calls + 3 refunds | 4.0 / 4.0 | 4.57 Tgas | 1,007,634 |

The table shows means across the three fresh repeats and two sessions per repeat. “Calls” are node `FunctionCall` receipts; callback and refund counts are as defined above. NEAR gas and EVM gas are native runtime measurements with different units and cost models; the numerical columns must not be compared across runtimes. Per-workflow receipt-type counts, all profiles, failure counts, and the EVM entry/execution/exit breakdown are in the CSV/JSON summaries and raw files.

## Independent-stage EVM result

The independent-stage route deploys a distinct `DAGStage` contract per graph node and a `DASDistributedCompositionHarness` Router. The Router performs the topological schedule; each node invocation is an actual external `CALL` to its Stage contract, whose stored parent inputs and checksum are checked after execution. Across 42 profile repeats (84 sessions), the run recorded 84 Router execution transactions, 1,956 Stage `CALL` frames, 1,956 `NodeExecuted` events, and 2,274 `ParentInputConsumed` events. Every session passed checksum and stored-input validation. All 84 injected cross-contract fault probes reverted partial state.

| Profile | Fused-control session gas | Independent-stage session gas (entry + Router + exit; setup excluded) | Deployment + registration setup gas per profile | Stage `CALL` frames per session |
|---|---:|---:|---:|---:|
| Chain d2, theta1 | 293,829 | 435,055 | 6,313,503 | 2 |
| Chain d2, theta4 | 714,419 | 1,201,484 | 6,313,503 | 8 |
| Chain d4, theta4 | 1,007,636 | 2,189,801 | 9,434,821 | 16 |
| Chain d8, theta4 | 1,660,371 | 4,235,880 | 15,677,474 | 32 |
| Fork-join d4, w2, theta4 | 1,376,807 | 3,883,587 | 12,527,376 | 24 |
| Fork-join d4, w4, theta4 | 2,406,974 | 8,832,224 | 18,734,561 | 40 |
| Mixed d4, w2, theta4 | 1,376,811 | 3,883,591 | 12,527,340 | 24 |
| Mixed d4, w4, theta4 | 2,252,388 | 6,999,355 | 18,705,019 | 40 |
| Chain d4, theta4, nu4 | 1,007,634 | 2,189,799 | 9,434,821 | 16 |

Values are means across three fresh chains and two sessions per chain. The full session total includes entry proxy, Router transaction, and exit proxy. Stage-frame gas is nested inside Router transaction gas and is diagnostic; do not add it again. Deployment and node registration occur once per profile per fresh chain and are reported separately, not amortized into session gas. The fused EVM figures in the preceding table are a single-contract control and must not be described as the independent-stage measurement. The two EVM implementations are both local EVM controls and do not model cross-shard execution.

The raw independent-stage records are [das_distributed_composition_raw.json](das_distributed_composition_raw.json) and [das_distributed_composition_results.json](das_distributed_composition_results.json); the fused-control records remain in [das_composition_full_raw.json](das_composition_full_raw.json) and [das_composition_full_results.json](das_composition_full_results.json). Figure [dag_evm_composition_controls.png](dag_evm_composition_controls.png) compares these EVM compositions, showing session costs separately from deployment and registration setup.

## Same-domain positive control from the earlier local screen

The separate 2026-10-27 local Anvil mechanism screen contains Avalanche-local and Teleport-local controls using the same local-composition route as its DAS-local case. At depth 4, all three used the same 6 transactions, had the same final state and 480,868 EVM gas, and measured about 6.0 seconds in the one-second-block model. This is the expected same-domain positive control: once work is local, the model shows no DAS-specific advantage. Those results are a separate hash-chain screen, not part of this generic DAG / NEAR run; the source findings and raw data are preserved separately under [supplemental_hash_chain_control](../supplemental_hash_chain_control/) and are not pooled with this table.

## Scope and limits

- The NEAR run uses `nearcore` 2.14.0 / protocol 88 in a single-validator Sandbox. Every contract edge is cross-contract, while the configured execution-domain edge proportion is `p = 0`; no edge is a cross-shard or cross-domain edge. This cannot establish cross-shard performance.
- The Type-II entry and exit transactions measure local session/commitment storage work. They do not implement bridge proofs, authentication, finality verification, real asset migration, or production network transfer. Both EVM results are composition-harness measurements, not end-to-end DAS deployment benchmarks. The independent-stage version includes per-node external contract calls but remains a single local-EVM transaction and is not a cross-shard experiment.
- The injected failures are expected runtime outcomes used to characterize completion and rollback. They are not security defects. NEAR async failure is reported per receipt and sink state; the originating transaction status alone is insufficient.
- Wall time is diagnostic only. The reported latency proxy is receipt/block dependency span in each local runtime. No cross-runtime latency or gas ratio is claimed.
- Values use a fixed deterministic operation and these graph sizes. They do not establish saturated throughput, production behavior, performance on other versions, or a universal DAS advantage.

## Runtime and retained evidence

- NEAR: nearcore 2.14.0, protocol 88, `near-sandbox` 0.3.4, `near-workspaces` 5.0.0; three fresh Sandbox chains.
- EVM: remote Linux x86_64 host, Python 3.10.12, Anvil 1.8.6, Solidity 0.8.24; three fresh Anvil chains per EVM implementation. Source commit: `1d6b6aa140374f4ce168eb629fe96c235ab61aa7` on `codex/das-independent-stage-run`.
- Every raw NEAR workflow stores originating and receipt outcome IDs, executor IDs, block heights, logs, edge events, and failure status in [near_dag_full_raw.json](../near_runtime_20261027/near_dag_full_raw.json). The parsed full result is [near_dag_full_results.json](../near_runtime_20261027/near_dag_full_results.json).
- The targeted receipt-action diagnostic, including raw receipt action payloads for depth-4 chain and width-4 fork-join, is [near_receipt_diagnostic_results.json](../near_runtime_20261027/near_receipt_diagnostic_results.json); the cross-profile classification and join-node assertions are in [receipt_classification_audit.json](receipt_classification_audit.json).
- Fused-control EVM sessions store transaction hashes, statuses, block numbers, gas, checksums, and failure-probe receipts in [das_composition_full_raw.json](das_composition_full_raw.json) and [das_composition_full_results.json](das_composition_full_results.json). Independent-stage EVM records include Router and Stage transaction traces, event/input validation, and cross-contract fault probes in [das_distributed_composition_raw.json](das_distributed_composition_raw.json) and [das_distributed_composition_results.json](das_distributed_composition_results.json). The Solidity sources and compiled artifacts are retained alongside the runners.
- Rebuild and run the NEAR workload with [run_macos_dag.sh](../near_runtime_20261027/run_macos_dag.sh) on macOS or [run_linux.sh](../near_runtime_20261027/run_linux.sh) on Linux; run the fused EVM control after NEAR results are available with `DAG_NEAR_RESULTS=../near_runtime_20261027/near_dag_full_results.json python run_das_composition.py`, and run the independent-stage EVM implementation with `DAG_NEAR_RESULTS=../near_runtime_20261027/near_dag_full_results.json python run_das_distributed_composition.py`. Use `summarize_dag_results.py` to regenerate tables.
- Dependencies and compiler/runtime caches are isolated under the near runtime `vendor/`, `node_modules/`, and `target/` directories and are ignored by Git.
