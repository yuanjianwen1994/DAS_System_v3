# ACK-free bounded comparison results

Run date: 2026-10-27. Source and destination are separate local Anvil nodes (chain IDs 31337 and 31338), mined at 12-second intervals. The source ACK transaction is absent. Each journey includes one source export, one destination import, a separately recorded 0.5-second business think interval, and one destination business action.

## Results

| Model | Concurrent journeys / model | Completed / failed / incomplete | Journeys / wall second | Transfer transactions / wall second | Latency P50 / P95 / P99 (s) | Mean gas / journey (export + import + business) |
|---|---:|---:|---:|---:|---:|---:|
| DAS | 1 | 3 / 0 / 0 | 0.027727 | 0.055455 | 36.055 / 36.086 / 36.086 | 488378.33 |
| DAS | 3 | 9 / 0 / 0 | 0.083230 | 0.166459 | 36.027 / 36.039 / 36.039 | 445965.44 |
| Teleport abstraction | 1 | 3 / 0 / 0 | 0.027727 | 0.055455 | 36.050 / 36.080 / 36.080 | 490723.33 |
| Teleport abstraction | 3 | 9 / 0 / 0 | 0.083230 | 0.166459 | 36.024 / 36.031 / 36.031 | 448307.78 |
| Atomic UTXO abstraction | 1 | 3 / 0 / 0 | 0.027727 | 0.055455 | 36.049 / 36.079 / 36.079 | 488362.33 |
| Atomic UTXO abstraction | 3 | 9 / 0 / 0 | 0.083230 | 0.166459 | 36.016 / 36.024 / 36.024 | 445959.22 |

Three fresh trials were run per load setting. Across the 36 journeys, all completed with zero failures or incomplete journeys. Source-local and destination-local accounting invariants held; every imported receipt matched a retained source export record, with zero unmatched source pending at the end. The source's local pending counter remains nonzero by design because destination import does not submit a state-changing ACK back to the source.

The roughly 36-second journey latency is consistent with three 12-second inclusion phases. Compared with the old ACK-containing run, the path has one fewer source-chain transaction and one fewer inclusion phase. This is a correction to the measurement path, not a DAS advantage over either comparator. At load 3 the harness submits three times as many journeys into the same phases; its roughly threefold completed-journey rate is expected from batching and does not show saturation scaling.

Gas totals are local contract measurements. The models are abstractions: DAS finality is represented by a test attestor; teleport is an EVM trust-list plus signed envelope; the atomic UTXO model is a signed receipt plus exact-once bitmap rather than Avalanche shared memory. The three models share the same two local block producers. These data do not establish production throughput, statistical equivalence, or a protocol performance lead. Upper percentiles use only 3 or 9 samples and are descriptive, not stable tail estimates.

## Functional and binding checks

The ACK-free one-second smoke passed: wrong source-chain binding, altered payload, duplicate import, and pre-import business spending were rejected; reordered receipts settled; all three model ledgers conserved value. The separate full-envelope audit rejected all 14 tampered cases, including receipt fields, destination chain/endpoint, signature, and replay; the positive control imported successfully.

## Reproducibility and provenance

Raw trial output: [`cross_chain_load_raw.json`](cross_chain_load_raw.json). Exact unedited runner output: [`cross_chain_load_runner_output.json`](cross_chain_load_runner_output.json). Per-journey data: [`cross_chain_load_journeys.csv`](cross_chain_load_journeys.csv). Smoke and binding audit: [`cross_chain_smoke.json`](cross_chain_smoke.json), [`cross_chain_binding_audit.json`](cross_chain_binding_audit.json). Source/runtime identifiers: [`provenance.json`](provenance.json).
