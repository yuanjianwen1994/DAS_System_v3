# Revision experiment: results and limits

> **Current audit correction:** the two-chain measurements below are a preserved pre-audit snapshot. They included a harness-only source acknowledgement transaction; do not cite those latency, throughput, or gas values as the corrected comparison. The corrected six-trial run completed with ACK removed and receipt-ID reconciliation retained. Its independent outputs are under [`ack_free_20261027/`](ack_free_20261027/); see [`ACK_FREE_RESULTS.md`](ack_free_20261027/ACK_FREE_RESULTS.md) for the new bounded results. No production performance lead is claimed.

Run date: 2026-10-27. Base source revision: `c6972971039e0d5ce19eda6b453d0a0988b36a8b`.

## What was run

The 1-second smoke test exercises source debit, an unspendable pending balance, signed receipt binding, settlement exactly once, and business spending only after destination credit. It also tests reordered messages, replay, wrong destination/payload, unauthorized signer, and an unbacked mint. The three comparator models are explicitly local abstractions:

- `DAS`: source debit to pending, a domain-bound signed receipt, and exact-once settlement.
- `TELEPORT_ABSTRACT`: trusted-source allowlisting, a bound transfer envelope, and exact-once destination settlement.
- `ATOMIC_UTXO_ABSTRACT`: a bound export record that is consumed before destination credit.

All are deployed to one local EVM. They do not implement Polkadot relay/parachain consensus, XCM execution/transport, Avalanche shared memory or consensus, or independent-chain finality. The Avalanche docs describe atomic export/import through shared-memory UTXOs; the Polkadot XCM materials describe teleport as a trust-dependent asset transfer. This harness tests selected state-transition safeguards, not production protocol equivalence.

## 12-second sample

Three independent fresh Anvil runs, each containing one seeded transfer/business journey per model, with 12-second block intervals. All 9 journeys completed; each ended with zero pending balance and a true accounting invariant. There were no failures or incomplete journeys. Every journey included one begin/export, one settle/import, and one business transaction.

| Model | Successful journeys / observed model seconds | Transfer-protocol tx / observed model seconds | Business calls / observed model seconds | Journey latency P50 / P95 / P99 (s) | Gas per journey (range) |
|---|---:|---:|---:|---:|---:|---:|
| DAS | 0.027720 | 0.055441 | 0.027720 | 36.092 / 36.099 / 36.099 | 501,937–501,951 |
| Teleport abstraction | 0.027738 | 0.055476 | 0.027738 | 36.032 / 36.091 / 36.091 | 503,244–503,253 |
| Atomic UTXO abstraction | 0.027738 | 0.055475 | 0.027738 | 36.033 / 36.092 / 36.092 | 440,107 |

Rates above pool three sequential independent repetitions for one model: 3 completed journeys divided by about 108 observed model-seconds. In each trial, all three models run concurrently, so the aggregate trial rate is about 0.083 successful journeys/s, 0.166 transfer-protocol tx/s, 0.083 business calls/s, and 0.249 total tx/s. These aggregate values must not be reported as per-model rates. Transfer protocol tx count begin/export and settle/import; business transactions are separate.

The latency is three 12-second block intervals plus local process and RPC overhead. With only three repetitions, the reported upper percentiles are simply the maximum observed sample and are not stable tail estimates. A single journey per model per run is a completion-time check, not a saturated throughput experiment. The same-EVM setup does not support cross-chain throughput claims. No statistical equivalence or performance advantage is inferred from these values.

## Applicability gas sample

The fast-mining gas check varies business calls per transfer (1 transfer with 1 or 5 business calls; 5 transfers with 1 business call). Every case preserved the accounting invariant. Total gas was:

| Model | 1 transfer / 1 call | 1 transfer / 5 calls | 5 transfers / 1 call |
|---|---:|---:|---:|
| DAS | 444,160 | 620,432 | 1,774,246 |
| Teleport abstraction | 498,466 | 676,428 | 2,044,442 |
| Atomic UTXO abstraction | 469,683 | 647,755 | 1,905,743 |

The 12-second sample uses transfer amount 10, a fresh deployment per repetition, and a nonzero destination balance after its business action. This gas-only boundary check uses amount 1 in its 1-transfer/1-call case, reuses each deployment across profiles, and spends the destination balance to zero. These differences affect storage transitions, calldata, and gas refunds; therefore absolute gas totals across the two scripts are not directly comparable. Within either script, totals are comparable only as local contract measurements under that script's stated setup. They do not establish that fixed migration costs are amortized in deployed systems or that DAS outperforms either protocol.

## Ablations and failure checks

The adversarial harness removes one rule at a time in test-only variants. It demonstrates the expected local failure modes: without source authorization, unauthorized value can be debited; without destination/payload binding, a signed transfer can be redirected; without replay consumption, the same receipt can credit twice; without source debit, a destination credit is unbacked. The unbacked-mint case violates conservation. These are mechanism checks, not comparative attack rates or field-security evidence.

## What remains unanswered for the revision

This work provides a runnable mechanism-level comparison and makes its assumptions explicit. It does not settle the reviewer’s request for a production-faithful comparison, multi-chain finality or transport costs, shard contention/hotspot behavior, or workload ranges grounded in representative traces. It also does not validate the numerical claims in historical result summaries under the new state machine. The manuscript should use these results only with the stated abstraction and limitations, and should not claim a measured performance lead or statistical equivalence.

## Reproduction

From the repository root, install `requirements.txt`, install Foundry/Anvil, and run:

```sh
python tests/migration_state_machine_smoke.py
python tests/formal_state_machine_sample.py
python tests/applicability_boundary_smoke.py
```

Raw outputs and run metadata are in this directory. The scripts compile the contracts and start their own local Anvil process. They do not contact public chains.

## Protocol references

- Avalanche atomic transactions and shared-memory UTXOs: <https://build.avax.network/docs/primary-network/coreth-architecture>
- Polkadot XCM teleport overview: <https://wiki.polkadot.com/learn/learn-xcm-pallet/>
- Polkadot XCM use cases: <https://wiki.polkadot.com/learn/learn-xcm-usecases/>

## Superseded two-chain bound-load comparison (archived)

Run date: 2026-10-27. Source and destination are separate local Anvil processes (chain IDs 31337 and 31338). This archived version included source export/debit, destination receipt verification/import, a harness-only source acknowledgement, a 0.5-second business think interval, and a destination business spend. The attestor signed the full source-event envelope and the import acknowledgement. The harness read the source event and relayed it locally; this did not model network transport or finality proofs.

Three fresh two-node trials were run for each of two small load settings: one and three concurrent journeys per model. All 36 journeys completed, with zero failed or incomplete journeys in that archived version. Wrong source-chain binding, altered payload, duplicate import, and pre-import business spending were rejected in the separate one-second adversarial smoke. A three-node binding audit also rejected 14 tampered cases covering every receipt field, the destination chain and endpoint contexts, and replay, while a valid positive-control import succeeded. Each archived load trial passed source-local, destination-local, and cross-chain pending accounting. In the ACK-containing version, source pending was zero and acknowledged exports equaled destination imports.

| Model / load | Completed journeys | Journey / wall second | Transfer protocol tx / wall second | Business calls / wall second | Journey latency P50 / P95 / P99 (s) | Mean gas: export / import / ack / business / total |
|---|---:|---:|---:|---:|---:|---:|
| DAS, 1 concurrent | 3 | 0.020814 | 0.062442 | 0.020814 | 48.043 / 48.057 / 48.057 | 304,677 / 126,048 / 91,381 / 59,973 / 582,079 |
| Teleport abstraction, 1 concurrent | 3 | 0.020814 | 0.062442 | 0.020814 | 48.035 / 48.051 / 48.051 | 304,681 / 128,385 / 91,386 / 59,977 / 584,430 |
| Atomic UTXO abstraction, 1 concurrent | 3 | 0.020814 | 0.062442 | 0.020814 | 48.034 / 48.049 / 48.049 | 304,673 / 126,040 / 91,372 / 59,969 / 582,055 |
| DAS, 3 concurrent | 9 | 0.062416 | 0.187248 | 0.062416 | 48.025 / 48.093 / 48.093 | 296,469 / 103,247 / 82,258 / 48,569 / 530,543 |
| Teleport abstraction, 3 concurrent | 9 | 0.062416 | 0.187248 | 0.062416 | 48.023 / 48.084 / 48.084 | 296,473 / 105,586 / 82,258 / 48,569 / 532,886 |
| Atomic UTXO abstraction, 3 concurrent | 9 | 0.062416 | 0.187248 | 0.062416 | 48.015 / 48.078 / 48.078 | 296,466 / 103,236 / 82,270 / 48,577 / 530,550 |

Rates pool three repeats by dividing the successful journeys or transactions for one model by the sum of the three trial wall-clock windows at that load. The old transfer transaction count includes export, import, and acknowledgement; the business transaction is separate. Journey latency includes the 0.5-second think interval; the raw file records it separately. Each path is paced by four 12-second block phases, so the roughly 48-second latency is block-interval dominated. At load three, the harness submits three times as many journeys into the same four phases; the resulting threefold throughput is expected from that setup and is not evidence of saturation capacity or better scaling.

P95 and P99 are descriptive nearest-rank values from only 3 observations at load one and 9 at load three; in both cases they equal the largest observed latency. They are not stable tail estimates, and no confidence interval or equivalence claim is made.

Across the models, mean gas totals differ by about 0.4% in these specific contracts; the latency differences are milliseconds against a 48-second block schedule. The teleport abstraction pays for an extra trusted-source check. DAS and the atomic UTXO abstraction are nearly identical here because both retain comparable envelope and one-time-consumption work. These are descriptive contract measurements, not statistical equivalence, a security ranking, or a measured production advantage. Gas per journey also falls in the three-concurrent case because zero-to-nonzero storage and aggregate counters are shared across transfers in a batch; per-transaction state costs therefore differ by load.

Deployment, funding, and teleport trust-configuration gas are setup costs, excluded from per-journey values above and retained in each raw trial. Mean setup gas per fresh trial was:

| Load | Model | Source deploy | Destination deploy | Source funding | Teleport trust setup |
|---|---|---:|---:|---:|---:|
| 1 concurrent | DAS | 880,349 | 682,913 | 96,538 | 0 |
| 1 concurrent | Teleport abstraction | 880,349 | 682,929 | 96,538 | 45,147 |
| 1 concurrent | Atomic UTXO abstraction | 880,349 | 682,925 | 96,534 | 0 |
| 3 concurrent | DAS | 880,349 | 682,917 | 221,190 | 0 |
| 3 concurrent | Teleport abstraction | 880,349 | 682,921 | 221,214 | 45,147 |
| 3 concurrent | Atomic UTXO abstraction | 880,349 | 682,929 | 221,202 | 0 |

The models are deliberately bounded. DAS uses a test attestor in place of source-chain consensus/finality verification. The teleport model combines a trusted-source allowlist with the authenticated envelope; it is not an XCM executor. The atomic model locks a source export and consumes a signed receipt once on destination; the signature stands in for shared-memory UTXO proof and does not run AvalancheGo. Both chains are local Anvil nodes without independent validator sets, real inter-node transport, or production fees. This comparison is suitable for checking cross-chain field binding, pending accounting, and a tiny load contrast only.

The pre-audit raw trial, journey, smoke, and binding-audit files are in `with_source_ack/`. The ACK-free smoke, binding audit, raw trials, journey CSV, summary, and provenance are in `ack_free_20261027/`. Reproduce the corrected short functional workflow with `CROSS_CHAIN_SMOKE_ONLY=1 python tests/cross_chain_load_experiment.py`, the field audit with `python tests/cross_chain_receipt_binding_audit.py`, or the bounded comparison with `python tests/cross_chain_load_experiment.py` after installing the documented dependencies and Anvil.

See [`MECHANISM_AUDIT_AND_HYPOTHESES.md`](MECHANISM_AUDIT_AND_HYPOTHESES.md) for protocol/harness boundaries and falsifiable hypotheses.
