# Revision experiment: results and limits

Run date: 2026-10-27. Base source revision: `c6972971039e0d5ce19eda6b453d0a0988b36a8b`.

## What was run

The 1-second smoke test exercises source debit, an unspendable pending balance, signed receipt binding, settlement exactly once, and business spending only after destination credit. It also tests reordered messages, replay, wrong destination/payload, unauthorized signer, and an unbacked mint. The three comparator models are explicitly local abstractions:

- `DAS`: source debit to pending, a domain-bound signed receipt, and exact-once settlement.
- `TELEPORT_ABSTRACT`: trusted-source allowlisting, a bound transfer envelope, and exact-once destination settlement.
- `ATOMIC_UTXO_ABSTRACT`: a bound export record that is consumed before destination credit.

All are deployed to one local EVM. They do not implement Polkadot relay/parachain consensus, XCM execution/transport, Avalanche shared memory or consensus, or independent-chain finality. The Avalanche docs describe atomic export/import through shared-memory UTXOs; the Polkadot XCM materials describe teleport as a trust-dependent asset transfer. This harness tests selected state-transition safeguards, not production protocol equivalence.

## 12-second sample

Three independent fresh Anvil runs per model, with 12-second block intervals and one seeded transfer/business journey per run. All 9 journeys completed; each ended with zero pending balance and a true accounting invariant. There were no failures or incomplete journeys. Every run included one begin/export, one settle/import, and one business transaction.

| Model | Successful journeys / wall-clock second | Protocol transactions / second | Journey latency P50 / P95 / P99 (s) | Gas per journey (range) |
|---|---:|---:|---:|---:|
| DAS | 0.0831 | 0.249 | 36.060 / 36.083 / 36.083 | 501,937–501,951 |
| Teleport abstraction | 0.0831 | 0.249 | 36.086 / 36.100 / 36.100 | 503,244–503,253 |
| Atomic UTXO abstraction | 0.0831 | 0.249 | 36.085 / 36.101 / 36.101 | 440,107 |

The latency is three 12-second block intervals plus local process and RPC overhead. With only three repetitions, the reported upper percentiles are simply the maximum observed sample and are not stable tail estimates. The same-EVM setup does not support cross-chain throughput claims. No statistical equivalence or performance advantage is inferred from these values.

## Applicability gas sample

The fast-mining gas check varies business calls per transfer (1 transfer with 1 or 5 business calls; 5 transfers with 1 business call). Every case preserved the accounting invariant. Total gas was:

| Model | 1 transfer / 1 call | 1 transfer / 5 calls | 5 transfers / 1 call |
|---|---:|---:|---:|
| DAS | 444,160 | 620,432 | 1,774,246 |
| Teleport abstraction | 498,466 | 676,428 | 2,044,442 |
| Atomic UTXO abstraction | 469,683 | 647,755 | 1,905,743 |

These measurements show the gas behavior of these specific contracts only. They do not establish that fixed migration costs are amortized in deployed systems or that DAS outperforms either protocol.

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
