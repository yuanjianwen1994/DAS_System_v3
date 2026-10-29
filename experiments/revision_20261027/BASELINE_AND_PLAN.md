# DAS migration experiment baseline and plan

Date: 2026-10-27 (Asia/Shanghai)

## Safety checkpoint

- Original experiment directory, including its `.git`, was archived before edits at `/private/tmp/DAS_System_v3-pre-experiment-20261027.tar.gz`.
- Existing GitHub repository is public and cloneable. Its default `main` head is `e66ae21951529a636e0a9830049ebe9c6f179798`; experiment branch `阶段四，保持真实出块间隔` is `c6972971039e0d5ce19eda6b453d0a0988b36a8b` (2026-05-28).
- The local nested `.git` is damaged: unborn branch, broken remote-tracking ref, corrupt index cache tree, and many missing blob objects. No local commit exists. Work will use a clean clone based on the existing experiment branch; the original directory and all files remain preserved.
- The parent manuscript repository ignores `/DAS_System_v3/`. Experiment code, logs, and this plan must stay out of Overleaf.

## Baseline source identity

- GitHub source baseline: experiment branch commit `c6972971039e0d5ce19eda6b453d0a0988b36a8b`.
- Current local sources differ from that baseline. This file records SHA-256 for every current local Python and Solidity source before experiment edits: `baseline_source_hashes.sha256`.
- Local snapshot size: 174 MB. The local directory has 254 files; the GitHub experiment branch tree has 1,183 blobs. The differences must be reconciled additively; absent remote files are not treated as intentional deletions.

## Effective baseline parameters (code inspection)

| Parameter | Effective value / behavior | Source |
|---|---:|---|
| Block time | 12 s for formal configuration | `config_global.py` |
| Anvil gas limit | 30,000,000; `core/network.py` imports from `config_global.py` | `core/network.py`, `config_global.py` |
| Macro/matrix nominal limit | 6,000,000 in those configs, but not effective for Anvil launch | `config_macro.py`, `config_matrix.py`, `core/network.py` |
| Matrix theta | 1–20 | `config_matrix.py` |
| Matrix users / processes | 1,200 users / 20 processes | `config_matrix.py` |
| Journeys per user | 5 | `config_matrix.py` |
| Compared modes | DAS, 2PC, BASELINE, TELEPORT, ATOMIC | `config_matrix.py` |
| Existing smoke profile | 1 s block time, 200 users, concurrency 3, one journey/user, theta 1 | `smoke_test_sota.py` |
| Runtime dependencies in current Python | `web3`, `py-solc-x`, `requests`, `urllib3`, `tqdm`, `pandas`, `pytest` are not installed | environment check 2026-10-27 |
| Node/compiler tools in PATH | `anvil`, `forge`, and `solc` were not found | environment check 2026-10-27 |

## Existing results inventory and limits

- Local `logs/` includes historical CSV/log outputs and seven `matrix_summary_*.csv` files (June 2, October 11, and October 15, 2026); the directory also contains a 20 MB `logs.zip` and a 29 MB `logs/logs.zip`.
- Existing comparison harnesses are `core/macro_traffic.py`, `core/macro_monitor.py`, `contracts/DASEndpoint.sol`, `contracts/SOTAEndpoints.sol`, and `contracts/Workload.sol`.
- Prior log review supplied with this task reports N=500, theta=10 complete-journey means: DAS 594,066 gas / 218.9 s; TELEPORT 598,158 / 214.7 s; ATOMIC 577,564 / 214.1 s. Treat as prior measured results, not evidence of a DAS lead.
- A prior N=100, theta=5 replay over the first-to-last 60 s window reported 0% completed for DAS and 2PC, while cumulative per-transaction percentages differed. Do not reuse the manuscript's 91.8% / 52.4% figures without a reproducible source and matching denominator.

## Mechanism boundary

| Variant | Required modeled safeguard | Explicit abstraction boundary |
|---|---|---|
| DAS | Source debit, pending amount, authenticated receipt binding source/destination/asset/amount/beneficiary/payload, replay protection, destination credit only after valid receipt | Trusted certificate/attestor simulates cross-domain authentication; not a production finality proof |
| XCM teleport-inspired | Source burn/debit, destination trust allowlist, same message payload, one-time receipt and destination credit | XCM execution, relay/parachain consensus, transport, fees, and trust governance are not implemented |
| Avalanche atomic export/import-inspired | Export creates a unique locked UTXO-like receipt; import atomically consumes it once and credits destination | Not AvalancheGo shared memory, real X/P/C transaction formats, node consensus/finality, or wallet signing |
| DAS ablations | Remove one named safeguard at a time and label the variant as an ablation | Never present an ablation as SOTA or as a faithful competitor |

Official references: [Avalanche Coreth / C-Chain architecture](https://build.avax.network/docs/primary-network/coreth-architecture), [Polkadot XCM pallet overview](https://wiki.polkadot.com/learn/learn-xcm-pallet/), [Polkadot XCM use cases](https://wiki.polkadot.com/learn/learn-xcm-usecases/).

## Experiment sequence

1. Implement a minimal balance-backed migration state machine: source debit, pending receipt, destination credit, and duplicate/altered/wrong-domain rejection. Bind both ends to the same signed receipt and full payload. Use a stated trusted-attestor assumption.
2. Add a business workload whose spend operation requires destination funds to have been credited. Assert conservation, no premature spend, no double credit, and pending/settled transitions.
3. Run a short-block functional fault matrix: valid, duplicate, delayed, reordered, wrong destination, payload tamper, and mint without a source debit. Keep the old code and logs unchanged.
4. Run a small 12 s-block comparison with a shared pre-generated workload, separate wall-clock measurement, gas decomposition, and at least three independent seeds. Report completed journeys/s, protocol tx/s, end-to-end P50/P95/P99, failures, and incomplete attempts separately.
5. Only expand saturation and applicability sweeps if the measured short run shows a mechanism-relevant boundary. Separate user think time, transport delay, confirmation delay, and queueing.

No experiment result may be described as statistically equivalent, noise-limited, or a DAS lead without data supporting that statement. Old outputs remain untouched; new outputs must include code/config/seed metadata.
