# θ session migration amortization — same-EVM control

The primary results below use three fresh-Anvil repeats at a 12-second block interval. The unchanged repository `MigrationHarness.sol` runs on one EVM with two logical domain labels and a trusted local ECDSA attestor. This is a state-machine/mechanism control, not a cross-VM bridge or a production attestation path. `same_domain` is the no-migration best-case control; both migrated arms move in, execute θ dependent balance spends, and move the remainder back out. The batch arm compresses the θ spends into one helper-contract transaction.

| Arm | θ | Completed / planned | Internal business median (s) | Entry median (s) | Exit median (s) | Full journey median (s) | Full journey / operation (s) | Receipt binding | Final accounting | Failures | Incomplete |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|
| same_domain | 1 | 3/3 | 11.992 | 0.000 | 0.000 | 11.992 | 11.992 | 0/0 | pass | 0 | 0 |
| same_domain | 2 | 3/3 | 11.998 | 0.000 | 0.000 | 11.998 | 5.999 | 0/0 | pass | 0 | 0 |
| same_domain | 4 | 3/3 | 12.010 | 0.000 | 0.000 | 12.010 | 3.002 | 0/0 | pass | 0 | 0 |
| same_domain | 8 | 3/3 | 12.039 | 0.000 | 0.000 | 12.039 | 1.505 | 0/0 | pass | 0 | 0 |
| migrated_unbatched | 1 | 3/3 | 11.991 | 24.010 | 23.995 | 59.996 | 59.996 | 6/6 | pass | 0 | 0 |
| migrated_unbatched | 2 | 3/3 | 11.996 | 24.001 | 23.996 | 59.992 | 29.996 | 6/6 | pass | 0 | 0 |
| migrated_unbatched | 4 | 3/3 | 12.011 | 24.001 | 23.999 | 60.013 | 15.003 | 6/6 | pass | 0 | 0 |
| migrated_unbatched | 8 | 3/3 | 11.938 | 23.992 | 23.999 | 60.021 | 7.503 | 6/6 | pass | 0 | 0 |
| migrated_batch | 1 | 3/3 | 11.986 | 24.002 | 24.002 | 59.988 | 59.988 | 6/6 | pass | 0 | 0 |
| migrated_batch | 2 | 3/3 | 11.986 | 23.995 | 23.996 | 59.977 | 29.988 | 6/6 | pass | 0 | 0 |
| migrated_batch | 4 | 3/3 | 11.988 | 24.004 | 23.998 | 60.088 | 15.022 | 6/6 | pass | 0 | 0 |
| migrated_batch | 8 | 3/3 | 11.987 | 23.997 | 24.006 | 60.002 | 7.500 | 6/6 | pass | 0 | 0 |

## Interpretation

Across θ=1,2,4,8, the same-domain full journey is about one 12-second block. Both migrated paths take about five block intervals: source-side migration-in start and settlement, one business-execution block, then migration-out start and settlement. Their absolute full-journey time therefore does not fall as θ grows in this same-EVM model. The fixed migration overhead per business action declines: the migrated full journey is about 60/θ seconds per action (approximately 60, 30, 15, and 7.5 seconds), while same-domain is about 12/θ (12, 6, 3, and 1.5 seconds). No break-even appears because this controlled VM gives destination execution no speed or locality advantage. This is an expected mechanism control, not evidence against a cross-domain benefit under a separately justified execution environment.

At θ=8, submitting eight individual dependent calls and batching them each occupies one interval in this Anvil configuration; the measured internal phase is therefore near one block interval in both migrated arms. This does not establish production batching equivalence.

## Pilot (separate; excluded from formal medians)

The 1-second pilot completed all 12 sessions (three arms × four θ values) with no failed/incomplete sessions and valid final accounting. It is an operational pilot only; its times are not pooled with the 12-second formal matrix.

| Arm | θ | Internal (s) | Full journey (s) | Binding checks |
|---|---:|---:|---:|---:|
| same_domain | 1 | 1.035 | 1.035 | 0/0 |
| migrated_unbatched | 1 | 1.034 | 5.010 | 2/2 |
| migrated_batch | 1 | 1.032 | 5.007 | 2/2 |
| same_domain | 2 | 1.045 | 1.045 | 0/0 |
| migrated_unbatched | 2 | 1.043 | 5.016 | 2/2 |
| migrated_batch | 2 | 1.032 | 5.002 | 2/2 |
| same_domain | 4 | 1.060 | 1.060 | 0/0 |
| migrated_unbatched | 4 | 1.059 | 5.032 | 2/2 |
| migrated_batch | 4 | 1.032 | 5.001 | 2/2 |
| same_domain | 8 | 1.086 | 1.086 | 0/0 |
| migrated_unbatched | 8 | 0.986 | 4.955 | 2/2 |
| migrated_batch | 8 | 1.032 | 4.998 | 2/2 |

## Reproducibility and safeguards

The raw JSON contains per-session timings, gas totals, source and destination domains, both receipt IDs/envelopes/nonces, transaction gas, terminal balance values, status/failure fields, source hashes, Anvil/solc/Web3 versions, and deployment addresses. Each formal repeat uses a fresh Anvil process, ledger, and batch executor. Migration arms must end with source spendable balance 1, both destination ledgers 0, pending 0, spent θ, funded θ+1, and a passing conservation invariant. Every migrated session verifies both pending receipt envelopes, event payload binding, settled flags, and settlement events.
