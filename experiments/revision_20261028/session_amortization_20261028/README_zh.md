# θ / session migration amortization

This experiment runs `MigrationHarness.sol` unchanged and adds `BatchBusinessExecutor.sol` only for the batching control. The ledger, receipt envelope, pending balance, signature verification, exact-once settlement, and business-spend logic therefore come from the repository's existing harness.

## Arms

- `same_domain`: θ sequential balance-consuming calls at the source logical domain; best-case no-migration control.
- `migrated_unbatched`: signed receipt migration into the destination logical domain, θ individual dependent spends, and a second signed receipt migrating the remainder back.
- `migrated_batch`: the same entry and exit receipts, but θ spends are executed in one helper-contract transaction.

The two domains live in one contract on one Anvil EVM, and a trusted local ECDSA key signs settlement. This measures the mechanism's accounting/receipt path and migration-cost amortization under a controlled VM. It is not a cross-VM bridge, production attestation, or evidence of a target-domain execution advantage. A no-migration same-domain baseline should therefore remain faster unless a later, separately labeled environment model supplies a justified execution benefit.

Each completed session must end with source spendable balance 1, destination spendable balance 0, pending balance 0, business spent θ, funded total θ+1, and a passing conservation invariant. Migrated arms must verify both receipt events, their complete pending envelopes, signature-authorized settlement, and exact-once settled state.

Pilot settings: θ ∈ {1,2,4,8}, one repeat, 1-second Anvil interval. Formal settings: same θ values, three fresh-Anvil repeats, 12-second block interval. The output includes phase and end-to-end wall seconds, gas, terminal balances, receipt-binding checks, and failed/incomplete sessions.

Run on the configured remote experiment host only:

```sh
python3 run_same_vm_session_amortization.py --mode pilot --output /path/to/pilot
python3 run_same_vm_session_amortization.py --mode formal --output /path/to/formal
```
