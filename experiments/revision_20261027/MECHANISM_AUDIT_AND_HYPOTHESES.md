# Protocol mechanism audit and falsifiable hypotheses

Date: 2026-10-27. This audit separates protocol operations from harness bookkeeping. It is a design boundary for the local experiment, not a claim that the harness implements the production protocols.

## Protocol path versus harness path

| System | Protocol-required transfer path | Not part of that path / abstraction in this harness |
|---|---|---|
| DAS as described by this manuscript | Source debit/burn and finalization; receipt binds source and destination contexts, asset, amount, accounts, payload, and nonce; destination validates source finality and envelope, rejects a consumed receipt, then credits/mints. | The local attestor signature stands in for source finality evidence; local event reading/relaying stands in for transport. There is no source-chain acknowledgement transaction after destination import in the manuscript algorithm. |
| Polkadot XCM teleport | Source withdraws/burns the teleported asset and sends `ReceiveTeleportedAssets`; destination accepts/mints/deposits only for a configured trusted teleporter/source and asset. Trust is bilateral/configuration-level. | The test's EVM allowlist and signed receipt approximate a trusted channel. They do not reproduce XCM routing, executor, relay/parachain consensus, or native teleport asset handling. |
| Avalanche atomic export/import | ExportTx creates transferable UTXO(s) in shared atomic memory; ImportTx consumes those UTXO(s) and creates destination EVM output(s). Import consumption is the one-time settlement point. | The signed receipt and destination bitmap approximate a transferable UTXO plus shared-memory proof. Independent Anvil instances do not share Avalanche atomic memory or consensus. |

The `acknowledgeImport` source transaction was harness-only. It cleared a source-side pending counter after import and added an extra block interval to every measured journey. It is removed from the current harness. Accounting remains auditable: source-side export records are immutable and remain pending locally; a separate audit matches their receipt IDs against destination `importedReceipt` state. `unmatched_source_pending == 0` means every tested export has a matching destination import. The audit does not mutate protocol state or submit an extra transaction.

`executeBusiness` remains a workload marker after import. The 0.5-second think interval is a user-workload delay, not a bridge step. Neither belongs in protocol transaction counts.

## Falsifiable mechanism hypotheses

| Hypothesis | Mechanism root | Minimal comparison | Expected advantage / cost | Falsification condition |
|---|---|---|---|---|
| H1: Per-receipt destination binding should reject a correctly signed receipt replayed into a different destination context, while a teleport channel is authorized at the configured source/asset scope. | DAS validates receipt-specific destination endpoint/domain and consumes a receipt ID; XCM teleport relies on configured trusted teleporter and asset/location. | On separate test deployments, hold source and asset constant and attempt imports with altered destination context, recipient, and duplicate message. Count accepted/rejected cases; report each trust assumption. | DAS may provide narrower, per-message authorization; it pays receipt verification and consumed-state costs. Teleport may reduce per-message authorization work but requires channel-level trust configuration. | H1 fails if the faithful DAS verifier accepts altered contexts/replays, or if the faithful teleport configuration provides the same per-message binding without extra mechanism/cost. Current EVM harness cannot establish this production-level result. |
| H2: Removing the source ACK should reduce measured completion time by one source-chain inclusion phase without changing receipt-level conservation. | Neither manuscript DAS algorithm, XCM teleport, nor Avalanche atomic import requires the harness's post-import ACK; Avalanche import consumes the exported UTXO. | Run the same small paired Anvil workflow with and without ACK, record transaction count and source/destination receipt-ID reconciliation. Keep the ACK version as historical raw data. | Expected one fewer transaction and one fewer source-chain block phase per journey; accounting remains available off-chain. | Falsified if ACK-free imports fail to reconcile, violate source/destination local invariants, or require a protocol step absent from the stated algorithms. This is a measurement-method correction, not a protocol performance advantage. |
| H3: A faithful shared-memory UTXO import should enforce one-time consumption through UTXO availability, rather than through DAS-style signature verification plus an imported-receipt bitmap. | Avalanche shared atomic memory exposes exported UTXOs until an ImportTx consumes them; DAS verifies a source-finality receipt and tracks consumed receipt IDs at destination. | Compare only after using AvalancheGo/shared-memory-compatible test nodes and a DAS finality verifier; submit the same export twice and measure verification/consumption work. | Atomic import may avoid a separate per-receipt attestor signature check but incurs shared-memory lookup and ImportTx costs. | Falsified if the faithful atomic path needs equivalent per-receipt attestation/consumption work, or if measured cost is not lower under matched security and workload assumptions. The current abstraction cannot test this. |

No performance-lead hypothesis is accepted from the current harness. In particular, it must not compare the current Teleport path's extra allowlist check against a DAS path without modeling DAS finality proof verification, or treat the atomic signed-receipt bitmap as Avalanche shared-memory UTXO execution.

## Primary protocol references

- [Polkadot XCM teleport overview](https://wiki.polkadot.com/learn/learn-teleport/)
- [Polkadot XCM configuration: trusted teleporters](https://docs.polkadot.com/develop/interoperability/xcm-config/)
- [Polkadot SDK XCM executor source](https://github.com/paritytech/polkadot-sdk/blob/master/polkadot/xcm/xcm-executor/src/lib.rs)
- [Avalanche C-Chain transaction format](https://docs.avax.network/docs/rpcs/c-chain/txn-format)
- [Avalanche C-Chain RPC: getUTXOs](https://docs.avax.network/docs/rpcs/c-chain)
- [Avalanche node architecture](https://docs.avax.network/docs/nodes/architecture)
