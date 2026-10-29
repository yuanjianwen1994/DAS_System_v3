# Seed 20261032 staged NEAR short verification

- Result: **PASS**; runner exit 0, cleanup complete.
- Scope: first 8 exact schedule offsets at each of λ=0.5 and λ=1.0, same seed 20261032; 20-second observation per phase. This checks deployment and receipt/sink auditing only, not performance.
- Schedule prefix SHA-256: `43952f0ae155175c136169cb2fa9867fb7e09308ffa6235010a6039588c0b659`; source full schedule SHA-256: `9684e531a7523cd2672ef91abd2a951224472550304a8c49833cac0cde823166`.
- Repaired runner SHA-256: `93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`.
- Actual WASM path: `/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/seed20261032_near_auditfix_verify2/input/artifacts/stage_contract.wasm`; SHA-256: `2c1a355b7eae3d59eef2fc66b331d7453c74e2cbcbf560708a30d1a13620b87a`.
- Module resolution target: `/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules`; `near-sandbox/package.json` SHA-256: `006e44655da1067679c791a4118c79f44945526520219963559eb468b9d7cc55`.
- Runtime: NEAR Sandbox 2.13.4, protocol 86.

| λ | Offered | Fully audited | Pending | Confirmed failures | Trace |
|---:|---:|---:|---:|---:|:---:|
| 0.5 | 8 | 8 | 0 | 0 | pass |
| 1 | 8 | 8 | 0 | 0 | pass |

- Total: 16/16 fully audited; every workflow has an observed tx hash, 8 stage states, receipt-event agreement, and matching sink state. Sink became queryable before receipt-tree completion in 0 workflow(s).
- RPC ledger: 16 send_tx HTTP attempts, 0 HTTP error records, 0 same-payload retry(s); all 16 rows link to a ledger transaction hash.
- Highest block height sampled in audit data: 291. Startup RPC status confirmed protocol 86.

## Output checksums

- Raw: `ab41abd14af84fa7713b30b135bf320a359b34a95c17a25f60b3b503e632e4e2`
- Results: `f5a5d53a07a92d4a667f90f55872f5c03b453291e2f22451ff0ac0f82af7ba34`
- Audit checkpoint: `160cdd7c60bf508995535497ba21c1a07414c71f12ae4d2334d256572816ed5c`
- Effective startup config: `452c93054f7128a9e0f0e437f555cd931837e9a2038d735cdc9f575365b2b60f`
