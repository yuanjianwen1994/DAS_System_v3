# Seed 20261032 NEAR audit state-machine repair

## Preservation

The original seed32 runner and its original `results/near_corrected` outputs remain unchanged. The repaired runner is a separate file, and smoke and full replay outputs use separate directories.

- Original runner: `input/run_randomized_near.mjs`
- Repaired runner: `input/run_randomized_near_auditfix.mjs`
- Original seed32 schedule SHA-256: `9684e531a7523cd2672ef91abd2a951224472550304a8c49833cac0cde823166`
- Original runner SHA-256 before repair: `05666ba84718ef479e311f45211456b3c896c10f0575dc7eec796260dc303aa0`

- Repaired runner SHA-256: `93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`

## Changes

- Both receipt-tree completion and first sink queryability must resolve before the workflow wait completes; the non-diagnostic branch now uses the same two-condition rule as the diagnostic branch.
- Sink reads are bounded by the same actual wall-clock deadline, and the runner records the start/deadline, resolved state, elapsed wait, sink read attempts, and error category.
- A receipt deadline expiry and an RPC/client error with unknown transaction outcome remain audit-pending evidence states; they are not reported as confirmed workflow execution failures.
- The RPC proxy records each `send_tx` request's request ID, HTTP status, duration, same-payload attempt count, response RPC error, returned transaction hash when present, and a payload fingerprint. HTTP error responses retain status and a bounded response excerpt. Raw signed transaction payloads are not stored.
- Workflow rows link to `send_tx` ledger entries when the returned transaction hash matches. Unmatched requests remain visible in the phase ledger.
- Startup assertions and an effective configuration snapshot record pipeline mode, schedule hash, expected trace counts, time windows, timeout settings, and environment overrides.
- At each phase checkpoint, the runner compares the scheduler's exact offsets to the loaded schedule and compares workflow planned timestamps to those offsets. A mismatch is saved and stops the runner before the next phase.
- Full replay can wait at an offline-audit gate before sandbox teardown. The sandbox is closed only after a passing audit gate; otherwise the runtime is preserved for supplemental audit.

## Static check

`node --check` passed for the repaired runner before remote execution. The seed32 source schedule checksum and file SHA-256 were verified unchanged.
