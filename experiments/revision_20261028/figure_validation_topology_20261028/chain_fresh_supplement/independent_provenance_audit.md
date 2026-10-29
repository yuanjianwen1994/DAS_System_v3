# Independent audit of C1 provenance supplement

## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-10-28
- Verification Status: ANALYZED
- Version Label: chain_fresh_provenance_supplement_v1

## Scope and evidence

This is an additive read-only check of `provenance_supplement.md`, its listed hashes, two recovered launch manifests (seeds 20261033 and 20261034), raw/result files, and the current PID probe. It preserves `../chain_fresh_independent_audit.md` as the original v1 report and does not change its historical findings.

- Supplement manifest: 22/22 files re-hash correctly; missing/mismatched: 0.
- Launch manifests: seed 20261033 hash check True, seed 20261034 hash check True.
- Both launch records provide NEAR Sandbox 2.13.4, protocol 86 and binary SHA-256 `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2`; both provide EVM runner source SHA-256 `64fb2582c2a41d13ea885e4a3d59015bd5d085cb84903595b2442d709934da2c` and Anvil 1.8.6.
- Neither formal launch manifest records an Anvil executable SHA-256. The supplement explicitly leaves this field missing; no later measurement is substituted.
- The raw result files retain per-method RPC-load and post-drain counters; NEAR raw files also retain send-transaction and HTTP-error ledgers. Their archive hashes are part of the supplement manifest.

## Process cleanup evidence

The recovered run records show `forced_after_timeout` after a teardown call with a 15-second limit. The reported 188.408s and 557.882s `CLEANUP_START` to `CLEANUP_FINISH` spans include the offline-audit gate wait; they do not measure teardown duration. No finish-time child-process probe was found in the formal run directories.

`current_pid_probe.txt` is timestamped 2026-10-28T13:54:53Z and records all eight listed PIDs absent at that later observation. It supports only that those PIDs were absent at probe time; it does not establish historical exit time or prove clean teardown at run completion.

## Conclusion

The supplemental original launch records close the provenance gap for NEAR version/protocol/binary hash and EVM runner source hash for seeds 20261033/34. They do not close the Anvil executable-hash gap or the historical cleanup gap. The current PID probe adds a later point-in-time observation only. These findings supplement rather than replace the original 81-item audit and do not amount to an independent rerun.
