# Fresh chain-pair provenance supplement

This supplement adds records recovered read-only from the completed remote runs under `/home/starshell/figure_validation_chain_20261028`. It does not modify the original 81-entry archive manifest or the independent audit report.

## Per-run launch records

| Seed | Remote launch manifest | Local copy SHA-256 | EVM runner SHA recorded at launch | Recorded EVM version | Recorded NEAR version / binary SHA-256 |
|---|---|---|---|---|---|
| 20261033 | `results/seed20261033/launch_manifest.txt` | `0c44f753f3ae0fd04bab57608da2b4b66af2edc74802fc3f48e909fe91778376` | `64fb2582c2a41d13ea885e4a3d59015bd5d085cb84903595b2442d709934da2c` | Anvil 1.8.6 | NEAR Sandbox 2.13.4 / protocol 86; `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2` |
| 20261034 | `results/seed20261034/launch_manifest.txt` | `916d944743d9e50af493a3282d365da8bf8991708f87991016c8df1d546819a0` | `64fb2582c2a41d13ea885e4a3d59015bd5d085cb84903595b2442d709934da2c` | Anvil 1.8.6 | NEAR Sandbox 2.13.4 / protocol 86; `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2` |

The launch manifests record the EVM runner source hash and Anvil version. They do **not** record an Anvil executable SHA-256; no such hash was found in either formal run directory. Do not substitute a hash measured later. The NEAR binary version, protocol, and binary SHA-256 are present in both formal launch manifests.

## RPC and raw-result evidence

The EVM raw/results files contain `rpc_load_path_by_method` and `rpc_post_drain_audit_by_method`. The NEAR raw/results files contain those RPC counters plus the `rpc_send_tx_request_ledger` and `rpc_http_error_ledger`. Their original remote paths and SHA-256 hashes are:

| Seed | Platform / remote raw path | SHA-256 |
|---|---|---|
| 20261033 | `results/seed20261033/evm/d8_lambda1_seed20261033_evm_evm_raw.json` | `3734f04438a406073e940f1deddc0fb3e33f002f550368ee61815773bf6d9077` |
| 20261033 | `results/seed20261033/near/d8_lambda1_seed20261033_near_near_raw.json` | `36d9710cb4ab50bb5d09775d262b3db7e68c29c337365910d24bcc0d8ad0c39c` |
| 20261034 | `results/seed20261034/evm/d8_lambda1_seed20261034_evm_evm_raw.json` | `18bae7e22117f8e55e89115a552a261eb988c9c5c722dd1517d2eec80f4ebb89` |
| 20261034 | `results/seed20261034/near/d8_lambda1_seed20261034_near_near_raw.json` | `6155289cc745aea63724ef5e268f86355604bc8e169a340373cff8fd166d09dd` |

The raw files already exist in the local `figure_validation_chain_20261028/results/` archive. The supplemental directory also contains the per-run result JSON, effective NEAR startup configuration, audit gate/request, pre-cleanup report, run-status records, and final NEAR runner log. Their hashes are listed in `supplement_sha256.txt`.

## Finalization and process probe

No historical post-finish child-process probe file was found in either formal seed directory. Both NEAR logs record `CLEANUP_PENDING|near_sandbox_teardown|timeout_ms=15000` and final status `forced_after_timeout`.

The log spans from `CLEANUP_START` to `CLEANUP_FINISH` are 188.408 s for seed 20261033 and 557.882 s for seed 20261034. These intervals include the offline-audit gate wait; they are not teardown durations. The teardown call had a 15 s limit. Do not infer a finish-time child exit from these spans or from the exit code.

`current_pid_probe.txt` records a separate current-time check at `2026-10-28T13:54:53Z`. All eight known launcher, runner, and NEAR Sandbox PIDs were absent then. This is a current-state observation only; it does not establish when they exited.
