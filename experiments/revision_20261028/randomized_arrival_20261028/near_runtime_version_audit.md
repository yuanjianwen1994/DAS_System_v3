# NEAR Sandbox version and audit evidence

This record distinguishes a runner's requested version from the binary and protocol observed during execution.

| Seed | Runner request | Binary / protocol evidence | Result status |
|---|---|---|---|
| 20261029 | 2.14.0 | No executable wrapper, binary output, process command line, or RPC `status` was preserved. | Actual binary/protocol unknown; don't call this a confirmed 2.14.0 run. |
| 20261030 | 2.13.4 | Captured wrapper resolves to 2.13.4; binary commit `44f7ae6cd7ef08bab604e20a473bf77e35d4c993`; RPC `status` showed protocol/latest 86. | Confirmed 2.13.4 / protocol 86; concurrent exploratory seed. |
| 20261031 | 2.13.4 | Captured process command line and binary output identify 2.13.4, commit `44f7ae6cd7ef08bab604e20a473bf77e35d4c993`; live RPC `status` showed protocol/latest 86. | Confirmed 2.13.4 / protocol 86; exclusive; full workflow and block audit completed. |
| 20261032 | 2.13.4 via per-run wrapper | Startup snapshot records binary 2.13.4, same commit, RPC protocol/latest 86; schedule and consumed offsets match the paired seed. | Confirmed 2.13.4 / protocol 86; exclusive; EVM audit complete, NEAR receipt audit limited. |

No seed 20261032 run used 2.14.0. Seed 32's startup record verifies pipeline mode, absent rate/warmup/measurement overrides, expected schedule SHA, both exact consumed offset traces, and the 780-second observation window. Its NEAR audit limitation is described in [the result note](randomized_d8_result_note.md), with unresolved workflow IDs in `formal_seed20261032/audit_classification.csv`.

The installed remote 2.13.4 executable was queried through the existing GLIBC 2.39 loader. It reports commit `44f7ae6cd7ef08bab604e20a473bf77e35d4c993`, `min_protocol=83`, and binary SHA-256 `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2`. The separate installed 2.14.0 executable reports commit `bc4e9e8cfb03d9e82f027883055624e18576993e`, `min_protocol=84`, and binary SHA-256 `797b9d7a9a23907048d68a427e86a55c94f1de232dbfb8b8a1eec09ecef3bf13`; those details do not establish that any completed run used 2.14.0.

The local `runtime_versions.json` describes a Darwin arm64 setup and is not evidence for these remote runs.
