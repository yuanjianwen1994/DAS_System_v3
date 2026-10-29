# C1 fresh chain-pair independent audit

## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-10-28
- Verification Status: ANALYZED
- Version Label: chain_fresh_audit_v1

## Validation Report

- **Source:** `figure_validation_chain_20261028` local archive, seeds `20261033` and `20261034`
- **Overall Confidence:** CAUTION
- **Scope:** read-only verification of the two EVM/NEAR fresh pairs. No experiment was rerun and no C1 file was edited.

### Summary

The archived measurements, per-workflow state evidence, schedule pairing, reported latency percentiles, and reported window-goodput values are reproducible from the local raw files. All 81 entries in `archive_manifest_sha256.txt` match their listed SHA-256 hashes. Both platforms have 753/753 audited full-phase workflows and 688/688 audited observation-offered workflows, with no business failures or pending audits.

Two limits matter for reuse. First, the formal NEAR per-run launch manifests are absent from the archive, so the formal runs' exact protocol and binary hash are not independently tied to those runs; the archived wrapper and process path point to Sandbox 2.13.4, and the expected binary hash is declared in the plan, but the only captured `--version`/hash output is for the excluded short checkpoint. Second, NEAR teardown was marked `forced_after_timeout` in both runs, and the logs say the Sandbox teardown call exceeded its 15-second bound. The total finalization windows were 188 seconds and 558 seconds, but include the offline-audit gate wait and must not be attributed wholly to teardown. The last resource samples still show the Sandbox child, with no post-finish process probe archived. Do not describe teardown as independently verified clean.

### Recomputed measurements

Latency is recalculated over successful workflows **offered during the 360-second observation cohort**, including workflows that reached a queryable sink during the subsequent drain. Percentiles reproduce the archived audit's nearest-order-statistic convention: sorted value at `round((n-1)q)`, with no interpolation.

| Seed | Platform | Full phase audited | Observation cohort audited | Successful sink completions inside window (warmup / observation cohort) | Goodput inside window | Cohort latency P50 / P95 |
|---|---|---:|---:|---:|---:|---:|
| 20261033 | EVM | 388/388 | 363/363 | 365 (2 / 363) | 1.0139/s | 693.320 / 1,435.086 ms |
| 20261033 | NEAR | 388/388 | 363/363 | 367 (12 / 355) | 1.0194/s | 9,658.763 / 10,065.888 ms |
| 20261034 | EVM | 365/365 | 325/325 | 323 (1 / 322) | 0.8972/s | 702.035 / 1,437.312 ms |
| 20261034 | NEAR | 365/365 | 325/325 | 322 (11 / 311) | 0.8944/s | 9,594.114 / 10,067.331 ms |

The P50/P95 values match `offline_audit_report.json` within rounding. The goodput numerator is the number of sink completions timestamped inside the measurement window divided by 360 seconds. It includes completions from warmup-offered work and can exclude observation-offered work that completes after the window. It is therefore distinct from the `363` or `325` observation offers and their eventual audited-success counts. The existing summary defines goodput this way, but a reader could conflate its adjacent count and rate columns; an explicit window-completion count and cohort split would remove that ambiguity.

### Evidence checks

| Check | Result | Evidence / limitation |
|---|---|---|
| Archive integrity | PASS | Recomputed SHA-256 for all 81 listed files; 0 missing or mismatched files. |
| Paired schedules | PASS, with bounded EVM timing deviation | For each seed, EVM and NEAR raw files refer to the same schedule SHA and exact planned-offset array. The NEAR trace checkpoints pass; their maximum row-offset deviations are 1.497 ms and 1.485 ms. EVM has no structured trace-validation object, so I independently matched rows by workflow sequence to the schedule; maximum `planned_at` deviation is 17.672 ms and 18.093 ms. |
| Runtime configuration | PARTIAL | EVM raw records Anvil 1.8.6, 1.0-second block time, Solidity 0.8.24, 64 workers, and 16 operations/node. NEAR raw records a 1-second block period; startup configs assert pipeline mode 1, 30-second warmup, 360-second observation, the selected schedule, and full diagnostics. The process command and archived wrapper point to `near-sandbox-2.13.4`. However, only `results/checkpoint/launch_manifest.txt` is archived; neither formal seed directory has a launch manifest. Thus formal NEAR protocol 86 and the expected SHA-256 `4e89fbc0…394cba2c2` are declared/pinned but not captured as per-run runtime output. The plan's claim that each formal launch manifest is retained is not supported by the current archive. |
| Runner provenance | PARTIAL | Each NEAR raw file records the expected runner SHA and it matches the archived runner. The EVM raw files do not record a runner SHA; the current local runner hash appears in the plan/audit output, but no formal launch manifest ties that hash to the remote process. |
| State and sink audits | PASS | EVM: every row has all eight stage events, matching state/event evidence, a passing pre-submit sink check, matching transaction identity/nonce, and transaction hash/outcome. NEAR: every row is complete and successful, matches its receipt/state evidence, has a queryable sink, and has 15 receipts. No full-phase failures or pending audits were found. |
| EVM event and gas accounting | PASS | 3,104 and 2,920 stage events equal eight events per workflow. Raw per-workflow gas sums to 436,411,122 and 410,541,532 total (about 1,124,771 gas/workflow). These are EVM-native values; the NEAR runner did not capture comparable native gas. |
| NEAR send attempts | PASS | 403 and 383 `send_tx` attempts; 15 and 18 HTTP 408 failures; the retry counter sums to the same 15 and 18. Every failed payload fingerprint has a later same-payload successful attempt with a transaction hash; no failed attempt was unmatched. All 33 full-phase transport errors recovered without a workflow failure or audit-pending row. |
| EVM RPC | PASS | The raw method counters sum to 47,060 calls and zero errors across the two runs. |
| Resource samples | PASS, sampled values only | Recomputed process-tree sampled maxima: EVM RSS 222.2/211.7 MiB and CPU 83%/83%; NEAR RSS 590.5/640.4 MiB and CPU 60%/62%. Samples are one-second process-tree observations, not dedicated host counters. |
| Audit gate ordering | PASS by code path and gate hash; event time not separately recorded | Each NEAR gate says `passed` and `allow_sandbox_teardown: true`; its report hash matches the pre-cleanup audit report. The runner writes the pre-gate raw/request, waits for that decision, and only then enters worker/proxy/Sandbox teardown. The final raw preserves the decision. There is no separate gate-acceptance timestamp, so the exact elapsed time between gate creation and cleanup start cannot be reconstructed. |
| Teardown completion | NOT VERIFIED | Both NEAR raws say `forced_after_timeout`, and the logs record `CLEANUP_PENDING|near_sandbox_teardown|timeout_ms=15000`. The broader `CLEANUP_START` to `CLEANUP_FINISH` finalization windows were 188.408 seconds and 557.882 seconds; these include waiting for the offline-audit gate, so they do not measure teardown duration alone. The last one-second resource sample before each finish still lists the Sandbox child PID. No post-finish process check is archived; `exit_code=0` alone does not establish that the child had exited. |

The paired P95 ratio is about 7.01× in each seed (NEAR/EVM); P50 ratios are about 13.94× and 13.67×. These are descriptive, configuration-specific differences. They are not evidence of statistical significance or a general architecture-level causal effect.

### Warnings

| Type | Detail | Affected |
|---|---|---|
| Goodput denominator | Window goodput counts sink completions in the window, including warmup work; it is not observation-cohort audited successes divided by 360. | Goodput interpretation |
| Runtime provenance | Formal NEAR version/protocol/hash and formal EVM runner hash lack per-run launch records in the archive. | Reproducibility / environment attribution |
| Cleanup status | NEAR teardown exceeded its bound and was forced; final child-process exit is not independently recorded. | Resource cleanup claim |
| Replication | There are two paired seed-level repeats. The 688 workflow observations per platform are nested within those two runs, not 688 independent experiment replications. | Generalization / inference |
| Missing inferential statistics | No confidence intervals, hypothesis tests, or multiplicity-controlled tests are reported. This audit recomputed descriptive statistics only. | Significance claims |

### Fallacy scan

Coverage: **11/11 checked**.

| Fallacy | Severity | Detail |
|---|---|---|
| Simpson's paradox | N/A | No subgroup aggregation or reversal claim is tested. |
| Ecological fallacy | N/A | The reported unit is the workflow; no individual-level inference is drawn from aggregate groups. |
| Berkson's paradox | N/A | No association is estimated from a selected clinical/admission-style population. |
| Collider bias | N/A | No conditioning or covariate adjustment is used. |
| Base-rate neglect | CAUTION | Keep nominal λ=1/s, realized observation arrivals, audited cohort successes, and completions/window as distinct denominators. |
| Regression to the mean | N/A | There is no selected-extreme pre/post design. |
| Survivorship bias | NOTE | Latencies include successful observation-cohort workflows only; all 688 were audited successes here, but these runs do not establish behavior under a failure-heavy or higher-load regime. |
| Look-elsewhere effect | N/A | This archive reports one workload/rate and two preselected seed pairs; it contains no multi-test significance search. |
| Garden of forking paths | NOTE | `run_plan.json` fixes the two seeds and configuration, but no independent preregistration artifact is archived. Treat this as a targeted supplemental validation, not a preregistered confirmatory study. |
| Correlation is not causation | CAUTION | The comparison supports observed differences under these two configured local runtimes, not a general causal attribution to an execution architecture. |
| Reverse causality | N/A | No directional observational relationship is being estimated. |

### Reproducibility

- **Method:** Read-only archive and code-path audit; no remote rerun.
- **Verdict:** CANNOT_VERIFY as an independent execution reproduction. The archived file hashes, metrics, row-level audits, and gate/report hashes were independently recomputed; this does not substitute for rerunning the experiment.

### Files examined

- `run_plan.json`, `validation_summary.md`, `archive_manifest_sha256.txt`
- `results/seed20261033/{evm,near}/` and `results/seed20261034/{evm,near}/` raw results, audit reports, run status, runner logs, startup config, and resource samples
- `input/run_randomized_evm.py`, `input/run_randomized_near_auditfix.mjs`, `input/near-sandbox-compatible.remote.sh`, `run_formal_pair_remote.sh`, and `offline_audit_pair.py`

