# d8 chain supplemental validation

## Scope

Two paired EVM/NEAR runs were completed at an offered rate of 1 workflow/s, with the same seed-specific exponential arrival schedule on both platforms, 30 s warmup, 360 s observation, 8 nodes, 16 operations per node, and a fixed 1 s block period. Both observation cohorts exceeded the 300-sample target. NEAR used Sandbox 2.13.4 / protocol 86; the binary SHA-256 is recorded in `run_plan.json` and each remote launch manifest.

The seed-33 15 s checkpoint was diagnostic only and is excluded below. Raw files, effective configurations, process/host resource samples, run status, and audit reports are retained per platform and seed. NEAR raw data was copied before gate approval; both audit gates passed before teardown. The runner reported `forced_after_timeout` because Sandbox teardown exceeded its 15 s limit; the associated process exited, and the final raw records the passed audit decision.

## Audited results

Latency is offered-to-sink-queryable for NEAR and offered-to-sink for EVM, measured over successful workflows in the 360 s observation cohort. Throughput is the runner's successful sink completions during the observation window divided by 360 s. The sample counts below distinguish scheduled observation arrivals from full-phase workflows.

| Seed | Platform | Full phase audited | Observation arrivals / audited successes | Successful workflows/s | Observation latency p50 / p95 | Receipts or stage events | Native gas |
|---|---|---:|---:|---:|---:|---:|---:|
| 20261033 | EVM | 388 / 388 | 363 / 363 | 1.014 | 693 ms / 1,435 ms | 3,104 stage events (8/workflow) | 436,411,122 total; 1.125M/workflow |
| 20261033 | NEAR | 388 / 388 | 363 / 363 | 1.019 | 9,659 ms / 10,066 ms | 5,820 receipts (15/workflow) | Not captured by this runner |
| 20261034 | EVM | 365 / 365 | 325 / 325 | 0.897 | 702 ms / 1,437 ms | 2,920 stage events (8/workflow) | 410,541,532 total; 1.125M/workflow |
| 20261034 | NEAR | 365 / 365 | 325 / 325 | 0.894 | 9,594 ms / 10,067 ms | 5,475 receipts (15/workflow) | Not captured by this runner |

All 753 full-phase workflows per platform were classified as audited successes, with zero audit-pending rows. EVM recorded 47,060 RPC calls and zero RPC errors across the two runs. NEAR recorded 786 `send_tx` HTTP attempts, 33 HTTP failures, and 33 same-payload retries; all workflows still passed receipt, eight-stage state, and sink audits. NEAR reported zero pending audits.

Sampled peak process-tree RSS was 212–222 MiB for EVM and 591–640 MiB for NEAR. The maximum sampled aggregate process `%CPU` was 83% for EVM and 62% for NEAR. Raw one-second process and host `vmstat` samples are retained; these are sampled process values rather than dedicated hardware counters.

## Interpretation

For this d8 workload, the eight-node chain creates eight stage events per EVM workflow; NEAR records 15 receipts per workflow because the complete receipt tree includes additional runtime receipts. Neither count represents completed workflows/s. The two repeats produced about 0.90–1.02 successful workflows/s while the offered workflow rate was 1/s. The measured result therefore does not support describing the eight-stage workflow as eight times the workflow throughput.

These runs do not test d20. A 20-stage workflow would create more stage executions and likely change latency, resource use, and completion rate, but a 20× throughput claim cannot be inferred from d8; it requires a separately designed and audited d20 experiment. No manuscript or Overleaf files were changed.

## Audit and archive

`offline_audit_pair.py` checks schedule/runner hashes, exact arrival-trace validation where available, all planned rows, per-workflow receipt/state/sink linkage, and final gate-backed cleanup status. The seed-specific audit reports and raw files are under `results/seed20261033/` and `results/seed20261034/`. `archive_manifest_sha256.txt` lists SHA-256 hashes for the complete local experiment archive.
