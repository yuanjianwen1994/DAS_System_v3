# Seed 20261032 full replay: platform-native metrics

The EVM side uses its already completed same-schedule baseline. The NEAR side uses the repaired full replay. These are descriptive system-specific measurements; gas accounting and receipt execution models differ by platform.

Latency p50/p95 use all fully audited successes in the full phase, including warmup rows and completions observed during drain; sample count is recorded as `offered_to_sink_ms.n` in the JSON. Measured-window goodput counts successful sink completions observed within the 780-second measurement window, divided by 780 seconds. These use different cohorts.

| Platform | λ | Offered | Successful workflows | Failures | Measured-window goodput (workflows/s) | p50 full-phase audited success (ms) | p95 full-phase audited success (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| EVM | 0.5 | 385 | 385 | 0 | 0.4717948717948718 | 571.9269050750881 | 1106.2666978687048 |
| EVM | 1 | 817 | 817 | 0 | 1.0012820512820513 | 687.033399939537 | 1625.8053448982537 |
| NEAR | 0.5 | 385 | 385 | 0 | 0.47307692307692306 | 9614.246960999968 | 10074.965357000008 |
| NEAR | 1 | 817 | 817 | 0 | 1.0115384615384615 | 9655.137402999913 | 10089.463804000057 |

## Native resource evidence

- EVM per-workflow transaction gas: reported as mean/p50/p95/max in JSON; process CPU and RSS are summarized from the original process monitor.
- NEAR native block gas: phase block-sample `gas_used`, `gas_limit`, and utilization distributions are in JSON; per-workflow receipt counts and receipt-tree timing are also included.
- NEAR process tree CPU: mean/p50/p95/max combined `%CPU` and RSS from 2491 time samples; host CPU busy and I/O wait are summarized separately.
- Full NEAR receipt/state audit: 1202 successful rows with stage/receipt-event agreement; see the audit report.

Latency, native gas, receipt counts, and CPU are reported separately. NEAR block-gas samples are not attributed to individual workflows.
