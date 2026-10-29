# Continuous cascade pipeline measurements

Primary statistics include only the pre-registered r1/r2/r3 run directories. Validation (`s1`) and recovered repeats are listed separately and excluded from the primary median and main figure.

Latency is in seconds from workflow offer to first queryable final sink. Window completions include any workflow whose sink became queryable during the fixed observation window, including workflows offered during preheat. State and receipt audits run after drain.

Accepted-rate timestamps use each runtime's observable acceptance boundary: EVM transaction hash returned by the local node; NEAR transaction outcome's included block timestamp. This platform-specific boundary is retained explicitly in `pipeline_time_bins.csv`.

| Run | Platform | Route | Depth | Offered/s | Accepted/s | Window completions | Goodput/s | p50 (s) | p95 (s) | Audit passed | Failures | Audit pending | Queue at window end |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| d20_r1_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d20_r1_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 1.000 | 1.000 | 60 | 1.000 | 1.224 | 1.226 | 90/90 | 0 | 0 | 1 |
| d20_r1_near_domain_control | NEAR Sandbox | same_receiver | 20 | 0.500 | 0.500 | 30 | 0.500 | 2.301 | 2.440 | 45/45 | 0 | 0 | 1 |
| d20_r1_near_domain_control | NEAR Sandbox | same_receiver | 20 | 1 | 1.000 | 60 | 1 | 2.146 | 3.014 | 90/90 | 0 | 0 | 2 |
| d20_r2_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.225 | 1.228 | 45/45 | 0 | 0 | 0 |
| d20_r2_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 1.000 | 1.000 | 59 | 0.983 | 0.972 | 0.999 | 90/90 | 0 | 0 | 1 |
| d20_r2_near_domain_control | NEAR Sandbox | same_receiver | 20 | 0.500 | 0.500 | 30 | 0.500 | 2.283 | 2.428 | 45/45 | 0 | 0 | 1 |
| d20_r2_near_domain_control | NEAR Sandbox | same_receiver | 20 | 1 | 1.000 | 60 | 1 | 2.171 | 3.014 | 90/90 | 0 | 0 | 2 |
| d20_r3_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.225 | 1.227 | 45/45 | 0 | 0 | 0 |
| d20_r3_evm_domain_control | EVM / Anvil | fused_single_contract | 20 | 1.000 | 1.000 | 59 | 0.983 | 0.977 | 1.012 | 90/90 | 0 | 0 | 1 |
| d20_r3_near_domain_control | NEAR Sandbox | same_receiver | 20 | 0.500 | 0.500 | 30 | 0.500 | 2.290 | 2.428 | 45/45 | 0 | 0 | 1 |
| d20_r3_near_domain_control | NEAR Sandbox | same_receiver | 20 | 1 | 1.000 | 60 | 1 | 2.188 | 3.011 | 90/90 | 0 | 0 | 2 |
| d8_r1_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.223 | 1.225 | 45/45 | 0 | 0 | 0 |
| d8_r1_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 1.000 | 1.000 | 60 | 1.000 | 1.224 | 1.226 | 90/90 | 0 | 0 | 1 |
| d8_r1_near_domain_control | NEAR Sandbox | same_receiver | 8 | 0.500 | 0.500 | 30 | 0.500 | 2.309 | 2.437 | 45/45 | 0 | 0 | 1 |
| d8_r1_near_domain_control | NEAR Sandbox | same_receiver | 8 | 1 | 1.000 | 60 | 1 | 2.168 | 3.016 | 90/90 | 0 | 0 | 2 |
| d8_r2_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d8_r2_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 1.000 | 1.000 | 60 | 1.000 | 1.224 | 1.227 | 90/90 | 0 | 0 | 1 |
| d8_r2_near_domain_control | NEAR Sandbox | same_receiver | 8 | 0.500 | 0.500 | 30 | 0.500 | 2.307 | 2.445 | 45/45 | 0 | 0 | 1 |
| d8_r2_near_domain_control | NEAR Sandbox | same_receiver | 8 | 1 | 1.000 | 60 | 1 | 2.176 | 3.013 | 90/90 | 0 | 0 | 2 |
| d8_r3_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.223 | 1.225 | 45/45 | 0 | 0 | 0 |
| d8_r3_evm_domain_control | EVM / Anvil | fused_single_contract | 8 | 1.000 | 1.000 | 59 | 0.983 | 0.998 | 1.225 | 90/90 | 0 | 0 | 1 |
| d8_r3_near_domain_control | NEAR Sandbox | same_receiver | 8 | 0.500 | 0.500 | 30 | 0.500 | 2.311 | 2.441 | 45/45 | 0 | 0 | 1 |
| d8_r3_near_domain_control | NEAR Sandbox | same_receiver | 8 | 1 | 1.000 | 60 | 1 | 2.184 | 3.015 | 90/90 | 0 | 0 | 2 |
| d20_r1_evm_independent | EVM / Anvil | independent_stage | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.225 | 1.227 | 45/45 | 0 | 0 | 0 |
| d20_r1_evm_independent | EVM / Anvil | independent_stage | 20 | 1.000 | 1.000 | 61 | 1.017 | 1.224 | 1.226 | 90/90 | 0 | 0 | 0 |
| d20_r1_near_independent | NEAR Sandbox | independent_stage | 20 | 0.500 | 0.500 | 30 | 0.500 | 21.344 | 21.471 | 45/45 | 0 | 0 | 10 |
| d20_r1_near_independent | NEAR Sandbox | independent_stage | 20 | 1 | 1.000 | 60 | 1 | 21.846 | 21.965 | 90/90 | 0 | 0 | 21 |
| d20_r2_evm_independent | EVM / Anvil | independent_stage | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d20_r2_evm_independent | EVM / Anvil | independent_stage | 20 | 1.000 | 1.000 | 61 | 1.017 | 1.223 | 1.225 | 90/90 | 0 | 0 | 0 |
| d20_r2_near_independent | NEAR Sandbox | independent_stage | 20 | 0.500 | 0.500 | 30 | 0.500 | 21.318 | 21.437 | 45/45 | 0 | 0 | 10 |
| d20_r2_near_independent | NEAR Sandbox | independent_stage | 20 | 1 | 1.000 | 60 | 1 | 21.798 | 21.913 | 90/90 | 0 | 0 | 21 |
| d20_r3_evm_independent | EVM / Anvil | independent_stage | 20 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d20_r3_evm_independent | EVM / Anvil | independent_stage | 20 | 1.000 | 1.000 | 60 | 1.000 | 1.021 | 1.226 | 90/90 | 0 | 0 | 1 |
| d20_r3_near_independent | NEAR Sandbox | multi_contract | 20 | 0.500 | 0.500 | 30 | 0.500 | 21.297 | 21.411 | 45/45 | 0 | 0 | 10 |
| d20_r3_near_independent | NEAR Sandbox | multi_contract | 20 | 1 | 1.000 | 60 | 1 | 21.821 | 21.953 | 90/90 | 0 | 0 | 21 |
| d8_r1_evm_independent | EVM / Anvil | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.227 | 45/45 | 0 | 0 | 0 |
| d8_r1_evm_independent | EVM / Anvil | independent_stage | 8 | 1.000 | 1.000 | 59 | 0.983 | 1.020 | 1.225 | 90/90 | 0 | 0 | 1 |
| d8_r1_near_independent | NEAR Sandbox | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 9.325 | 9.414 | 45/45 | 0 | 0 | 4 |
| d8_r1_near_independent | NEAR Sandbox | independent_stage | 8 | 1 | 1.000 | 60 | 1 | 9.535 | 9.671 | 90/90 | 0 | 0 | 9 |
| d8_r2_evm_independent | EVM / Anvil | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d8_r2_evm_independent | EVM / Anvil | independent_stage | 8 | 1.000 | 1.000 | 60 | 1.000 | 1.224 | 1.225 | 90/90 | 0 | 0 | 1 |
| d8_r2_near_independent | NEAR Sandbox | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 9.318 | 9.437 | 45/45 | 0 | 0 | 4 |
| d8_r2_near_independent | NEAR Sandbox | independent_stage | 8 | 1 | 1.000 | 60 | 1 | 9.590 | 9.703 | 90/90 | 0 | 0 | 9 |
| d8_r3_evm_independent | EVM / Anvil | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 1.224 | 1.226 | 45/45 | 0 | 0 | 0 |
| d8_r3_evm_independent | EVM / Anvil | independent_stage | 8 | 1.000 | 1.000 | 60 | 1.000 | 1.224 | 1.225 | 90/90 | 0 | 0 | 1 |
| d8_r3_near_independent | NEAR Sandbox | independent_stage | 8 | 0.500 | 0.500 | 30 | 0.500 | 9.292 | 9.432 | 45/45 | 0 | 0 | 4 |
| d8_r3_near_independent | NEAR Sandbox | independent_stage | 8 | 1 | 1.000 | 60 | 1 | 9.502 | 9.595 | 90/90 | 0 | 0 | 9 |

## Reading cascade depth

Each workflow invokes one node operation at each cascade level. Moving from d8 to d20 therefore changes per-workflow work by 20/8 = 2.5x. Reported goodput counts completed workflows per second; it is not multiplied by cascade depth. Report workflow latency and per-node resource use alongside it, and do not read depth growth itself as a throughput speedup.

## Matched NEAR route controls at 1 workflow/s

Values below are medians across available repeats. `cross_contract` groups the independent-stage and multi-contract labels used by the runner versions; these rows all execute the per-level cascade across contracts. This is a route comparison, not a pure contract-count intervention.

| Route | Depth | Repeats | Median goodput (workflows/s) | Median offer-to-sink p95 (s) |
|---|---:|---:|---:|---:|
| same_receiver | 8 | 3 | 1.000 | 3.015 |
| same_receiver | 20 | 3 | 1.000 | 3.014 |
| cross_contract | 8 | 3 | 1.000 | 9.671 |
| cross_contract | 20 | 3 | 1.000 | 21.953 |

At this offered rate, the same-receiver d8 and d20 controls have nearly identical p95 latency, while the cross-contract d20 median is about 2.3x the cross-contract d8 median. Goodput remains about 1 workflow/s in both. The much larger cross-contract versus same-receiver latency reflects the route/deployment change in this setup; it does not show that adding cascade levels multiplies throughput loss one-for-one. Do not present this as a universal platform law.

## Resource and RPC context

Block gas values use each platform's native gas unit. Block samples are restricted to the observation window; host CPU is the peak across 1-second resource samples for the run. RPC totals are runner-observed request counts.

| Run | Blocks sampled | Mean block utilization | Mean block gas | Peak block gas | RPC calls | RPC errors | Peak host CPU (%) |
|---|---:|---:|---:|---:|---:|---:|---:|
| d20_r1_evm_domain_control | 60 | 0.017 | 517669.800 | 1035340 | 3814 | 1 | 9.810 |
| d20_r1_evm_domain_control | 60 | 0.035 | 1035335.683 | 1035340 | 7552 | 1 | 9.810 |
| d20_r1_near_domain_control | 60 | 0.002 | 2337607363624.733 | 4136742810455 | 225 | 0 | 7.890 |
| d20_r1_near_domain_control | 59 | 0.005 | 4767766794611.390 | 8703804676371 | 450 | 0 | 7.890 |
| d20_r2_evm_domain_control | 60 | 0.017 | 517670.000 | 1035340 | 3816 | 1 | 12.120 |
| d20_r2_evm_domain_control | 60 | 0.035 | 1035335.683 | 1035340 | 7490 | 1 | 12.120 |
| d20_r2_near_domain_control | 60 | 0.002 | 2337359202916.867 | 4136435052608 | 225 | 0 | 14.630 |
| d20_r2_near_domain_control | 59 | 0.005 | 4767454804640.593 | 8704144952058 | 450 | 0 | 14.630 |
| d20_r3_evm_domain_control | 60 | 0.017 | 517670.000 | 1035340 | 3836 | 1 | 22.500 |
| d20_r3_evm_domain_control | 60 | 0.035 | 1035335.683 | 1035340 | 7504 | 1 | 22.500 |
| d20_r3_near_domain_control | 60 | 0.002 | 2337314959694.083 | 4136175564874 | 225 | 0 | 18.180 |
| d20_r3_near_domain_control | 59 | 0.005 | 4768346757593.288 | 8749408523560 | 450 | 0 | 18.180 |
| d8_r1_evm_domain_control | 60 | 0.007 | 204664.300 | 409329 | 2196 | 1 | 12.120 |
| d8_r1_evm_domain_control | 60 | 0.014 | 409324.683 | 409329 | 4334 | 1 | 12.120 |
| d8_r1_near_domain_control | 60 | 0.002 | 1551380687351.433 | 2564959681160 | 225 | 0 | 10.530 |
| d8_r1_near_domain_control | 59 | 0.003 | 3161179833828.763 | 5653079889923 | 450 | 0 | 10.530 |
| d8_r2_evm_domain_control | 60 | 0.007 | 204664.500 | 409329 | 2196 | 1 | 11.760 |
| d8_r2_evm_domain_control | 60 | 0.014 | 409324.683 | 409329 | 4334 | 1 | 11.760 |
| d8_r2_near_domain_control | 60 | 0.002 | 1551244274418.067 | 2564266316093 | 225 | 0 | 8.110 |
| d8_r2_near_domain_control | 59 | 0.003 | 3161111137495.119 | 5654284941740 | 450 | 0 | 8.110 |
| d8_r3_evm_domain_control | 60 | 0.007 | 204664.500 | 409329 | 2195 | 1 | 12.120 |
| d8_r3_evm_domain_control | 60 | 0.014 | 409324.683 | 409329 | 4256 | 1 | 12.120 |
| d8_r3_near_domain_control | 60 | 0.002 | 1551191320396.583 | 2563888936433 | 225 | 0 | 7.890 |
| d8_r3_near_domain_control | 59 | 0.003 | 3161086399234.576 | 5652836817353 | 450 | 0 | 7.890 |
| d20_r1_evm_independent | 60 | 0.049 | 1455366.300 | 2910733 | 846 | 1 | 9.960 |
| d20_r1_evm_independent | 60 | 0.097 | 2910728.683 | 2910733 | 1621 | 1 | 9.960 |
| d20_r1_near_independent | 60 | 0.029 | 28839770545936.383 | 29100899239574 | 315 | 90 | 11.680 |
| d20_r1_near_independent | 60 | 0.058 | 57784417909321.883 | 57920949077676 | 630 | 180 | 11.680 |
| d20_r2_evm_independent | 60 | 0.049 | 1455366.500 | 2910733 | 847 | 1 | 22.220 |
| d20_r2_evm_independent | 60 | 0.097 | 2910728.683 | 2910733 | 1579 | 1 | 22.220 |
| d20_r2_near_independent | 60 | 0.029 | 28894569708534.566 | 29129041181973 | 315 | 90 | 16.670 |
| d20_r2_near_independent | 60 | 0.058 | 57894388645099.414 | 58031078272172 | 630 | 180 | 16.670 |
| d20_r3_evm_independent | 60 | 0.047 | 1406854.283 | 2910733 | 847 | 1 | 14.710 |
| d20_r3_evm_independent | 60 | 0.095 | 2862216.467 | 2910733 | 1595 | 1 | 14.710 |
| d20_r3_near_independent | 60 | 0.029 | 28866647280348.383 | 29100762540346 | 315 | 90 | 14.290 |
| d20_r3_near_independent | 60 | 0.058 | 57839020996901.297 | 57975410560134 | 630 | 180 | 14.290 |
| d8_r1_evm_independent | 60 | 0.019 | 562387.300 | 1124775 | 852 | 1 | 9.700 |
| d8_r1_evm_independent | 60 | 0.037 | 1124770.683 | 1124775 | 1566 | 1 | 9.700 |
| d8_r1_near_independent | 60 | 0.011 | 11351006894019.184 | 11607767519765 | 227 | 2 | 14.630 |
| d8_r1_near_independent | 60 | 0.023 | 22732742616019.883 | 22795017886947 | 450 | 0 | 14.630 |
| d8_r2_evm_independent | 60 | 0.019 | 562387.500 | 1124775 | 857 | 1 | 12.120 |
| d8_r2_evm_independent | 60 | 0.037 | 1124770.683 | 1124775 | 1631 | 1 | 12.120 |
| d8_r2_near_independent | 60 | 0.011 | 11327825422055.066 | 11607134336796 | 227 | 2 | 17.500 |
| d8_r2_near_independent | 60 | 0.023 | 22686896951188.832 | 22749585176502 | 450 | 0 | 17.500 |
| d8_r3_evm_independent | 60 | 0.019 | 562387.500 | 1124775 | 853 | 1 | 12.500 |
| d8_r3_evm_independent | 60 | 0.037 | 1124770.683 | 1124775 | 1607 | 1 | 12.500 |
| d8_r3_near_independent | 60 | 0.011 | 11350877679121.301 | 11607621654135 | 227 | 2 | 12.500 |
| d8_r3_near_independent | 60 | 0.023 | 22733327513487.234 | 22796110055461 | 450 | 0 | 12.500 |

### Interpreting NEAR `send_tx` RPC errors

The proxy marks an observation as an error when the upstream HTTP response is non-2xx or fetch throws; it does not retain response status/body or correlate attempts with a final response. In the registered NEAR d20 cross-contract runs, each 135-workflow repeat recorded 405 `send_tx` observations, of which 270 were marked errors (90 during the 45-workflow phase and 180 during the 90-workflow phase). All 45/45 and 90/90 workflows nevertheless completed with zero audited workflow failures and zero audit-pending rows. The installed near-api-js provider (`@near-js/providers` 1.0.3) retries HTTP 500/503/408 and fetch errors, up to 12 attempts; the runner uses a single custom `JsonRpcProvider`, not a multi-endpoint failover provider. This makes intermediate same-endpoint SDK retries the likely explanation, rather than 270 failed business operations. Because the proxy did not retain status codes, the exact mix cannot be verified per request. Report these as retried-or-unclassified transport observations, separate from workflow failures.

The short-window depth pilot remains separate and is not pooled with these continuous-pipeline runs.

## Retained high-load short-window diagnostics

The earlier 30-second pilot is retained as a separate queue/censoring diagnostic. At 2 offered workflows/s, EVM d20 completed 29/60 in-window (0.967/s, p95 28.95 s) versus d2 58/60 (1.933/s, p95 1.23 s); both d20 repeats required about 30 s to drain. NEAR d20 completed 18/60 (0.600/s, p95 21.88 s) versus d2 53/60 (1.767/s, p95 4.02 s), then required about 21 s to drain. The NEAR d20 window is left-censored by its roughly 22-second route latency; these figures are not steady-state capacity estimates. See `../depth_comparison_report.md` for the full pilot rows and hashes.

## Cleanup status

`d20_r1_near_independent`: cleanup status `None`, runner exit code `143`. Both 0.5/s and 1/s phases completed and passed state/event audit (45/45 and 90/90); no workflow failures or unfinished calls. Raw and result JSON were fully written. Exit 143 was caused by ending the runner/Sandbox during post-measurement teardown after it stopped making progress; it is a cleanup-only status, separate from workflow outcomes. Cleanup is separate from workflow and audit-pending counts.

`d20_r2_near_independent`: cleanup status `None`, runner exit code `143`. Both 0.5/s and 1/s phases completed and passed state/event audit (45/45 and 90/90); no workflow failures or unfinished calls. Raw and result JSON were fully written. Exit 143 was caused by ending the runner/Sandbox during post-measurement teardown after it stopped making progress; it is a cleanup-only status, separate from workflow outcomes. Cleanup is separate from workflow and audit-pending counts.

`d20_r3_near_independent`: cleanup status `None`, runner exit code `143`. Both 0.5/s and 1/s phases completed and passed state/event audit (45/45 and 90/90); no workflow failures or unfinished calls. Raw and result JSON were fully written. Exit 143 was caused by ending the runner/Sandbox during post-measurement teardown after it stopped making progress; it is a cleanup-only status, separate from workflow outcomes. Cleanup is separate from workflow and audit-pending counts.


## Supplemental validation and recovery runs (excluded from primary statistics)

| Run | Platform | Route | Rate (workflow/s) | Goodput/s | p95 (s) | Audited / rows | Failures | Audit pending | Cleanup |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| d8_s1_near_same_receiver | NEAR Sandbox | same_receiver | 1 | 1 | 2.38735197399999 | 90/90 | 0 | 0 | complete |
| d20_r2_near_recovered | NEAR Sandbox | multi_contract | 0.5 | 0.5 | 21.43561631799996 | 45/45 | 0 | 0 | forced_after_timeout |
| d20_r2_near_recovered | NEAR Sandbox | multi_contract | 1 | 1 | 21.958773762000025 | 90/90 | 0 | 0 | forced_after_timeout |
| d8_s1_pair_evm | EVM / Anvil | independent_stage | 1.0 | 1.0 | 1.225597710115835 | 90/90 | 0 | 0 | not_recorded |
| d8_s1_pair_near_rerun | NEAR Sandbox | independent_stage | 1 | 1 | 9.396512940999994 | 90/90 | 0 | 0 | not_recorded |
