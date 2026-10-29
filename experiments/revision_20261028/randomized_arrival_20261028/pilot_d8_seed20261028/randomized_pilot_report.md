# Randomized arrival d8 pilot

## Result

The d8 experiment retains a large EVM/NEAR latency difference under randomized open-loop arrivals. In this single-seed pilot, the measured-window median offer-to-sink latency was about 13.1× higher on NEAR at both tested rates. The p95 ratio was 9.5× at 0.5 workflow/s and 6.4× at 1 workflow/s. This is evidence that the difference is not only caused by fixed-period arrivals. It is not a stable multiplier estimate and does not support extrapolating d8 to d20.

| Offered rate | EVM p50 / p95 | NEAR p50 / p95 | p95 sample counts EVM / NEAR | p50 ratio | p95 ratio |
|---|---:|---:|---:|---:|---:|
| 0.5 workflow/s | 725.5 / 1,046.8 ms | 9,499.2 / 9,994.2 ms | 32 / 26 | 13.1× | 9.5× |
| 1 workflow/s | 729.1 / 1,556.1 ms | 9,590.0 / 10,020.7 ms | 56 / 50 | 13.2× | 6.4× |

Each rate had one independent seed. At p95, the upper tail therefore contains only a few observations. The 1 workflow/s EVM tail is notably wider than its median, which lowers the cross-platform p95 ratio; this is exactly why the rounded p95 figures should not be presented as a precise or invariant factor.

## Protocol and validation

- Both platforms used seed `20261028`, the same d8 workload, 64 senders, 15 seconds of warmup and 60 seconds of measurement, with rates of 0.5 and 1 workflow/s.
- The first offer began at the same randomized 0.516130527-second phase offset after runner startup. Exponential gaps produced interarrival CVs of 0.84 at both rates.
- Both runners used the same schedule file SHA-256: `d018b80e49346af61712bf4f9a5b4f31b57daaf0b5ec8303d78739897471ca0c`. The per-rate trace digests and all planned offsets match, and each platform recorded the same workflow ID set.
- All 36 and 72 offers per rate completed successfully on both platforms. There were no failed workflows, unfinished workflows, or pending audits in the corrected run.
- Sink observation uncertainty was at most 60.8 ms for the EVM receipt poller and 58–62 ms for NEAR's 50 ms sink poller. That is small relative to the approximately 9–10 second NEAR completion latency.
- Recorded block spans were EVM 11–90 and 91–174; NEAR 259–340 and 343–425. These include the phase's warmup and drain activity, so they describe the run span rather than just the 60-second observation window.

An initial NEAR attempt is retained under `near_invalid_fixed_fallback/` but excluded. A string-key mismatch made its 1 workflow/s phase fall back to fixed intervals. The runner now fails fast if a randomized rate has no unique seeded trajectory, validates trajectory bounds and ordering, and records a trace digest for each rate. The corrected same-seed run is under `near_corrected/`.

## Existing four-route controls

These earlier d8/d20 controls used a fixed 1 workflow/s cadence and three independent repeats. The table reports the median of the three run-level p95 values; it is separate from the randomized pilot above.

| Depth | EVM fused contract | EVM independent stages | NEAR same receiver | NEAR cross-contract |
|---|---:|---:|---:|---:|
| d8 | 1.225 s | 1.225 s | 3.015 s | 9.671 s |
| d20 | 1.012 s | 1.226 s | 3.014 s | 21.953 s |

The fixed-cadence d20 cross-contract result is about 17.9× the EVM independent-stage result. That is close enough to the reviewer concern that it should be treated as a real interpretation risk. The controls also show that NEAR same-receiver is about 3 seconds at both depths, while NEAR cross-contract grows from about 9.7 to 22.0 seconds. This points to the cross-contract dependency path as a major factor in this setup. EVM fused composition is a control implementation; the result does not establish a DAS-exclusive synchronous-composition benefit.

## How to interpret the cascade concern

This pilot supports a substantial d8 latency penalty for the tested NEAR cross-contract path under randomized arrivals. The measurements are consistent with receipt and block progression adding delay across dependent contract stages. They do not establish that each additional contract adds one fixed unit of latency, that the ratio remains constant across depths or loads, or that a 20-contract cascade is 20× slower. A d20 randomized experiment with independent seeds and enough completed observations would be needed before making that claim.

The raw workflow rows are retained without artificial jitter. Full-scale and zoom views show their actual spread; the zoom views intentionally use a restricted axis and should be read with the full-scale versions.

## Artifacts

- `../randomized_pilot_summary.csv`: per-rate latency, p95 sample counts, failures, sampling bounds and block spans.
- `../randomized_pilot_workflow_samples.csv`: all 216 EVM and NEAR workflow rows, including warmups, failures and schedule lag.
- `../randomized_pilot_trace_validation.csv`: schedule and per-rate trajectory hashes.
- `run_provenance.json`: runner, WASM, schedule and raw-output hashes plus runtime metadata.
- `../randomized_pilot_scatter_full.svg` and `../randomized_pilot_ecdf_full.svg`: full latency scale.
- `../randomized_pilot_scatter_zoom.svg` and `../randomized_pilot_ecdf_zoom.svg`: local scale for visible EVM variation.
- `evm/` and `near_corrected/`: raw runs, summaries, checkpoints, runner logs and remote resource samples.
- `near_invalid_fixed_fallback/`: preserved but excluded initial NEAR attempt and its reason.
- Four-route fixed-cadence source data and plots are in the existing [pipeline formal control folder](../../../revision_20261027/timing_comparison_20261027/paired_load/depth_profiles/pipeline_formal/).
