# Figure-validation d8 chain repeats

This isolated directory contains two pre-generated lambda=1 paired exponential schedules (seeds 20261033 and 20261034), the previously audited EVM/NEAR runners, and the unchanged stage WASM. The formal plan is two fresh EVM+NEAR runtime pairs, each with 30 s continuous warmup and 360 s observation. Both runtimes use a fixed 1 s block.

A short paired startup/audit checkpoint used the seed-33 trajectory prefix with 30 s warmup and 15 s measurement. It is diagnostic only and excluded from the two formal repeats.

Do not reuse another repeat's runtime. Do not change rates, depth, workload, or scenario. Keep raw full-phase and observation-window cohorts separate in downstream summaries. No manuscript or Overleaf files belong in this experiment directory.
