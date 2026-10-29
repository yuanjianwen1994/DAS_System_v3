# Cascade-depth pilot: end-to-end workflow results

## Measurement definition

Profiles are same-domain linear chains at depths d=2, 4, 8, and 20, with width=1, theta=1, and 16 operations per node. The offered rate is workflows per second. Each workflow is counted once when its final sink is successfully audited; internal contract calls and receipts are not added to goodput.

Runs used a 1-second block period, 5-second warmup, 30-second measurement window, and 64 sender accounts. Latency is offered-to-final-sink time in seconds (nearest-rank p50/p95). Goodput counts only sink completions inside the measurement window; eventual successes and drain time are shown separately.

Runtime and configuration provenance: Anvil 1.8.6, Solidity 0.8.24, NEAR Sandbox 2.14.0. Profile manifest SHA-256: `546227e9ba11e6e0b0241fbd5dbbb49d5fc8bcc3678ec306bf67e700ffdaa36a`; load schedule SHA-256: `7e76b41fa781210b441fffc974a6520e71db31b00ebc840c28d40a3980885b34`. EVM runner SHA-256: `6a0a13a2d747b835cf88ce621e80735f0c6087ca5ee2e20f5c8bd9c2cab317ed`; NEAR runner SHA-256: `31aa3c0da762086d2c57414d8066f3e9e7097bf9bb7a6f0de00a2fe4c82acaa7`.

Across the ten recorded runs, all 1,020 formal measurement rows (510 per platform) had unique workflow IDs, successful final sinks, and matching per-node state/event audits; there were no failed workflows.

## Results

| Run | Platform | Depth | Offered workflows/s | Window completions | Goodput/s | p50/p95 sink (s) | Eventual success / offered | Failures | Drain (s) | Raw SHA-256 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| d2-r33 | EVM | 2 | 0.5 | 15/15 | 0.500 | 1.22/1.23 | 15/15 | 0 | 0.00 | `f801e383eff93a3cb8d9b33aeef850dfced2f1b5c2ae6e6ea726262c8845223b` |
| d2-r33 | EVM | 2 | 1.0 | 29/30 | 0.967 | 1.22/1.23 | 30/30 | 0 | 0.22 | `f801e383eff93a3cb8d9b33aeef850dfced2f1b5c2ae6e6ea726262c8845223b` |
| d2-r33 | EVM | 2 | 2.0 | 58/60 | 1.933 | 0.57/1.23 | 60/60 | 0 | 0.22 | `f801e383eff93a3cb8d9b33aeef850dfced2f1b5c2ae6e6ea726262c8845223b` |
| d2-r33 | NEAR | 2 | 0.5 | 14/15 | 0.467 | 3.11/4.04 | 15/15 | 0 | 1.13 | `e0d1f159671af587d8928f891e475b29ed8a1f6433b69cd729165b951947b5ff` |
| d2-r33 | NEAR | 2 | 1.0 | 27/30 | 0.900 | 3.10/4.03 | 30/30 | 0 | 2.12 | `e0d1f159671af587d8928f891e475b29ed8a1f6433b69cd729165b951947b5ff` |
| d2-r33 | NEAR | 2 | 2.0 | 53/60 | 1.767 | 3.54/4.02 | 60/60 | 0 | 3.11 | `e0d1f159671af587d8928f891e475b29ed8a1f6433b69cd729165b951947b5ff` |
| d4-r34 | EVM | 4 | 0.5 | 15/15 | 0.500 | 1.22/1.23 | 15/15 | 0 | 0.00 | `49bbdc560ec49f2b9ee51db9a0b6d0123e689405b5ea7af778c9982f8b744e4e` |
| d4-r34 | EVM | 4 | 1.0 | 29/30 | 0.967 | 1.22/1.24 | 30/30 | 0 | 0.23 | `49bbdc560ec49f2b9ee51db9a0b6d0123e689405b5ea7af778c9982f8b744e4e` |
| d4-r34 | EVM | 4 | 2.0 | 60/60 | 2.000 | 0.84/1.22 | 60/60 | 0 | 0.00 | `49bbdc560ec49f2b9ee51db9a0b6d0123e689405b5ea7af778c9982f8b744e4e` |
| d4-r34 | NEAR | 4 | 0.5 | 13/15 | 0.433 | 5.10/6.04 | 15/15 | 0 | 3.12 | `3d487128334d19b56215da8f2e10263226980a550dba20b538efbd5834f675f9` |
| d4-r34 | NEAR | 4 | 1.0 | 25/30 | 0.833 | 5.13/6.04 | 30/30 | 0 | 4.14 | `3d487128334d19b56215da8f2e10263226980a550dba20b538efbd5834f675f9` |
| d4-r34 | NEAR | 4 | 2.0 | 48/60 | 1.600 | 5.54/6.02 | 60/60 | 0 | 5.04 | `3d487128334d19b56215da8f2e10263226980a550dba20b538efbd5834f675f9` |
| d8-r31 | EVM | 8 | 0.5 | 15/15 | 0.500 | 1.22/1.23 | 15/15 | 0 | 0.00 | `b0faa7a03a5b35f09b18a20a7cf6e77cc57a780718b8980c8adab074395a189b` |
| d8-r31 | EVM | 8 | 1.0 | 29/30 | 0.967 | 1.22/1.25 | 30/30 | 0 | 0.22 | `b0faa7a03a5b35f09b18a20a7cf6e77cc57a780718b8980c8adab074395a189b` |
| d8-r31 | EVM | 8 | 2.0 | 58/60 | 1.933 | 0.55/1.03 | 60/60 | 0 | 0.02 | `b0faa7a03a5b35f09b18a20a7cf6e77cc57a780718b8980c8adab074395a189b` |
| d8-r32 | NEAR | 8 | 0.5 | 11/15 | 0.367 | 9.60/9.68 | 15/15 | 0 | 7.68 | `2e177bf275bcde0ad52bab5324f2de0f249d0f4b16384dc8514c66115e7b4ec4` |
| d8-r32 | NEAR | 8 | 1.0 | 21/30 | 0.700 | 9.99/10.53 | 30/30 | 0 | 8.10 | `2e177bf275bcde0ad52bab5324f2de0f249d0f4b16384dc8514c66115e7b4ec4` |
| d8-r32 | NEAR | 8 | 2.0 | 40/60 | 1.333 | 9.56/10.52 | 60/60 | 0 | 9.52 | `2e177bf275bcde0ad52bab5324f2de0f249d0f4b16384dc8514c66115e7b4ec4` |
| d20-r35 | EVM | 20 | 0.5 | 15/15 | 0.500 | 0.93/1.22 | 15/15 | 0 | 0.00 | `3ba79c345e171bbc5947aa2dacad5c0a9c9e00750be94dbbf8f3eb9841a990cc` |
| d20-r35 | EVM | 20 | 1.0 | 29/30 | 0.967 | 1.22/1.22 | 30/30 | 0 | 0.22 | `3ba79c345e171bbc5947aa2dacad5c0a9c9e00750be94dbbf8f3eb9841a990cc` |
| d20-r35 | EVM | 20 | 2.0 | 29/60 | 0.967 | 15.64/28.95 | 60/60 | 0 | 29.97 | `3ba79c345e171bbc5947aa2dacad5c0a9c9e00750be94dbbf8f3eb9841a990cc` |
| d20-r35 | NEAR | 20 | 0.5 | 4/15 | 0.133 | 22.04/22.11 | 15/15 | 0 | 20.11 | `598529115f2cdf9f85ba36fc1b2bf6328b69dceaba81ecfe4f31a5f214436d67` |
| d20-r35 | NEAR | 20 | 1.0 | 9/30 | 0.300 | 21.95/22.01 | 30/30 | 0 | 21.01 | `598529115f2cdf9f85ba36fc1b2bf6328b69dceaba81ecfe4f31a5f214436d67` |
| d20-r35 | NEAR | 20 | 2.0 | 18/60 | 0.600 | 21.40/21.88 | 60/60 | 0 | 20.89 | `598529115f2cdf9f85ba36fc1b2bf6328b69dceaba81ecfe4f31a5f214436d67` |
| d20-repeat-r36 | EVM | 20 | 1.0 | 30/30 | 1.000 | 0.98/0.99 | 30/30 | 0 | 0.00 | `c2a010053149faf5b866a60473470b48767cce123f131fb8f7b78586fbc66569` |
| d20-repeat-r36 | EVM | 20 | 2.0 | 29/60 | 0.967 | 15.64/29.04 | 60/60 | 0 | 30.04 | `c2a010053149faf5b866a60473470b48767cce123f131fb8f7b78586fbc66569` |
| d20-repeat-r36 | NEAR | 20 | 1.0 | 9/30 | 0.300 | 21.78/21.86 | 30/30 | 0 | 20.86 | `2908796bc2a76818366eecb28587fbedad47bc52b27a1a99f8b245c8ffcfda12` |
| d20-repeat-r36 | NEAR | 20 | 2.0 | 18/60 | 0.600 | 21.37/21.86 | 60/60 | 0 | 20.87 | `2908796bc2a76818366eecb28587fbedad47bc52b27a1a99f8b245c8ffcfda12` |

## Reading the result

At 2 offered workflows/s, the d20 EVM first run and repeat both reached about 0.97 completed workflows/s in-window, with p95 latency about 29 seconds and a 30-second drain; d2 reached 1.93 workflows/s with p95 about 1.23 seconds. The d20 NEAR first run and repeat both reached 0.60 workflows/s, with p95 latency about 21.9 seconds and about 20.9 seconds of drain; d2 reached 1.77 workflows/s with p95 about 4.0 seconds. These runs show queueing and latency growth under this workload, not a 20-fold throughput gain.

This is a controlled pilot rather than a paper-ready statistical claim: d2/d4/d8 have one measured run per platform; d20 has one full three-rate run plus a repeat at 1 and 2 workflows/s. Use the repeated d20 points as a reproducibility check and retain the single-run depths as exploratory until additional repetitions are collected.

## Files

- [`depth_comparison_summary.csv`](depth_comparison_summary.csv)
- [`depth_comparison.svg`](depth_comparison.svg)
- Raw run files are stored in the per-run directories beside this report.

No files were staged, committed, or synchronized to Overleaf or GitHub.
