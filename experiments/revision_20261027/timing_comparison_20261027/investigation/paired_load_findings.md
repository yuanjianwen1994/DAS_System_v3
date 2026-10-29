# Paired workflow load results

## Measurement unit

The reported unit is one successfully completed end-to-end workflow, counted once after its sink stage becomes queryable. The measured workload is a four-contract linear cascade with 16 operations at each stage (64 stage operations per workflow). It uses the same 1-second block interval and paired offered-arrival schedules on both local simulators. A larger cascade must be measured directly; its depth is not a multiplier for a throughput ratio.

## Results

Goodput counts successful sink completions inside the measurement window. All submitted workflows in these runs eventually drained successfully, with zero transaction failures. The table reports the measured window goodput and sink latency across all successful rows, including those that completed during drain.

| Window | Offered workflows/s | NEAR goodput/s | EVM goodput/s | NEAR sink p50/p95 (s) | EVM sink p50/p95 (s) |
|---|---:|---:|---:|---:|---:|
| 20 s | 0.25 | 0.20 | 0.25 | 6.02 / 6.02 | 0.99 / 0.99 |
| 20 s | 0.5 | 0.35 | 0.50 | 6.02 / 6.11 | 0.99 / 1.00 |
| 20 s | 1 | 0.75 | 0.95 | 5.09 / 6.12 | 1.02 / 1.03 |
| 20 s | 2 | 1.40 | 2.00 | 5.59 / 6.09 | 0.49 / 0.99 |
| 10 s pilot | 4 | 1.80 | 3.90 | 5.59 / 6.01 | 0.50 / 1.03 |
| 10 s pilot | 8 | 3.60 | 3.60 | 5.51 / 6.00 | 5.81 / 10.17 |
| 10 s pilot | 16 | 6.50 | 3.50 | 5.59 / 6.03 | 16.67 / 28.72 |
| 30 s steady | 8 | 6.43 | 3.80 | 5.58 / 6.00 | 16.90 / 29.00 |
| 30 s steady | 16 | 12.80 | 3.60 | 5.59 / 6.02 | 47.32 / 86.27 |

At 8 workflows/s, the 10-second pilot had identical window goodput on both platforms. In the 30-second steady phase, NEAR reached 6.43 workflows/s and EVM 3.80 workflows/s. At 16 workflows/s, the steady-phase ratio was 12.80/3.60 = 3.56x. This is a measured four-stage result under these simulator settings, not an extrapolation to 20 stages or production networks.

The queue behavior differs at the highest offered load. NEAR's steady 16/s phase completed 384 of 480 submissions inside the 30-second window and drained the rest about 5.1 seconds later; its peak in-flight count was 98 of 256 senders. EVM completed 108 of 480 inside the window, had 198 in flight at window end, and took about 90.1 seconds to drain; its Anvil process reached about 125% CPU (more than one core), while overall host CPU remained low. Eventual success counts alone would hide this queue growth, so the graph and CSV keep goodput, latency, in-flight count, and drain time separate.

## Integrity and limits

- The low-load run audited 93 paired workflow rows; the high-load run audited 1,176 paired rows. Workflow IDs and session IDs matched across platforms, and the schedule hash matched the shared schedule file.
- Every pre-submit sink was empty. All successful NEAR workflows had seven successful receipt outcomes. The four node events matched the stored state, operations, checksums, and transaction identity on both platforms.
- Sender pools had 256 accounts per platform. At the highest NEAR steady point, peak in-flight work remained below that pool size and client queue delay averaged about 1.1 ms. EVM reached 256 in flight and its offered-to-submit queue delay averaged about 11.8 s at 16/s.
- These are single runs per point on local simulators. They do not estimate confidence intervals, network latency, validator diversity, or public-chain throughput. The 10-second pilot is transient and should not be compared with the 30-second steady phase as if they were identical windows.

Raw rows, audits, resource samples, the shared schedule, and the vector plot are stored beside this note.
