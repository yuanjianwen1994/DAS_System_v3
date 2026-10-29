# Invalid NEAR pilot attempt

This attempt is preserved for provenance and excluded from the paired randomized-arrival analysis.

The schedule stored the 1 workflow/s trajectory under the JSON key `"1.0"`, while the NEAR runner looked up `String(1)`, which produced `"1"`. The lookup silently fell back to the runner's fixed-period schedule for that rate. The 0.5 workflow/s trajectory was selected correctly. The run therefore did not use the same randomized trace as EVM at both rates.

The corrected runner now resolves keys numerically, validates that each requested rate has exactly one seeded trajectory with valid warmup/measurement bounds, and records a per-rate trace digest. The corrected same-seed NEAR run is stored separately under `near_corrected/`.
