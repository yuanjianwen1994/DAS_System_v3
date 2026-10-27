# DAS System v3 experimental harness

This repository contains local EVM experiments for cross-shard transaction protocols and workloads. It is a research harness, not a production implementation of Polkadot XCM or Avalanche atomic import/export.

The `TELEPORT` and `ATOMIC` endpoints are abstractions. Any comparison must preserve the protocol's required safeguards and report its assumptions. See `experiments/revision_20261027/BASELINE_AND_PLAN.md` for the current baseline, comparison boundaries, and experiment sequence.

## Requirements

- Python 3.9 or newer
- Foundry's `anvil` local EVM node
- Python packages listed in `requirements.txt`
- Solidity compiler 0.8.24 (installed by `py-solc-x` on first compile)

Create an isolated Python environment and install dependencies:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Install Foundry using its official instructions: <https://getfoundry.sh/getting-started/installation/>

## Existing runners

```sh
python smoke_test_sota.py
python run_sota_comparison.py
```

The smoke script uses a short block interval for functional checks. It is not a performance result. The comparison runner's current defaults and effective Anvil settings are documented in `experiments/revision_20261027/BASELINE_AND_PLAN.md`; inspect the script before launching a long run.

## Migration-state checks

With Foundry on `PATH`, run the 1-second functional/adversarial checks and the separate 12-second-block microbenchmark:

```sh
python tests/migration_state_machine_smoke.py
python tests/formal_state_machine_sample.py
python tests/applicability_boundary_smoke.py
CROSS_CHAIN_SMOKE_ONLY=1 python tests/cross_chain_load_experiment.py
python tests/cross_chain_receipt_binding_audit.py
python tests/cross_chain_load_experiment.py
```

The smoke output and 12-second sample data are written under `experiments/revision_20261027/`. The latter starts three fresh Anvil processes and records three independent seeded repetitions. It measures same-EVM contract state transitions only; it is not a cross-chain throughput result.

The applicability script reports gas for three small transfer/business-call ratios. It is a contract-level cost check, not a queueing, shard-contention, or throughput experiment.

The cross-chain scripts start independent local Anvil processes with distinct chain IDs. The binding audit mutates every receipt field and checks wrong destination contexts and replay. The load runner performs 12-second-block trials at one and three concurrent journeys per model, with three fresh-node repeats per load. It takes several minutes and is a bounded concurrency comparison, not a capacity or production-bridge benchmark. `CROSS_CHAIN_SMOKE_ONLY=1` runs the faster one-second workflow/adversarial check.

## Results and reproducibility

Historical outputs are under `logs/`. Do not overwrite them. New experiment output should use a new dated/versioned directory and include source revision, configuration, seed, runtime/tool versions, wall-clock timestamps, sample counts, completion/failure counts, and analysis code. Report complete successful journeys per wall-clock second separately from protocol transactions per second. A timeout or incomplete journey is not a success.

Use only local development accounts and local nodes. Never place real keys or credentials in this repository.
