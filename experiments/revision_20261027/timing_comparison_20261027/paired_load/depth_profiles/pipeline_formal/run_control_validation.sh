#!/usr/bin/env bash
set -u
R=/home/starshell/das_independent_stage_evm_20261027
REV=$R/experiments/revision_20261027
T=$REV/timing_comparison_20261027
N=$REV/near_runtime_20261027
P=$T/paired_load/depth_profiles
ROOT=$P/pipeline_formal/control_validation
mkdir -p "$ROOT"
printf 'platform,route,profile,repeat,pid,exit_code,started_utc,finished_utc\n' > "$ROOT/runs.csv"
profile=dag-07-chain-d8-w1-theta1-nu1

O=$ROOT/d8_s1_evm_fused
mkdir -p "$O"
export PATH="$R/.local/bin:/home/starshell/.foundry/bin:$PATH" PYTHONPATH="$REV/dag_workflow_20261027"
export ANVIL_BIN="$R/.local/bin/anvil" STRESS_PROFILE_MANIFEST_PATH="$P/profile_manifest_depth20.json"
export STRESS_PROFILE_ID="$profile" STRESS_SCHEDULE_SCENARIOS=matched_low STRESS_REPEAT=301
export STRESS_TAG=d8_s1_evm_fused STRESS_OUTPUT_DIR="$O" STRESS_WORKERS=64 STRESS_RATE_OVERRIDE=1
export STRESS_WARMUP_OVERRIDE_SEC=30 STRESS_MEASUREMENT_OVERRIDE_SEC=60 STRESS_PIPELINE_MODE=1
export STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 STRESS_DIAGNOSTIC_LIGHT=1
export STRESS_RECEIPT_POLL_SEC=0.2 STRESS_RECEIPT_BATCH_SIZE=128 ANVIL_MAX_TRANSACTIONS=1000 STRESS_EVM_ROUTE=fused_single_contract
started=$(date -u +%FT%TZ)
"$R/.venv/bin/python" "$T/open_loop_evm_pipeline_control.py" > "$O/runner.log" 2>&1 & pid=$!; echo "$pid" > "$O/runner.pid"
/bin/bash "$T/monitor_host_resources.sh" "$pid" "$O/resources.csv" > "$O/monitor.log" 2>&1 & mpid=$!; echo "$mpid" > "$O/monitor.pid"
wait "$pid"; code=$?; wait "$mpid" 2>/dev/null || true
finished=$(date -u +%FT%TZ); echo "$code" > "$O/exit.code"
printf 'EVM,fused_single_contract,%s,301,%s,%s,%s,%s\n' "$profile" "$pid" "$code" "$started" "$finished" >> "$ROOT/runs.csv"

O=$ROOT/d8_s1_near_same_receiver
mkdir -p "$O"
export PATH="$R/.local/bin:$PATH" NEAR_SANDBOX_BIN_PATH="$R/.local/bin/near-sandbox-glibc239"
export STRESS_PROFILE_MANIFEST_PATH="$P/profile_manifest_depth20.json"
export STRESS_PROFILE_ID="$profile" STRESS_SCHEDULE_SCENARIOS=matched_low STRESS_REPEAT=301
export STRESS_TAG=d8_s1_near_same_receiver STRESS_OUTPUT_DIR="$O" STRESS_WORKERS=64 STRESS_RATE_OVERRIDE=1
export STRESS_WARMUP_OVERRIDE_SEC=30 STRESS_MEASUREMENT_OVERRIDE_SEC=60 STRESS_PIPELINE_MODE=1
export STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 STRESS_DIAGNOSTIC_LIGHT=1 STRESS_NEAR_ROUTE=same_receiver
started=$(date -u +%FT%TZ)
(cd "$N" && node "$N/open_loop_near_pipeline_control.mjs") > "$O/runner.log" 2>&1 & pid=$!; echo "$pid" > "$O/runner.pid"
/bin/bash "$T/monitor_host_resources.sh" "$pid" "$O/resources.csv" > "$O/monitor.log" 2>&1 & mpid=$!; echo "$mpid" > "$O/monitor.pid"
wait "$pid"; code=$?; wait "$mpid" 2>/dev/null || true
finished=$(date -u +%FT%TZ); echo "$code" > "$O/exit.code"
printf 'NEAR,same_receiver,%s,301,%s,%s,%s,%s\n' "$profile" "$pid" "$code" "$started" "$finished" >> "$ROOT/runs.csv"
