#!/usr/bin/env bash
set -u
R=/home/starshell/das_independent_stage_evm_20261027
REV=$R/experiments/revision_20261027
T=$REV/timing_comparison_20261027
N=$REV/near_runtime_20261027
P=$T/paired_load/depth_profiles
ROOT=$P/pipeline_formal
mkdir -p "$ROOT"
printf 'started_utc=%s\n' "$(date -u +%FT%TZ)" > "$ROOT/independent_matrix_provenance.txt"
sha256sum "$T/open_loop_evm_pipeline.py" "$N/open_loop_near_pipeline.mjs" "$P/profile_manifest_depth20.json" "$T/paired_load_schedule.json" >> "$ROOT/independent_matrix_provenance.txt"
printf 'evm_version=%s\n' "$("$R/.local/bin/anvil" --version | head -n 1)" >> "$ROOT/independent_matrix_provenance.txt"
printf 'near_sandbox_version=2.14.0\nnode_version=%s\n' "$(node --version)" >> "$ROOT/independent_matrix_provenance.txt"
printf 'platform,profile,repeat,tag,pid,exit_code,started_utc,finished_utc\n' > "$ROOT/independent_matrix_runs.csv"

run_evm() {
  profile=$1 depth=$2 rep=$3 tag=$4
  O=$ROOT/$tag
  mkdir -p "$O"
  export PATH="$R/.local/bin:/home/starshell/.foundry/bin:$PATH"
  export PYTHONPATH="$REV/dag_workflow_20261027"
  export ANVIL_BIN="$R/.local/bin/anvil" STRESS_PROFILE_MANIFEST_PATH="$P/profile_manifest_depth20.json"
  export STRESS_PROFILE_ID="$profile" STRESS_SCHEDULE_SCENARIOS=matched_low STRESS_REPEAT="$rep"
  export STRESS_TAG="$tag" STRESS_OUTPUT_DIR="$O" STRESS_WORKERS=64 STRESS_RATE_OVERRIDE=0.5,1
  export STRESS_WARMUP_OVERRIDE_SEC=30 STRESS_MEASUREMENT_OVERRIDE_SEC=60 STRESS_PIPELINE_MODE=1
  export STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60
  export STRESS_DIAGNOSTIC_LIGHT=1 STRESS_RECEIPT_POLL_SEC=0.2 STRESS_RECEIPT_BATCH_SIZE=128 ANVIL_MAX_TRANSACTIONS=1000
  started=$(date -u +%FT%TZ)
  "$R/.venv/bin/python" "$T/open_loop_evm_pipeline.py" > "$O/runner.log" 2>&1 &
  pid=$!; echo "$pid" > "$O/runner.pid"
  /bin/bash "$T/monitor_host_resources.sh" "$pid" "$O/resources.csv" > "$O/monitor.log" 2>&1 & mpid=$!; echo "$mpid" > "$O/monitor.pid"
  wait "$pid"; code=$?
  wait "$mpid" 2>/dev/null || true
  finished=$(date -u +%FT%TZ)
  echo "$code" > "$O/exit.code"
  printf 'EVM,%s,%s,%s,%s,%s,%s,%s\n' "$profile" "$rep" "$tag" "$pid" "$code" "$started" "$finished" >> "$ROOT/independent_matrix_runs.csv"
}

run_near() {
  profile=$1 depth=$2 rep=$3 tag=$4
  O=$ROOT/$tag
  mkdir -p "$O"
  export PATH="$R/.local/bin:$PATH" NEAR_SANDBOX_BIN_PATH="$R/.local/bin/near-sandbox-glibc239"
  export STRESS_PROFILE_MANIFEST_PATH="$P/profile_manifest_depth20.json"
  export STRESS_PROFILE_ID="$profile" STRESS_SCHEDULE_SCENARIOS=matched_low STRESS_REPEAT="$rep"
  export STRESS_TAG="$tag" STRESS_OUTPUT_DIR="$O" STRESS_WORKERS=64 STRESS_RATE_OVERRIDE=0.5,1
  export STRESS_WARMUP_OVERRIDE_SEC=30 STRESS_MEASUREMENT_OVERRIDE_SEC=60 STRESS_PIPELINE_MODE=1
  export STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 STRESS_DIAGNOSTIC_LIGHT=1
  started=$(date -u +%FT%TZ)
  (cd "$N" && node "$N/open_loop_near_pipeline.mjs") > "$O/runner.log" 2>&1 &
  pid=$!; echo "$pid" > "$O/runner.pid"
  /bin/bash "$T/monitor_host_resources.sh" "$pid" "$O/resources.csv" > "$O/monitor.log" 2>&1 & mpid=$!; echo "$mpid" > "$O/monitor.pid"
  wait "$pid"; code=$?
  wait "$mpid" 2>/dev/null || true
  finished=$(date -u +%FT%TZ)
  echo "$code" > "$O/exit.code"
  printf 'NEAR,%s,%s,%s,%s,%s,%s,%s\n' "$profile" "$rep" "$tag" "$pid" "$code" "$started" "$finished" >> "$ROOT/independent_matrix_runs.csv"
}

for platform in EVM NEAR; do
  for item in 'dag-07-chain-d8-w1-theta1-nu1 d8' 'dag-20-chain-d20-w1-theta1-nu1 d20'; do
    read -r profile depth <<< "$item"
    for rep in 1 2 3; do
      tag="${depth}_r${rep}_${platform,,}_independent"
      if [ "$platform" = EVM ]; then run_evm "$profile" "$depth" "$rep" "$tag"; else run_near "$profile" "$depth" "$rep" "$tag"; fi
    done
  done
done
printf 'finished_utc=%s\n' "$(date -u +%FT%TZ)" >> "$ROOT/independent_matrix_provenance.txt"
