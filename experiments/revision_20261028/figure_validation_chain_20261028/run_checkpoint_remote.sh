#!/usr/bin/env bash
set -euo pipefail
ROOT=/home/starshell/figure_validation_chain_20261028
DAS_HOST_ROOT=/home/starshell/das_independent_stage_evm_20261027
DAS_DAG="$DAS_HOST_ROOT/experiments/revision_20261027/dag_workflow_20261027"
DAS_NEAR="$DAS_HOST_ROOT/experiments/revision_20261027/near_runtime_20261027"
SCHEDULE="$ROOT/input/poisson_d8_lambda1_checkpoint_seed20261033.json"
SCENARIO=poisson_d8_checkpoint
REPEAT=6
WORKERS=64
RUN="$ROOT/results/checkpoint"
EVM_OUT="$RUN/evm"
NEAR_OUT="$RUN/near"
mkdir -p "$EVM_OUT" "$NEAR_OUT"
test ! -e "$EVM_OUT/d8_lambda1_checkpoint_seed20261033_evm_evm_raw.json"
test ! -e "$NEAR_OUT/d8_lambda1_checkpoint_seed20261033_near_near_raw.json"
export PATH="$DAS_HOST_ROOT/.local/bin:$PATH"
export PYTHONPATH="$DAS_DAG${PYTHONPATH:+:$PYTHONPATH}"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
export STRESS_PIPELINE_MODE=1 STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=15
export STRESS_PROFILE_MANIFEST_PATH="$DAS_DAG/profile_manifest.json" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
export STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO="$SCENARIO" STRESS_REPEAT="$REPEAT" STRESS_WORKERS="$WORKERS"
export STRESS_RECEIPT_TIMEOUT_MS=120000 STRESS_RPC_PROXY_TIMEOUT_MS=60000 STRESS_READ_TIMEOUT_MS=15000 STRESS_READ_ATTEMPTS=3 STRESS_DIAGNOSTIC_LIGHT=0 STRESS_AUDIT_CHECKPOINT_EVERY=5

monitor_and_wait() {
  local out="$1"; shift
  "$@" > "$out/runner.log" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" > "$out/runner.pid"
  printf 'started_utc=%s\nrunner_pid=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$pid" > "$out/run_status.txt"
  vmstat -n 1 > "$out/host_vmstat_samples.txt" & local vm_pid=$!
  (
    descendants() {
      local parent="$1" child
      for child in $(pgrep -P "$parent" 2>/dev/null || true); do
        printf '%s\n' "$child"
        descendants "$child"
      done
    }
    while kill -0 "$pid" 2>/dev/null; do
      stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
      pids=$( { printf '%s\n' "$pid"; descendants "$pid"; } | paste -sd, - )
      printf '\nSAMPLE %s\n' "$stamp" >> "$out/process_resource_samples.txt"
      ps -p "$pids" -o pid,ppid,pcpu,pmem,rss,stat,etime,args >> "$out/process_resource_samples.txt" 2>&1 || true
      sleep 1
    done
  ) & local monitor_pid=$!
  set +e
  wait "$pid"; local code=$?
  set -e
  kill -TERM "$vm_pid" 2>/dev/null || true
  wait "$vm_pid" 2>/dev/null || true
  wait "$monitor_pid" 2>/dev/null || true
  printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$code" >> "$out/run_status.txt"
  return "$code"
}
{
  printf 'seed=20261033\nscenario=%s\nrepeat=%s\nblock_period_sec=1\nwarmup_sec=30\nobservation_sec=15\n' "$SCENARIO" "$REPEAT"
  printf 'node='; node --version
  printf 'near='; "$ROOT/input/near-sandbox-compatible.remote.sh" --version | head -n 1
  printf 'near_binary_sha256='; sha256sum "$DAS_NEAR/node_modules/.pnpm/near-sandbox@0.3.4/node_modules/near-sandbox/bin/near-sandbox-2.13.4/near-sandbox"
  sha256sum "$SCHEDULE" "$ROOT/input/run_randomized_evm.py" "$ROOT/input/run_randomized_near_auditfix.mjs" "$ROOT/input/artifacts/stage_contract.wasm"
} > "$RUN/launch_manifest.txt"
export DAG_ANVIL_BLOCK_TIME_SEC=1
monitor_and_wait "$EVM_OUT" env STRESS_TAG=d8_lambda1_checkpoint_seed20261033_evm STRESS_OUTPUT_DIR="$EVM_OUT" ANVIL_BIN="$DAS_HOST_ROOT/.local/bin/anvil" "$DAS_HOST_ROOT/.venv/bin/python" "$ROOT/input/run_randomized_evm.py"
export STRESS_TAG=d8_lambda1_checkpoint_seed20261033_near STRESS_OUTPUT_DIR="$NEAR_OUT" NEAR_SANDBOX_BIN_PATH="$ROOT/input/near-sandbox-compatible.remote.sh"
node --check "$ROOT/input/run_randomized_near_auditfix.mjs"
(cd "$ROOT/input" && node --input-type=module -e 'await import("near-sandbox")')
monitor_and_wait "$NEAR_OUT" node "$ROOT/input/run_randomized_near_auditfix.mjs"
