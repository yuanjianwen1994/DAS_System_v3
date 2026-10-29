#!/usr/bin/env bash
set -euo pipefail

BASE=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032
INPUT="$BASE/input"
RESULTS="$BASE/results"
EVM_OUT="$RESULTS/evm"
NEAR_OUT="$RESULTS/near_corrected"
HOST=/home/starshell/das_independent_stage_evm_20261027
DAG="$HOST/experiments/revision_20261027/dag_workflow_20261027"
NEAR_RUNTIME="$HOST/experiments/revision_20261027/near_runtime_20261027/randomized_arrival_20261028"
MANIFEST="$DAG/profile_manifest.json"
SCHEDULE="$INPUT/poisson_d8_seed20261032.json"

mkdir -p "$EVM_OUT" "$NEAR_OUT"
export PATH="$HOST/.local/bin:$PATH"
export PYTHONPATH="$DAG${PYTHONPATH:+:$PYTHONPATH}"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
unset STRESS_PIPELINE_PREHEAT_SEC STRESS_PIPELINE_OBSERVATION_SEC
export STRESS_PIPELINE_MODE=1
export STRESS_PIPELINE_PREHEAT_SEC=30
export STRESS_PIPELINE_OBSERVATION_SEC=780

sample_tree() {
  local pid="$1" out="$2" hostout="$3"
  printf 'timestamp,pid,ppid,cpu_pct,mem_pct,rss_kb,state,elapsed\n' > "$out"
  printf 'timestamp,vmstat_raw\n' > "$hostout"
  while kill -0 "$pid" 2>/dev/null; do
    local stamp
    stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    ps -eo pid=,ppid=,%cpu=,%mem=,rss=,stat=,etime= | awk -v t="$stamp" -v r="$pid" '$1==r || $2==r {printf "%s,%s,%s,%s,%s,%s,%s,%s\n",t,$1,$2,$3,$4,$5,$6,$7}' >> "$out"
    vmstat -n 1 2 | tail -n 1 | sed "s/^/$stamp,/" >> "$hostout"
    sleep 1
  done
}

record_affinity() {
  local pid="$1" path="$2"
  {
    printf 'Recorded %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    taskset -pc "$pid" 2>&1 || true
    printf '/sys/fs/cgroup/cpu.max='
    cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
    printf '/sys/fs/cgroup/cpuset.cpus.effective='
    cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true
    printf '/sys/fs/cgroup/memory.max='
    cat /sys/fs/cgroup/memory.max 2>/dev/null || true
  } > "$path"
}

run_evm() {
  env STRESS_PROFILE_MANIFEST_PATH="$MANIFEST" \
      STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1 \
      STRESS_SCHEDULE_PATH="$SCHEDULE" \
      STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5 \
      STRESS_TAG=d8_poisson_seed20261032_evm STRESS_OUTPUT_DIR="$EVM_OUT" \
      STRESS_WORKERS=64 ANVIL_BIN="$HOST/.local/bin/anvil" \
      "$HOST/.venv/bin/python" "$INPUT/run_randomized_evm.py" > "$EVM_OUT/runner.log" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" > "$EVM_OUT/runner.pid"
  record_affinity "$pid" "$EVM_OUT/process_affinity_and_budget.txt"
  sample_tree "$pid" "$EVM_OUT/process_cpu_memory.csv" "$EVM_OUT/host_vmstat.csv" &
  local monitor=$!
  set +e
  wait "$pid"
  local code=$?
  set -e
  wait "$monitor" || true
  printf '%s\n' "$code" > "$EVM_OUT/exit_code.txt"
  return "$code"
}

capture_near_rpc_status() {
  local pid="$1" out="$NEAR_OUT/rpc_status_startup.json"
  for _ in $(seq 1 180); do
    local child cmd port
    child=$(pgrep -P "$pid" | head -n 1 || true)
    if [[ -n "$child" && -r "/proc/$child/cmdline" ]]; then
      cmd=$(tr '\0' ' ' < "/proc/$child/cmdline")
      port=$(printf '%s' "$cmd" | sed -n 's/.*--rpc-addr 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p')
      if [[ -n "$port" ]]; then
        printf '%s\n' "$cmd" > "$NEAR_OUT/sandbox_process_commandline.txt"
        if curl -fsS --max-time 5 -X POST "http://127.0.0.1:$port" -H 'Content-Type: application/json' \
          --data '{"jsonrpc":"2.0","id":1,"method":"status","params":[]}' > "$out"; then
          return 0
        fi
      fi
    fi
    sleep 1
  done
  return 1
}

run_near() {
  cp "$INPUT/run_randomized_near.mjs" "$NEAR_RUNTIME/run_randomized_near.mjs"
  cp "$INPUT/artifacts/stage_contract.wasm" "$NEAR_RUNTIME/artifacts/stage_contract.wasm"
  "$INPUT/near-sandbox-compatible.remote.sh" --version > "$NEAR_OUT/near_binary_version.txt" 2>&1
  env STRESS_PROFILE_MANIFEST_PATH="$MANIFEST" \
      STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1 \
      STRESS_SCHEDULE_PATH="$SCHEDULE" \
      STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5 \
      STRESS_TAG=d8_poisson_seed20261032_near STRESS_OUTPUT_DIR="$NEAR_OUT" \
      STRESS_WORKERS=64 NEAR_SANDBOX_BIN_PATH="$INPUT/near-sandbox-compatible.remote.sh" \
      node "$NEAR_RUNTIME/run_randomized_near.mjs" > "$NEAR_OUT/runner.log" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" > "$NEAR_OUT/runner.pid"
  record_affinity "$pid" "$NEAR_OUT/process_affinity_and_budget.txt"
  sample_tree "$pid" "$NEAR_OUT/process_cpu_memory.csv" "$NEAR_OUT/host_vmstat.csv" &
  local monitor=$!
  capture_near_rpc_status "$pid" || printf 'RPC status capture failed\n' > "$NEAR_OUT/rpc_status_capture_error.txt"
  set +e
  wait "$pid"
  local code=$?
  set -e
  wait "$monitor" || true
  printf '%s\n' "$code" > "$NEAR_OUT/exit_code.txt"
  return "$code"
}

{
  printf 'seed=20261032\nprofile=dag-07-chain-d8-w1-theta1-nu1\nrates=0.5,1.0\nwarmup_sec=30\nmeasurement_sec=780\npipeline_mode=1 (paired randomized Poisson arrivals)\nworkload=8-stage serial chain, 7 dependent edges, 16 operations per stage\nnear_sandbox_requested=2.13.4\npython=3.10.12 (project .venv)\nweb3=7.16.0\nanvil=1.8.6\n' 
  sha256sum "$SCHEDULE" "$INPUT/run_randomized_near.mjs" "$INPUT/run_randomized_evm.py" "$INPUT/artifacts/stage_contract.wasm"
} > "$BASE/run_manifest.txt"

run_evm
run_near
