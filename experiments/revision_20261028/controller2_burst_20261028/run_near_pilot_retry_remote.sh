#!/usr/bin/env bash
set -uo pipefail

TASK=/home/starshell/das_controller2_burst_20261028
SHARED=/home/starshell/das_independent_stage_evm_20261027
INPUT="$TASK/input"
CPUSET=16-19
NODE="$SHARED/.local/node-v24.22.1/bin/node"
SANDBOX_BIN="$SHARED/.local/bin/near-sandbox-glibc239"
SCHEDULE="$INPUT/burst_d8_seed20261028.json"
PROFILE="$INPUT/profile_manifest.json"
PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
REPEAT=410
ROOT="$TASK/results/pilot_seed20261028"

date -Is > "$ROOT/near_retry1_started_at.txt"
{
  echo "affinity_budget=$CPUSET"
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  echo "controller1_processes_at_retry_start:"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E '[r]un_randomized_(evm|near)\.py|[r]un_randomized_near\.mjs|[a]nvil|[n]ear-sandbox' || true
} > "$ROOT/near_retry1_resource_before.txt"
taskset -c "$CPUSET" vmstat 5 > "$ROOT/near_retry1_vmstat.csv" &
MON_PID=$!
echo "$MON_PID" > "$ROOT/near_retry1_monitor.pid"

run_near() {
  local route="$1" runner="$2" outname="near_$1"
  [[ "$route" == "multi_contract" ]] && outname=near_independent
  local out="$ROOT/$outname"
  local home="$TASK/home/retry1_$1" tmp="$TASK/tmp/retry1_$1" cache="$TASK/cache/retry1_$1"
  mkdir -p "$out" "$home" "$tmp" "$cache"
  set +e
  taskset -c "$CPUSET" env \
    HOME="$home" TMPDIR="$tmp" TMP="$tmp" TEMP="$tmp" XDG_CACHE_HOME="$cache" \
    NEAR_HOME="$home" NEAR_SANDBOX_HOME="$home/sandbox" NEAR_WORKSPACES_CACHE_DIR="$cache/workspaces" \
    NEAR_SANDBOX_BIN_PATH="$SANDBOX_BIN" \
    STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID="$PROFILE_ID" \
    STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
    STRESS_REPEAT="$REPEAT" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
    STRESS_PIPELINE_PREHEAT_SEC=10 STRESS_PIPELINE_OBSERVATION_SEC=20 \
    STRESS_DIAGNOSTIC_LIGHT=1 STRESS_TIME_BIN_SEC=1 STRESS_SINK_POLL_MS=50 \
    STRESS_AUDIT_CHECKPOINT_EVERY=10 NEAR_SANDBOX_PID_FILE="$out/sandbox.pid" \
    STRESS_NEAR_ROUTE="$route" STRESS_OUTPUT_DIR="$out" STRESS_TAG=burst_pilot_seed20261028_near \
    "$NODE" "$INPUT/$runner" > "$out/runner.log" 2>&1 &
  local run_pid=$!
  echo "$run_pid" > "$out/runner.pid"
  wait "$run_pid"
  local rc=$?
  set -e
  echo "$rc" > "$out/exit_code.txt"
}

run_near multi_contract run_burst_near.mjs "$REPEAT"
run_near same_receiver run_burst_near_control.mjs "$REPEAT"

date -Is > "$ROOT/near_retry1_finished_at.txt"
{
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E '[r]un_burst|[n]ear-sandbox' || true
} > "$ROOT/near_retry1_resource_after.txt"
kill "$MON_PID" 2>/dev/null || true
