#!/usr/bin/env bash
set -euo pipefail

TASK=/home/starshell/das_controller2_burst_20261028
SHARED=/home/starshell/das_independent_stage_evm_20261027
INPUT="$TASK/input"
CPUSET=16-19
PY="$SHARED/.venv/bin/python"
NODE="$SHARED/.local/node-v24.22.1/bin/node"
ANVIL="$SHARED/.local/bin/anvil"
SCHEDULE="$INPUT/burst_d8_seed20261028.json"
PROFILE="$INPUT/profile_manifest.json"
PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
REPEAT=410

mkdir -p "$TASK/results/pilot_seed20261028/evm/anvil_state" \
  "$TASK/results/pilot_seed20261028/near_independent" \
  "$TASK/results/pilot_seed20261028/near_same_receiver" \
  "$TASK/tmp/evm" "$TASK/tmp/near_independent" "$TASK/tmp/near_same_receiver" \
  "$TASK/home/evm" "$TASK/home/near_independent" "$TASK/home/near_same_receiver" \
  "$TASK/cache/near_independent" "$TASK/cache/near_same_receiver"

date -Is > "$TASK/results/pilot_seed20261028/started_at.txt"
{
  echo "affinity_budget=$CPUSET"
  echo "cpu_allowed=$(awk '/Cpus_allowed_list/ {print $2}' /proc/self/status)"
  echo "cpu_quota=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || true)"
  echo "cpu_set=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true)"
  echo "memory_limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || true)"
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  echo "controller1_pid=2278199"
  ps -p 2278199 -o pid=,etime=,%cpu=,%mem=,stat=,cmd= || true
} > "$TASK/results/pilot_seed20261028/resource_before.txt"

taskset -c "$CPUSET" vmstat 5 > "$TASK/results/pilot_seed20261028/host_vmstat.csv" &
MON_PID=$!
echo "$MON_PID" > "$TASK/results/pilot_seed20261028/monitor.pid"

run_evm() {
  local dir="$TASK/results/pilot_seed20261028/evm"
  set +e
  taskset -c "$CPUSET" env \
    HOME="$TASK/home/evm" TMPDIR="$TASK/tmp/evm" XDG_CACHE_HOME="$TASK/cache/evm" \
    PYTHONPATH="$INPUT" \
    STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID="$PROFILE_ID" \
    STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
    STRESS_REPEAT="$REPEAT" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
    STRESS_PIPELINE_PREHEAT_SEC=10 STRESS_PIPELINE_OBSERVATION_SEC=20 \
    STRESS_DIAGNOSTIC_LIGHT=1 STRESS_RECEIPT_POLL_SEC=0.05 STRESS_RECEIPT_BATCH_SIZE=128 \
    STRESS_TIME_BIN_SEC=1 ANVIL_MAX_TRANSACTIONS=1000 ANVIL_BIN="$ANVIL" \
    ANVIL_WORK_DIR="$dir/anvil_state" ANVIL_PID_FILE="$dir/anvil.pid" \
    STRESS_OUTPUT_DIR="$dir" STRESS_TAG=burst_pilot_seed20261028_evm \
    PATH="$SHARED/.local/bin:$PATH" "$PY" "$INPUT/run_burst_evm.py" \
    > "$dir/runner.log" 2>&1 &
  local run_pid=$!
  echo "$run_pid" > "$dir/runner.pid"
  wait "$run_pid"
  local rc=$?
  set -e
  echo "$rc" > "$dir/exit_code.txt"
}

run_near() {
  local route="$1" runner="$2" repeat="$3" out="$TASK/results/pilot_seed20261028/near_$1"
  local home="$TASK/home/near_$1" tmp="$TASK/tmp/near_$1" cache="$TASK/cache/near_$1"
  mkdir -p "$out" "$home" "$tmp" "$cache"
  set +e
  taskset -c "$CPUSET" env \
    HOME="$home" TMPDIR="$tmp" TMP="$tmp" TEMP="$tmp" XDG_CACHE_HOME="$cache" \
    NEAR_HOME="$home" NEAR_SANDBOX_HOME="$home/sandbox" NEAR_WORKSPACES_CACHE_DIR="$cache/workspaces" \
    STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID="$PROFILE_ID" \
    STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
    STRESS_REPEAT="$repeat" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
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

run_evm
run_near multi_contract run_burst_near.mjs "$REPEAT"
run_near same_receiver run_burst_near_control.mjs "$REPEAT"

date -Is > "$TASK/results/pilot_seed20261028/finished_at.txt"
{
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E 'run_burst|anvil|near-sandbox' | grep -v grep || true
} > "$TASK/results/pilot_seed20261028/resource_after.txt"
kill "$MON_PID" 2>/dev/null || true
