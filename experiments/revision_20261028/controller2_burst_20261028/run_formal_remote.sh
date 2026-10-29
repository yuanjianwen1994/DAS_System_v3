#!/usr/bin/env bash
set -uo pipefail

TASK=/home/starshell/das_controller2_burst_20261028
SHARED=/home/starshell/das_independent_stage_evm_20261027
INPUT="$TASK/input"
CPUSET=16-19
PY="$SHARED/.venv/bin/python"
NODE="$SHARED/.local/node-v24.22.1/bin/node"
ANVIL="$SHARED/.local/bin/anvil"
SANDBOX_BIN="$SHARED/.local/bin/near-sandbox-glibc239"
SOLCX_DIR=/home/starshell/.solcx
PROFILE="$INPUT/profile_manifest.json"
PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1

mkdir -p "$TASK/results/formal"
date -Is > "$TASK/results/formal/started_at.txt"
{
  echo "affinity_budget=$CPUSET"
  echo "cpu_quota=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || true)"
  echo "cpu_set=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true)"
  echo "memory_limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || true)"
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E '[r]un_randomized_(evm|near)\.py|[r]un_randomized_near\.mjs|[a]nvil|[n]ear-sandbox' || true
} > "$TASK/results/formal/resource_before.txt"
taskset -c "$CPUSET" vmstat 5 > "$TASK/results/formal/host_vmstat.csv" &
HOST_MON=$!
echo "$HOST_MON" > "$TASK/results/formal/host_monitor.pid"

sample_process() {
  local run_pid="$1" resource_file="$2" sandbox_pid_file="${3:-}"
  (while kill -0 "$run_pid" 2>/dev/null; do
    printf 'sample,%s\n' "$(date -Is)"
    ps -p "$run_pid" -o pid=,psr=,etime=,%cpu=,%mem=,rss=,stat=,args= || true
    if [[ -n "$sandbox_pid_file" && -s "$sandbox_pid_file" ]]; then
      local child_pid
      child_pid=$(cat "$sandbox_pid_file")
      ps -p "$child_pid" -o pid=,psr=,etime=,%cpu=,%mem=,rss=,stat=,args= || true
    fi
    sleep 5
  done) > "$resource_file" 2>&1 &
}

run_evm() {
  local seed="$1" repeat="$2" root="$TASK/results/formal/seed_$1" out="$TASK/results/formal/seed_$1/evm"
  local schedule="$INPUT/seed_$seed/burst_d8_seed$seed.json"
  mkdir -p "$out/anvil_state" "$TASK/home/formal_evm_$seed" "$TASK/tmp/formal_evm_$seed" "$TASK/cache/formal_evm_$seed"
  echo "$(date -Is) BEGIN evm seed=$seed" >> "$TASK/results/formal/progress.log"
  set +e
  taskset -c "$CPUSET" env \
    HOME="$TASK/home/formal_evm_$seed" TMPDIR="$TASK/tmp/formal_evm_$seed" XDG_CACHE_HOME="$TASK/cache/formal_evm_$seed" \
    SOLCX_BINARY_PATH="$SOLCX_DIR" \
    PYTHONPATH="$INPUT" STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID="$PROFILE_ID" \
    STRESS_SCHEDULE_PATH="$schedule" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
    STRESS_REPEAT="$repeat" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
    STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 \
    STRESS_DIAGNOSTIC_LIGHT=1 STRESS_RECEIPT_POLL_SEC=0.05 STRESS_RECEIPT_BATCH_SIZE=128 \
    STRESS_TIME_BIN_SEC=1 ANVIL_MAX_TRANSACTIONS=1000 ANVIL_BIN="$ANVIL" \
    ANVIL_WORK_DIR="$out/anvil_state" ANVIL_PID_FILE="$out/anvil.pid" \
    STRESS_OUTPUT_DIR="$out" STRESS_TAG=burst_formal_seed${seed}_evm \
    PATH="$SHARED/.local/bin:$PATH" "$PY" "$INPUT/run_burst_evm.py" > "$out/runner.log" 2>&1 &
  local run_pid=$!
  echo "$run_pid" > "$out/runner.pid"
  sample_process "$run_pid" "$out/task_resources.csv"
  wait "$run_pid"
  local rc=$?
  set -e
  echo "$rc" > "$out/exit_code.txt"
  echo "$(date -Is) END evm seed=$seed rc=$rc" >> "$TASK/results/formal/progress.log"
}

run_near() {
  local seed="$1" repeat="$2" route="$3" runner="$4"
  local outname="$5" out="$TASK/results/formal/seed_$1/$5"
  local schedule="$INPUT/seed_$seed/burst_d8_seed$seed.json"
  local home="$TASK/home/formal_${outname}_$seed" tmp="$TASK/tmp/formal_${outname}_$seed" cache="$TASK/cache/formal_${outname}_$seed"
  mkdir -p "$out" "$home" "$tmp" "$cache"
  echo "$(date -Is) BEGIN $outname seed=$seed" >> "$TASK/results/formal/progress.log"
  set +e
  taskset -c "$CPUSET" env \
    HOME="$home" TMPDIR="$tmp" TMP="$tmp" TEMP="$tmp" XDG_CACHE_HOME="$cache" \
    NEAR_HOME="$home" NEAR_SANDBOX_HOME="$home/sandbox" NEAR_WORKSPACES_CACHE_DIR="$cache/workspaces" \
    NEAR_SANDBOX_BIN_PATH="$SANDBOX_BIN" \
    STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID="$PROFILE_ID" \
    STRESS_SCHEDULE_PATH="$schedule" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
    STRESS_REPEAT="$repeat" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
    STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 \
    STRESS_DIAGNOSTIC_LIGHT=1 STRESS_TIME_BIN_SEC=1 STRESS_SINK_POLL_MS=50 \
    STRESS_AUDIT_CHECKPOINT_EVERY=10 NEAR_SANDBOX_PID_FILE="$out/sandbox.pid" \
    STRESS_NEAR_ROUTE="$route" STRESS_OUTPUT_DIR="$out" STRESS_TAG=burst_formal_seed${seed}_${outname} \
    "$NODE" "$INPUT/$runner" > "$out/runner.log" 2>&1 &
  local run_pid=$!
  echo "$run_pid" > "$out/runner.pid"
  sample_process "$run_pid" "$out/task_resources.csv" "$out/sandbox.pid"
  wait "$run_pid"
  local rc=$?
  set -e
  echo "$rc" > "$out/exit_code.txt"
  echo "$(date -Is) END $outname seed=$seed rc=$rc" >> "$TASK/results/formal/progress.log"
}

index=0
for seed in 20261101 20261102 20261103; do
  repeat=$((421 + index))
  run_evm "$seed" "$repeat"
  run_near "$seed" "$repeat" multi_contract run_burst_near.mjs near_independent
  run_near "$seed" "$repeat" same_receiver run_burst_near_control.mjs near_same_receiver
  index=$((index + 1))
done

date -Is > "$TASK/results/formal/finished_at.txt"
{
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E '[r]un_burst|[a]nvil|[n]ear-sandbox' || true
} > "$TASK/results/formal/resource_after.txt"
kill "$HOST_MON" 2>/dev/null || true
