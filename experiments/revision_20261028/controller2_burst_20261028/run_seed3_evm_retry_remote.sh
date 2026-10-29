#!/usr/bin/env bash
set -uo pipefail
TASK=/home/starshell/das_controller2_burst_20261028
SHARED=/home/starshell/das_independent_stage_evm_20261027
OUT="$TASK/results/formal/seed_20261103/evm"
SEED=20261103
REPEAT=423
CPUSET=16-19
PY="$SHARED/.venv/bin/python"
ANVIL="$SHARED/.local/bin/anvil"
SOLCX_DIR=/home/starshell/.solcx
PROFILE="$TASK/input/profile_manifest.json"
SCHEDULE="$TASK/input/seed_$SEED/burst_d8_seed$SEED.json"

mkdir -p "$OUT/anvil_state" "$TASK/home/formal_evm_$SEED" "$TASK/tmp/formal_evm_$SEED" "$TASK/cache/formal_evm_$SEED"
if [[ -f "$OUT/runner.log" && ! -f "$OUT/runner.solc_download_failed.log" ]]; then mv "$OUT/runner.log" "$OUT/runner.solc_download_failed.log"; fi
if [[ -f "$OUT/exit_code.txt" && ! -f "$OUT/exit_code.solc_download_failed.txt" ]]; then cp "$OUT/exit_code.txt" "$OUT/exit_code.solc_download_failed.txt"; fi
{
  date -Is
  echo "cpu_allowed=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true)"
  echo "selected_cpus=$CPUSET"
  echo "cpu_quota=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || true)"
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
  sha256sum "$SOLCX_DIR/solc-v0.8.24" "$SCHEDULE"
  ps -eo pid,etime,%cpu,%mem,stat,cmd | grep -E '[r]un_randomized|[r]un_burst|[n]ear-sandbox|[a]nvil' || true
} > "$OUT/resource_retry_before.txt"

set +e
taskset -c "$CPUSET" env \
  HOME="$TASK/home/formal_evm_$SEED" TMPDIR="$TASK/tmp/formal_evm_$SEED" XDG_CACHE_HOME="$TASK/cache/formal_evm_$SEED" \
  SOLCX_BINARY_PATH="$SOLCX_DIR" \
  PYTHONPATH="$TASK/input" STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1 \
  STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIOS=poisson_d8,burst_d8 \
  STRESS_REPEAT="$REPEAT" STRESS_WORKERS=64 STRESS_PIPELINE_MODE=1 \
  STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=60 \
  STRESS_DIAGNOSTIC_LIGHT=1 STRESS_RECEIPT_POLL_SEC=0.05 STRESS_RECEIPT_BATCH_SIZE=128 \
  STRESS_TIME_BIN_SEC=1 ANVIL_MAX_TRANSACTIONS=1000 ANVIL_BIN="$ANVIL" \
  ANVIL_WORK_DIR="$OUT/anvil_state" ANVIL_PID_FILE="$OUT/anvil.pid" \
  STRESS_OUTPUT_DIR="$OUT" STRESS_TAG=burst_formal_seed${SEED}_evm \
  PATH="$SHARED/.local/bin:$PATH" "$PY" "$TASK/input/run_burst_evm.py" > "$OUT/runner.log" 2>&1
rc=$?
set -e
echo "$rc" > "$OUT/exit_code.txt"
{
  date -Is
  echo "exit_code=$rc"
  echo "memory_current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || true)"
  echo "memory_available_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
} > "$OUT/resource_retry_after.txt"
exit "$rc"
