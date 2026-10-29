#!/usr/bin/env bash
set -Eeuo pipefail

TASK=${DAG_TASK_ROOT:-/home/starshell/das_figure_validation_topology_20261028/failure3}
RESULTS="$TASK/results/pilot"
TASK_HOME_DIR="$TASK/task_home"
TASK_TMP_DIR="$TASK/tmp"
mkdir -p "$TASK_HOME_DIR" "$TASK_TMP_DIR"
export TMPDIR="$TASK_TMP_DIR" TMP="$TASK_TMP_DIR"
export COREPACK_HOME=/home/starshell/.cache/node/corepack
export ANVIL_BIN=/home/starshell/das_independent_stage_evm_20261027/.local/bin/anvil
export PATH="/home/starshell/das_independent_stage_evm_20261027/.local/node-v24.22.1/bin:$PATH"
export NEAR_SANDBOX_BIN_PATH=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/input/near-sandbox-compatible.remote.sh
export NEAR_SANDBOX_REAL_BIN=/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules/.pnpm/near-sandbox@0.3.4/node_modules/near-sandbox/bin/near-sandbox-2.13.4/near-sandbox
PYTHON_BIN=/home/starshell/das_independent_stage_evm_20261027/.venv/bin/python
EXPECTED_NEAR_BINARY_SHA256=4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2
ACTUAL_NEAR_BINARY_SHA256=$(sha256sum "$NEAR_SANDBOX_REAL_BIN" | awk '{print $1}')
test "$ACTUAL_NEAR_BINARY_SHA256" = "$EXPECTED_NEAR_BINARY_SHA256"
PROFILE=topology-fanout6-join-8n-pilot
mkdir -p "$RESULTS" "$TASK_HOME_DIR" "$TASK_TMP_DIR" "$TASK/pids"
cd "$TASK"
sha256sum -c "$TASK/failure_expected_sha256.txt" > "$RESULTS/failure_preflight_sha256.txt"

snapshot() {
  {
    printf 'timestamp='; date -u +%Y-%m-%dT%H:%M:%SZ
    printf 'load='; cat /proc/loadavg
    grep Cpus_allowed_list /proc/self/status || true
    grep MemAvailable /proc/meminfo || true
    printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
    printf 'memory.max='; cat /sys/fs/cgroup/memory.max 2>/dev/null || true
    ps -eo pid=,comm=,pcpu=,pmem= | grep -E 'near|anvil|python|node' | head -80 || true
  }
}

snapshot > "$RESULTS/failure_resource_before.txt"
python3 "$TASK/input/resource_monitor.py" "$$" "$RESULTS/failure_resource_samples.csv" "$RESULTS/failure_resource_summary.json" &
MONITOR_PID=$!
echo "$$" > "$TASK/pids/failure_orchestrator.pid"
echo "$MONITOR_PID" > "$TASK/pids/failure_resource_monitor.pid"
finish() {
  rc=$?
  trap - EXIT
  snapshot > "$RESULTS/failure_resource_after.txt" 2>/dev/null || true
  if kill -0 "$MONITOR_PID" 2>/dev/null; then kill -TERM "$MONITOR_PID" 2>/dev/null || true; fi
  wait "$MONITOR_PID" 2>/dev/null || true
  date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS/failure_finished_at.txt"
  exit "$rc"
}
trap finish EXIT

set +e
{ time taskset -c 16-19 env DAG_OUTPUT_TAG=near_figure_validation_failure3 DAG_REPEATS=3 \
  DAG_PROFILE_REGEX="$PROFILE" DAG_INCLUDE_RECEIPT_DETAILS=1 DAG_SESSIONS_OVERRIDE=1 \
  DAG_OPERATIONS_OVERRIDE=16 DAG_RUN_FAILURE_PROBE=1 NEARCORE_VERSION=2.13.4 NEAR_EXPECTED_PROTOCOL=86 \
  node "$TASK/near_runtime/run_near_topology_pilot.mjs"; } \
  > "$RESULTS/near_topology_failure.log" 2>&1
NEAR_RC=$?
echo "$NEAR_RC" > "$RESULTS/near_topology_failure_exit_code.txt"
if [[ $NEAR_RC -ne 0 ]]; then exit "$NEAR_RC"; fi

{ time taskset -c 16-19 env DAG_OUTPUT_TAG=evm_figure_validation_failure3 DAG_REPEATS=3 \
  DAG_PROFILE_REGEX="$PROFILE" DAG_RUN_FAILURE_PROBE=1 \
  DAG_NEAR_RESULTS="/home/starshell/das_figure_validation_topology_20261028/topology5/results/pilot/near_figure_validation_topology5_results.json" SOLCX_BINARY_PATH=/home/starshell/.solcx \
  "$PYTHON_BIN" "$TASK/evm/run_evm_topology_pilot.py"; } \
  > "$RESULTS/evm_topology_failure.log" 2>&1
EVM_RC=$?
echo "$EVM_RC" > "$RESULTS/evm_topology_failure_exit_code.txt"
exit "$EVM_RC"
