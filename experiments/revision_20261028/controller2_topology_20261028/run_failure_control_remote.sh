#!/usr/bin/env bash
set -Eeuo pipefail

TASK=/home/starshell/das_controller2_topology_20261028
RESULTS="$TASK/results/pilot"
HOME="$TASK/home"
TMPDIR="$TASK/tmp"
export HOME TMPDIR TMP="$TMPDIR"
export COREPACK_HOME=/home/starshell/.cache/node/corepack
export ANVIL_BIN=/home/starshell/das_independent_stage_evm_20261027/.local/bin/anvil
export PATH="/home/starshell/das_independent_stage_evm_20261027/.local/node-v24.22.1/bin:$PATH"
export NEAR_SANDBOX_BIN_PATH=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/input/near-sandbox-compatible.remote.sh
export NEAR_SANDBOX_REAL_BIN=/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules/.pnpm/near-sandbox@0.3.4/node_modules/near-sandbox/bin/near-sandbox-2.13.4/near-sandbox
PYTHON_BIN=/home/starshell/das_independent_stage_evm_20261027/.venv/bin/python
PROFILE=topology-fanout6-join-8n-pilot
mkdir -p "$RESULTS" "$HOME" "$TMPDIR" "$TASK/pids"
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
{ time taskset -c 16-19 env DAG_OUTPUT_TAG=near_topology_failure DAG_REPEATS=1 \
  DAG_PROFILE_REGEX="$PROFILE" DAG_INCLUDE_RECEIPT_DETAILS=1 DAG_SESSIONS_OVERRIDE=1 \
  DAG_OPERATIONS_OVERRIDE=16 DAG_RUN_FAILURE_PROBE=1 NEARCORE_VERSION=2.13.4 NEAR_EXPECTED_PROTOCOL=86 \
  node "$TASK/near_runtime/run_near_topology_pilot.mjs"; } \
  > "$RESULTS/near_topology_failure.log" 2>&1
NEAR_RC=$?
echo "$NEAR_RC" > "$RESULTS/near_topology_failure_exit_code.txt"
if [[ $NEAR_RC -ne 0 ]]; then exit "$NEAR_RC"; fi

{ time taskset -c 16-19 env DAG_OUTPUT_TAG=evm_topology_failure DAG_REPEATS=1 \
  DAG_PROFILE_REGEX="$PROFILE" DAG_RUN_FAILURE_PROBE=1 \
  DAG_NEAR_RESULTS="$RESULTS/near_topology_formal_results.json" SOLCX_BINARY_PATH=/home/starshell/.solcx \
  "$PYTHON_BIN" "$TASK/evm/run_evm_topology_pilot.py"; } \
  > "$RESULTS/evm_topology_failure.log" 2>&1
EVM_RC=$?
echo "$EVM_RC" > "$RESULTS/evm_topology_failure_exit_code.txt"
exit "$EVM_RC"
