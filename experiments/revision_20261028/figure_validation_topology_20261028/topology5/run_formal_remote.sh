#!/usr/bin/env bash
set -Eeuo pipefail

TASK=${DAG_TASK_ROOT:-/home/starshell/das_figure_validation_topology_20261028/topology5}
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
mkdir -p "$RESULTS" "$TASK_HOME_DIR" "$TASK_TMP_DIR" "$TASK/pids"
cd "$TASK"
sha256sum -c "$TASK/formal_expected_sha256.txt" > "$RESULTS/formal_preflight_sha256.txt"

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

snapshot > "$RESULTS/formal_resource_before.txt"
sha256sum profile_manifest.json near_runtime/src/lib.rs near_runtime/artifacts/stage_contract.wasm \
  near_runtime/artifacts/same_receiver_contract.wasm evm/contracts/DASDistributedCompositionHarness.sol \
  near_runtime/run_near_topology_pilot.mjs evm/run_evm_topology_pilot.py input/resource_monitor.py > "$RESULTS/formal_input_sha256.txt"

python3 "$TASK/input/resource_monitor.py" "$$" "$RESULTS/formal_resource_samples.csv" "$RESULTS/formal_resource_summary.json" &
MONITOR_PID=$!
finish() {
  rc=$?
  trap - EXIT
  snapshot > "$RESULTS/formal_resource_after.txt" 2>/dev/null || true
  if kill -0 "$MONITOR_PID" 2>/dev/null; then kill -TERM "$MONITOR_PID" 2>/dev/null || true; fi
  wait "$MONITOR_PID" 2>/dev/null || true
  date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS/formal_finished_at.txt"
  exit "$rc"
}
trap finish EXIT
echo "$$" > "$TASK/pids/formal_orchestrator.pid"
echo "$MONITOR_PID" > "$TASK/pids/formal_resource_monitor.pid"

cd "$TASK/near_runtime"
test -L "$TASK/near_runtime/node_modules"
test "$(readlink -f "$TASK/near_runtime/node_modules")" = /home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules
node --input-type=module -e "await import('near-sandbox')"
cd "$TASK"

"$ANVIL_BIN" --version | head -1 > "$RESULTS/formal_anvil_version.txt"
sha256sum "$ANVIL_BIN" > "$RESULTS/formal_anvil_sha256.txt"
"$PYTHON_BIN" -c 'import importlib.metadata as m, web3, solcx; print("python=", __import__("sys").version.split()[0], "web3=", web3.__version__, "py-solc-x=", m.version("py-solc-x"))' > "$RESULTS/formal_python_runtime.txt"

set +e
{ time taskset -c 16-19 env DAG_OUTPUT_TAG=near_figure_validation_topology5 DAG_REPEATS=5 DAG_INCLUDE_RECEIPT_DETAILS=1 \
  DAG_SESSIONS_OVERRIDE=1 DAG_OPERATIONS_OVERRIDE=16 NEARCORE_VERSION=2.13.4 NEAR_EXPECTED_PROTOCOL=86 \
  DAG_RUN_FAILURE_PROBE=0 node "$TASK/near_runtime/run_near_topology_pilot.mjs"; } \
  > "$RESULTS/near_topology_formal.log" 2>&1
NEAR_RC=$?
echo "$NEAR_RC" > "$RESULTS/near_topology_formal_exit_code.txt"
if [[ $NEAR_RC -ne 0 ]]; then exit "$NEAR_RC"; fi

{ time taskset -c 16-19 env DAG_OUTPUT_TAG=evm_figure_validation_topology5 DAG_REPEATS=5 DAG_RUN_FAILURE_PROBE=0 \
  DAG_NEAR_RESULTS="$RESULTS/near_figure_validation_topology5_results.json" SOLCX_BINARY_PATH=/home/starshell/.solcx \
  "$PYTHON_BIN" "$TASK/evm/run_evm_topology_pilot.py"; } > "$RESULTS/evm_topology_formal.log" 2>&1
EVM_RC=$?
echo "$EVM_RC" > "$RESULTS/evm_topology_formal_exit_code.txt"
exit "$EVM_RC"
