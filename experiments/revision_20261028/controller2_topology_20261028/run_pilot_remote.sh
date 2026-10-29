#!/usr/bin/env bash
set -Eeuo pipefail

TASK=/home/starshell/das_controller2_topology_20261028
RESULTS="$TASK/results/pilot"
HOME="$TASK/home"
TMPDIR="$TASK/tmp"
export HOME TMPDIR TMP="$TMPDIR"
export COREPACK_HOME=/home/starshell/.cache/node/corepack
export ANVIL_BIN=/home/starshell/das_independent_stage_evm_20261027/.local/bin/anvil
PYTHON_BIN=/home/starshell/das_independent_stage_evm_20261027/.venv/bin/python
export PATH="/home/starshell/das_independent_stage_evm_20261027/.local/node-v24.22.1/bin:$PATH"
export NEAR_SANDBOX_BIN_PATH=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/input/near-sandbox-compatible.remote.sh
export NEAR_SANDBOX_REAL_BIN=/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules/.pnpm/near-sandbox@0.3.4/node_modules/near-sandbox/bin/near-sandbox-2.13.4/near-sandbox
mkdir -p "$RESULTS" "$HOME" "$TMPDIR"

cd "$TASK"
date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS/started_at.txt"
{
  printf 'load='; cat /proc/loadavg
  grep Cpus_allowed_list /proc/self/status || true
  grep MemAvailable /proc/meminfo || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max 2>/dev/null || true
  ps -eo pid=,comm=,pcpu=,pmem= | grep -E 'near|anvil|python|node' | head -80 || true
} > "$RESULTS/resource_before.txt"

sha256sum profile_manifest.json near_runtime/src/lib.rs near_runtime/artifacts/stage_contract.wasm \
  near_runtime/artifacts/same_receiver_contract.wasm evm/contracts/DASDistributedCompositionHarness.sol \
  near_runtime/run_near_topology_pilot.mjs evm/run_evm_topology_pilot.py > "$RESULTS/input_sha256.txt"

cd "$TASK/near_runtime"
corepack pnpm@10.18.3 install --frozen-lockfile --ignore-scripts

cd "$TASK"
"$ANVIL_BIN" --version | head -1 > "$RESULTS/anvil_version.txt"
sha256sum "$ANVIL_BIN" > "$RESULTS/anvil_sha256.txt"
"$PYTHON_BIN" -c 'import importlib.metadata as m, web3, solcx; print("python=", __import__("sys").version.split()[0], "web3=", web3.__version__, "py-solc-x=", m.version("py-solc-x"))' > "$RESULTS/python_runtime.txt"

set +e
{ time taskset -c 16-19 env DAG_OUTPUT_TAG=near_topology_pilot DAG_REPEATS=1 \
  DAG_SESSIONS_OVERRIDE=1 DAG_OPERATIONS_OVERRIDE=16 NEARCORE_VERSION=2.13.4 NEAR_EXPECTED_PROTOCOL=86 \
  DAG_RUN_FAILURE_PROBE=0 node "$TASK/near_runtime/run_near_topology_pilot.mjs"; } \
  > "$RESULTS/near_driver.log" 2>&1
NEAR_RC=$?
echo "$NEAR_RC" > "$RESULTS/near_exit_code.txt"
if [[ $NEAR_RC -ne 0 ]]; then
  date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS/finished_at.txt"
  exit "$NEAR_RC"
fi

{ time taskset -c 16-19 env DAG_OUTPUT_TAG=evm_topology_pilot DAG_REPEATS=1 \
  DAG_RUN_FAILURE_PROBE=0 DAG_NEAR_RESULTS="$RESULTS/near_topology_pilot_results.json" SOLCX_BINARY_PATH=/home/starshell/.solcx \
  "$PYTHON_BIN" "$TASK/evm/run_evm_topology_pilot.py"; } > "$RESULTS/evm_driver.log" 2>&1
EVM_RC=$?
set -e
echo "$EVM_RC" > "$RESULTS/evm_exit_code.txt"

date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS/finished_at.txt"
{
  printf 'load='; cat /proc/loadavg
  grep Cpus_allowed_list /proc/self/status || true
  grep MemAvailable /proc/meminfo || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max 2>/dev/null || true
  ps -eo pid=,comm=,pcpu=,pmem= | grep -E 'near|anvil|python|node' | head -80 || true
} > "$RESULTS/resource_after.txt"
exit "$EVM_RC"
