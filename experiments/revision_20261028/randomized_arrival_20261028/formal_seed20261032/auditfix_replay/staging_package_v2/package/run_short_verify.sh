#!/usr/bin/env bash
set -euo pipefail
RUN_DIR=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/seed20261032_near_auditfix_verify2
INPUT="$RUN_DIR/input"
RESULTS="$RUN_DIR/results"
MANIFEST="$RUN_DIR/manifest.json"
HOST=/home/starshell/das_independent_stage_evm_20261027
DAG="$HOST/experiments/revision_20261027/dag_workflow_20261027"
PROFILE="$DAG/profile_manifest.json"
RUNNER="$INPUT/run_randomized_near_auditfix.mjs"
SCHEDULE="$INPUT/poisson_d8_seed20261032_smoke_prefix.json"
WASM="$INPUT/artifacts/stage_contract.wasm"
WRAPPER="$INPUT/near-sandbox-compatible.remote.sh"
NODE_MODULES=/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules
NEAR_SANDBOX_PACKAGE_JSON="$NODE_MODULES/near-sandbox/package.json"

mkdir -p "$RESULTS"
python3 - "$MANIFEST" "$RUNNER" "$SCHEDULE" "$WASM" "$WRAPPER" <<'PY'
import hashlib,json,sys
from pathlib import Path
m=json.loads(Path(sys.argv[1]).read_text())
paths=[Path(x) for x in sys.argv[2:]]
assert str(paths[2])==m['wasm_path'], (str(paths[2]),m['wasm_path'])
for key,p in zip(('runner_sha256','schedule_sha256','wasm_sha256','wrapper_sha256'),paths):
    got=hashlib.sha256(p.read_bytes()).hexdigest()
    assert got==m[key], f'{key}: expected {m[key]} got {got}'
assert m['runner_wasm_reference']=='new URL(\'artifacts/stage_contract.wasm\', ROOT)'
assert all(p.is_file() and p.stat().st_size for p in paths)
print(json.dumps({'assertions':'pass','runner_path':str(paths[0]),'wasm_path':str(paths[2]),'wasm_sha256':m['wasm_sha256']},indent=2))
PY

test -L "$INPUT/node_modules"
test "$(readlink -f "$INPUT/node_modules")" = "$NODE_MODULES"
test -r "$NEAR_SANDBOX_PACKAGE_JSON"
EXPECTED_NEAR_PACKAGE_SHA=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["near_sandbox_package_json_sha256"])' "$MANIFEST")
ACTUAL_NEAR_PACKAGE_SHA=$(sha256sum "$NEAR_SANDBOX_PACKAGE_JSON" | awk '{print $1}')
test "$EXPECTED_NEAR_PACKAGE_SHA" = "$ACTUAL_NEAR_PACKAGE_SHA"
(cd "$INPUT" && node --input-type=module -e "await import('near-sandbox'); console.log('near-sandbox import: PASS')")

export PATH="$HOST/.local/bin:$PATH" PYTHONPATH="$DAG${PYTHONPATH:+:$PYTHONPATH}"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
export STRESS_PIPELINE_MODE=1 STRESS_PIPELINE_PREHEAT_SEC=0 STRESS_PIPELINE_OBSERVATION_SEC=20
export STRESS_PROFILE_MANIFEST_PATH="$PROFILE" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
export STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5
export STRESS_TAG=d8_seed20261032_auditfix_staged_verify2 STRESS_OUTPUT_DIR="$RESULTS"
export STRESS_WORKERS=64 STRESS_RECEIPT_TIMEOUT_MS=120000 STRESS_RPC_PROXY_TIMEOUT_MS=60000
export STRESS_READ_TIMEOUT_MS=15000 STRESS_READ_ATTEMPTS=3 STRESS_DIAGNOSTIC_LIGHT=0
export NEAR_SANDBOX_BIN_PATH="$WRAPPER"
{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  hostname
  cat /proc/loadavg
  cat /sys/fs/cgroup/cpu.max
  cat /sys/fs/cgroup/cpuset.cpus.effective
  node --version
  "$WRAPPER" --version
  sha256sum "$RUNNER" "$SCHEDULE" "$WASM" "$WRAPPER" "$NEAR_SANDBOX_PACKAGE_JSON"
} > "$RESULTS/startup_manifest.txt"
node --check "$RUNNER"
node "$RUNNER" > "$RESULTS/runner.log" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$RESULTS/runner.pid"
{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  taskset -pc "$pid" 2>&1 || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max
  printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective
} > "$RESULTS/process_affinity_and_budget.txt"
for _ in $(seq 1 120); do
  child=$(pgrep -P "$pid" | head -n 1 || true)
  if [[ -n "$child" && -r "/proc/$child/cmdline" ]]; then
    cmd=$(tr '\0' ' ' < "/proc/$child/cmdline")
    port=$(printf '%s' "$cmd" | sed -n 's/.*--rpc-addr 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p')
    if [[ -n "$port" ]]; then
      printf '%s\n' "$cmd" > "$RESULTS/sandbox_process_commandline.txt"
      if curl -fsS --max-time 5 -X POST "http://127.0.0.1:$port" -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"status","params":[]}' > "$RESULTS/rpc_status_startup.json"; then break; fi
    fi
  fi
  sleep 1
done
set +e
wait "$pid"; code=$?
set -e
printf '%s\n' "$code" > "$RESULTS/exit_code.txt"
printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$code" > "$RESULTS/run_status.txt"
exit "$code"
