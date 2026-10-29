#!/usr/bin/env bash
set -euo pipefail
BASE=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/auditfix_replay
INPUT="$BASE/input"
RESULTS="$BASE/results"
EVM_OUT="$RESULTS/evm"
NEAR_OUT="$RESULTS/near_corrected"
HOST=/home/starshell/das_independent_stage_evm_20261027
DAG="$HOST/experiments/revision_20261027/dag_workflow_20261027"
NEAR_RUNTIME="$HOST/experiments/revision_20261027/near_runtime_20261027/randomized_arrival_20261028"
MANIFEST="$DAG/profile_manifest.json"
SCHEDULE="$INPUT/poisson_d8_seed20261032.json"
RUNNER="$INPUT/run_randomized_near_auditfix.mjs"
WRAPPER="$INPUT/near-sandbox-compatible.remote.sh"
TAG=d8_poisson_seed20261032_auditfix_near
mkdir -p "$EVM_OUT" "$NEAR_OUT" "$NEAR_RUNTIME/artifacts" "$INPUT/artifacts"
cp "$INPUT/stage_contract.wasm" "$INPUT/artifacts/stage_contract.wasm"
export PATH="$HOST/.local/bin:$PATH"
export PYTHONPATH="$DAG${PYTHONPATH:+:$PYTHONPATH}"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
unset STRESS_PIPELINE_PREHEAT_SEC STRESS_PIPELINE_OBSERVATION_SEC
export STRESS_PIPELINE_MODE=1 STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=780
export STRESS_PROFILE_MANIFEST_PATH="$MANIFEST" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
export STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5 STRESS_WORKERS=64
export STRESS_RECEIPT_TIMEOUT_MS=120000 STRESS_RPC_PROXY_TIMEOUT_MS=60000 STRESS_READ_TIMEOUT_MS=15000 STRESS_READ_ATTEMPTS=3 STRESS_DIAGNOSTIC_LIGHT=0

record_environment() {
  local dest="$1"
  {
    printf 'recorded_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'hostname='; hostname
    printf 'loadavg='; cat /proc/loadavg
    printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
    printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true
    printf 'memory.max='; cat /sys/fs/cgroup/memory.max 2>/dev/null || true
    printf 'node='; node --version
    printf 'python='; "$HOST/.venv/bin/python" --version
    printf 'anvil='; "$HOST/.local/bin/anvil" --version | head -n 1
    printf 'near='; "$WRAPPER" --version | head -n 1
    sha256sum "$SCHEDULE" "$INPUT/run_randomized_evm.py" "$RUNNER" "$INPUT/artifacts/stage_contract.wasm" "$WRAPPER"
  } > "$dest"
}

record_environment "$RESULTS/environment_start.txt"
python3 - "$SCHEDULE" "$RESULTS/effective_config_startup.json" "$RUNNER" "$WRAPPER" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
schedule_path,out_path,runner_path,wrapper_path=map(Path,sys.argv[1:])
s=json.loads(schedule_path.read_text()); c=s['scenarios']['poisson_d8']
assert int(s['seed'])==20261032 and c['arrival_rule']=='paired_exponential'
assert c['rates_per_sec']==[0.5,1.0]
assert {k:len(v) for k,v in c['arrival_offsets_sec_by_rate'].items()}=={'0.5':385,'1.0':817}
assert os.environ['STRESS_PIPELINE_MODE']=='1'
assert os.environ['STRESS_PIPELINE_PREHEAT_SEC']=='30' and os.environ['STRESS_PIPELINE_OBSERVATION_SEC']=='780'
assert not any(os.environ.get(k) for k in ('STRESS_RATE_OVERRIDE','STRESS_WARMUP_OVERRIDE_SEC','STRESS_MEASUREMENT_OVERRIDE_SEC'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
meta={'seed':s['seed'],'profile_id':s['profile_id'],'scenario':'poisson_d8','rates_per_sec':c['rates_per_sec'],'expected_counts':{k:len(v) for k,v in c['arrival_offsets_sec_by_rate'].items()},'arrival_rule':c['arrival_rule'],'arrival_seed':c['arrival_seed'],'phase_random_offset_sec':c['phase_random_offset_sec'],'pipeline_mode':os.environ['STRESS_PIPELINE_MODE'],'preheat_sec':30,'observation_sec':780,'repeat':5,'workers':64,'receipt_timeout_ms':120000,'rpc_proxy_timeout_ms':60000,'read_timeout_ms':15000,'read_attempts':3,'runner_sha256':sha(runner_path),'schedule_sha256':sha(schedule_path),'sandbox_wrapper_sha256':sha(wrapper_path),'startup_assertions_passed':True}
out_path.write_text(json.dumps(meta,indent=2)+'\n')
PY
sha256sum "$SCHEDULE" "$INPUT/run_randomized_evm.py" "$RUNNER" "$INPUT/artifacts/stage_contract.wasm" "$WRAPPER" > "$RESULTS/input_sha256.txt"

run_monitored() {
  local out="$1"; shift
  "$@" > "$out/runner.log" 2>&1 &
  local pid=$!
  printf '%s\n' "$pid" > "$out/runner.pid"
  {
    printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    taskset -pc "$pid" 2>&1 || true
    cat /sys/fs/cgroup/cpu.max 2>/dev/null || true
    cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || true
    cat /sys/fs/cgroup/memory.max 2>/dev/null || true
  } > "$out/process_affinity_and_budget.txt"
  set +e; wait "$pid"; local code=$?; set -e
  printf '%s\n' "$code" > "$out/exit_code.txt"
  printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$code" > "$out/run_status.txt"
  return "$code"
}

# Replay the paired EVM reference against the exact unmodified schedule.
set +e
run_monitored "$EVM_OUT" env STRESS_PROFILE_MANIFEST_PATH="$MANIFEST" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1 STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5 STRESS_TAG=d8_poisson_seed20261032_auditfix_evm STRESS_OUTPUT_DIR="$EVM_OUT" STRESS_WORKERS=64 ANVIL_BIN="$HOST/.local/bin/anvil" "$HOST/.venv/bin/python" "$INPUT/run_randomized_evm.py"
evm_code=$?
set -e
if [[ "$evm_code" != 0 ]]; then record_environment "$RESULTS/environment_after_evm_failure.txt"; exit "$evm_code"; fi

cp "$RUNNER" "$NEAR_RUNTIME/run_randomized_near_auditfix_seed20261032_full.mjs"
cp "$INPUT/artifacts/stage_contract.wasm" "$NEAR_RUNTIME/artifacts/stage_contract.wasm"
"$WRAPPER" --version > "$NEAR_OUT/near_binary_version.txt" 2>&1
node --version > "$NEAR_OUT/node_version.txt" 2>&1
export STRESS_TAG="$TAG" STRESS_OUTPUT_DIR="$NEAR_OUT" NEAR_SANDBOX_BIN_PATH="$WRAPPER"
export STRESS_AUDIT_GATE_PATH="$NEAR_OUT/offline_audit_gate.json" STRESS_AUDIT_GATE_TIMEOUT_MS=3600000
node "$NEAR_RUNTIME/run_randomized_near_auditfix_seed20261032_full.mjs" > "$NEAR_OUT/runner.log" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$NEAR_OUT/runner.pid"
{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  taskset -pc "$pid" 2>&1 || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max
  printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max
} > "$NEAR_OUT/process_affinity_and_budget.txt"
ps -p "$pid" -o pid,ppid,pcpu,pmem,rss,stat,etime,args > "$NEAR_OUT/process_start.txt" || true
for _ in $(seq 1 180); do
  child=$(pgrep -P "$pid" | head -n 1 || true)
  if [[ -n "$child" && -r "/proc/$child/cmdline" ]]; then
    cmd=$(tr '\0' ' ' < "/proc/$child/cmdline")
    port=$(printf '%s' "$cmd" | sed -n 's/.*--rpc-addr 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p')
    if [[ -n "$port" ]]; then
      printf '%s\n' "$cmd" > "$NEAR_OUT/sandbox_process_commandline.txt"
      if curl -fsS --max-time 5 -X POST "http://127.0.0.1:$port" -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"status","params":[]}' > "$NEAR_OUT/rpc_status_startup.json"; then break; fi
    fi
  fi
  sleep 1
done
set +e
wait "$pid"; near_code=$?
set -e
printf '%s\n' "$near_code" > "$NEAR_OUT/exit_code.txt"
printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$near_code" > "$NEAR_OUT/run_status.txt"
record_environment "$RESULTS/environment_after_near.txt"
exit "$near_code"
