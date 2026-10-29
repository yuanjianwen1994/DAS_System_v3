#!/usr/bin/env bash
set -euo pipefail
BASE=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/auditfix_smoke
INPUT="$BASE/input"
RESULTS="$BASE/results"
HOST=/home/starshell/das_independent_stage_evm_20261027
DAG="$HOST/experiments/revision_20261027/dag_workflow_20261027"
NEAR_RUNTIME="$HOST/experiments/revision_20261027/near_runtime_20261027/randomized_arrival_20261028"
SCHEDULE="$INPUT/poisson_d8_seed20261032_smoke_prefix.json"
RUNNER="$INPUT/run_randomized_near_auditfix.mjs"
WRAPPER="$INPUT/near-sandbox-compatible.remote.sh"
PROVENANCE="$INPUT/schedule_prefix_provenance.json"
mkdir -p "$RESULTS" "$NEAR_RUNTIME/artifacts"
chmod 755 "$WRAPPER"
export PATH="$HOST/.local/bin:$PATH"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
export STRESS_PIPELINE_MODE=1 STRESS_PIPELINE_PREHEAT_SEC=0 STRESS_PIPELINE_OBSERVATION_SEC=20
export STRESS_PROFILE_MANIFEST_PATH="$DAG/profile_manifest.json"
export STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
export STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO=poisson_d8
export STRESS_REPEAT=5 STRESS_TAG=d8_seed20261032_auditfix_smoke STRESS_OUTPUT_DIR="$RESULTS"
export STRESS_WORKERS=64 STRESS_RECEIPT_TIMEOUT_MS=120000 STRESS_RPC_PROXY_TIMEOUT_MS=60000
export STRESS_READ_TIMEOUT_MS=15000 STRESS_READ_ATTEMPTS=3 STRESS_DIAGNOSTIC_LIGHT=0
export NEAR_SANDBOX_BIN_PATH="$WRAPPER"
python3 - "$SCHEDULE" "$BASE/launch_effective_config_startup.json" "$RUNNER" "$WRAPPER" "$PROVENANCE" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
schedule_path,out_path,runner_path,wrapper_path,provenance_path=map(Path,sys.argv[1:])
schedule=json.loads(schedule_path.read_text()); cfg=schedule['scenarios']['poisson_d8']
provenance=json.loads(provenance_path.read_text())
assert int(schedule['seed'])==20261032 and cfg['arrival_rule']=='paired_exponential'
assert all(len(v)==8 for v in cfg['arrival_offsets_sec_by_rate'].values())
assert os.environ['STRESS_PIPELINE_MODE']=='1'
assert not any(os.environ.get(k) for k in ('STRESS_RATE_OVERRIDE','STRESS_WARMUP_OVERRIDE_SEC','STRESS_MEASUREMENT_OVERRIDE_SEC'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
meta={'seed':schedule['seed'],'profile_id':schedule['profile_id'],'pipeline_mode':os.environ['STRESS_PIPELINE_MODE'],
 'preheat_sec':0,'observation_sec':20,'rates_per_sec':schedule['rates_per_sec'],'expected_counts':{k:len(v) for k,v in cfg['arrival_offsets_sec_by_rate'].items()},
 'phase_offset_sec':cfg['phase_random_offset_sec'],'source_schedule_sha256':provenance['source_schedule_sha256'],'smoke_schedule_sha256':sha(schedule_path),
 'runner_sha256':sha(runner_path),'sandbox_wrapper_sha256':sha(wrapper_path),'startup_assertions_passed':True,
 'effective_environment_subset':{k:os.environ.get(k) for k in ('STRESS_PIPELINE_MODE','STRESS_PIPELINE_PREHEAT_SEC','STRESS_PIPELINE_OBSERVATION_SEC','STRESS_PROFILE_ID','STRESS_SCHEDULE_SCENARIO','STRESS_REPEAT','STRESS_TAG','STRESS_WORKERS','STRESS_RECEIPT_TIMEOUT_MS','STRESS_RPC_PROXY_TIMEOUT_MS','STRESS_DIAGNOSTIC_LIGHT')}}
out_path.write_text(json.dumps(meta,indent=2)+'\n')
PY
cp "$RUNNER" "$NEAR_RUNTIME/run_randomized_near_auditfix_seed20261032_smoke.mjs"
cp "$INPUT/artifacts/stage_contract.wasm" "$NEAR_RUNTIME/artifacts/stage_contract.wasm"
sha256sum "$SCHEDULE" "$RUNNER" "$INPUT/artifacts/stage_contract.wasm" "$WRAPPER" > "$RESULTS/input_sha256.txt"
"$WRAPPER" --version > "$RESULTS/near_binary_version.txt" 2>&1
node --version > "$RESULTS/node_version.txt" 2>&1
node "$NEAR_RUNTIME/run_randomized_near_auditfix_seed20261032_smoke.mjs" > "$RESULTS/runner.log" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$RESULTS/runner.pid"
{
  printf 'recorded_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  taskset -pc "$pid" 2>&1 || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max
  printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max
} > "$RESULTS/process_affinity_and_budget.txt"
ps -p "$pid" -o pid,ppid,pcpu,pmem,rss,stat,etime,args > "$RESULTS/process_start.txt" || true
rpcCaptured=0
for _ in $(seq 1 120); do
  child=$(pgrep -P "$pid" | head -n 1 || true)
  if [[ -n "$child" && -r "/proc/$child/cmdline" ]]; then
    cmd=$(tr '\0' ' ' < "/proc/$child/cmdline")
    port=$(printf '%s' "$cmd" | sed -n 's/.*--rpc-addr 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p')
    if [[ -n "$port" ]]; then
      printf '%s\n' "$cmd" > "$RESULTS/sandbox_process_commandline.txt"
      if curl -fsS --max-time 5 -X POST "http://127.0.0.1:$port" -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"status","params":[]}' > "$RESULTS/rpc_status_startup.json"; then rpcCaptured=1; break; fi
    fi
  fi
  sleep 1
done
if [[ "$rpcCaptured" != 1 ]]; then echo 'RPC status capture failed' > "$RESULTS/rpc_status_capture_error.txt"; fi
set +e
wait "$pid"
code=$?
set -e
printf '%s\n' "$code" > "$RESULTS/exit_code.txt"
printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$code" > "$RESULTS/run_status.txt"
exit "$code"
