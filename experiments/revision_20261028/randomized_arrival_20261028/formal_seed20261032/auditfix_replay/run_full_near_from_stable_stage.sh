#!/usr/bin/env bash
set -euo pipefail

RUN=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/seed20261032_near_auditfix_full_replay
STAGE=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/seed20261032_near_auditfix_verify2
STAGE_INPUT="$STAGE/input"
SOURCE_SCHEDULE=/home/starshell/das_randomized_arrival_20261028/formal_seed20261032/input/poisson_d8_seed20261032.json
INPUT="$RUN/input_snapshot"
OUT="$RUN/results/near"
HOST=/home/starshell/das_independent_stage_evm_20261027
DAG="$HOST/experiments/revision_20261027/dag_workflow_20261027"
NEAR_RUNTIME="$HOST/experiments/revision_20261027/near_runtime_20261027"
MANIFEST="$DAG/profile_manifest.json"
RUNNER="$STAGE_INPUT/run_randomized_near_auditfix.mjs"
WASM="$STAGE_INPUT/artifacts/stage_contract.wasm"
WRAPPER="$STAGE_INPUT/near-sandbox-compatible.remote.sh"
SCHEDULE="$INPUT/poisson_d8_seed20261032.json"
TAG=d8_poisson_seed20261032_near_auditfix_full

test ! -e "$RUN"
test -r "$RUNNER" && test -r "$WASM" && test -r "$WRAPPER" && test -r "$SOURCE_SCHEDULE"
test -L "$STAGE_INPUT/node_modules"
test "$(readlink -f "$STAGE_INPUT/node_modules")" = "$NEAR_RUNTIME/node_modules"
mkdir -p "$INPUT" "$OUT"
cp "$SOURCE_SCHEDULE" "$SCHEDULE"

python3 - "$RUN" "$RUNNER" "$WASM" "$WRAPPER" "$STAGE_INPUT/node_modules" "$SCHEDULE" "$NEAR_RUNTIME/node_modules/near-sandbox/package.json" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
run,runner,wasm,wrapper,node_modules,schedule,pkg_json=map(Path,sys.argv[1:])
s=json.loads(schedule.read_text()); c=s['scenarios']['poisson_d8']
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
assert int(s['seed'])==20261032
assert c['arrival_rule']=='paired_exponential' and c['rates_per_sec']==[0.5,1.0]
assert {k:len(v) for k,v in c['arrival_offsets_sec_by_rate'].items()}=={'0.5':385,'1.0':817}
assert runner.is_file() and wasm.is_file() and os.access(wasm,os.R_OK)
assert runner.read_text().find("new URL('artifacts/stage_contract.wasm', ROOT)")>=0
assert node_modules.is_symlink() and str(node_modules.resolve())==str(Path('/home/starshell/das_independent_stage_evm_20261027/experiments/revision_20261027/near_runtime_20261027/node_modules'))
assert pkg_json.is_file()
m={'seed':s['seed'],'profile_id':s['profile_id'],'scenario':'poisson_d8','rates_per_sec':c['rates_per_sec'],'expected_schedule_items':{k:len(v) for k,v in c['arrival_offsets_sec_by_rate'].items()},'arrival_rule':c['arrival_rule'],'arrival_seed':c['arrival_seed'],'phase_random_offset_sec':c['phase_random_offset_sec'],'pipeline_mode':1,'preheat_sec':30,'observation_sec':780,'repeat':5,'workers':64,'receipt_timeout_ms':120000,'rpc_proxy_timeout_ms':60000,'read_timeout_ms':15000,'read_attempts':3,'schedule_path':str(schedule),'schedule_sha256':sha(schedule),'runner_path':str(runner),'runner_sha256':sha(runner),'runner_wasm_reference':"new URL('artifacts/stage_contract.wasm', ROOT)",'wasm_path':str(wasm),'wasm_sha256':sha(wasm),'wrapper_path':str(wrapper),'wrapper_sha256':sha(wrapper),'node_modules_symlink':str(node_modules),'node_modules_target':str(node_modules.resolve()),'near_sandbox_package_json_path':str(pkg_json),'near_sandbox_package_json_sha256':sha(pkg_json),'startup_assertions_passed':True}
(run/'launch_manifest.json').write_text(json.dumps(m,indent=2)+'\n')
print(json.dumps(m,indent=2))
PY

export PATH="$HOST/.local/bin:$PATH" PYTHONPATH="$DAG${PYTHONPATH:+:$PYTHONPATH}"
unset STRESS_RATE_OVERRIDE STRESS_WARMUP_OVERRIDE_SEC STRESS_MEASUREMENT_OVERRIDE_SEC
unset STRESS_PIPELINE_PREHEAT_SEC STRESS_PIPELINE_OBSERVATION_SEC
export STRESS_PIPELINE_MODE=1 STRESS_PIPELINE_PREHEAT_SEC=30 STRESS_PIPELINE_OBSERVATION_SEC=780
export STRESS_PROFILE_MANIFEST_PATH="$MANIFEST" STRESS_PROFILE_ID=dag-07-chain-d8-w1-theta1-nu1
export STRESS_SCHEDULE_PATH="$SCHEDULE" STRESS_SCHEDULE_SCENARIO=poisson_d8 STRESS_REPEAT=5 STRESS_WORKERS=64
export STRESS_RECEIPT_TIMEOUT_MS=120000 STRESS_RPC_PROXY_TIMEOUT_MS=60000 STRESS_READ_TIMEOUT_MS=15000 STRESS_READ_ATTEMPTS=3 STRESS_DIAGNOSTIC_LIGHT=0
export STRESS_TAG="$TAG" STRESS_OUTPUT_DIR="$OUT" NEAR_SANDBOX_BIN_PATH="$WRAPPER"
export STRESS_AUDIT_GATE_PATH="$OUT/offline_audit_gate.json" STRESS_AUDIT_GATE_TIMEOUT_MS=3600000

{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  hostname
  printf 'loadavg='; cat /proc/loadavg
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max
  printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max
  printf 'node='; node --version
  printf 'python='; python3 --version
  printf 'near='; "$WRAPPER" --version | head -n 1
  sha256sum "$SCHEDULE" "$RUNNER" "$WASM" "$WRAPPER" "$NEAR_RUNTIME/node_modules/near-sandbox/package.json"
} > "$RUN/startup_environment.txt"

node --check "$RUNNER"
(cd "$STAGE_INPUT" && node --input-type=module -e "await import('near-sandbox')")
node "$RUNNER" > "$OUT/runner.log" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$OUT/runner.pid"
vmstat -n 1 > "$OUT/host_vmstat_samples.txt" &
vmstat_pid=$!
{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  taskset -pc "$pid" 2>&1 || true
  printf 'cpu.max='; cat /sys/fs/cgroup/cpu.max
  printf 'cpuset.cpus.effective='; cat /sys/fs/cgroup/cpuset.cpus.effective
  printf 'memory.max='; cat /sys/fs/cgroup/memory.max
} > "$OUT/process_affinity_and_budget.txt"

(
  while kill -0 "$pid" 2>/dev/null; do
    stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    pids=("$pid")
    for parent in "${pids[@]}"; do
      while read -r child; do
        [[ -n "$child" ]] && pids+=("$child")
      done < <(pgrep -P "$parent" 2>/dev/null || true)
    done
    joined=$(IFS=,; printf '%s' "${pids[*]}")
    printf '\nSAMPLE %s\n' "$stamp" >> "$OUT/process_resource_samples.txt"
    ps -p "$joined" -o pid,ppid,pcpu,pmem,rss,stat,etime,args >> "$OUT/process_resource_samples.txt" 2>&1 || true
    sleep 1
  done
) &
monitor=$!

rpc_captured=0
for _ in $(seq 1 180); do
  child=$(pgrep -P "$pid" | head -n 1 || true)
  if [[ -n "$child" && -r "/proc/$child/cmdline" ]]; then
    command_line=$(tr '\0' ' ' < "/proc/$child/cmdline")
    port=$(printf '%s' "$command_line" | sed -n 's/.*--rpc-addr 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p')
    if [[ -n "$port" ]]; then
      printf '%s\n' "$command_line" > "$OUT/sandbox_process_commandline.txt"
      if curl -fsS --max-time 5 -X POST "http://127.0.0.1:$port" -H 'Content-Type: application/json' --data '{"jsonrpc":"2.0","id":1,"method":"status","params":[]}' > "$OUT/rpc_status_startup.json"; then rpc_captured=1; break; fi
    fi
  fi
  sleep 1
done
if [[ "$rpc_captured" != 1 ]]; then echo 'RPC status capture failed' > "$OUT/rpc_status_capture_error.txt"; fi

set +e
wait "$pid"; code=$?
set -e
kill -TERM "$vmstat_pid" 2>/dev/null || true
wait "$vmstat_pid" 2>/dev/null || true
wait "$monitor" || true
printf '%s\n' "$code" > "$OUT/exit_code.txt"
printf 'finished_utc=%s\nexit_code=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$code" > "$OUT/run_status.txt"
exit "$code"
