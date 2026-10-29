#!/usr/bin/env bash
set -euo pipefail

REMOTE_ROOT="/home/starshell/das_independent_stage_evm_20261027"
TIMING_DIR="${REMOTE_ROOT}/experiments/revision_20261027/timing_comparison_20261027"
PROFILE_DIR="${TIMING_DIR}/paired_load/depth_profiles"
RUNNER="${TIMING_DIR}/open_loop_evm_stress.py"
PYTHON="${REMOTE_ROOT}/.venv/bin/python"
ANVIL="${REMOTE_ROOT}/.local/bin/anvil"
MONITOR="${TIMING_DIR}/monitor_host_resources.sh"

profile="$1"
tag="$2"
repeat="$3"
workers="${4:-64}"
rates="${5:-0.5,1,2}"
measurement_sec="${6:-30}"
out_dir="${PROFILE_DIR}/${tag}"
manifest="${PROFILE_DIR}/profile_manifest_depth20.json"

export PATH="${REMOTE_ROOT}/.local/bin:/home/starshell/.foundry/bin:${PATH}"
export PYTHONPATH="${REMOTE_ROOT}/experiments/revision_20261027/dag_workflow_20261027"
export PYTHON ANVIL_BIN="${ANVIL}" RUNNER OUT_DIR="${out_dir}"
export STRESS_PROFILE_MANIFEST_PATH="${manifest}"
export STRESS_PROFILE_ID="${profile}"
export STRESS_SCHEDULE_SCENARIOS="matched_low"
export STRESS_REPEAT="${repeat}" STRESS_TAG="${tag}" STRESS_OUTPUT_DIR="${out_dir}"
export STRESS_WORKERS="${workers}" STRESS_RATE_OVERRIDE="${rates}"
export STRESS_WARMUP_OVERRIDE_SEC="5" STRESS_MEASUREMENT_OVERRIDE_SEC="${measurement_sec}"
export STRESS_DIAGNOSTIC_LIGHT="1" STRESS_RECEIPT_POLL_SEC="0.2"
export STRESS_RECEIPT_BATCH_SIZE="128" ANVIL_MAX_TRANSACTIONS="1000"

cd "${REMOTE_ROOT}"
test -x "${PYTHON}" && test -x "${ANVIL}"
test -f "${RUNNER}" && test -f "${manifest}" && test -f "${MONITOR}"
"${PYTHON}" -c 'import web3, solcx, run_das_distributed_composition; print("python_modules_ok", web3.__version__)'
"${PYTHON}" -c 'import ast,os; ast.parse(open(os.environ["RUNNER"]).read()); print("runner_syntax_ok")'
mkdir -p "${out_dir}"
test -w "${out_dir}"
if test -f "${out_dir}/runner.pid" || test -f "${out_dir}/exit.code"; then
  echo "Refusing to reuse existing output directory: ${out_dir}" >&2
  exit 3
fi
if pgrep -af "open_loop_evm_stress.py" | grep -v "pgrep -af" >/dev/null; then
  echo "Another EVM runner is active" >&2
  exit 4
fi
nohup bash -c '
  "$PYTHON" "$RUNNER"
  result=$?
  printf "%s\n" "$result" > "$OUT_DIR/exit.code"
  exit "$result"
' >> "${out_dir}/runner.log" 2>&1 </dev/null &
wrapper_pid=$!
sleep 1
runner_pid="$(pgrep -P "${wrapper_pid}" | head -n 1 || true)"
if [ -z "${runner_pid}" ]; then
  sed -n '1,40p' "${out_dir}/runner.log" >&2 || true
  exit 5
fi
printf '%s\n' "${wrapper_pid}" > "${out_dir}/wrapper.pid"
printf '%s\n' "${runner_pid}" > "${out_dir}/runner.pid"
nohup /bin/bash "${MONITOR}" "${runner_pid}" "${out_dir}/resources.csv" \
  > "${out_dir}/monitor.log" 2>&1 </dev/null &
printf '%s\n' "$!" > "${out_dir}/monitor.pid"
sleep 2
if test -f "${out_dir}/exit.code"; then
  echo "Runner exited during startup: $(cat "${out_dir}/exit.code")" >&2
  sed -n '1,40p' "${out_dir}/runner.log" >&2 || true
  exit 6
fi
echo "RUNNING ${tag} pid=${runner_pid} output=${out_dir}"
