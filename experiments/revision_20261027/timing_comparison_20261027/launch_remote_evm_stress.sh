#!/usr/bin/env bash
set -euo pipefail

# Remote-only, reproducible launcher for the d4 Anvil diagnostic.
# Adjust only this configuration block for a new run; keep credentials out.
REMOTE_ROOT="/home/starshell/das_independent_stage_evm_20261027"
RUNNER="${REMOTE_ROOT}/experiments/revision_20261027/timing_comparison_20261027/open_loop_evm_stress.py"
DAG_DIR="${REMOTE_ROOT}/experiments/revision_20261027/dag_workflow_20261027"
MONITOR="${REMOTE_ROOT}/experiments/revision_20261027/timing_comparison_20261027/monitor_host_resources.sh"
PYTHON="${REMOTE_ROOT}/.venv/bin/python"
ANVIL="${REMOTE_ROOT}/.local/bin/anvil"
FOUNDRY_BIN="${REMOTE_ROOT}/.local/bin"
OUT_DIR="${REMOTE_ROOT}/experiments/revision_20261027/timing_comparison_20261027/paired_load/anvil_max1000_d4_r10"
TAG="anvil_max1000_d4_r10"
PROFILE="dag-04-chain-d4-w1-theta1-nu1"
SCENARIOS="high_steady"
REPEAT="10"
WORKERS="256"
RATES="8,16"
WARMUP_SEC="5"
MEASUREMENT_SEC="30"
RECEIPT_POLL_SEC="0.2"
RECEIPT_BATCH_SIZE="128"
ANVIL_MAX_TRANSACTIONS="1000"

export PATH="${FOUNDRY_BIN}:/home/starshell/.foundry/bin:${PATH}"
export PYTHONPATH="${DAG_DIR}"
export PYTHON
export ANVIL_BIN="${ANVIL}"
export RUNNER
export OUT_DIR
export STRESS_PROFILE_MANIFEST_PATH="${DAG_DIR}/profile_manifest.json"
export STRESS_PROFILE_ID="${PROFILE}"
export STRESS_SCHEDULE_SCENARIOS="${SCENARIOS}"
export STRESS_REPEAT="${REPEAT}"
export STRESS_TAG="${TAG}"
export STRESS_OUTPUT_DIR="${OUT_DIR}"
export STRESS_WORKERS="${WORKERS}"
export STRESS_RATE_OVERRIDE="${RATES}"
export STRESS_WARMUP_OVERRIDE_SEC="${WARMUP_SEC}"
export STRESS_MEASUREMENT_OVERRIDE_SEC="${MEASUREMENT_SEC}"
export STRESS_DIAGNOSTIC_LIGHT="1"
export STRESS_RECEIPT_POLL_SEC="${RECEIPT_POLL_SEC}"
export STRESS_RECEIPT_BATCH_SIZE="${RECEIPT_BATCH_SIZE}"
export ANVIL_MAX_TRANSACTIONS

cd "${REMOTE_ROOT}"
test -x "${PYTHON}"
test -x "${ANVIL}"
test -f "${RUNNER}"
test -f "${DAG_DIR}/run_das_distributed_composition.py"
test -f "${STRESS_PROFILE_MANIFEST_PATH}"
test -f "${MONITOR}"
"${PYTHON}" -c 'import web3, solcx, run_das_distributed_composition; print("python_modules_ok", web3.__version__)'
anvil_version="$("${ANVIL}" --version | head -n 1)"
case "${anvil_version}" in *"1.8.6"*) ;; *) echo "Unexpected Anvil version: ${anvil_version}" >&2; exit 2 ;; esac
"${PYTHON}" -c 'import ast,os; ast.parse(open(os.environ["RUNNER"]).read()); print("runner_syntax_ok")'

mkdir -p "${OUT_DIR}"
test -w "${OUT_DIR}"
touch "${OUT_DIR}/.write_check"
rm "${OUT_DIR}/.write_check"
if pgrep -af "${RUNNER}" | grep -v "pgrep -af" >/dev/null; then
  echo "A runner using ${RUNNER} is already active" >&2
  exit 3
fi

nohup bash -c '
  "$PYTHON" "$RUNNER"
  result=$?
  printf "%s\n" "$result" > "$OUT_DIR/exit.code"
  exit "$result"
' >> "${OUT_DIR}/runner.log" 2>&1 </dev/null &
wrapper_pid=$!
printf '%s\n' "${wrapper_pid}" > "${OUT_DIR}/wrapper.pid"
sleep 1
runner_pid="$(pgrep -P "${wrapper_pid}" | head -n 1 || true)"
if [ -z "${runner_pid}" ]; then
  echo "Runner did not start; first log lines:" >&2
  sed -n '1,30p' "${OUT_DIR}/runner.log" >&2 || true
  exit 4
fi
printf '%s\n' "${runner_pid}" > "${OUT_DIR}/runner.pid"
if ! kill -0 "${runner_pid}" 2>/dev/null; then
  echo "Runner PID is not alive" >&2
  sed -n '1,30p' "${OUT_DIR}/runner.log" >&2 || true
  exit 5
fi
nohup /bin/bash "${MONITOR}" "${runner_pid}" "${OUT_DIR}/resources.csv" \
  > "${OUT_DIR}/monitor.log" 2>&1 </dev/null &
printf '%s\n' "$!" > "${OUT_DIR}/monitor.pid"
sleep 2
if test -f "${OUT_DIR}/exit.code"; then
  echo "Runner exited during startup with status $(cat "${OUT_DIR}/exit.code")" >&2
  sed -n '1,40p' "${OUT_DIR}/runner.log" >&2 || true
  exit 6
fi
echo "RUNNING runner_pid=${runner_pid} wrapper_pid=${wrapper_pid} anvil=${anvil_version} output=${OUT_DIR}"
sed -n '1,8p' "${OUT_DIR}/runner.log" || true
