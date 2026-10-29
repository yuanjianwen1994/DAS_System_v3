#!/usr/bin/env bash
set -u
R=/home/starshell/das_independent_stage_evm_20261027
T=$R/experiments/revision_20261027/timing_comparison_20261027
ROOT=$T/paired_load/depth_profiles/pipeline_formal
MATRIX_PID=$(cat "$ROOT/matrix_controller.pid")
echo "waiting_for_independent_matrix pid=$MATRIX_PID started=$(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"
while kill -0 "$MATRIX_PID" 2>/dev/null; do sleep 30; done
echo "independent_matrix_finished=$(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"

bash "$ROOT/run_recovered_d20_near_repeat.sh" >> "$ROOT/control_continuation.log" 2>&1
recovery_code=$?
echo "recovered_d20_near_exit_code=$recovery_code finished=$(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"

bash "$ROOT/run_control_validation.sh" >> "$ROOT/control_continuation.log" 2>&1
validation_code=$?
validation_csv="$ROOT/control_validation/runs.csv"
if [ "$validation_code" -ne 0 ] || [ ! -s "$validation_csv" ] || ! awk -F, 'NR>1 && $6 != 0 {bad=1} END {exit (NR==3 && !bad) ? 0 : 1}' "$validation_csv"; then
  echo "CONTROL_VALIDATION_FAILED exit_code=$validation_code; control matrix not started at $(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"
  exit 1
fi
echo "control_validation_passed=$(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"
bash "$ROOT/run_control_matrix.sh" >> "$ROOT/control_continuation.log" 2>&1
echo "control_matrix_finished=$(date -u +%FT%TZ)" >> "$ROOT/control_continuation.log"
