#!/usr/bin/env bash
set -eu
target_pid="$1"
output="$2"

if [ ! -e "$output" ]; then
  printf 'timestamp_utc,cpu_busy_pct,load_1m,mem_total_kb,mem_available_kb,tracked_process_count,tracked_cpu_pct,tracked_rss_kb,runner_pid,runner_cpu_pct,runner_rss_kb,engine_pid,engine_cpu_pct,engine_rss_kb\n' > "$output"
fi

read_cpu() {
  read -r _ user nice system idle iowait irq softirq steal _ < /proc/stat
  total=$((user + nice + system + idle + iowait + irq + softirq + steal))
  idle_total=$((idle + iowait))
  printf '%s %s\n' "$total" "$idle_total"
}

previous="$(read_cpu)"
previous_total="${previous%% *}"
previous_idle="${previous##* }"
ticks_per_second="$(getconf CLK_TCK)"
read_process() {
  pid="$1"
  if [ -r "/proc/$pid/stat" ]; then
    ticks="$(awk '{print $14+$15}' "/proc/$pid/stat")"
    rss="$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status")"
    printf '%s %s\n' "${ticks:-0}" "${rss:-0}"
  else
    printf '0 0\n'
  fi
}
runner_previous="$(read_process "$target_pid")"
runner_previous_ticks="${runner_previous%% *}"
engine_pid="$(pgrep -P "$target_pid" | head -n 1 || true)"
engine_previous="$(read_process "${engine_pid:-0}")"
engine_previous_ticks="${engine_previous%% *}"
while kill -0 "$target_pid" 2>/dev/null; do
  stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  load_1m="$(awk '{print $1}' /proc/loadavg)"
  mem_total="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  mem_available="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
  current="$(read_cpu)"
  current_total="${current%% *}"
  current_idle="${current##* }"
  total_delta=$((current_total - previous_total))
  idle_delta=$((current_idle - previous_idle))
  cpu_busy="0"
  if [ "$total_delta" -gt 0 ]; then
    cpu_busy="$(awk -v t="$total_delta" -v i="$idle_delta" 'BEGIN {printf "%.2f", 100*(t-i)/t}')"
  fi
  tracked="$(ps -eo pcpu,rss,args | awk '$0 ~ /near-sandbox|anvil|open_loop_(near|evm)/ && $0 !~ /monitor_host_resources/ {n++; cpu+=$1; rss+=$2} END {printf "%d %.2f %.0f", n, cpu, rss}')"
  tracked_count="${tracked%% *}"
  tracked_rest="${tracked#* }"
  tracked_cpu="${tracked_rest%% *}"
  tracked_rss="${tracked_rest##* }"
  runner_now="$(read_process "$target_pid")"
  runner_ticks="${runner_now%% *}"
  runner_rss="${runner_now##* }"
  runner_cpu="$(awk -v now="$runner_ticks" -v prev="$runner_previous_ticks" -v hz="$ticks_per_second" 'BEGIN {printf "%.2f", 100*(now-prev)/hz}')"
  if [ -z "$engine_pid" ] || [ ! -r "/proc/$engine_pid/stat" ]; then
    engine_pid="$(pgrep -P "$target_pid" | head -n 1 || true)"
    engine_previous="$(read_process "${engine_pid:-0}")"
    engine_previous_ticks="${engine_previous%% *}"
  fi
  engine_now="$(read_process "${engine_pid:-0}")"
  engine_ticks="${engine_now%% *}"
  engine_rss="${engine_now##* }"
  engine_cpu="$(awk -v now="$engine_ticks" -v prev="$engine_previous_ticks" -v hz="$ticks_per_second" 'BEGIN {printf "%.2f", 100*(now-prev)/hz}')"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$stamp" "$cpu_busy" "$load_1m" "$mem_total" "$mem_available" "$tracked_count" "$tracked_cpu" "$tracked_rss" \
    "$target_pid" "$runner_cpu" "$runner_rss" "$engine_pid" "$engine_cpu" "$engine_rss" >> "$output"
  previous_total="$current_total"
  previous_idle="$current_idle"
  runner_previous_ticks="$runner_ticks"
  engine_previous_ticks="$engine_ticks"
  sleep 1
done
