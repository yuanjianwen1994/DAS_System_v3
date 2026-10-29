#!/usr/bin/env bash
set -eu
target_pid="$1"
output="$2"
pattern="$3"
while kill -0 "$target_pid" 2>/dev/null; do
  stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  ps -eo pid,ppid,pcpu,rss,comm,args | awk -v stamp="$stamp" -v pattern="$pattern" '$0 ~ pattern && $0 !~ /awk -v/ { print stamp "\t" $0 }' >> "$output"
  sleep 1
done
