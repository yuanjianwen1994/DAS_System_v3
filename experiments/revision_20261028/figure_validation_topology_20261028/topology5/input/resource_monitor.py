#!/usr/bin/env python3
"""Sample the remote task's process tree and shared-container resource context."""
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

root_pid = int(sys.argv[1])
csv_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])
hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
stop = False


def halt(_sig, _frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, halt)
signal.signal(signal.SIGINT, halt)
previous_cpu = {}
previous_time = time.monotonic()
started_monotonic = previous_time
max_rss = 0
max_cpu = 0.0
max_count = 0
samples = 0
started = datetime.now(timezone.utc).isoformat()


def proc_snapshot():
    rows = {}
    for item in Path("/proc").iterdir():
        if not item.name.isdigit():
            continue
        try:
            stat = (item / "stat").read_text()
            tail = stat.rsplit(")", 1)[1].split()
            ppid = int(tail[1])
            cpu_ticks = int(tail[11]) + int(tail[12])
            rows[int(item.name)] = (ppid, cpu_ticks)
        except (OSError, ValueError, IndexError):
            continue
    descendants = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _) in rows.items():
            if ppid in descendants and pid not in descendants:
                descendants.add(pid)
                changed = True
    tracked = {}
    rss_kb = 0
    for pid in descendants:
        if pid == os.getpid() or pid not in rows:
            continue
        tracked[pid] = rows[pid][1]
        try:
            status = (Path("/proc") / str(pid) / "status").read_text()
            for line in status.splitlines():
                if line.startswith("VmRSS:"):
                    rss_kb += int(line.split()[1])
                    break
        except (OSError, ValueError, IndexError):
            pass
    return tracked, rss_kb


with csv_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp_utc", "elapsed_sec", "task_process_count", "task_tree_rss_kb", "task_tree_cpu_pct", "container_mem_available_kb", "container_memory_current_bytes"])
    while not stop:
        now = time.monotonic()
        tracked, rss_kb = proc_snapshot()
        elapsed = max(0.001, now - previous_time)
        cpu_pct = (sum(max(0, tracked.get(pid, 0) - previous_cpu.get(pid, 0)) for pid in tracked) / hz / elapsed * 100) if previous_cpu else 0.0
        previous_cpu = tracked
        previous_time = now
        mem_available = None
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    mem_available = int(line.split()[1])
                    break
        except OSError:
            pass
        try:
            cgroup_memory = Path("/sys/fs/cgroup/memory.current").read_text().strip()
        except OSError:
            cgroup_memory = None
        writer.writerow([datetime.now(timezone.utc).isoformat(), round(now - started_monotonic, 3), len(tracked), rss_kb, round(cpu_pct, 2), mem_available, cgroup_memory])
        f.flush()
        max_rss = max(max_rss, rss_kb)
        max_cpu = max(max_cpu, cpu_pct)
        max_count = max(max_count, len(tracked))
        samples += 1
        time.sleep(1)

summary_path.write_text(json.dumps({
    "started_at": started,
    "finished_at": datetime.now(timezone.utc).isoformat(),
    "sample_interval_sec": 1,
    "samples": samples,
    "task_tree_peak_sampled_rss_kb": max_rss,
    "task_tree_peak_sampled_cpu_pct_of_one_core": max_cpu,
    "task_tree_peak_sampled_process_count": max_count,
    "limitation": "sampled process-tree RSS sums can double-count shared mappings; container MemAvailable and cgroup memory are shared context, not exclusive task totals",
}, indent=2) + "\n")
