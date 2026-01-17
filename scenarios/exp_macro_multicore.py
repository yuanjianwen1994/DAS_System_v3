"""
Macro‑benchmark multi‑process matrix experiment for Phase 4+.
Uses multiprocessing to bypass GIL bottleneck, supporting BASELINE, DAS, and 2PC journey types.
Each process runs its own ConnectionManager, UserManager, Injector, and TrafficGenerator.
Logs raw transaction‑level data per process, optionally merges CSVs.
"""
import sys
import os
import time
import subprocess
import csv
import multiprocessing
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add parent directory to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3 import Web3

from config_matrix import (
    get_topology,
    NUM_USERS,
    MACRO_TX_TIMEOUT,
    MACRO_TX_INTERVAL,
    MATRIX_CONCURRENCY_LEVELS,
    MATRIX_AMORTIZATION_FACTORS,
    MATRIX_JOURNEYS_PER_USER,
    MATRIX_PROCESSES,
    MATRIX_SCENARIOS,
)
from core.identity import UserManager
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer
from core.macro_injector import MacroTransactionInjector
from core.macro_traffic import MacroTrafficGenerator
from core.macro_monitor import MacroMonitor


# ========== Helper Functions (copied from exp_macro_matrix) ==========
def kill_ganache():
    """Aggressively kill all ganache/node processes."""
    try:
        if os.name == 'nt':
            subprocess.call(["taskkill", "/F", "/IM", "node.exe", "/T"], stderr=subprocess.DEVNULL)
        else:
            subprocess.call(["pkill", "-f", "ganache"], stderr=subprocess.DEVNULL)
    except Exception:
        pass


def wait_for_nodes(network: ConnectionManager, timeout=60):
    """Block until all RPC nodes are responding."""
    print("   [System] Waiting for RPC nodes to warm up...")
    nodes = ["shard_0", "shard_1", "execution", "baseline"]
    start = time.time()
    for node in nodes:
        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"Node {node} did not start within {timeout}s")
            try:
                w3 = network.get_web3(node)
                if w3.is_connected() and w3.eth.block_number >= 0:
                    break
            except Exception:
                time.sleep(1)
            time.sleep(1)
    print("   [System] All nodes online.")


def dump_csv(data: List[Dict[str, Any]], filename: str, fieldnames: List[str]) -> None:
    """Write a list of dicts to CSV."""
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    path = logs_dir / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"      [Data] {filename} saved ({len(data)} rows).")


# ========== Worker Process Function ==========
def run_worker_process(
    proc_id: int,
    user_start: int,
    user_end: int,
    concurrency: int,
    journeys_per_user: int,
    ops_per_journey: int,
    journey_type: str,
    topology: Dict[str, Any],
    registry: Dict[str, Dict[str, Any]],
    timestamp: str,
) -> None:
    """
    Single‑process traffic generation.
    Creates its own managers, runs traffic, writes logs to a per‑process CSV.
    """
    print(f"[Worker {proc_id}] Starting with users {user_start}‑{user_end} (total concurrency {concurrency})")
    
    # 1. Create independent managers
    network = ConnectionManager(topology)
    from config_matrix import MNEMONIC
    identity = UserManager(MNEMONIC)
    injector = MacroTransactionInjector(network, identity)
    
    # 2. Wait for nodes (they should already be up)
    wait_for_nodes(network)
    
    # 3. Create traffic generator with process_id and user_offset
    traffic = MacroTrafficGenerator(
        network, identity, injector, registry,
        process_id=proc_id,
        user_offset=user_start,
    )
    
    # 4. Run traffic (only for the assigned user range)
    local_concurrency = user_end - user_start
    print(f"[Worker {proc_id}] Running {local_concurrency} users, {journeys_per_user} journeys each, type={journey_type}")
    
    raw_logs = traffic.run_task_based(
        concurrency=local_concurrency,
        journeys_per_user=journeys_per_user,
        ops_per_journey=ops_per_journey,
        journey_type=journey_type,
        process_id=proc_id,
    )
    
    # 5. Save logs to per‑process CSV
    if raw_logs:
        dump_csv(
            raw_logs,
            f"raw_txs_p{proc_id}_{journey_type}_N{concurrency}_q{ops_per_journey}_{timestamp}.csv",
            fieldnames=["timestamp", "worker_id", "tx_type", "latency_s", "gas_used", "block_number", "status"]
        )
    else:
        print(f"[Worker {proc_id}] WARNING: No raw logs captured.")
    
    print(f"[Worker {proc_id}] Finished.")


# ========== Main Experiment Loop ==========
def main():
    print("=== DAS System v3 Macro‑Benchmark Multi‑Core Matrix (Phase 4+) ===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Kill previous Ganache instances
    print("[Preflight] Killing previous Ganache processes...")
    kill_ganache()
    
    # Prepare summary results
    summary_rows = []
    
    # Outer loop: journey type
    for journey_type in MATRIX_SCENARIOS:
        print(f"\n=== Journey Type: {journey_type} ===")
        
        # Loop over amortization factor q
        for q in MATRIX_AMORTIZATION_FACTORS:
            print(f"\n--- Amortization Factor q = {q} ---")
            
            # Inner loop: concurrency N
            for N in MATRIX_CONCURRENCY_LEVELS:
                print(f"\n   --- Concurrency N = {N} ---")
                iteration_start = time.time()
                
                # 1. Start Ganache network (single network for all processes)
                print("   1. Starting Ganache network...")
                topology = get_topology()
                ganache = GanacheManager()
                max_retries = 5
                started = False
                for attempt in range(max_retries):
                    try:
                        print(f"      [System] Attempt {attempt+1}/{max_retries}...")
                        ganache.start_network(topology)
                        started = True
                        break
                    except RuntimeError as e:
                        if "already in use" in str(e):
                            print(f"      [System] Ports in use. Killing and waiting 10s...")
                            kill_ganache()
                            time.sleep(10)
                        else:
                            raise e
                if not started:
                    raise RuntimeError("Failed to start Ganache after multiple retries.")
                time.sleep(2)
                
                # 2. Prepare managers and deploy contracts (once for this iteration)
                network = ConnectionManager(topology)
                from config_matrix import MNEMONIC
                identity = UserManager(MNEMONIC)
                deployer = ContractDeployer(network, identity)
                injector = MacroTransactionInjector(network, identity)
                
                wait_for_nodes(network)
                
                print("   2. Deploying contracts...")
                registry = deployer.deploy_infrastructure(topology)
                print(f"      Registry keys: {list(registry.keys())}")
                print("      Waiting for contracts to be fully mined (15 seconds)...")
                time.sleep(15)
                
                # 3. Start monitor (optional, may interfere with multi‑process)
                monitor = MacroMonitor(network)
                monitor.start()
                
                # 4. Launch worker processes
                print(f"   3. Launching {MATRIX_PROCESSES} worker processes...")
                processes = []
                users_per_proc = N // MATRIX_PROCESSES
                remainder = N % MATRIX_PROCESSES
                user_start = 0
                for proc_id in range(MATRIX_PROCESSES):
                    user_end = user_start + users_per_proc + (1 if proc_id < remainder else 0)
                    if user_start >= user_end:
                        # No users assigned to this process (should not happen with N >= MATRIX_PROCESSES)
                        continue
                    p = multiprocessing.Process(
                        target=run_worker_process,
                        args=(
                            proc_id,
                            user_start,
                            user_end,
                            N,  # total concurrency (for logging)
                            MATRIX_JOURNEYS_PER_USER,
                            q,
                            journey_type,
                            topology,
                            registry,
                            timestamp,
                        )
                    )
                    processes.append(p)
                    p.start()
                    user_start = user_end
                
                # 5. Wait for all processes to finish
                for p in processes:
                    p.join()
                    if p.exitcode != 0:
                        print(f"   [Warning] Process {p.name} exited with code {p.exitcode}")
                
                # 6. Stop monitor
                monitor.stop()
                
                # 7. Calculate aggregate metrics (monitor saw all transactions across processes)
                metrics = monitor.calculate()
                print(f"   4. Results: TPS = {metrics['tps']:.2f}, Gas/sec = {metrics['gas_per_sec']:.0f}")
                
                # 8. Dump block‑level logs
                if monitor.block_logs:
                    dump_csv(
                        monitor.block_logs,
                        f"matrix_blocks_{journey_type}_N{N}_q{q}_{timestamp}.csv",
                        fieldnames=["block_number", "timestamp", "tx_count", "gas_used", "gas_limit"]
                    )
                else:
                    print("      WARNING: No block logs captured.")
                
                # 9. Record summary row
                iteration_end = time.time()
                makespan = iteration_end - iteration_start
                summary_rows.append({
                    "journey_type": journey_type,
                    "concurrency": N,
                    "amortization_factor": q,
                    "journeys_per_user": MATRIX_JOURNEYS_PER_USER,
                    "total_txs": metrics["total_txs"],
                    "total_gas": metrics["total_gas"],
                    "total_blocks": metrics["total_blocks"],
                    "total_time": metrics["total_time"],
                    "tps": metrics["tps"],
                    "gas_per_sec": metrics["gas_per_sec"],
                    "makespan_seconds": makespan,
                    "processes": MATRIX_PROCESSES,
                })
                
                # 10. Clean up before next iteration
                print("   5. Cleaning up Ganache...")
                ganache.stop_network()
                kill_ganache()
                time.sleep(5)
    
    # 11. Save summary CSV
    print("\n=== Saving experiment summary ===")
    dump_csv(
        summary_rows,
        f"matrix_multicore_summary_{timestamp}.csv",
        fieldnames=[
            "journey_type",
            "concurrency",
            "amortization_factor",
            "journeys_per_user",
            "total_txs",
            "total_gas",
            "total_blocks",
            "total_time",
            "tps",
            "gas_per_sec",
            "makespan_seconds",
            "processes",
        ]
    )
    
    print("\nMulti‑core matrix experiment completed successfully.")


if __name__ == "__main__":
    main()