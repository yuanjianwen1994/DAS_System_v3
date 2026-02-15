"""
Macro‑benchmark matrix experiment for Phase 4 (Top‑Tier Paper quality).
Runs task‑based workloads with varying concurrency (N) and amortization factor (q).
Logs raw transaction‑level and block‑level data for CDF/saturation analysis.

*** MULTIPROCESSING VERSION ***
Uses multiprocessing to bypass GIL bottleneck, distributing workload across CPU cores.
Each process runs its own ConnectionManager, UserManager, Injector, and TrafficGenerator.
Logs are merged automatically after each iteration via consolidate_logs.
"""
import sys
import os
import time
import subprocess
import csv
import glob
import pandas as pd
import multiprocessing
import traceback
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
from core.network import AnvilManager, ConnectionManager
from core.deployer import ContractDeployer
from core.macro_injector import MacroTransactionInjector
from core.macro_traffic import MacroTrafficGenerator
from core.macro_monitor import MacroMonitor


# ========== Experiment Parameters (imported from config_matrix) ==========
CONCURRENCY_LEVELS = MATRIX_CONCURRENCY_LEVELS
AMORTIZATION_FACTORS = MATRIX_AMORTIZATION_FACTORS
JOURNEYS_PER_USER = MATRIX_JOURNEYS_PER_USER
NUM_PROCESSES = MATRIX_PROCESSES  # number of worker processes
SCENARIOS = MATRIX_SCENARIOS


# ========== Helper Functions ==========
def kill_nodes():
    """Aggressively kill all anvil processes."""
    try:
        if os.name == 'nt':
            subprocess.call(["taskkill", "/F", "/IM", "anvil.exe", "/T"], stderr=subprocess.DEVNULL)
        else:
            subprocess.call(["pkill", "-f", "anvil"], stderr=subprocess.DEVNULL)
    except Exception:
        pass


def wait_for_ports_released(ports=[8580, 8581, 9000, 9999], timeout=30):
    """Wait until all specified TCP ports become available."""
    import socket
    start = time.time()
    for port in ports:
        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"Port {port} still in use after {timeout}s")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("127.0.0.1", port))
                    s.close()
                    break  # port is free
                except OSError:
                    time.sleep(0.5)
    print(f"   [System] Ports {ports} are now free.")


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


def dump_csv(data, filename, fieldnames):
    """Write a list of dicts to CSV."""
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    path = logs_dir / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"      [Data] {filename} saved ({len(data)} rows).")


def consolidate_logs(journey_type: str, n: int, q: int, timestamp: str):
    """
    Finds all partial process logs for a specific run, merges them into one,
    and deletes the partial files.
    """
    logs_dir = Path(__file__).parent.parent / "logs"
    # Pattern match: raw_txs_p*_{journey_type}_N{n}_q{q}_{timestamp}.csv
    pattern = f"raw_txs_p*_{journey_type}_N{n}_q{q}_{timestamp}.csv"
    files = glob.glob(str(logs_dir / pattern))
    
    if not files:
        print(f"[System] No log files found to merge for pattern: {pattern}")
        return

    print(f"[System] Merging {len(files)} log files for N={n}, q={q}...")
    
    try:
        # Read and Concat
        df_list = []
        for f in files:
            try:
                df = pd.read_csv(f)
                df_list.append(df)
            except pd.errors.EmptyDataError:
                pass # Ignore empty files
        
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            
            # Save Combined File
            combined_filename = f"combined_raw_txs_{journey_type}_N{n}_q{q}_{timestamp}.csv"
            combined_path = logs_dir / combined_filename
            combined_df.to_csv(combined_path, index=False)
            print(f"[System] Saved combined log: {combined_filename} ({len(combined_df)} records)")
            
            # Delete Partial Files (Only if merge succeeded)
            for f in files:
                try:
                    os.remove(f)
                except OSError as e:
                    print(f"Warning: Could not delete {f}: {e}")
        else:
            print("[System] Warning: All log files were empty.")
            
    except Exception as e:
        print(f"[System] Error during log consolidation: {e}")


# ========== Worker Process Function ==========
def run_worker_process(
    process_id: int,
    user_offset: int,
    num_users: int,
    total_N: int,
    q: int,
    journey_type: str,
    target_journeys: int,
    timestamp: str,
    topology: Dict[str, Any],
    registry: Dict[str, Dict[str, Any]],
):
    """
    Worker process entry point. Re-initializes stack to avoid pickling locks.
    """
    print(f"[Process {process_id}] Starting {num_users} users (Offset {user_offset})...")
    try:
        # Re-init Managers (Fast, just connecting to ports)
        from config_matrix import MNEMONIC
        network = ConnectionManager(topology)
        identity = UserManager(MNEMONIC)
        injector = MacroTransactionInjector(network, identity)
        
        # Wait for nodes (they should already be up)
        wait_for_nodes(network)
        
        # Create traffic generator with process_id and user_offset
        traffic = MacroTrafficGenerator(
            network_manager=network,
            identity_manager=identity,
            injector=injector,
            registry=registry,
            process_id=process_id,
            user_offset=user_offset,
        )
        
        # Run task‑based traffic
        traffic.run_task_based(
            concurrency=num_users,
            journey_type=journey_type,
            ops_per_journey=q,
            journeys_per_user=target_journeys,
        )
        
        # Save logs to per‑process CSV (MacroTrafficGenerator already writes raw logs to file?)
        # The generator's raw_logs are stored in memory; we need to dump them.
        if traffic.raw_logs:
            dump_csv(
                traffic.raw_logs,
                f"raw_txs_p{process_id}_{journey_type}_N{total_N}_q{q}_{timestamp}.csv",
                fieldnames=["timestamp", "journey_id", "worker_id", "tx_type", "latency_s", "gas_used", "block_number", "status"]
            )
        else:
            print(f"[Process {process_id}] WARNING: No raw logs captured.")
            
    except Exception as e:
        print(f"[Process {process_id}] CRASHED: {e}")
        traceback.print_exc()
    finally:
        print(f"[Process {process_id}] Finished.")


# ========== Main Experiment Loop ==========
def main():
    print("=== DAS System v3 Macro‑Benchmark Matrix (Phase 4) ===")
    print("*** MULTIPROCESSING MODE with {} worker processes ***".format(NUM_PROCESSES))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Kill previous Anvil instances
    print("[Preflight] Killing previous Anvil processes...")
    kill_nodes()
    wait_for_ports_released()

    # Prepare summary results
    summary_rows = []

    # Outer loop: amortization factor q
    for q in AMORTIZATION_FACTORS:
        print(f"\n--- Amortization Factor q = {q} ---")
        # Inner loop: concurrency N
        for N in CONCURRENCY_LEVELS:
            print(f"\n   --- Concurrency N = {N} ---")
            # Innermost loop: journey type scenarios
            for journey_type in SCENARIOS:
                print(f"\n>>> Starting Iteration: N={N}, q={q}, Type={journey_type} <<<")
                iteration_start = time.time()

                # 1. Start Anvil network with robust retry loop
                print("   1. Starting Anvil network...")
                topology = get_topology()
                ganache = AnvilManager()
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
                            kill_nodes()
                            time.sleep(10)
                        else:
                            raise e
                if not started:
                    raise RuntimeError("Failed to start Anvil after multiple retries.")

                time.sleep(2)  # let processes stabilize

                # 2. Prepare managers and deploy contracts (once for this iteration)
                network = ConnectionManager(topology)
                from config_matrix import MNEMONIC
                identity = UserManager(MNEMONIC)
                # Force reset nonce cache to match fresh Anvil state
                identity.nonce_manager.reset()
                deployer = ContractDeployer(network, identity)
                injector = MacroTransactionInjector(network, identity)

                # 3. Wait for nodes to be fully reachable
                wait_for_nodes(network)

                # 4. Deploy contracts
                print("   2. Deploying contracts...")
                registry = deployer.deploy_infrastructure(topology)
                print(f"      Registry keys: {list(registry.keys())}")
                print("      Waiting for contracts to be fully mined (15 seconds)...")
                time.sleep(15)

                # 5. Start monitor (optional, may interfere with multi‑process)
                monitor = MacroMonitor(network)
                monitor.start()

                # 6. Launch worker processes
                print(f"   3. Spawning {NUM_PROCESSES} worker processes for N={N}...")
                procs = []
                total_users_assigned = 0
                # Calculate distribution
                base_users = N // NUM_PROCESSES
                remainder = N % NUM_PROCESSES
                current_offset = 0
                
                for i in range(NUM_PROCESSES):
                    # Distribute remainder to first few processes
                    count = base_users + (1 if i < remainder else 0)
                    if count == 0:
                        continue  # Skip if N < NUM_PROCESSES
                    
                    p = multiprocessing.Process(
                        target=run_worker_process,
                        args=(
                            i,              # process_id
                            current_offset, # user_offset
                            count,          # num_users
                            N,              # total_N (total concurrency for logging)
                            q,              # amortization_factor
                            journey_type,   # dynamic journey_type
                            JOURNEYS_PER_USER,
                            timestamp,
                            topology,
                            registry,
                        )
                    )
                    p.start()
                    procs.append(p)
                    current_offset += count
                    
                    # === NEW: Desynchronize Processes ===
                    # Wait 2 seconds between launching each process group.
                    # This spreads the 16 groups over ~32 seconds, breaking the resonance.
                    print(f"   [System] Process {i} started. Staggering next launch in 2s...")
                    time.sleep(2.0)
                    
                    total_users_assigned += count
                
                if total_users_assigned != N:
                    print(f"   [Warning] User distribution mismatch: assigned {total_users_assigned}, expected {N}")

                # 7. Wait for all processes to finish
                print("   4. Waiting for worker processes to finish...")
                for p in procs:
                    p.join()
                    if p.exitcode != 0:
                        print(f"   [Warning] Process {p.name} exited with code {p.exitcode}")

                # 8. Stop monitor
                monitor.stop()

                # 9. Calculate aggregate metrics
                metrics = monitor.calculate()
                print(f"   5. Results: TPS = {metrics['tps']:.2f}, Gas/sec = {metrics['gas_per_sec']:.0f}")

                # 10. Dump block‑level logs
                print("   6. Dumping block logs...")
                if monitor.block_logs:
                    dump_csv(
                        monitor.block_logs,
                        f"matrix_blocks_{journey_type}_N{N}_q{q}_{timestamp}.csv",
                        fieldnames=["node", "block_number", "timestamp", "tx_count", "gas_used", "gas_limit"]
                    )
                else:
                    print("      WARNING: No block logs captured.")

                # 11. Auto‑merge per‑process raw logs
                print("   7. Merging per‑process raw logs...")
                consolidate_logs(journey_type, N, q, timestamp)

                # 12. Record summary row
                iteration_end = time.time()
                makespan = iteration_end - iteration_start
                summary_rows.append({
                    "journey_type": journey_type,
                    "concurrency": N,
                    "amortization_factor": q,
                    "journeys_per_user": JOURNEYS_PER_USER,
                    "total_txs": metrics["total_txs"],
                    "total_gas": metrics["total_gas"],
                    "total_blocks": metrics["total_blocks"],
                    "total_time": metrics["total_time"],
                    "tps": metrics["tps"],
                    "gas_per_sec": metrics["gas_per_sec"],
                    "makespan_seconds": makespan,
                    "processes": NUM_PROCESSES,
                })

                # 13. Clean up before next iteration
                print("   8. Cleaning up Anvil...")
                ganache.stop_network()
                kill_nodes()
                time.sleep(5)

    # 14. Save summary CSV
    print("\n=== Saving experiment summary ===")
    dump_csv(
        summary_rows,
        f"matrix_summary_{timestamp}.csv",
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

    print("\nMatrix experiment completed successfully.")


if __name__ == "__main__":
    main()