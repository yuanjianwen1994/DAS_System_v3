"""
Macro‑benchmark matrix experiment for Phase 4 (Top‑Tier Paper quality).
Runs task‑based workloads with varying concurrency (N) and amortization factor (q).
Logs raw transaction‑level and block‑level data for CDF/saturation analysis.
"""
import sys
import os
import time
import subprocess
import csv
from pathlib import Path
from datetime import datetime

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
)
from core.identity import UserManager
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer
from core.macro_injector import MacroTransactionInjector
from core.macro_traffic import MacroTrafficGenerator
from core.macro_monitor import MacroMonitor


# ========== Experiment Parameters (imported from config_matrix) ==========
CONCURRENCY_LEVELS = MATRIX_CONCURRENCY_LEVELS
AMORTIZATION_FACTORS = MATRIX_AMORTIZATION_FACTORS
JOURNEYS_PER_USER = MATRIX_JOURNEYS_PER_USER

# ========== Helper Functions ==========
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


def main():
    print("=== DAS System v3 Macro‑Benchmark Matrix (Phase 4) ===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Kill previous Ganache instances
    print("[Preflight] Killing previous Ganache processes...")
    kill_ganache()

    # Prepare summary results
    summary_rows = []

    # Outer loop: amortization factor q
    for q in AMORTIZATION_FACTORS:
        print(f"\n--- Amortization Factor q = {q} ---")
        # Inner loop: concurrency N
        for N in CONCURRENCY_LEVELS:
            print(f"\n   --- Concurrency N = {N} ---")
            iteration_start = time.time()

            # 1. Start Ganache network with robust retry loop
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

            time.sleep(2)  # let processes stabilize

            # 2. Prepare managers
            network = ConnectionManager(topology)
            from config_matrix import MNEMONIC
            identity = UserManager(MNEMONIC)
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

            # 5. Create traffic generator and monitor
            traffic = MacroTrafficGenerator(network, identity, injector, registry)
            monitor = MacroMonitor(network)

            # 6. Start monitor
            monitor.start()

            # 7. Start traffic in TASK‑BASED mode
            print(f"   3. Starting {N} DAS workers, each completing {JOURNEYS_PER_USER} journeys with q={q}...")
            traffic.run_task_based(
                concurrency=N,
                journeys_per_user=JOURNEYS_PER_USER,
                journey_type="DAS",
                ops_per_journey=q,
            )

            # 8. Stop monitor
            monitor.stop()

            # 9. Calculate aggregate metrics
            metrics = monitor.calculate()
            print(f"   4. Results: TPS = {metrics['tps']:.2f}, Gas/sec = {metrics['gas_per_sec']:.0f}")

            # 10. Dump raw data
            print("   5. Dumping raw logs...")
            # Raw transaction logs
            if traffic.raw_logs:
                dump_csv(
                    traffic.raw_logs,
                    f"matrix_raw_txs_N{N}_q{q}_{timestamp}.csv",
                    fieldnames=["timestamp", "worker_id", "tx_type", "latency_s", "gas_used", "block_number", "status"]
                )
            else:
                print("      WARNING: No raw transaction logs captured.")

            # Block‑level logs
            if monitor.block_logs:
                dump_csv(
                    monitor.block_logs,
                    f"matrix_blocks_N{N}_q{q}_{timestamp}.csv",
                    fieldnames=["block_number", "timestamp", "tx_count", "gas_used", "gas_limit"]
                )
            else:
                print("      WARNING: No block logs captured.")

            # 11. Record summary row
            iteration_end = time.time()
            makespan = iteration_end - iteration_start
            summary_rows.append({
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
            })

            # 12. Clean up before next iteration
            print("   6. Cleaning up Ganache...")
            ganache.stop_network()
            kill_ganache()
            time.sleep(5)

    # 13. Save summary CSV
    print("\n=== Saving experiment summary ===")
    dump_csv(
        summary_rows,
        f"matrix_summary_{timestamp}.csv",
        fieldnames=[
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
        ]
    )

    print("\nMatrix experiment completed successfully.")


if __name__ == "__main__":
    main()