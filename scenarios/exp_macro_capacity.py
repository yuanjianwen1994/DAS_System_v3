"""
Macro‑benchmark capacity experiment for Phase 4.
Measures system throughput under high concurrent load.

Strict rule: DO NOT modify existing files in core/.
"""
import sys
import os
import time
import subprocess
import signal
import csv
from pathlib import Path
from datetime import datetime

# Add parent directory to sys.path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3 import Web3

from config_macro import (
    MACRO_DURATION,
    MACRO_WARMUP,
    MACRO_CONCURRENCY_LEVELS,
    MACRO_OPS_PER_JOURNEY,
    get_topology,
    NUM_USERS,
)
from core.identity import UserManager
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer
from core.macro_injector import MacroTransactionInjector
from core.macro_traffic import MacroTrafficGenerator
from core.macro_monitor import MacroMonitor


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


def run():
    print("=== DAS System v3 Macro‑Benchmark (Phase 4) ===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Kill previous Ganache instances
    print("[Preflight] Killing previous Ganache processes...")
    kill_ganache()

    # 1. Start Ganache network with robust retry loop
    print("\n1. Starting Ganache network...")
    topology = get_topology()
    ganache = GanacheManager()
    
    # Robust startup loop
    max_retries = 5
    started = False
    for attempt in range(max_retries):
        try:
            print(f"   [System] Attempting to start network (Attempt {attempt+1}/{max_retries})...")
            ganache.start_network(topology)
            started = True
            break
        except RuntimeError as e:
            if "already in use" in str(e):
                print(f"   [System] Ports in use. Killing and waiting 10s...")
                kill_ganache()
                time.sleep(10)
            else:
                raise e
    
    if not started:
        raise RuntimeError("Failed to start Ganache after multiple retries due to port issues.")

    time.sleep(2)  # let processes stabilize

    # 2. Prepare managers
    network = ConnectionManager(topology)
    from config_macro import MNEMONIC
    identity = UserManager(MNEMONIC)
    deployer = ContractDeployer(network, identity)
    injector = MacroTransactionInjector(network, identity)

    # 3. Wait for nodes to be fully reachable
    wait_for_nodes(network)

    # 4. Deploy contracts
    print("\n2. Deploying contracts...")
    registry = deployer.deploy_infrastructure(topology)
    print(f"   Registry keys: {list(registry.keys())}")
    print("   Waiting for contracts to be fully mined (15 seconds)...")
    time.sleep(15)

    # 6. Create traffic generator and monitor
    traffic = MacroTrafficGenerator(network, identity, injector, registry)
    monitor = MacroMonitor(network)

    # 7. Experiment loop over concurrency levels
    results = []
    for concurrency in MACRO_CONCURRENCY_LEVELS:
        print(f"\n--- Concurrency Level: {concurrency} ---")
        # Warm‑up period
        if MACRO_WARMUP > 0:
            print(f"   Warm‑up for {MACRO_WARMUP} seconds...")
            time.sleep(MACRO_WARMUP)

        # Start monitor
        monitor.start()

        # Start traffic (DAS journey) for the exact duration
        print(f"   Starting {concurrency} DAS workers for {MACRO_DURATION} seconds...")
        traffic.run_for_duration(
            concurrency=concurrency,
            duration_seconds=MACRO_DURATION,
            journey_type="DAS",
            ops_per_journey=MACRO_OPS_PER_JOURNEY,
        )

        # Stop monitor and traffic (by interrupting? we can't easily stop traffic)
        # We'll just let the thread continue in background after experiment.
        monitor.stop()

        # Calculate metrics
        metrics = monitor.calculate()
        print(f"   Results: TPS = {metrics['tps']:.2f}, Gas/sec = {metrics['gas_per_sec']:.0f}")

        results.append({
            "concurrency": concurrency,
            "tps": metrics["tps"],
            "gas_per_sec": metrics["gas_per_sec"],
            "total_txs": metrics["total_txs"],
            "total_gas": metrics["total_gas"],
            "total_blocks": metrics["total_blocks"],
            "total_time": metrics["total_time"],
        })

        # Kill Ganache between runs to avoid state contamination
        print("   Killing Ganache for clean state...")
        ganache.stop_network()
        kill_ganache()
        # No longer wait for ports to be released; rely on retry loop
        time.sleep(10)  # increased from 3s to 10s
        # Restart Ganache with robust retry loop
        print("   Restarting Ganache...")
        started = False
        for attempt in range(max_retries):
            try:
                ganache.start_network(topology)
                started = True
                break
            except RuntimeError as e:
                if "already in use" in str(e):
                    print(f"   [System] Ports in use. Killing and waiting 10s...")
                    kill_ganache()
                    time.sleep(10)
                else:
                    raise e
        if not started:
            raise RuntimeError("Failed to restart Ganache after multiple retries.")
        time.sleep(2)
        wait_for_nodes(network)
        # Redeploy contracts (optional, but we need fresh registry)
        registry = deployer.deploy_infrastructure(topology)
        time.sleep(15)
        # Re‑create injector and traffic because network connections may have changed
        injector = MacroTransactionInjector(network, identity)
        traffic = MacroTrafficGenerator(network, identity, injector, registry)
        monitor = MacroMonitor(network)

    # 8. Save results
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / f"macro_capacity_{timestamp}.csv"
    with open(csv_path, "w", newline="") as f:
        fieldnames = ["concurrency", "tps", "gas_per_sec", "total_txs", "total_gas", "total_blocks", "total_time"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved to {csv_path}")

    # 9. Final cleanup
    print("\nStopping Ganache network...")
    ganache.stop_network()
    kill_ganache()
    print("Experiment completed.")
