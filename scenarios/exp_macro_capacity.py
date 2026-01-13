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


def wait_for_port_release(port: int, timeout: int = 30) -> None:
    """
    Wait until a TCP port becomes available (i.e., the previous process has released it).
    Raises TimeoutError if the port is still occupied after `timeout` seconds.
    """
    import socket
    start_time = time.time()
    while time.time() - start_time < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                # Successfully bound -> port is free
                print(f"[Port {port}] Released.")
                return
            except OSError:
                pass
        time.sleep(1)
    raise TimeoutError(f"Port {port} still occupied after {timeout}s")

def wait_for_nodes(network: ConnectionManager, topology: dict) -> None:
    """
    Wait until all nodes in the topology are reachable via web3.isConnected().
    """
    nodes = list(topology.get("shards", {}).keys()) + ["execution", "baseline"]
    print("[Wait] Checking node connectivity...")
    for node in nodes:
        web3 = network.get_web3(node)
        connected = False
        for attempt in range(30):  # 30 attempts, 1 second each
            try:
                if web3.is_connected():
                    connected = True
                    break
            except Exception:
                pass
            time.sleep(1)
        if not connected:
            raise RuntimeError(f"Node {node} failed to become reachable after 30 seconds")
        print(f"  ✓ {node}")
    print("[Wait] All nodes ready.")




def kill_ganache() -> None:
    """
    Force kill any lingering Ganache processes to avoid port conflicts.
    """
    # Windows
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/IM", "ganache*.exe"], shell=True, capture_output=True)
    else:
        subprocess.run(["pkill", "-f", "ganache"], capture_output=True)
    time.sleep(2)


def run():
    print("=== DAS System v3 Macro‑Benchmark (Phase 4) ===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Kill previous Ganache instances
    print("[Preflight] Killing previous Ganache processes...")
    kill_ganache()

    # 1. Start Ganache network
    print("\n1. Starting Ganache network...")
    topology = get_topology()
    ganache = GanacheManager()
    ganache.start_network(topology)
    time.sleep(2)  # let processes stabilize

    # 2. Prepare managers
    network = ConnectionManager(topology)
    from config_macro import MNEMONIC
    identity = UserManager(MNEMONIC)
    deployer = ContractDeployer(network, identity)
    injector = MacroTransactionInjector(network, identity)

    # 3. Wait for nodes to be fully reachable
    wait_for_nodes(network, topology)

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
        # Wait for ports to be released before restart
        for port in [8580, 8581, 9000, 9999]:
            wait_for_port_release(port, timeout=30)
        time.sleep(10)  # increased from 3s to 10s
        # Restart Ganache
        print("   Restarting Ganache...")
        ganache.start_network(topology)
        time.sleep(2)
        wait_for_nodes(network, topology)
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

