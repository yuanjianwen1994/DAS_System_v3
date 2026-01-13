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


def deposit_funds_for_all_users(
    network: ConnectionManager,
    identity: UserManager,
    injector: MacroTransactionInjector,
    registry: dict,
) -> None:
    """
    Serial deposit loop for all test users (0 to NUM_USERS‑1).
    Each user performs a DAS deposit (Burn on Shard 0 → Mint on Execution).
    """
    print(f"[Deposit] Funding {NUM_USERS} users...")
    # We'll do batched deposits to speed up, but for simplicity do one by one
    for user_idx in range(NUM_USERS):
        # Build burn on shard 0
        def build_burn(web3, from_addr, nonce, **kwargs):
            shard_name = "shard_0"
            contract_addr = registry[shard_name]["DAS"]
            contract_abi = registry[shard_name]["DAS_ABI"]
            contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
            return contract.functions.burn(from_addr, 100).build_transaction({
                "from": from_addr,
                "nonce": nonce,
            })

        def build_mint(web3, from_addr, nonce, **kwargs):
            contract_addr = registry["execution"]["DAS"]
            contract_abi = registry["execution"]["DAS_ABI"]
            contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
            return contract.functions.mint(from_addr, 100).build_transaction({
                "from": from_addr,
                "nonce": nonce,
            })

        # Send burn
        injector.send_batch(shard_id=0, users=[user_idx], contract_func=build_burn)
        time.sleep(0.01)
        # Send mint
        injector.send_batch(shard_id=-1, users=[user_idx], contract_func=build_mint)
        time.sleep(0.01)

        if (user_idx + 1) % 20 == 0:
            print(f"  Deposited for {user_idx + 1} users")
    print("[Deposit] All users funded.")


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


def main():
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

    # 5. Pre‑flight deposit for all users
    print("\n3. Pre‑flight deposit for all test users...")
    deposit_funds_for_all_users(network, identity, injector, registry)

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
        time.sleep(3)
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


if __name__ == "__main__":
    main()