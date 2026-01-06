"""
Micro‑benchmark for DAS System v3 – Statistical Fix.

This script corrects two issues:
1. Misleading per‑transaction stats → per‑journey aggregation.
2. Baseline lag diagnosis with explicit monitoring hygiene.

Usage: `python -m scenarios.exp_micro_stats`
"""
import csv
import time
import typing as t
from dataclasses import dataclass, field
from pathlib import Path
import sys
import os
from collections import defaultdict
import statistics
import random

from web3 import Web3

from config import BLOCK_TIME, get_topology, TEST_USER_INDEX, GAS_LIMIT, MICRO_BENCHMARK_ITERATIONS
from core.identity import UserManager
from core.injector import TransactionInjector
from core.monitor import NetworkMonitor
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer


@dataclass
class JourneyStats:
    """
    Holds latencies and gas consumptions for a single journey (sequence of steps).
    """
    latencies: t.List[float] = field(default_factory=list)
    gas_used: t.List[int] = field(default_factory=list)

    def total_time(self) -> float:
        return sum(self.latencies)

    def total_gas(self) -> int:
        return sum(self.gas_used)

    def add_step(self, latency: float, gas: int) -> None:
        self.latencies.append(latency)
        self.gas_used.append(gas)

    def avg_latency(self) -> float:
        return statistics.mean(self.latencies) if self.latencies else 0.0

    def avg_gas(self) -> float:
        return statistics.mean(self.gas_used) if self.gas_used else 0.0


def run():
    print("=== DAS System v3 Micro-Benchmark (Statistical Fix) ===")

    # 1. Start Ganache network
    print("\n1. Starting Ganache network...")
    topology = get_topology()
    ganache = GanacheManager()
    ganache.start_network(topology)
    time.sleep(2)  # let processes stabilize

    # 2. Prepare managers
    network = ConnectionManager(topology)
    from config import MNEMONIC
    identity = UserManager(MNEMONIC)
    injector = TransactionInjector(network, identity)
    monitor = NetworkMonitor(network)
    deployer = ContractDeployer(network, identity)

    # 3. Deploy contracts
    print("\n2. Deploying contracts...")
    registry = deployer.deploy_infrastructure(topology)
    print(f"   Registry: {list(registry.keys())}")
    print("   Waiting for contracts to be fully mined (15 seconds)...")
    time.sleep(15)

    # 4. Define user (dedicated test user)
    user_account = identity.get_user(TEST_USER_INDEX)
    user_address = user_account.address
    print(f"\n3. Using test user (index {TEST_USER_INDEX}): {user_address}")

    # Helper to send a single transaction and wait for inclusion
    def send_and_wait(shard_id: int, contract_addr: str, abi: list, function_name: str, args: tuple = (), iteration: int = 0) -> t.Dict[str, t.Any]:
        """Send a transaction and wait for it to be mined, return metrics."""
        node_name = f"shard_{shard_id}" if shard_id >= 0 else ("execution" if shard_id == -1 else "baseline")
        web3 = network.get_web3(node_name)
        contract = web3.eth.contract(address=contract_addr, abi=abi)
        # Build transaction
        nonce = identity.nonce_manager.get_and_increment(user_address, scope=node_name)
        tx = contract.functions[function_name](*args).build_transaction({
            "from": user_address,
            "gas": GAS_LIMIT,
            "gasPrice": web3.to_wei(20, "gwei"),
            "nonce": nonce,
        })
        signed = user_account.sign_transaction(tx)
        
        # 1. Define the Latency Model (e.g., Gaussian: Mean=0.5s, StdDev=0.2s)
        # Clamp it so it's never negative, minimum 50ms.
        network_delay = max(0.05, random.gauss(0.5, 0.2))
        
        # 2. Capture "User Click Time" (The TRUE start of latency)
        user_click_time = time.time()
        
        # 3. Simulate the propagation delay
        print(f"   [Net] Simulating network latency: {network_delay*1000:.0f}ms")
        time.sleep(network_delay)
        
        # 4. Actual Submission to Ganache
        tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hash_hex = Web3.to_hex(tx_hash)
        
        # 5. Track using the USER CLICK TIME, not the submission time
        # This counts the network delay as part of the system latency.
        monitor.track([tx_hash_hex], shard_id if shard_id >= 0 else node_name, submission_time=user_click_time)
        monitor.start_polling(interval=0.5)
        success = monitor.wait_until_complete(timeout=BLOCK_TIME * 2)
        if not success:
            raise TimeoutError(f"Transaction {tx_hash_hex} not mined within timeout")
        # Get results
        results = monitor.get_results()
        # Normalize input
        target_hash = tx_hash_hex if tx_hash_hex.startswith("0x") else f"0x{tx_hash_hex}"
        # Find the relevant record
        found_record = None
        for r in results:
            # Normalize result hash too, just in case
            res_hash = r["tx_hash"]
            if not res_hash.startswith("0x"):
                res_hash = f"0x{res_hash}"
            if res_hash.lower() == target_hash.lower():
                found_record = r
                break
        if not found_record:
            # Debugging aid
            print(f"❌ Mismatch! Looking for {target_hash}")
            print(f"   Available results: {[r['tx_hash'] for r in results]}")
            raise TimeoutError(f"Transaction {target_hash} confirmed by monitor but not found in results lookup.")
        # Add iteration information
        found_record["iteration"] = iteration
        return found_record

    # Prepare result storage
    RESULTS = {"DAS": [], "2PC": [], "Single": []}

    for i in range(MICRO_BENCHMARK_ITERATIONS):
        print(f"\n--- Round {i+1} / {MICRO_BENCHMARK_ITERATIONS} ---")

        # 1. DAS Journey
        print("   DAS Journey")
        print(f"   [System] Idling for user arrival (Jitter)...")
        time.sleep(random.uniform(2, 14))
        das_metrics = []
        shard0 = "shard_0"
        exec_node = "execution"
        # Step 1: Deposit (burn on shard 0)
        shard0_das_addr = registry[shard0]["DAS"]
        shard0_das_abi = registry[shard0]["DAS_ABI"]
        burn_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "burn", (user_address, 100), i)
        das_metrics.append(burn_result)
        time.sleep(0.1)
        # mint on execution
        exec_das_addr = registry[exec_node]["DAS"]
        exec_das_abi = registry[exec_node]["DAS_ABI"]
        mint_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "mint", (user_address, 100), i)
        das_metrics.append(mint_result)
        # Step 2: Action (doWork on execution)
        exec_workload_addr = registry[exec_node]["Workload"]
        exec_workload_abi = registry[exec_node]["Workload_ABI"]
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), i)
        das_metrics.append(work_result)
        # Step 3: Withdraw (burn on execution, mint on shard 0)
        burn_exec_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "burn", (user_address, 100), i)
        das_metrics.append(burn_exec_result)
        time.sleep(0.1)
        mint_shard0_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "mint", (user_address, 100), i)
        das_metrics.append(mint_shard0_result)

        # Create JourneyStats for DAS
        das_stats = JourneyStats()
        for m in das_metrics:
            das_stats.add_step(m.get("latency", 0) or 0, m.get("gas_used", 0) or 0)
        RESULTS["DAS"].append(das_stats)

        # 2. 2PC Journey
        print("   2PC Journey")
        print(f"   [System] Idling for user arrival (Jitter)...")
        time.sleep(random.uniform(2, 14))
        tpc_metrics = []
        shard0_2pc_addr = registry[shard0]["2PC"]
        shard0_2pc_abi = registry[shard0]["2PC_ABI"]
        exec_2pc_addr = registry[exec_node]["2PC"]
        exec_2pc_abi = registry[exec_node]["2PC_ABI"]
        # Generate a unique ID
        tpc_id = random.randbytes(32)
        # Parallel lock (sequential for simplicity)
        lock_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "lock", (tpc_id,), i)
        lock_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "lock", (tpc_id,), i)
        tpc_metrics.append(lock_shard)
        tpc_metrics.append(lock_exec)
        # doWork
        work_result2 = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), i)
        tpc_metrics.append(work_result2)
        # Parallel commit
        commit_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "commit", (tpc_id,), i)
        commit_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "commit", (tpc_id,), i)
        tpc_metrics.append(commit_shard)
        tpc_metrics.append(commit_exec)

        # Create JourneyStats for 2PC
        tpc_stats = JourneyStats()
        for m in tpc_metrics:
            tpc_stats.add_step(m.get("latency", 0) or 0, m.get("gas_used", 0) or 0)
        RESULTS["2PC"].append(tpc_stats)

        # 3. Single Chain Journey (With Diagnosis & Monitor Hygiene)
        print("   Single Chain Journey")
        # Reset monitor to clear any baggage
        monitor.stop_polling()
        monitor = NetworkMonitor(network)

        print(f"   [System] Idling for user arrival (Jitter)...")
        time.sleep(random.uniform(2, 14))

        baseline_workload_addr = registry["baseline"]["Workload"]
        baseline_workload_abi = registry["baseline"]["Workload_ABI"]
        single_result = send_and_wait(-2, baseline_workload_addr, baseline_workload_abi, "doWork", (100,), i)
        # Explicit diagnostic print
        latency = single_result.get("latency", 0) or 0
        block = single_result.get("block_number", "?")
        print(f"   [Single Debug] Iter {i}: Latency {latency:.2f}s (Block {block})")

        single_stats = JourneyStats()
        single_stats.add_step(latency, single_result.get("gas_used", 0) or 0)
        RESULTS["Single"].append(single_stats)

        # Wait between iterations to let network settle
        if i < MICRO_BENCHMARK_ITERATIONS - 1:
            time.sleep(2)

    # Statistical analysis per journey
    print("\n" + "="*60)
    print("Statistical Summary (Per‑Journey)")
    print("="*60)

    for journey in ["DAS", "2PC", "Single"]:
        stats_list = RESULTS.get(journey, [])
        if not stats_list:
            continue

        # Extract total journey times and total gas
        total_times = [s.total_time() for s in stats_list]
        total_gas = [s.total_gas() for s in stats_list]

        avg_time = statistics.mean(total_times) if total_times else 0
        min_time = min(total_times) if total_times else 0
        max_time = max(total_times) if total_times else 0
        std_time = statistics.stdev(total_times) if len(total_times) > 1 else 0

        avg_gas = statistics.mean(total_gas) if total_gas else 0
        min_gas = min(total_gas) if total_gas else 0
        max_gas = max(total_gas) if total_gas else 0
        std_gas = statistics.stdev(total_gas) if len(total_gas) > 1 else 0

        print(f"\n{journey}:")
        print(f"  Journey Latency (s): avg={avg_time:.2f}, min={min_time:.2f}, max={max_time:.2f}, std={std_time:.2f}")
        print(f"  Journey Gas: avg={avg_gas:.0f}, min={min_gas}, max={max_gas}, std={std_gas:.0f}")
        print(f"  Steps per journey: {len(stats_list[0].latencies) if stats_list else 0}")

    # Also compute per‑step statistics (optional)
    print("\n" + "="*60)
    print("Per‑Step Latency (average across all journeys)")
    print("="*60)

    # For each journey type, compute average latency per step position
    for journey in ["DAS", "2PC", "Single"]:
        stats_list = RESULTS.get(journey, [])
        if not stats_list:
            continue
        num_steps = len(stats_list[0].latencies)
        # Build a list of latencies for each step index
        step_latencies = [[] for _ in range(num_steps)]
        for s in stats_list:
            for idx, lat in enumerate(s.latencies):
                step_latencies[idx].append(lat)
        print(f"\n{journey} step latencies (avg):")
        for idx, lats in enumerate(step_latencies):
            avg = statistics.mean(lats) if lats else 0
            print(f"  Step {idx+1}: {avg:.2f}s")

    # Write detailed CSV with per‑journey totals
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / "micro_benchmark_stats.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["iteration", "journey", "total_latency", "total_gas", "step_count"])
        for journey in ["DAS", "2PC", "Single"]:
            stats_list = RESULTS.get(journey, [])
            for i, s in enumerate(stats_list):
                writer.writerow([
                    i,
                    journey,
                    s.total_time(),
                    s.total_gas(),
                    len(s.latencies),
                ])
    print(f"\nJourney‑aggregated logs saved to {csv_path}")

    # Cleanup
    print("\nStopping Ganache network...")
    ganache.stop_network()
    print("Benchmark completed.")


if __name__ == "__main__":
    run()