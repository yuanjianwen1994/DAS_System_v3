"""
Micro‑benchmark for DAS System v3.
Measure latency and gas cost of the Deposit‑>Execute‑>Withdraw lifecycle.
"""
import csv
import time
import typing as t
from pathlib import Path
import sys
import os

from web3 import Web3

from config import BLOCK_TIME, get_topology, TEST_USER_INDEX, GAS_LIMIT, MICRO_BENCHMARK_ITERATIONS
from core.identity import UserManager
from core.injector import TransactionInjector
from core.monitor import NetworkMonitor
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer


def run():
    print("=== DAS System v3 Micro-Benchmark (Cost Analysis) ===")

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
    time.sleep(15)  # Ensure contracts are fully mined before experiment

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
        tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hash_hex = Web3.to_hex(tx_hash)
        # Track with monitor
        monitor.track([tx_hash_hex], shard_id if shard_id >= 0 else node_name)
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

    # Collect all metrics across iterations
    all_metrics = []  # list of (journey_name, step_name, metrics_dict)

    for iteration in range(MICRO_BENCHMARK_ITERATIONS):
        print(f"\n--- Iteration {iteration + 1}/{MICRO_BENCHMARK_ITERATIONS} ---")

        # DAS Journey
        print("   DAS Journey")
        das_metrics = []
        shard0 = "shard_0"
        exec_node = "execution"
        # Step 1: Deposit (burn on shard 0)
        shard0_das_addr = registry[shard0]["DAS"]
        shard0_das_abi = registry[shard0]["DAS_ABI"]
        burn_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "burn", (user_address, 100), iteration)
        das_metrics.append(("Burn (Shard0)", burn_result))
        time.sleep(0.1)
        # mint on execution
        exec_das_addr = registry[exec_node]["DAS"]
        exec_das_abi = registry[exec_node]["DAS_ABI"]
        mint_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "mint", (user_address, 100), iteration)
        das_metrics.append(("Mint (Execution)", mint_result))
        # Step 2: Action (doWork on execution)
        exec_workload_addr = registry[exec_node]["Workload"]
        exec_workload_abi = registry[exec_node]["Workload_ABI"]
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration)
        das_metrics.append(("Work (Execution)", work_result))
        # Step 3: Withdraw (burn on execution, mint on shard 0)
        burn_exec_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "burn", (user_address, 100), iteration)
        das_metrics.append(("Burn (Execution)", burn_exec_result))
        time.sleep(0.1)
        mint_shard0_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "mint", (user_address, 100), iteration)
        das_metrics.append(("Mint (Shard0)", mint_shard0_result))

        # 2PC Journey
        print("   2PC Journey")
        tpc_metrics = []
        shard0_2pc_addr = registry[shard0]["2PC"]
        shard0_2pc_abi = registry[shard0]["2PC_ABI"]
        exec_2pc_addr = registry[exec_node]["2PC"]
        exec_2pc_abi = registry[exec_node]["2PC_ABI"]
        # Generate a unique ID
        import random
        tpc_id = random.randbytes(32)
        # Parallel lock (sequential for simplicity)
        lock_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "lock", (tpc_id,), iteration)
        lock_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "lock", (tpc_id,), iteration)
        tpc_metrics.append(("Lock (Shard0)", lock_shard))
        tpc_metrics.append(("Lock (Execution)", lock_exec))
        # doWork
        work_result2 = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration)
        tpc_metrics.append(("Work (Execution)", work_result2))
        # Parallel commit
        commit_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "commit", (tpc_id,), iteration)
        commit_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "commit", (tpc_id,), iteration)
        tpc_metrics.append(("Commit (Shard0)", commit_shard))
        tpc_metrics.append(("Commit (Execution)", commit_exec))

        # Single Chain Journey
        print("   Single Chain Journey")
        baseline_workload_addr = registry["baseline"]["Workload"]
        baseline_workload_abi = registry["baseline"]["Workload_ABI"]
        single_result = send_and_wait(-2, baseline_workload_addr, baseline_workload_abi, "doWork", (100,), iteration)
        single_metrics = [("Work (Baseline)", single_result)]

        # Append to global list
        for journey_name, metrics_list in [("DAS", das_metrics), ("2PC", tpc_metrics), ("Single", single_metrics)]:
            for step_name, m in metrics_list:
                all_metrics.append((journey_name, step_name, m))

        # Wait between iterations to let network settle
        if iteration < MICRO_BENCHMARK_ITERATIONS - 1:
            time.sleep(2)

    # Statistical analysis
    print("\n" + "="*60)
    print("Statistical Summary (across all iterations)")
    print("="*60)

    # Group by journey
    from collections import defaultdict
    journey_stats = defaultdict(list)
    for journey_name, step_name, m in all_metrics:
        journey_stats[journey_name].append(m)

    # Compute statistics for each journey
    for journey in ["DAS", "2PC", "Single"]:
        metrics = journey_stats.get(journey, [])
        if not metrics:
            continue
        latencies = [m.get("latency", 0) or 0 for m in metrics]
        gas_used = [m.get("gas_used", 0) or 0 for m in metrics]
        import statistics
        avg_latency = statistics.mean(latencies) if latencies else 0
        min_latency = min(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        std_latency = statistics.stdev(latencies) if len(latencies) > 1 else 0
        avg_gas = statistics.mean(gas_used) if gas_used else 0
        min_gas = min(gas_used) if gas_used else 0
        max_gas = max(gas_used) if gas_used else 0
        std_gas = statistics.stdev(gas_used) if len(gas_used) > 1 else 0

        print(f"\n{journey}:")
        print(f"  Latency (s): avg={avg_latency:.2f}, min={min_latency:.2f}, max={max_latency:.2f}, std={std_latency:.2f}")
        print(f"  Gas: avg={avg_gas:.0f}, min={min_gas}, max={max_gas}, std={std_gas:.0f}")

    # Write detailed CSV with iteration column
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / "micro_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["iteration", "journey", "step", "tx_hash", "shard_id", "start_time", "end_time", "latency", "gas_used", "status"])
        for journey_name, step_name, m in all_metrics:
            writer.writerow([
                m.get("iteration", ""),
                journey_name,
                step_name,
                m.get("tx_hash", ""),
                m.get("shard_id", ""),
                m.get("start_time", ""),
                m.get("end_time", ""),
                m.get("latency", ""),
                m.get("gas_used", ""),
                m.get("status", ""),
            ])
    print(f"\nDetailed logs saved to {csv_path}")

    # Cleanup
    print("\nStopping Ganache network...")
    ganache.stop_network()
    print("Benchmark completed.")
