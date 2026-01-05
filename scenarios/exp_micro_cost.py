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

from config import BLOCK_TIME, get_topology, TEST_USER_INDEX, GAS_LIMIT
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
    def send_and_wait(shard_id: int, contract_addr: str, abi: list, function_name: str, args: tuple = ()) -> t.Dict[str, t.Any]:
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
        return found_record

    # 5. DAS Journey
    print("\n4. Running DAS Journey...")
    das_metrics = []
    shard0 = "shard_0"
    exec_node = "execution"
    # Step 1: Deposit (burn on shard 0)
    print("   Step 1: Deposit (burn on shard 0)")
    shard0_das_addr = registry[shard0]["DAS"]
    shard0_das_abi = registry[shard0]["DAS_ABI"]
    burn_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "burn", (user_address, 100))
    das_metrics.append(("Burn (Shard0)", burn_result))
    time.sleep(0.1)  # Simulate relay
    # mint on execution
    exec_das_addr = registry[exec_node]["DAS"]
    exec_das_abi = registry[exec_node]["DAS_ABI"]
    mint_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "mint", (user_address, 100))
    das_metrics.append(("Mint (Execution)", mint_result))
    # Step 2: Action (doWork on execution)
    print("   Step 2: Action (doWork on execution)")
    exec_workload_addr = registry[exec_node]["Workload"]
    exec_workload_abi = registry[exec_node]["Workload_ABI"]
    work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,))
    das_metrics.append(("Work (Execution)", work_result))
    # Step 3: Withdraw (burn on execution, mint on shard 0)
    print("   Step 3: Withdraw")
    burn_exec_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "burn", (user_address, 100))
    das_metrics.append(("Burn (Execution)", burn_exec_result))
    time.sleep(0.1)
    mint_shard0_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "mint", (user_address, 100))
    das_metrics.append(("Mint (Shard0)", mint_shard0_result))

    # 6. 2PC Journey
    print("\n5. Running 2PC Journey...")
    tpc_metrics = []
    shard0_2pc_addr = registry[shard0]["2PC"]
    shard0_2pc_abi = registry[shard0]["2PC_ABI"]
    exec_2pc_addr = registry[exec_node]["2PC"]  # assuming deployed
    exec_2pc_abi = registry[exec_node]["2PC_ABI"]
    # Generate a unique ID
    import random
    tpc_id = random.randbytes(32)
    # Parallel lock (send both, then wait)
    print("   Lock (parallel)")
    # We'll use injector.send_batch for parallel sending
    # For simplicity, we'll do sequential for now
    lock_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "lock", (tpc_id,))
    lock_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "lock", (tpc_id,))
    tpc_metrics.append(("Lock (Shard0)", lock_shard))
    tpc_metrics.append(("Lock (Execution)", lock_exec))
    # doWork
    work_result2 = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,))
    tpc_metrics.append(("Work (Execution)", work_result2))
    # Parallel commit
    print("   Commit (parallel)")
    commit_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "commit", (tpc_id,))
    commit_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "commit", (tpc_id,))
    tpc_metrics.append(("Commit (Shard0)", commit_shard))
    tpc_metrics.append(("Commit (Execution)", commit_exec))

    # 7. Single Chain Journey
    print("\n6. Running Single Chain Journey...")
    baseline_workload_addr = registry["baseline"]["Workload"]
    baseline_workload_abi = registry["baseline"]["Workload_ABI"]
    single_result = send_and_wait(-2, baseline_workload_addr, baseline_workload_abi, "doWork", (100,))
    single_metrics = [("Work (Baseline)", single_result)]

    # 8. Reporting
    print("\n7. Generating report...")
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / "micro_benchmark.csv"

    # Calculate totals
    def total_time_and_gas(metrics):
        total_time = sum(m.get("latency", 0) or 0 for _, m in metrics)
        total_gas = sum(m.get("gas_used", 0) or 0 for _, m in metrics)
        return total_time, total_gas

    das_time, das_gas = total_time_and_gas(das_metrics)
    tpc_time, tpc_gas = total_time_and_gas(tpc_metrics)
    single_time, single_gas = total_time_and_gas(single_metrics)

    # Print table
    print("\n" + "="*60)
    print("Comparison of Total Time and Total Gas")
    print("="*60)
    print(f"{'Journey':<20} {'Total Time (s)':<15} {'Total Gas':<10}")
    print("-"*60)
    print(f"{'DAS':<20} {das_time:<15.2f} {das_gas:<10}")
    print(f"{'2PC':<20} {tpc_time:<15.2f} {tpc_gas:<10}")
    print(f"{'Single Chain':<20} {single_time:<15.2f} {single_gas:<10}")
    print("="*60)

    # Write detailed CSV
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["journey", "step", "tx_hash", "shard_id", "start_time", "end_time", "latency", "gas_used", "status"])
        for journey_name, metrics_list in [("DAS", das_metrics), ("2PC", tpc_metrics), ("Single", single_metrics)]:
            for step_name, m in metrics_list:
                writer.writerow([
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

    # 9. Cleanup
    print("\n8. Stopping Ganache network...")
    ganache.stop_network()
    print("Benchmark completed.")

