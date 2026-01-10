"""
Amortized-Cost Experiment for DAS System v3.
Compare DAS vs 2PC vs Single Chain over N consecutive operations.
"""
import csv
import time
import random
import typing as t
from pathlib import Path
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3 import Web3

from config_amortized import BLOCK_TIME, get_topology, TEST_USER_INDEX, GAS_LIMIT, AMORTIZED_OPS_COUNT
from core.identity import UserManager
from core.injector import TransactionInjector
from core.monitor import NetworkMonitor
from core.network import GanacheManager, ConnectionManager
from core.deployer import ContractDeployer


def run():
    print("=== DAS System v3 Amortized-Cost Experiment ===")

    # 1. Start Ganache network
    print("\n1. Starting Ganache network...")
    topology = get_topology()
    ganache = GanacheManager()
    ganache.start_network(topology)
    time.sleep(2)  # let processes stabilize

    # 2. Prepare managers
    network = ConnectionManager(topology)
    from config_amortized import MNEMONIC
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

    # Jitter helper
    def jitter():
        """Random sleep to avoid phase locking."""
        time.sleep(random.uniform(0.1, 0.5))

    N = AMORTIZED_OPS_COUNT
    print(f"\n4. Running amortized-cost experiment with N = {N} consecutive operations.")

    # Data collection
    results = []  # each entry is a dict with type, ops_count, total_latency, avg_latency_per_op, total_gas, avg_gas_per_op

    # --- Scenario A: DAS (Residency Model) ---
    print("\n--- Scenario A: DAS (Residency Model) ---")
    jitter()
    start_time = time.time()
    total_gas = 0

    # Step 1: Deposit (Burn S0 -> Mint Exec)
    shard0 = "shard_0"
    exec_node = "execution"
    shard0_das_addr = registry[shard0]["DAS"]
    shard0_das_abi = registry[shard0]["DAS_ABI"]
    exec_das_addr = registry[exec_node]["DAS"]
    exec_das_abi = registry[exec_node]["DAS_ABI"]
    exec_workload_addr = registry[exec_node]["Workload"]
    exec_workload_abi = registry[exec_node]["Workload_ABI"]

    burn_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "burn", (user_address, 100), iteration=0)
    total_gas += burn_result.get("gas_used", 0)
    mint_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "mint", (user_address, 100), iteration=0)
    total_gas += mint_result.get("gas_used", 0)

    # Step 2: Loop N times doWork on Execution (local)
    for i in range(N):
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration=i)
        total_gas += work_result.get("gas_used", 0)

    # Step 3: Withdraw (Burn Exec -> Mint S0)
    burn_exec_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "burn", (user_address, 100), iteration=0)
    total_gas += burn_exec_result.get("gas_used", 0)
    mint_shard0_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "mint", (user_address, 100), iteration=0)
    total_gas += mint_shard0_result.get("gas_used", 0)

    end_time = time.time()
    total_latency = end_time - start_time
    avg_latency_per_op = total_latency / N
    avg_gas_per_op = total_gas / N

    results.append({
        "type": "DAS",
        "ops_count": N,
        "total_latency": total_latency,
        "avg_latency_per_op": avg_latency_per_op,
        "total_gas": total_gas,
        "avg_gas_per_op": avg_gas_per_op,
    })
    print(f"   Total latency: {total_latency:.2f}s, Avg per op: {avg_latency_per_op:.2f}s")
    print(f"   Total gas: {total_gas}, Avg per op: {avg_gas_per_op:.0f}")

    # --- Scenario B: 2PC (Remote Call Model) ---
    print("\n--- Scenario B: 2PC (Remote Call Model) ---")
    jitter()
    start_time = time.time()
    total_gas = 0

    shard0_2pc_addr = registry[shard0]["2PC"]
    shard0_2pc_abi = registry[shard0]["2PC_ABI"]
    exec_2pc_addr = registry[exec_node]["2PC"]
    exec_2pc_abi = registry[exec_node]["2PC_ABI"]

    for i in range(N):
        # Generate a unique ID for each cycle
        import random as rand
        tpc_id = rand.randbytes(32)
        # Lock on both shards (sequential for simplicity)
        lock_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "lock", (tpc_id,), iteration=i)
        total_gas += lock_shard.get("gas_used", 0)
        lock_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "lock", (tpc_id,), iteration=i)
        total_gas += lock_exec.get("gas_used", 0)
        # doWork
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration=i)
        total_gas += work_result.get("gas_used", 0)
        # Commit on both shards
        commit_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "commit", (tpc_id,), iteration=i)
        total_gas += commit_shard.get("gas_used", 0)
        commit_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "commit", (tpc_id,), iteration=i)
        total_gas += commit_exec.get("gas_used", 0)

    end_time = time.time()
    total_latency = end_time - start_time
    avg_latency_per_op = total_latency / N
    avg_gas_per_op = total_gas / N

    results.append({
        "type": "2PC",
        "ops_count": N,
        "total_latency": total_latency,
        "avg_latency_per_op": avg_latency_per_op,
        "total_gas": total_gas,
        "avg_gas_per_op": avg_gas_per_op,
    })
    print(f"   Total latency: {total_latency:.2f}s, Avg per op: {avg_latency_per_op:.2f}s")
    print(f"   Total gas: {total_gas}, Avg per op: {avg_gas_per_op:.0f}")

    # --- Scenario C: Single Chain (Baseline) ---
    print("\n--- Scenario C: Single Chain (Baseline) ---")
    jitter()
    start_time = time.time()
    total_gas = 0

    baseline_workload_addr = registry["baseline"]["Workload"]
    baseline_workload_abi = registry["baseline"]["Workload_ABI"]

    for i in range(N):
        single_result = send_and_wait(-2, baseline_workload_addr, baseline_workload_abi, "doWork", (100,), iteration=i)
        total_gas += single_result.get("gas_used", 0)

    end_time = time.time()
    total_latency = end_time - start_time
    avg_latency_per_op = total_latency / N
    avg_gas_per_op = total_gas / N

    results.append({
        "type": "Single",
        "ops_count": N,
        "total_latency": total_latency,
        "avg_latency_per_op": avg_latency_per_op,
        "total_gas": total_gas,
        "avg_gas_per_op": avg_gas_per_op,
    })
    print(f"   Total latency: {total_latency:.2f}s, Avg per op: {avg_latency_per_op:.2f}s")
    print(f"   Total gas: {total_gas}, Avg per op: {avg_gas_per_op:.0f}")

    # --- Output & Visualization ---
    print("\n" + "="*60)
    print("Amortized-Cost Comparison")
    print("="*60)
    for r in results:
        print(f"{r['type']}:")
        print(f"  Ops count: {r['ops_count']}")
        print(f"  Total latency: {r['total_latency']:.2f}s")
        print(f"  Avg latency per op: {r['avg_latency_per_op']:.2f}s")
        print(f"  Total gas: {r['total_gas']}")
        print(f"  Avg gas per op: {r['avg_gas_per_op']:.0f}")
        print()

    # Save CSV
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / "amortized_benchmark.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["type", "ops_count", "total_latency", "avg_latency_per_op", "total_gas", "avg_gas_per_op"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Results saved to {csv_path}")

    # Cleanup
    print("\nStopping Ganache network...")
    ganache.stop_network()
    print("Experiment completed.")


if __name__ == "__main__":
    run()