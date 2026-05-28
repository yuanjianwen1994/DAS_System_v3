"""
DAS System v3 分摊成本实验。
比较在N次连续操作中 DAS vs 2PC vs 单链。
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

from config_amortized import BLOCK_TIME, get_topology, TEST_USER_INDEX, GAS_LIMIT, AMORTIZED_OPS_COUNT, NETWORK_LATENCY_MEAN, NETWORK_LATENCY_STD, NETWORK_LATENCY_MIN, USER_JITTER_MIN, USER_JITTER_MAX
from core.identity import UserManager
from core.injector import TransactionInjector
from core.monitor import NetworkMonitor
from core.network import AnvilManager, ConnectionManager
from core.deployer import ContractDeployer
from datetime import datetime


def run():
    print("=== DAS System v3 分摊成本实验 ===")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 原始数据收集
    RAW_DATA = []

    # 1. 启动Anvil网络
    print("\n1. 启动Anvil网络...")
    topology = get_topology()
    ganache = AnvilManager()
    ganache.start_network(topology)
    time.sleep(2)

    # 2. 准备管理器
    network = ConnectionManager(topology)
    from config_amortized import MNEMONIC
    identity = UserManager(MNEMONIC)
    injector = TransactionInjector(network, identity)
    monitor = NetworkMonitor(network)
    deployer = ContractDeployer(network, identity)

    # 3. 部署合约
    print("\n2. 部署合约...")
    registry = deployer.deploy_infrastructure(topology)
    print(f"   注册表：{list(registry.keys())}")
    print("   等待合约完全挖出（15秒）...")
    time.sleep(15)

    # 4. 定义用户（专用测试用户）
    user_account = identity.get_user(TEST_USER_INDEX)
    user_address = user_account.address
    print(f"\n3. 使用测试用户（索引{TEST_USER_INDEX}）：{user_address}")

    # 辅助函数：发送单个交易并等待打包
    def send_and_wait(shard_id: int, contract_addr: str, abi: list, function_name: str, args: tuple = (), iteration: int = 0, journey: str = None, step_type: str = None) -> t.Dict[str, t.Any]:
        """发送交易并等待挖出，返回指标。"""
        node_name = f"shard_{shard_id}" if shard_id >= 0 else ("execution" if shard_id == -1 else "baseline")
        web3 = network.get_web3(node_name)
        contract = web3.eth.contract(address=contract_addr, abi=abi)
        # 构建交易
        nonce = identity.nonce_manager.get_and_increment(user_address, scope=node_name)
        tx = contract.functions[function_name](*args).build_transaction({
            "from": user_address,
            "gas": GAS_LIMIT,
            "gasPrice": web3.to_wei(20, "gwei"),
            "nonce": nonce,
        })
        signed = user_account.sign_transaction(tx)
        
        # 在传输前模拟网络延迟
        # 关键网络延迟模拟代码位置：第75-78行
        # 使用高斯（正态）分布模拟真实网络延迟
        # 参数来自config_amortized：均值=NETWORK_LATENCY_MEAN, 标准差=NETWORK_LATENCY_STD
        # 使用max()确保延迟不会小于NETWORK_LATENCY_MIN（防止负值）
        delay = max(NETWORK_LATENCY_MIN, random.gauss(NETWORK_LATENCY_MEAN, NETWORK_LATENCY_STD))
        start_time = time.time()
        time.sleep(delay)
        
        # 发送交易
        tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hash_hex = Web3.to_hex(tx_hash)
        # 使用提交时间跟踪
        monitor.track([tx_hash_hex], shard_id if shard_id >= 0 else node_name, submission_time=start_time)
        monitor.start_polling(interval=0.5)
        success = monitor.wait_until_complete(timeout=BLOCK_TIME * 2)
        if not success:
            raise TimeoutError(f"交易{tx_hash_hex}在超时时间内未挖出")
        # 获取结果
        results = monitor.get_results()
        # 规范化输入
        target_hash = tx_hash_hex if tx_hash_hex.startswith("0x") else f"0x{tx_hash_hex}"
        # 查找相关记录
        found_record = None
        for r in results:
            res_hash = r["tx_hash"]
            if not res_hash.startswith("0x"):
                res_hash = f"0x{res_hash}"
            if res_hash.lower() == target_hash.lower():
                found_record = r
                break
        if not found_record:
            print(f"❌ 不匹配！查找{target_hash}")
            print(f"   可用结果：{[r['tx_hash'] for r in results]}")
            raise TimeoutError(f"交易{target_hash}已被监控确认但在结果查找中未找到")
        # 添加迭代信息
        found_record["iteration"] = iteration
        # 记录原始数据
        raw_entry = {
            "journey": journey,
            "step_type": step_type,
            "op_index": iteration,
            "tx_hash": found_record["tx_hash"],
            "latency": found_record.get("latency"),
            "gas_used": found_record.get("gas_used"),
            "status": found_record.get("status"),
            "block_number": found_record.get("block_number"),
        }
        RAW_DATA.append(raw_entry)
        return found_record

    # 抖动辅助函数
    def jitter():
        """随机睡眠以避免相位锁定。"""
        time.sleep(random.uniform(USER_JITTER_MIN, USER_JITTER_MAX))

    N = AMORTIZED_OPS_COUNT
    print(f"\n4. 运行分摊成本实验，N = {N}次连续操作。")

    results = []

    # --- 场景A: DAS（驻留模型）---
    print("\n--- 场景A: DAS（驻留模型）---")
    jitter()
    start_time = time.time()
    total_gas = 0

    shard0 = "shard_0"
    exec_node = "execution"
    shard0_das_addr = registry[shard0]["DAS"]
    shard0_das_abi = registry[shard0]["DAS_ABI"]
    exec_das_addr = registry[exec_node]["DAS"]
    exec_das_abi = registry[exec_node]["DAS_ABI"]
    exec_workload_addr = registry[exec_node]["Workload"]
    exec_workload_abi = registry[exec_node]["Workload_ABI"]

    burn_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "burn", (user_address, 100), iteration=0, journey="DAS", step_type="deposit_burn")
    total_gas += burn_result.get("gas_used", 0)
    mint_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "mint", (user_address, 100), iteration=0, journey="DAS", step_type="deposit_mint")
    total_gas += mint_result.get("gas_used", 0)

    for i in range(N):
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration=i, journey="DAS", step_type="work")
        total_gas += work_result.get("gas_used", 0)

    burn_exec_result = send_and_wait(-1, exec_das_addr, exec_das_abi, "burn", (user_address, 100), iteration=0, journey="DAS", step_type="withdraw_burn")
    total_gas += burn_exec_result.get("gas_used", 0)
    mint_shard0_result = send_and_wait(0, shard0_das_addr, shard0_das_abi, "mint", (user_address, 100), iteration=0, journey="DAS", step_type="withdraw_mint")
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
    print(f"   总延迟：{total_latency:.2f}秒，每操作平均：{avg_latency_per_op:.2f}秒")
    print(f"   总gas：{total_gas}，每操作平均：{avg_gas_per_op:.0f}")

    # --- 场景B: 2PC（远程调用模型）---
    print("\n--- 场景B: 2PC（远程调用模型）---")
    jitter()
    start_time = time.time()
    total_gas = 0

    shard0_2pc_addr = registry[shard0]["2PC"]
    shard0_2pc_abi = registry[shard0]["2PC_ABI"]
    exec_2pc_addr = registry[exec_node]["2PC"]
    exec_2pc_abi = registry[exec_node]["2PC_ABI"]

    for i in range(N):
        import random as rand
        tpc_id = rand.randbytes(32)
        lock_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "lock", (tpc_id,), iteration=i, journey="2PC", step_type="lock_shard")
        total_gas += lock_shard.get("gas_used", 0)
        lock_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "lock", (tpc_id,), iteration=i, journey="2PC", step_type="lock_exec")
        total_gas += lock_exec.get("gas_used", 0)
        work_result = send_and_wait(-1, exec_workload_addr, exec_workload_abi, "doWork", (100,), iteration=i, journey="2PC", step_type="work")
        total_gas += work_result.get("gas_used", 0)
        commit_shard = send_and_wait(0, shard0_2pc_addr, shard0_2pc_abi, "commit", (tpc_id,), iteration=i, journey="2PC", step_type="commit_shard")
        total_gas += commit_shard.get("gas_used", 0)
        commit_exec = send_and_wait(-1, exec_2pc_addr, exec_2pc_abi, "commit", (tpc_id,), iteration=i, journey="2PC", step_type="commit_exec")
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
    print(f"   总延迟：{total_latency:.2f}秒，每操作平均：{avg_latency_per_op:.2f}秒")
    print(f"   总gas：{total_gas}，每操作平均：{avg_gas_per_op:.0f}")

    # --- 场景C: 单链（基准）---
    print("\n--- 场景C: 单链（基准）---")
    jitter()
    start_time = time.time()
    total_gas = 0

    baseline_workload_addr = registry["baseline"]["Workload"]
    baseline_workload_abi = registry["baseline"]["Workload_ABI"]

    for i in range(N):
        single_result = send_and_wait(-2, baseline_workload_addr, baseline_workload_abi, "doWork", (100,), iteration=i, journey="Single", step_type="work")
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
    print(f"   总延迟：{total_latency:.2f}秒，每操作平均：{avg_latency_per_op:.2f}秒")
    print(f"   总gas：{total_gas}，每操作平均：{avg_gas_per_op:.0f}")

    # --- 输出与可视化 ---
    print("\n" + "="*60)
    print("分摊成本比较")
    print("="*60)
    for r in results:
        print(f"{r['type']}：")
        print(f"  操作次数：{r['ops_count']}")
        print(f"  总延迟：{r['total_latency']:.2f}秒")
        print(f"  每操作平均延迟：{r['avg_latency_per_op']:.2f}秒")
        print(f"  总gas：{r['total_gas']}")
        print(f"  每操作平均gas：{r['avg_gas_per_op']:.0f}")
        print()

    # 保存CSV
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    csv_path = logs_dir / f"amortized_benchmark_{timestamp}.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["type", "ops_count", "total_latency", "avg_latency_per_op", "total_gas", "avg_gas_per_op"])
        writer.writeheader()
        writer.writerows(results)
    print(f"结果已保存到{csv_path}")

    # 保存原始CSV
    raw_csv_path = logs_dir / f"amortized_benchmark_raw_{timestamp}.csv"
    with open(raw_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["journey", "step_type", "op_index", "tx_hash", "latency", "gas_used", "status", "block_number"])
        writer.writeheader()
        writer.writerows(RAW_DATA)
    print(f"原始数据已保存到{raw_csv_path}")

    # 清理
    print("\n停止Anvil网络...")
    ganache.stop_network()
    print("实验完成。")


if __name__ == "__main__":
    run()