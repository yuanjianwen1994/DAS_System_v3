"""
Phase 4+ 宏基准测试流量生成器。
特性：基于任务的执行、原始日志、分片分布和模拟抖动。
"""
import typing as t
import time
import random
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.types import TxParams
from tqdm import tqdm

from config_matrix import (
    MACRO_TX_TIMEOUT,
    SIM_THINK_TIME_RANGE,
    HTTP_RETRIES
)
from config_macro import MACRO_GAS_PRICE
from .macro_injector import MacroTransactionInjector
from .identity import UserManager
from .network import ConnectionManager


class MacroTrafficGenerator:
    def __init__(
        self,
        network_manager: ConnectionManager,
        identity_manager: UserManager,
        injector: MacroTransactionInjector,
        registry: t.Dict[str, t.Dict[str, t.Any]],
        process_id: int = 0,
        user_offset: int = 0,
    ) -> None:
        self.network = network_manager
        self.identity = identity_manager
        self.injector = injector
        self.registry = registry
        self.process_id = process_id
        self.user_offset = user_offset
        self.completed_journeys = []
        self.raw_logs = []  # 原始数据日志记录
        
        # 发现可用的分片用于分布
        # 假设注册表中存在类似'shard_0'、'shard_1'的键
        self.shard_ids = [
            int(k.split('_')[1]) for k in registry.keys() 
            if k.startswith('shard_') and k.split('_')[1].isdigit()
        ]
        if not self.shard_ids:
            self.shard_ids = [0] # 后备
        self.shard_ids.sort()
        print(f"[流量] 在分片间进行负载均衡：{self.shard_ids}")

        self._builders = {
            "das_burn": self._build_das_burn,
            "das_mint": self._build_das_mint,
            "das_work": self._build_das_work,
            "tpc_lock": self._build_tpc_lock,
            "tpc_commit": self._build_tpc_commit,
        }

    # ---------- 合约函数构建器 ----------
    def _build_das_burn(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """构建DAS burn交易。"""
        shard_id = kwargs["shard_id"]
        amount = kwargs.get("amount", 100)
        # 修复：处理提款的执行分片(-1)
        shard_name = f"shard_{shard_id}" if shard_id >= 0 else "execution"
        
        contract_addr = self.registry[shard_name]["DAS"]
        contract_abi = self.registry[shard_name]["DAS_ABI"]
        contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
        return contract.functions.burn(from_address, amount).build_transaction(
            {
                "from": from_address,
                "nonce": nonce,
            }
        )

    def _build_das_mint(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """构建DAS mint交易。"""
        shard_id = kwargs["shard_id"]
        amount = kwargs.get("amount", 100)
        shard_name = f"shard_{shard_id}" if shard_id >= 0 else "execution"
        contract_addr = self.registry[shard_name]["DAS"]
        contract_abi = self.registry[shard_name]["DAS_ABI"]
        contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
        return contract.functions.mint(from_address, amount).build_transaction(
            {
                "from": from_address,
                "nonce": nonce,
            }
        )

    def _build_das_work(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """构建Workload.doWork交易。"""
        amount = kwargs.get("amount", 100)
        contract_addr = self.registry["execution"]["Workload"]
        contract_abi = self.registry["execution"]["Workload_ABI"]
        contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
        return contract.functions.doWork(amount).build_transaction(
            {
                "from": from_address,
                "nonce": nonce,
            }
        )

    def _build_tpc_lock(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """构建2PC lock交易。"""
        shard_id = kwargs["shard_id"]
        tpc_id = kwargs["tpc_id"]
        shard_name = f"shard_{shard_id}" if shard_id >= 0 else "execution"
        contract_addr = self.registry[shard_name]["2PC"]
        contract_abi = self.registry[shard_name]["2PC_ABI"]
        contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
        return contract.functions.lock(tpc_id).build_transaction(
            {
                "from": from_address,
                "nonce": nonce,
            }
        )

    def _build_tpc_commit(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """构建2PC commit交易。"""
        shard_id = kwargs["shard_id"]
        tpc_id = kwargs["tpc_id"]
        shard_name = f"shard_{shard_id}" if shard_id >= 0 else "execution"
        contract_addr = self.registry[shard_name]["2PC"]
        contract_abi = self.registry[shard_name]["2PC_ABI"]
        contract = web3.eth.contract(address=contract_addr, abi=contract_abi)
        return contract.functions.commit(tpc_id).build_transaction(
            {
                "from": from_address,
                "nonce": nonce,
            }
        )

    # ---------- 带重试的健壮发送 ----------
    def _send_and_wait(self, func_type, user_idx, journey_id=None, **kwargs):
        # 连接稳定性的重试循环
        journey_start_time = time.time()
        # Gas重试配置
        gas_retry_count = 0
        max_gas_retries = 5
        current_gas_price = MACRO_GAS_PRICE
        for attempt in range(HTTP_RETRIES):
            try:
                
                # 复制kwargs以避免重试时的变异问题
                call_kwargs = kwargs.copy()
                call_kwargs['gas_price'] = current_gas_price
                
                if "shard_id" not in call_kwargs:
                     raise ValueError(f"缺少{func_type}的'shard_id'")
                shard_id = call_kwargs.pop("shard_id")

                contract_func = self._builders[func_type]

                # 1. 发送
                tx_hashes = self.injector.send_batch(
                    shard_id,
                    users=[user_idx],
                    contract_func=contract_func,
                    **call_kwargs
                )
                
                if not tx_hashes:
                     raise Exception("没有返回tx哈希")
                
                tx_hash = tx_hashes[0]

                # 2. 等待
                node_name = "execution" if shard_id == -1 else f"shard_{shard_id}"
                web3 = self.network.get_web3(node_name)
                
                receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=MACRO_TX_TIMEOUT)
                if receipt.status != 1:
                    raise Exception(f"Tx {tx_hash}回滚")

                # 3. 记录原始数据
                duration = time.time() - journey_start_time
                self.raw_logs.append({
                    "timestamp": time.time(),
                    "journey_id": journey_id if journey_id else "N/A",
                    "worker_id": user_idx,
                    "tx_type": func_type,
                    "latency_s": duration,
                    "gas_used": receipt['gasUsed'],
                    "block_number": receipt['blockNumber'],
                    "status": receipt['status']
                })
                
                return receipt

            except ValueError as e:
                # 处理由HTTP重试引起的"Nonce太低"/"Nonce不正确"错误
                err_str = str(e).lower()
                if "nonce" in err_str:
                    print(f"[流量] Worker {user_idx} Nonce不匹配（Tx可能在重试期间成功）。跳过。")
                    # 我们将其记录为status=0（未知结果）但严格不崩溃
                    self.raw_logs.append({
                        "timestamp": time.time(),
                        "journey_id": journey_id if journey_id else "N/A",
                        "worker_id": user_idx,
                        "tx_type": func_type,
                        "latency_s": time.time() - journey_start_time,
                        "gas_used": 0,
                        "block_number": -1,
                        "status": 0
                    })
                    return None
                else:
                    raise e  # 重新抛出其他ValueError
                
            except Exception as e:
                # 检查超时
                error_msg = str(e)
                is_timeout = "timeout" in error_msg.lower() or isinstance(e, TimeoutError)
                if is_timeout:
                    tx_hash_str = tx_hash[:10] if 'tx_hash' in locals() else 'unknown'
                    print(f"[流量] Worker {user_idx} Tx {func_type}超时（> {MACRO_TX_TIMEOUT}秒）")
                    # 记录超时原始日志
                    self.raw_logs.append({
                        "timestamp": time.time(),
                        "journey_id": journey_id if journey_id else "N/A",
                        "worker_id": user_idx,
                        "tx_type": func_type,
                        "latency_s": time.time() - journey_start_time,
                        "gas_used": 0,
                        "block_number": -1,
                        "status": 0  # 0表示失败/超时
                    })
                    # 返回None通知调用者跳过后续步骤
                    return None
                
                # 检查Gas不足错误(-32003)
                is_gas_error = "-32003" in error_msg or "max fee per gas less than block base fee" in error_msg
                if is_gas_error and gas_retry_count < max_gas_retries:
                    gas_retry_count += 1
                    current_gas_price = int(current_gas_price * 1.5)  # 增加50%
                    print(f"[流量] Worker {user_idx} gas价格太低（尝试{gas_retry_count}/{max_gas_retries}）。"
                          f"增加到{current_gas_price/1e9:.2f} Gwei并等待下一个区块...")
                    # 等待下一个区块
                    node_name = "execution" if shard_id == -1 else f"shard_{shard_id}"
                    try:
                        self._wait_for_next_block(node_name, timeout=120)
                    except TimeoutError:
                        print(f"[流量] Worker {user_idx}区块等待超时，继续执行")
                    continue  # 重试当前attempt
                
                # 捕获连接错误并重试
                is_conn_error = "Connection aborted" in error_msg or "Connection refused" in error_msg or "Available sockets" in error_msg
                
                if is_conn_error and attempt < HTTP_RETRIES - 1:
                    sleep_time = (attempt + 1) * 2
                    time.sleep(sleep_time)
                    continue
                else:
                    # 重试耗尽或其他错误
                    total_latency = time.time() - journey_start_time
                    print(f"[流量] Worker {user_idx}尝试{attempt+1}失败：{e}")
                    self.raw_logs.append({
                        "timestamp": time.time(),
                        "journey_id": journey_id if journey_id else "N/A",
                        "worker_id": user_idx,
                        "tx_type": func_type,
                        "latency_s": total_latency,
                        "gas_used": 0,
                        "block_number": -1,
                        "status": 0
                    })
                    return None

    def _sleep_random(self):
        """
        注入模拟抖动。
        
        关键网络延迟模拟代码位置：第286-288行
        使用random.uniform()在SIM_THINK_TIME_RANGE范围内均匀分布延迟
        模拟用户思考时间和网络抖动的随机性
        """
        # 从配置的范围(0.5, 2.0)秒中均匀随机选择延迟
        time.sleep(random.uniform(*SIM_THINK_TIME_RANGE))

    def _wait_for_next_block(self, node_name, timeout=120):
        """等待在给定节点上挖出下一个区块。"""
        web3 = self.network.get_web3(node_name)
        start_block = web3.eth.block_number
        start_time = time.time()
        while time.time() - start_time < timeout:
            current_block = web3.eth.block_number
            if current_block > start_block:
                return True
            time.sleep(1)  # 每秒检查一次
        raise TimeoutError(f"在{node_name}上{timeout}秒内未看到新区块")

    # ---------- 带分片的Worker逻辑 ----------
    def _worker_loop_das_task(self, worker_id: int, ops_per_journey: int, target_journeys: int, progress_queue: t.Any):
        # 1. 交错启动：避免"雷鸣群"效应
        # 等待最多1.5个区块时间的随机时间以使worker去同步
        time.sleep(random.uniform(0, 15))
        global_worker_id = worker_id + self.user_offset
        amount = 100
        
        # 分布用户：基于全局worker ID以轮询方式分配到分片
        source_shard = self.shard_ids[global_worker_id % len(self.shard_ids)]
        
        journeys_done = 0
        successful_journeys = 0
        while journeys_done < target_journeys:
            # 为此尝试生成唯一旅程ID
            journey_id = f"{global_worker_id}_{journeys_done}"
            
            # 1. 存款（源分片 -> 执行节点）
            result = self._send_and_wait("das_burn", global_worker_id, journey_id=journey_id, shard_id=source_shard, amount=amount)
            if result is None:
                # 发生超时，跳过此旅程的其余部分
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue
            self._sleep_random()
            
            result = self._send_and_wait("das_mint", global_worker_id, journey_id=journey_id, shard_id=-1, amount=amount)
            if result is None:
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue
            self._sleep_random()

            # 2. 工作（执行节点）
            work_failed = False
            for _ in range(ops_per_journey):
                # 随机犹豫以打破12秒区块同步
                time.sleep(random.uniform(1.0, 10.0))
                result = self._send_and_wait("das_work", global_worker_id, journey_id=journey_id, shard_id=-1, amount=amount)
                if result is None:
                    work_failed = True
                    break
                self._sleep_random()
            if work_failed:
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue

            # 3. 提款（执行节点 -> 源分片）
            result = self._send_and_wait("das_burn", global_worker_id, journey_id=journey_id, shard_id=-1, amount=amount)
            if result is None:
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue
            self._sleep_random()
            
            result = self._send_and_wait("das_mint", global_worker_id, journey_id=journey_id, shard_id=source_shard, amount=amount)
            if result is None:
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue
            self._sleep_random()

            # 旅程成功完成
            journeys_done += 1
            successful_journeys += 1
            if progress_queue:
                progress_queue.put(1)

            # 记录完成以跟踪进度（可选）
        # Worker结束时总结日志
        print(f"[Worker {worker_id}] 完成。计划：{target_journeys}，完成：{successful_journeys}")

    def _worker_loop_baseline(self, worker_id: int, ops_per_journey: int, target_journeys: int, progress_queue: t.Any):
        # 纯本地工作在分片0上。无跨分片移动。
        # 1. 交错启动：避免"雷鸣群"效应
        time.sleep(random.uniform(0, 15))
        global_worker_id = worker_id + self.user_offset
        amount = 100
        journeys_done = 0
        successful_journeys = 0
        while journeys_done < target_journeys:
            # 为此尝试生成唯一旅程ID
            journey_id = f"{global_worker_id}_{journeys_done}"
            # 只需在分片0上执行N次操作（Workload合约也必须部署在那里）
            # 如果Workload仅在执行节点上，我们将基准映射到执行节点(-1)
            # 为简单起见，假设基准=完全在执行节点上运行
            try:
                for _ in range(ops_per_journey):
                     # 随机犹豫以打破12秒区块同步
                     time.sleep(random.uniform(1.0, 10.0))
                     result = self._send_and_wait("das_work", global_worker_id, journey_id=journey_id, shard_id=-1, amount=amount)
                     if result is None:
                         # 发生超时，跳过此旅程的其余部分
                         break
                     self._sleep_random()
                else:
                    # 没有发生中断，旅程成功完成
                    journeys_done += 1
                    successful_journeys += 1
                    if progress_queue:
                        progress_queue.put(1)
                    continue
                # 如果由于超时而中断，仍然计为已完成的旅程（跳过）
                journeys_done += 1
                if progress_queue:
                    progress_queue.put(1)
                continue
            except Exception as e:
                print(f"[流量] Worker {worker_id}（全局{global_worker_id}）失败：{e}")
                raise
        # Worker结束时总结日志
        print(f"[Worker {worker_id}] 完成。计划：{target_journeys}，完成：{successful_journeys}")

    def _worker_loop_2pc_task(self, worker_id: int, ops_per_journey: int, target_journeys: int, progress_queue: t.Any):
        """
        2PC生命周期：循环N * (Lock -> Work -> Commit)。
        严格顺序：Lock S -> Lock E -> Work E -> Commit S -> Commit E。
        """
        # 1. 交错启动：避免"雷鸣群"效应
        time.sleep(random.uniform(0, 15))
        global_worker_id = worker_id + self.user_offset
        amount = 100
        # 基于全局worker ID的轮询分片分配
        source_shard = self.shard_ids[global_worker_id % len(self.shard_ids)]
        
        journeys_done = 0
        successful_journeys = 0
        while journeys_done < target_journeys:
            try:
                # 为此尝试生成唯一旅程ID
                journey_id = f"{global_worker_id}_{journeys_done}"
                # 在2PC中，"旅程"由`ops_per_journey`个原子交易组成
                for _ in range(ops_per_journey):
                    # 为此交易生成唯一TPC ID
                    tpc_id = random.randbytes(32)

                    # 1. 在源分片上锁定
                    result = self._send_and_wait("tpc_lock", global_worker_id, journey_id=journey_id, shard_id=source_shard, tpc_id=tpc_id)
                    if result is None:
                        # 发生超时，跳过此交易和旅程的其余部分
                        break
                    
                    # 2. 在执行节点上锁定
                    result = self._send_and_wait("tpc_lock", global_worker_id, journey_id=journey_id, shard_id=-1, tpc_id=tpc_id)
                    if result is None:
                        break
                    
                    # 3. 在执行节点上工作（模拟业务逻辑）
                    # 随机犹豫以打破12秒区块同步
                    time.sleep(random.uniform(1.0, 10.0))
                    result = self._send_and_wait("das_work", global_worker_id, journey_id=journey_id, shard_id=-1, amount=amount)
                    if result is None:
                        break
                    
                    # 4. 在源分片上提交
                    result = self._send_and_wait("tpc_commit", global_worker_id, journey_id=journey_id, shard_id=source_shard, tpc_id=tpc_id)
                    if result is None:
                        break
                    
                    # 5. 在执行节点上提交
                    result = self._send_and_wait("tpc_commit", global_worker_id, journey_id=journey_id, shard_id=-1, tpc_id=tpc_id)
                    if result is None:
                        break

                    # 模拟抖动
                    self._sleep_random()
                else:
                    # 没有发生中断，所有交易成功完成
                    journeys_done += 1
                    successful_journeys += 1
                    if progress_queue: progress_queue.put(1)
                    continue
                # 如果由于超时而中断，仍然计为已完成的旅程（跳过）
                journeys_done += 1
                if progress_queue: progress_queue.put(1)
                continue
            except Exception as e:
                # 记录错误但让线程死亡，以便主进程知道
                print(f"[流量] 2PC Worker {worker_id}（全局{global_worker_id}）失败：{e}")
                raise e
        # Worker结束时总结日志
        print(f"[Worker {worker_id}] 完成。计划：{target_journeys}，完成：{successful_journeys}")

    def run_task_based(
        self,
        concurrency: int,
        journeys_per_user: int,
        ops_per_journey: int,
        journey_type: str = "DAS",
        process_id: int = None,
        progress_queue: t.Any = None,
    ) -> t.List[t.Dict[str, t.Any]]:
        """
        矩阵基准测试入口：运行直到每个用户完成N个旅程。
        直接返回原始日志（不保存到文件）。
        """
        if process_id is not None:
            self.process_id = process_id  # 如果提供则覆盖
        total_journeys = concurrency * journeys_per_user
        print(f"[流量 P{self.process_id}] 开始{journey_type}：{concurrency}个用户，每个{Journeys_per_user}个旅程（总计：{total_journeys}）")
        
        # 清除先前的日志
        self.raw_logs.clear()
        
        # 使用ThreadPoolExecutor运行workers
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for i in range(concurrency):
                # 确定要使用的worker函数
                if journey_type == "DAS":
                    futures.append(executor.submit(
                        self._worker_loop_das_task, i, ops_per_journey, journeys_per_user, progress_queue
                    ))
                elif journey_type == "2PC":
                    futures.append(executor.submit(
                        self._worker_loop_2pc_task, i, ops_per_journey, journeys_per_user, progress_queue
                    ))
                elif journey_type == "BASELINE":
                    futures.append(executor.submit(
                        self._worker_loop_baseline, i, ops_per_journey, journeys_per_user, progress_queue
                    ))
                else:
                    raise ValueError(f"未知旅程类型：{journey_type}")
            
            # 等待所有
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    # 打印但不立即崩溃整个进程，允许其他完成
                    print(f"[流量 P{self.process_id}] 关键worker失败：{e}")

        # 返回原始日志供调用者保存
        return self.raw_logs

    # ---------- 遗留方法（占位符）----------
    # 保留这些以避免破坏现有脚本，但它们可以是存根。
    def start_concurrent(self, concurrency: int, journey_type: str = "DAS", ops_per_journey: int = None) -> None:
        """遗留：启动并发workers（非旅程限制）。"""
        raise NotImplementedError("start_concurrent已弃用；使用run_task_based。")

    def run_for_duration(self, concurrency: int, duration_seconds: float, journey_type: str = "DAS", ops_per_journey: int = None) -> None:
        """遗留：运行workers一段固定时间。"""
        raise NotImplementedError("run_for_duration已弃用。")

    def _worker_loop_das(self, worker_id: int, ops_per_journey: int) -> None:
        """遗留worker循环（单旅程）。"""
        # 重定向到目标旅程=1的基于任务的循环
        self._worker_loop_das_task(worker_id, ops_per_journey, 1, None)

    def _worker_loop_2pc(self, worker_id: int, ops_per_journey: int) -> None:
        """遗留2PC循环。"""
        raise NotImplementedError("此版本中未实现2PC循环。")

    def _repeating_worker(self, worker_id: int, journeys_per_user: int, worker_func, ops_per_journey: int):
        """遗留内部辅助函数。"""
        for _ in range(journeys_per_user):
            worker_func(worker_id, ops_per_journey)