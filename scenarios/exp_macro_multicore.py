"""
Phase 4+ 宏基准测试多进程矩阵实验。
使用多进程绕过GIL瓶颈，支持BASELINE、DAS和2PC旅程类型。
每个进程运行自己的ConnectionManager、UserManager、Injector和TrafficGenerator。
记录每个进程的原始交易级数据，可选合并CSV。
"""
import sys
import os
import time
import subprocess
import csv
import glob
import pandas as pd
import multiprocessing
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# 添加父目录到sys.path用于导入
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
    MATRIX_PROCESSES,
    MATRIX_SCENARIOS,
)
from core.identity import UserManager
from core.network import AnvilManager, ConnectionManager
from core.deployer import ContractDeployer
from core.macro_injector import MacroTransactionInjector
from core.macro_traffic import MacroTrafficGenerator
from core.macro_monitor import MacroMonitor
from tqdm import tqdm


# ========== 辅助函数（从exp_macro_matrix复制）==========
def kill_ganache():
    """积极杀死所有ganache/node进程。"""
    try:
        if os.name == 'nt':
            # Windows：强制杀死node.exe（Ganache）
            subprocess.call(["taskkill", "/F", "/IM", "node.exe", "/T"], stderr=subprocess.DEVNULL)
            subprocess.call(["taskkill", "/F", "/IM", "ganache.cmd", "/T"], stderr=subprocess.DEVNULL) # 以防万一
        else:
            subprocess.call(["pkill", "-f", "ganache"], stderr=subprocess.DEVNULL)
    except Exception:
        pass


def wait_for_nodes(network: ConnectionManager, timeout=60):
    """阻塞直到所有RPC节点响应。"""
    print("   [系统] 等待RPC节点预热...")
    nodes = ["shard_0", "shard_1", "execution", "baseline"]
    start = time.time()
    for node in nodes:
        while True:
            if time.time() - start > timeout:
                raise TimeoutError(f"节点{node}在{timeout}秒内未启动")
            try:
                w3 = network.get_web3(node)
                if w3.is_connected() and w3.eth.block_number >= 0:
                    break
            except Exception:
                time.sleep(1)
            time.sleep(1)
    print("   [系统] 所有节点已上线。")


def dump_csv(data: List[Dict[str, Any]], filename: str, fieldnames: List[str]) -> None:
    """将字典列表写入CSV。"""
    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    path = logs_dir / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"      [数据] {filename}已保存（{len(data)}行）。")


def consolidate_logs(journey_type: str, n: int, q: int, timestamp: str):
    """
    找到特定运行的所有部分进程日志，合并它们，
    并删除部分文件。
    """
    logs_dir = Path(__file__).parent.parent / "logs"
    # 1. 模式匹配：raw_txs_p*_{journey_type}_N{n}_q{q}_{timestamp}.csv
    pattern = f"raw_txs_p*_{journey_type}_N{n}_q{q}_{timestamp}.csv"
    files = glob.glob(str(logs_dir / pattern))
    
    if not files:
        print(f"[系统] 未找到要合并的日志文件，模式：{pattern}")
        return

    print(f"[系统] 合并{len(files)}个日志文件用于N={n}，q={q}...")
    
    try:
        # 2. 读取并连接
        df_list = []
        for f in files:
            try:
                df = pd.read_csv(f)
                df_list.append(df)
            except pd.errors.EmptyDataError:
                pass # 忽略空文件
        
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            
            # 3. 保存合并文件
            combined_filename = f"combined_raw_txs_{journey_type}_N{n}_q{q}_{timestamp}.csv"
            combined_path = logs_dir / combined_filename
            combined_df.to_csv(combined_path, index=False)
            print(f"[系统] 已保存合并日志：{combined_filename}（{len(combined_df)}条记录）")
            
            # 4. 删除部分文件（仅在合并成功时）
            for f in files:
                try:
                    os.remove(f)
                except OSError as e:
                    print(f"警告：无法删除{f}：{e}")
        else:
            print("[系统] 警告：所有日志文件都是空的。")
            
    except Exception as e:
        print(f"[系统] 日志合并期间出错：{e}")


# ========== Worker进程函数 ==========
def run_worker_process(
    proc_id: int,
    user_start: int,
    user_end: int,
    concurrency: int,
    journeys_per_user: int,
    ops_per_journey: int,
    journey_type: str,
    topology: Dict[str, Any],
    registry: Dict[str, Dict[str, Any]],
    timestamp: str,
    progress_queue: Any = None,
) -> None:
    """
    单进程流量生成。
    创建自己的管理器，运行流量，将日志写入每个进程的CSV。
    """
    print(f"[Worker {proc_id}] 启动，用户范围{user_start}‑{user_end}（总并发{concurrency}）")
    
    # 1. 创建独立管理器
    network = ConnectionManager(topology)
    from config_matrix import MNEMONIC
    identity = UserManager(MNEMONIC)
    injector = MacroTransactionInjector(network, identity)
    
    # 2. 等待节点（它们应该已经启动）
    wait_for_nodes(network)
    
    # 3. 使用process_id和user_offset创建流量生成器
    traffic = MacroTrafficGenerator(
        network, identity, injector, registry,
        process_id=proc_id,
        user_offset=user_start,
    )
    
    # 4. 运行流量（仅针对分配的用户范围）
    local_concurrency = user_end - user_start
    print(f"[Worker {proc_id}] 运行{local_concurrency}个用户，每个用户{journeys_per_user}个旅程，类型={journey_type}")
    
    raw_logs = traffic.run_task_based(
        concurrency=local_concurrency,
        journeys_per_user=journeys_per_user,
        ops_per_journey=ops_per_journey,
        journey_type=journey_type,
        process_id=proc_id,
        progress_queue=progress_queue,
    )
    
    # 5. 将日志保存到每个进程的CSV
    if raw_logs:
        dump_csv(
            raw_logs,
            f"raw_txs_p{proc_id}_{journey_type}_N{concurrency}_q{ops_per_journey}_{timestamp}.csv",
            fieldnames=["timestamp", "journey_id", "worker_id", "tx_type", "latency_s", "gas_used", "block_number", "status"]
        )
    else:
        print(f"[Worker {proc_id}] 警告：未捕获原始日志。")
    
    print(f"[Worker {proc_id}] 完成。")


# ========== 主实验循环 ==========
def main():
    print("=== DAS System v3 宏基准测试多核矩阵（Phase 4+）===")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 杀死先前的Ganache实例
    print("[预检] 杀死先前的Ganache进程...")
    kill_ganache()
    
    # 准备摘要结果
    summary_rows = []
    
    # 外层循环：旅程类型
    for journey_type in MATRIX_SCENARIOS:
        print(f"\n=== 旅程类型：{journey_type} ===")
        
        # 循环分摊因子q
        for q in MATRIX_AMORTIZATION_FACTORS:
            print(f"\n--- 分摊因子 q = {q} ---")
            
            # 内层循环：并发N
            for N in MATRIX_CONCURRENCY_LEVELS:
                print(f"\n   --- 并发 N = {N} ---")
                iteration_start = time.time()
                
                # 1. 启动Anvil网络（所有进程的单一网络）
                print("   1. 启动Anvil网络...")
                topology = get_topology()
                ganache = AnvilManager()
                max_retries = 5
                started = False
                for attempt in range(max_retries):
                    try:
                        print(f"      [系统] 尝试{attempt+1}/{max_retries}...")
                        ganache.start_network(topology)
                        started = True
                        break
                    except RuntimeError as e:
                        if "already in use" in str(e):
                            print(f"      [系统] 端口正在使用。杀死并等待10秒...")
                            kill_ganache()
                            time.sleep(10)
                        else:
                            raise e
                if not started:
                    raise RuntimeError("多次重试后启动Ganache失败。")
                time.sleep(2)
                
                # 2. 准备管理器和部署合约（每次迭代一次）
                network = ConnectionManager(topology)
                from config_matrix import MNEMONIC
                identity = UserManager(MNEMONIC)
                deployer = ContractDeployer(network, identity)
                injector = MacroTransactionInjector(network, identity)
                
                wait_for_nodes(network)
                
                print("   2. 部署合约...")
                registry = deployer.deploy_infrastructure(topology)
                print(f"      注册表键：{list(registry.keys())}")
                print("      等待合约完全挖出（15秒）...")
                time.sleep(15)
                
                # 3. 启动监控器（可选，可能干扰多进程）
                monitor = MacroMonitor(network)
                monitor.start()
                
                # 4. 使用集中式进度条启动worker进程
                print(f"   3. 启动{MATRIX_PROCESSES}个worker进程...")
                # 用于IPC的全局管理器
                with multiprocessing.Manager() as manager:
                    progress_queue = manager.Queue()
                    
                    # 计算总工作量
                    total_journeys = N * MATRIX_JOURNEYS_PER_USER
                    
                    processes = []
                    users_per_proc = N // MATRIX_PROCESSES
                    remainder = N % MATRIX_PROCESSES
                    user_start = 0
                    for proc_id in range(MATRIX_PROCESSES):
                        user_end = user_start + users_per_proc + (1 if proc_id < remainder else 0)
                        if user_start >= user_end:
                            # 此进程未分配用户（N < MATRIX_PROCESSES时不应发生）
                            continue
                        p = multiprocessing.Process(
                            target=run_worker_process,
                            args=(
                                proc_id,
                                user_start,
                                user_end,
                                N,  # 总并发（用于日志）
                                MATRIX_JOURNEYS_PER_USER,
                                q,
                                journey_type,
                                topology,
                                registry,
                                timestamp,
                                progress_queue,
                            )
                        )
                        processes.append(p)
                        p.start()
                        user_start = user_end
                    
                    # 全局进度条循环
                    with tqdm(total=total_journeys, unit="旅程", desc=f"总进度（N={N}，q={q}）") as pbar:
                        completed = 0
                        while completed < total_journeys:
                            # 非阻塞检查以允许检查进程是否死亡
                            while not progress_queue.empty():
                                progress_queue.get()
                                pbar.update(1)
                                completed += 1
                            
                            # 检查进程是否仍然存活（如果全部死亡则紧急退出）
                            if not any(p.is_alive() for p in processes) and completed < total_journeys:
                                print("进程过早死亡！")
                                break
                            
                            time.sleep(0.1)
                    
                    # 等待进程完成（它们应该已经完成）
                    for p in processes:
                        p.join()
                        if p.exitcode != 0:
                            print(f"   [警告] 进程{p.name}退出，代码{p.exitcode}")
                    
                    # === 新增：自动合并日志 ===
                    consolidate_logs(journey_type, N, q, timestamp)
                    # ============================
                
                # 6. 停止监控器
                monitor.stop()
                
                # 7. 计算聚合指标（监控器跨进程看到所有交易）
                metrics = monitor.calculate()
                print(f"   4. 结果：TPS = {metrics['tps']:.2f}，Gas/秒 = {metrics['gas_per_sec']:.0f}")
                
                # 8. 转储区块级日志
                if monitor.block_logs:
                    dump_csv(
                        monitor.block_logs,
                        f"matrix_blocks_{journey_type}_N{N}_q{q}_{timestamp}.csv",
                        fieldnames=["node", "block_number", "timestamp", "tx_count", "gas_used", "gas_limit"]
                    )
                else:
                    print("      警告：未捕获区块日志。")
                
                # 9. 记录摘要行
                iteration_end = time.time()
                makespan = iteration_end - iteration_start
                summary_rows.append({
                    "journey_type": journey_type,
                    "concurrency": N,
                    "amortization_factor": q,
                    "journeys_per_user": MATRIX_JOURNEYS_PER_USER,
                    "total_txs": metrics["total_txs"],
                    "total_gas": metrics["total_gas"],
                    "total_blocks": metrics["total_blocks"],
                    "total_time": metrics["total_time"],
                    "tps": metrics["tps"],
                    "gas_per_sec": metrics["gas_per_sec"],
                    "makespan_seconds": makespan,
                    "processes": MATRIX_PROCESSES,
                })
                
                # 10. 在下一次迭代前清理
                print("   [系统] 清理Ganache...")
                try:
                    ganache.stop_network()
                except Exception as e:
                    print(f"   [系统] 停止时警告：{e}")
                
                kill_ganache()
                
                print("   [系统] 冷却40秒以释放TCP端口...")
                # 关键：在先前5000用户运行中等待Windows释放TIME_WAIT套接字
                # 如果太短，下次运行将立即失败，WinError 10061。
                time.sleep(40)
    
    # 11. 保存摘要CSV
    print("\n=== 保存实验摘要 ===")
    dump_csv(
        summary_rows,
        f"matrix_multicore_summary_{timestamp}.csv",
        fieldnames=[
            "journey_type",
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
            "processes",
        ]
    )
    
    print("\n多核矩阵实验成功完成。")


if __name__ == "__main__":
    main()