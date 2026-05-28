"""
DAS System v3 网络管理模块。
Anvil生命周期与Web3连接管理。
"""
import os
import shutil
import socket
import subprocess
import sys
import time
import typing as t
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from web3 import Web3
from web3.providers import HTTPProvider

from config_global import MNEMONIC, BLOCK_TIME, GAS_LIMIT, NUM_USERS, ACCOUNT_BALANCE_ETH, get_topology, GANACHE_DATA_DIR


class AnvilManager:
    """
    管理拓扑中每个节点的Anvil进程。
    """
    def __init__(self) -> None:
        self.processes: t.Dict[str, subprocess.Popen] = {}

    def _is_port_available(self, port: int) -> bool:
        """
        检查本地主机上的TCP端口是否可用。
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return True
            except OSError:
                return False

    def start_network(self, topology: t.Dict[str, t.Any]) -> None:
        """
        为拓扑中的每个节点启动anvil子进程。
        输出：丢弃到DEVNULL以节省磁盘空间。
        """
        # 创建日志目录（保留其他日志）
        os.makedirs("logs", exist_ok=True)

        # 构建扁平节点列表
        flat_nodes = []
        if "shards" in topology:
            for name, cfg in topology["shards"].items():
                flat_nodes.append((name, cfg))
        if "execution" in topology:
            flat_nodes.append(("execution", topology["execution"]))
        if "baseline" in topology:
            flat_nodes.append(("baseline", topology["baseline"]))

        for name, cfg in flat_nodes:
            port = cfg["port"]
            # 强制启动：绕过严格的Python级别检查
            # 让Anvil (Rust)处理绑定。它经常在Python失败的地方成功。
            if not self._is_port_available(port):
                print(f"      [系统] 警告：端口{port}似乎繁忙（内部检查）。仍然尝试启动{name}...")

            # E:盘上的数据库路径
            node_db_path = os.path.join(GANACHE_DATA_DIR, name)
            # 清理先前的数据库以防止累积并确保全新状态
            if os.path.exists(node_db_path):
                try:
                    shutil.rmtree(node_db_path)
                except Exception as e:
                    print(f"[系统] 警告：无法清理{name}的数据库：{e}")
            os.makedirs(node_db_path, exist_ok=True)

            # 验证打印
            print(f"正在端口{port}上启动{name}，BlockTime={BLOCK_TIME}...")

            # 使用等号语法构建命令
            cmd = [
                "anvil",
                f"--port={port}",
                f"--host=127.0.0.1",  # 强制IPv4绑定
                f"--block-time={BLOCK_TIME}",
                f"--mnemonic={MNEMONIC}",
                f"--accounts={NUM_USERS}",
                f"--balance={ACCOUNT_BALANCE_ETH}",
                f"--gas-limit={GAS_LIMIT}",
                "--disable-code-size-limit",  # 禁用EIP-170合约大小限制
                "--hardfork=shanghai",
                "--order=fifo",
                "--prune-history",
                "--chain-id=31337",
                # Anvil不持久化状态；省略database.dbPath
            ]

            print(f"启动{name}：{' '.join(cmd)}")
            if name == "baseline":
                sys.stderr.write(f"[调试] 基准命令：{' '.join(cmd)}\n")

            log_file = open(f"logs/{name}.log", "w")
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            self.processes[name] = proc

            # 等待进程启动并检查是否崩溃
            time.sleep(2)
            if proc.poll() is not None:
                # 进程终止，但我们没有日志
                raise RuntimeError(f"{name}启动失败！（退出代码：{proc.returncode})")

            print(f"已在端口{port}上启动{name}（PID：{proc.pid}）")

    def stop_network(self) -> None:
        """
        优雅地终止所有anvil子进程。
        """
        for name, proc in self.processes.items():
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
                print(f"已停止{name}")
        self.processes.clear()


class ConnectionManager:
    """
    使用健壮的线程安全HTTP会话管理Web3连接。
    """
    def __init__(self, topology: t.Dict[str, t.Any]) -> None:
        self.topology = topology
        self._connections: t.Dict[str, Web3] = {}
        # 为此进程创建单个全局会话
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        创建具有高连接池和激进重试的健壮HTTP会话。
        对N=1600高并发实验至关重要。
        """
        session = requests.Session()
        # 高池大小以防止负载下的"NewConnectionError"
        adapter = HTTPAdapter(
            pool_connections=500,
            pool_maxsize=500,
            max_retries=Retry(
                total=10,
                backoff_factor=1.0,  # 减慢重试以让Ganache喘口气
                status_forcelist=[500, 502, 503, 504]
            )
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_web3(self, node_name: str, timeout: int = 30) -> Web3:
        """
        返回具有共享会话的Web3实例。
        热路径中无阻塞检查以避免日志泛滥。
        """
        # 如果可用，返回缓存的实例
        if node_name in self._connections:
            return self._connections[node_name]

        # 解析端口
        port = None
        if node_name in self.topology.get("shards", {}):
            port = self.topology["shards"][node_name]["port"]
        elif node_name == "execution":
            port = self.topology["execution"]["port"]
        elif node_name == "baseline":
            port = self.topology["baseline"]["port"]
        else:
            raise ValueError(f"未知节点：{node_name}")

        url = f"http://127.0.0.1:{port}"
        
        # 确保会话存在
        if not hasattr(self, '_session') or self._session is None:
            self._session = self._create_session()

        # 使用健壮会话创建Web3实例
        provider = HTTPProvider(
            url,
            session=self._session,
            request_kwargs={"timeout": 120}
        )
        w3 = Web3(provider)
        
        # 缓存并返回（此处不要检查is_connected()，会导致DoS）
        self._connections[node_name] = w3
        return w3