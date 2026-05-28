"""
DAS System v3 网络监控模块。
交易状态后台轮询和指标收集。
"""
import threading
import time
import typing as t
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

from web3 import Web3
from web3.types import TxReceipt
from web3.exceptions import TransactionNotFound

from .network import ConnectionManager


class TxStatus(Enum):
    PENDING = "pending"
    MINED = "mined"
    FAILED = "failed"


@dataclass
class TransactionRecord:
    tx_hash: str
    shard_id: int
    start_time: float
    end_time: t.Optional[float] = None
    status: TxStatus = TxStatus.PENDING
    block_number: t.Optional[int] = None
    gas_used: t.Optional[int] = None
    error: t.Optional[str] = None


class NetworkMonitor:
    """
    跨分片追踪交易哈希并在后台轮询其状态。

    用法：
        monitor = NetworkMonitor(network_manager)
        monitor.track(["0xabc..."], shard_id=0)
        monitor.start_polling()
        # ... 做其他工作 ...
        monitor.wait_until_complete(timeout=300)
        results = monitor.get_results()
    """

    def __init__(self, network_manager: ConnectionManager) -> None:
        self.network = network_manager
        self._pending: t.Dict[int, t.Dict[str, float]] = defaultdict(dict)
        self._completed: t.List[TransactionRecord] = []
        self._lock = threading.RLock()
        self._thread: t.Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._start_time: t.Optional[float] = None
        self._last_debug_print: t.Dict[str, float] = {}

    def _get_node_name(self, shard_id: t.Union[int, str]) -> str:
        """
        将分片标识符（int或str）转换为拓扑中使用的节点名称。
        """
        if isinstance(shard_id, int):
            return f"shard_{shard_id}"
        return str(shard_id)  # 用于"execution"或"baseline"

    def track(self, tx_hashes: t.List[str], shard_id: int, submission_time: float = None) -> None:
        """
        添加要监控的特定分片的交易哈希列表。

        submission_time：交易提交时的时间戳（默认为当前时间）。
        """
        if submission_time is None:
            submission_time = time.time()
        with self._lock:
            pending_dict = self._pending[shard_id]
            for tx in tx_hashes:
                pending_dict[tx] = submission_time

    def start_polling(self, interval: float = 1.0) -> None:
        """
        启动定期检查交易状态的后台线程。

        线程运行直到调用`stop_polling()`或没有更多待处理交易。
        """
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._polling_loop,
            args=(interval,),
            daemon=True,
        )
        self._start_time = time.time()
        self._thread.start()

    def stop_polling(self) -> None:
        """
        信号轮询线程停止并等待它。
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _polling_loop(self, interval: float) -> None:
        """
        每隔`interval`秒检查待处理交易的主循环。
        """
        while not self._stop_event.is_set():
            with self._lock:
                # 复制待处理项以避免在迭代期间修改
                pending_copy = {
                    shard_id: dict(hashes)
                    for shard_id, hashes in self._pending.items()
                }

            if not pending_copy:
                # 没有更多待处理交易，退出循环
                break

            # 检查每个分片
            for shard_id, hashes in pending_copy.items():
                node_name = self._get_node_name(shard_id)
                web3 = self.network.get_web3(node_name)
                for tx_hash, start_time in list(hashes.items()):
                    status, receipt = self._check_transaction(web3, tx_hash, node_name)
                    if status in (TxStatus.MINED, TxStatus.FAILED):
                        # 交易已最终确定
                        with self._lock:
                            # 从待处理字典中弹出开始时间
                            pending_start = self._pending[shard_id].pop(tx_hash, None)
                            if pending_start is None:
                                pending_start = start_time  # 后备到副本
                            if not self._pending[shard_id]:  # 如果字典为空
                                del self._pending[shard_id]  # 删除键
                            record = TransactionRecord(
                                tx_hash=tx_hash,
                                shard_id=shard_id,
                                start_time=pending_start,
                                end_time=time.time(),
                                status=status,
                                block_number=receipt.get("blockNumber") if receipt else None,
                                gas_used=receipt.get("gasUsed") if receipt else None,
                            )
                            self._completed.append(record)
                            # 打印确认
                            print(f"[监控] 已确认 {tx_hash[:10]} 在 {node_name}（区块 {receipt.blockNumber}）")
                            # 清理调试打印追踪
                            self._last_debug_print.pop(tx_hash, None)
                    # else: 保持待处理

            # 休眠间隔
            time.sleep(interval)

    def _check_transaction(
        self, web3: Web3, tx_hash: str, node_name: str
    ) -> t.Tuple[TxStatus, t.Optional[TxReceipt]]:
        """
        检查交易的当前状态，并进行调试日志记录。

        返回：
            (status, receipt)，其中receipt仅对已挖掘/失败的tx为非None。
        """
        now = time.time()
        # 首先，尝试获取收据
        try:
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                # 检查交易是否成功（status == 1）
                if receipt.get("status") == 1:
                    return TxStatus.MINED, receipt
                else:
                    return TxStatus.FAILED, receipt
        except Exception:
            # TransactionNotFound或其他错误 - 视为尚未挖掘
            pass

        # 尚未挖掘，决定是否打印调试
        last_print = self._last_debug_print.get(tx_hash, 0)
        if now - last_print >= 5.0:
            print(f"[监控] 在 {node_name} 上检查 {tx_hash[:10]}... 状态：未找到")
            self._last_debug_print[tx_hash] = now

        # 检查交易是否仍在mempool中
        try:
            tx = web3.eth.get_transaction(tx_hash)
            if tx is not None:
                return TxStatus.PENDING, None
        except Exception:
            # 也不在mempool中 - 保持待处理（可能稍后出现）
            pass

        # 任何地方都找不到 - 仍然视为待处理（稍后将重新检查）
        return TxStatus.PENDING, None

    def get_results(self) -> t.List[t.Dict[str, t.Any]]:
        """
        返回所有已完成交易的指标字典列表。
        """
        with self._lock:
            return [
                {
                    "tx_hash": r.tx_hash,
                    "shard_id": r.shard_id,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "latency": r.end_time - r.start_time if r.end_time else None,
                    "status": r.status.value,
                    "block_number": r.block_number,
                    "gas_used": r.gas_used,
                    "error": r.error,
                }
                for r in self._completed
            ]

    def wait_until_complete(self, timeout: float = 300.0) -> bool:
        """
        阻塞直到没有待处理交易或达到超时。

        返回：
            如果所有待处理交易都已挖掘/最终确定，则为True，
            如果发生超时，则为False。
        """
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if not self._pending:
                    return True
            time.sleep(0.5)
        return False

    def __del__(self) -> None:
        self.stop_polling()