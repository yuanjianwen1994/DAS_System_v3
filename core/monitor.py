"""
Network monitoring for DAS System v3.
Background polling of transaction status and metrics collection.
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
    Tracks transaction hashes across shards and polls their status in background.

    Usage:
        monitor = NetworkMonitor(network_manager)
        monitor.track(["0xabc..."], shard_id=0)
        monitor.start_polling()
        # ... do other work ...
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
        Convert a shard identifier (int or str) to the node name used in the topology.
        """
        if isinstance(shard_id, int):
            return f"shard_{shard_id}"
        return str(shard_id)  # For "execution" or "baseline"

    def track(self, tx_hashes: t.List[str], shard_id: int, submission_time: float = None) -> None:
        """
        Add a list of transaction hashes to be monitored for a specific shard.

        submission_time: timestamp when the transaction was submitted (defaults to current time).
        """
        if submission_time is None:
            submission_time = time.time()
        with self._lock:
            pending_dict = self._pending[shard_id]
            for tx in tx_hashes:
                pending_dict[tx] = submission_time

    def start_polling(self, interval: float = 1.0) -> None:
        """
        Start a background thread that periodically checks transaction status.

        The thread runs until `stop_polling()` is called or there are no more
        pending transactions.
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
        Signal the polling thread to stop and wait for it.
        """
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def _polling_loop(self, interval: float) -> None:
        """
        Main loop that checks pending transactions every `interval` seconds.
        """
        while not self._stop_event.is_set():
            with self._lock:
                # Copy pending items to avoid modification during iteration
                pending_copy = {
                    shard_id: dict(hashes)
                    for shard_id, hashes in self._pending.items()
                }

            if not pending_copy:
                # No more pending transactions, exit loop
                break

            # Check each shard
            for shard_id, hashes in pending_copy.items():
                node_name = self._get_node_name(shard_id)
                web3 = self.network.get_web3(node_name)
                for tx_hash, start_time in list(hashes.items()):
                    status, receipt = self._check_transaction(web3, tx_hash, node_name)
                    if status in (TxStatus.MINED, TxStatus.FAILED):
                        # Transaction finalized
                        with self._lock:
                            # Pop the start time from pending dict
                            pending_start = self._pending[shard_id].pop(tx_hash, None)
                            if pending_start is None:
                                pending_start = start_time  # fallback to copy
                            if not self._pending[shard_id]:  # If dict is empty
                                del self._pending[shard_id]  # Remove the key
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
                            # Print confirmation
                            print(f"[Monitor] CONFIRMED {tx_hash[:10]} on {node_name} (Block {receipt.blockNumber})")
                            # Clean up debug print tracking
                            self._last_debug_print.pop(tx_hash, None)
                    # else: keep pending

            # Sleep for the interval
            time.sleep(interval)

    def _check_transaction(
        self, web3: Web3, tx_hash: str, node_name: str
    ) -> t.Tuple[TxStatus, t.Optional[TxReceipt]]:
        """
        Check the current status of a transaction, with debug logging.

        Returns:
            (status, receipt) where receipt is non‑None only for mined/failed txs.
        """
        now = time.time()
        # First, try to get the receipt
        try:
            receipt = web3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                # Check if the transaction succeeded (status == 1)
                if receipt.get("status") == 1:
                    return TxStatus.MINED, receipt
                else:
                    return TxStatus.FAILED, receipt
        except Exception:
            # TransactionNotFound or other error – treat as not yet mined
            pass

        # Not mined yet, decide whether to print debug
        last_print = self._last_debug_print.get(tx_hash, 0)
        if now - last_print >= 5.0:
            print(f"[Monitor] Checking {tx_hash[:10]} on {node_name}... Status: Not Found")
            self._last_debug_print[tx_hash] = now

        # Check if transaction is still in the mempool
        try:
            tx = web3.eth.get_transaction(tx_hash)
            if tx is not None:
                return TxStatus.PENDING, None
        except Exception:
            # Not in mempool either – keep pending (might appear later)
            pass

        # Not found anywhere – still treat as pending (will be re‑checked later)
        return TxStatus.PENDING, None

    def get_results(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Return a list of dictionaries with metrics for all completed transactions.
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
        Block until there are no pending transactions or timeout is reached.

        Returns:
            True if all pending transactions have been mined/finalized,
            False if timeout occurred.
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