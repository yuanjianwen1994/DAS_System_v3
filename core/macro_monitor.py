"""
Macro‑benchmark monitor for Phase 4.
Strictly for Block‑level Throughput (TPS) – avoids tracking individual tx latencies.
"""
import typing as t
from web3 import Web3
from .network import ConnectionManager


class MacroMonitor:
    """
    Measures block‑level throughput across shards and execution chain.

    Usage:
        monitor = MacroMonitor(network_manager)
        monitor.start()
        # ... run load ...
        monitor.stop()
        metrics = monitor.calculate()
    """

    def __init__(self, network_manager: ConnectionManager) -> None:
        self.network = network_manager
        self._start_blocks: t.Dict[str, int] = {}
        self._end_blocks: t.Dict[str, int] = {}
        self._started = False
        self._stopped = False

    def start(self) -> None:
        """Record current block numbers on Shard 0, 1, and Execution."""
        if self._started:
            return
        nodes = ["shard_0", "shard_1", "execution"]
        for node in nodes:
            web3 = self.network.get_web3(node)
            try:
                block = web3.eth.get_block("latest")
                self._start_blocks[node] = block.number
            except Exception as e:
                print(f"[MacroMonitor] Failed to get start block for {node}: {e}")
                self._start_blocks[node] = 0
        self._started = True
        print(f"[MacroMonitor] Started at blocks: {self._start_blocks}")

    def stop(self) -> None:
        """Record end block numbers on the same nodes."""
        if self._stopped:
            return
        nodes = ["shard_0", "shard_1", "execution"]
        for node in nodes:
            web3 = self.network.get_web3(node)
            try:
                block = web3.eth.get_block("latest")
                self._end_blocks[node] = block.number
            except Exception as e:
                print(f"[MacroMonitor] Failed to get end block for {node}: {e}")
                self._end_blocks[node] = 0
        self._stopped = True
        print(f"[MacroMonitor] Stopped at blocks: {self._end_blocks}")

    def calculate(self) -> t.Dict[str, float]:
        """
        Iterate blocks start to end, sum len(block.transactions) and block.gasUsed.

        Returns:
            Dictionary with aggregate metrics:
            {
                "total_txs": total transactions mined across all observed chains,
                "total_gas": total gas used across all observed chains,
                "total_time": approximate total time (based on BLOCK_TIME * blocks),
                "tps": transactions per second,
                "gas_per_sec": gas per second,
            }
        """
        if not self._started or not self._stopped:
            raise RuntimeError("Monitor must be started and stopped before calculation")

        from config_macro import BLOCK_TIME

        total_txs = 0
        total_gas = 0
        total_blocks = 0

        for node in self._start_blocks.keys():
            start = self._start_blocks[node]
            end = self._end_blocks[node]
            if start == 0 or end == 0 or end < start:
                # No blocks or invalid range, skip
                continue
            web3 = self.network.get_web3(node)
            # Iterate each block in range (inclusive of start, exclusive of end?)
            # We'll include both start and end blocks for throughput.
            for block_num in range(start, end + 1):
                try:
                    block = web3.eth.get_block(block_num, full_transactions=False)
                    total_txs += len(block.transactions)
                    total_gas += block.gasUsed
                    total_blocks += 1
                except Exception as e:
                    print(f"[MacroMonitor] Error fetching block {block_num} on {node}: {e}")
                    continue

        if total_blocks == 0:
            return {
                "total_txs": 0,
                "total_gas": 0,
                "total_time": 0.0,
                "tps": 0.0,
                "gas_per_sec": 0.0,
            }

        # Approximate total time = number of blocks * BLOCK_TIME
        total_time = total_blocks * BLOCK_TIME
        tps = total_txs / total_time if total_time > 0 else 0.0
        gas_per_sec = total_gas / total_time if total_time > 0 else 0.0

        return {
            "total_txs": total_txs,
            "total_gas": total_gas,
            "total_blocks": total_blocks,
            "total_time": total_time,
            "tps": tps,
            "gas_per_sec": gas_per_sec,
        }