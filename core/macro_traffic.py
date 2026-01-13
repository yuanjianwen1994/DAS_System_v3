"""
Macro‑benchmark traffic generator for Phase 4.
Generates concurrent "Full Lifecycle" journeys (Deposit → N*Work → Withdraw) without touching existing traffic logic.
"""
import typing as t
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.types import TxParams

from config_macro import MACRO_OPS_PER_JOURNEY, MACRO_TX_INTERVAL, MACRO_TX_TIMEOUT
from .macro_injector import MacroTransactionInjector
from .identity import UserManager
from .network import ConnectionManager


class MacroTrafficGenerator:
    """
    Generates high‑load traffic for DAS and 2PC full lifecycles.
    Each worker runs a dedicated user through a complete journey.
    """

    def __init__(
        self,
        network_manager: ConnectionManager,
        identity_manager: UserManager,
        injector: MacroTransactionInjector,
        registry: t.Dict[str, t.Dict[str, t.Any]],
    ) -> None:
        self.network = network_manager
        self.identity = identity_manager
        self.injector = injector
        self.registry = registry
        self.completed_journeys = []  # track completed full lifecycles

        # Helper: contract function builders
        self._builders = {
            "das_burn": self._build_das_burn,
            "das_mint": self._build_das_mint,
            "das_work": self._build_das_work,
            "tpc_lock": self._build_tpc_lock,
            "tpc_commit": self._build_tpc_commit,
        }

    # ---------- Contract function builders ----------
    def _build_das_burn(
        self, web3: Web3, from_address: str, nonce: int, **kwargs
    ) -> TxParams:
        """Build a DAS burn transaction."""
        shard_id = kwargs["shard_id"]
        amount = kwargs.get("amount", 100)
        shard_name = f"shard_{shard_id}"
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
        """Build a DAS mint transaction."""
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
        """Build a Workload.doWork transaction."""
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
        """Build a 2PC lock transaction."""
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
        """Build a 2PC commit transaction."""
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

    # ---------- Worker loops ----------
    def _send_and_wait(self, func_type, user_idx, **kwargs):
        """
        Send a transaction and wait for its receipt.
        """
        # Map func_type to builder
        builder_map = {
            "das_burn": self._build_das_burn,
            "das_mint": self._build_das_mint,
            "das_work": self._build_das_work,
            "tpc_lock": self._build_tpc_lock,
            "tpc_commit": self._build_tpc_commit,
        }
        contract_func = builder_map[func_type]
        
        # Determine shard_id from kwargs or default
        shard_id = kwargs.get("shard_id", 0 if func_type in ("das_burn", "das_mint") else -1)
        
        # Send batch (single user)
        tx_hashes = self.injector.send_batch(
            shard_id=shard_id,
            users=[user_idx],
            contract_func=contract_func,
            **kwargs
        )
        if not tx_hashes or not tx_hashes[0]:
            raise Exception("Send failed")
        
        # Wait for receipt
        node_name = "execution" if shard_id == -1 else f"shard_{shard_id}" if shard_id >= 0 else "baseline"
        web3 = self.network.get_web3(node_name)
        tx_hash = tx_hashes[0]
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=MACRO_TX_TIMEOUT)
        if receipt.status != 1:
            raise Exception("Tx reverted")
        return receipt

    def _worker_loop_das(self, worker_id: int, ops_per_journey: int) -> None:
        """
        DAS Full Lifecycle: Deposit (Burn S0 -> Mint Exec) -> Work -> Withdraw (Burn Exec -> Mint S0).
        Each step waits for transaction receipt, ensuring true cross‑shard sequencing.
        """
        user_idx = worker_id  # each worker gets a dedicated user
        amount = 100

        try:
            # 1. Deposit: Burn on Shard 0
            self._send_and_wait("das_burn", user_idx, shard_id=0, amount=amount)
            # Mint on Execution
            self._send_and_wait("das_mint", user_idx, shard_id=-1, amount=amount)

            # 2. Work: Loop N operations on Execution
            for _ in range(ops_per_journey):
                self._send_and_wait("das_work", user_idx, shard_id=-1, amount=amount)

            # 3. Withdraw: Burn on Execution
            self._send_and_wait("das_burn", user_idx, shard_id=-1, amount=amount)
            # Mint on Shard 0
            self._send_and_wait("das_mint", user_idx, shard_id=0, amount=amount)

            # Record successful journey
            self.completed_journeys.append({
                "worker_id": worker_id,
                "timestamp": time.time(),
                "ops": ops_per_journey,
            })
        except Exception as e:
            print(f"[MacroTraffic] Worker {worker_id} failed: {e}")
            raise

    def _worker_loop_2pc(self, worker_id: int, ops_per_journey: int) -> None:
        """
        2PC Lifecycle: Loop N * (Lock -> Work -> Commit).
        Each iteration locks both shard and execution, does work, commits both.
        """
        user_idx = worker_id
        amount = 100

        for i in range(ops_per_journey):
            # Generate a unique TPC ID for this iteration
            tpc_id = random.randbytes(32)

            # Lock on Shard 0
            self.injector.send_batch(
                shard_id=0,
                users=[user_idx],
                contract_func=self._build_tpc_lock,
                tpc_id=tpc_id,
            )
            time.sleep(MACRO_TX_INTERVAL)

            # Lock on Execution
            self.injector.send_batch(
                shard_id=-1,
                users=[user_idx],
                contract_func=self._build_tpc_lock,
                tpc_id=tpc_id,
            )
            time.sleep(MACRO_TX_INTERVAL)

            # Work on Execution
            self.injector.send_batch(
                shard_id=-1,
                users=[user_idx],
                contract_func=self._build_das_work,
                amount=amount,
            )
            time.sleep(MACRO_TX_INTERVAL)

            # Commit on Shard 0
            self.injector.send_batch(
                shard_id=0,
                users=[user_idx],
                contract_func=self._build_tpc_commit,
                tpc_id=tpc_id,
            )
            time.sleep(MACRO_TX_INTERVAL)

            # Commit on Execution
            self.injector.send_batch(
                shard_id=-1,
                users=[user_idx],
                contract_func=self._build_tpc_commit,
                tpc_id=tpc_id,
            )
            time.sleep(MACRO_TX_INTERVAL)

    # ---------- Public interface ----------
    def start_concurrent(
        self,
        concurrency: int,
        journey_type: str = "DAS",
        ops_per_journey: int = None,
    ) -> None:
        """
        Start `concurrency` worker threads, each running a full lifecycle.

        Args:
            concurrency: Number of concurrent users/workers.
            journey_type: "DAS" or "2PC".
            ops_per_journey: Override default MACRO_OPS_PER_JOURNEY.
        """
        if ops_per_journey is None:
            ops_per_journey = MACRO_OPS_PER_JOURNEY

        if journey_type not in ("DAS", "2PC"):
            raise ValueError(f"Unknown journey_type: {journey_type}")

        worker_func = (
            self._worker_loop_das if journey_type == "DAS" else self._worker_loop_2pc
        )

        # Use a thread pool to launch all workers
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for worker_id in range(concurrency):
                future = executor.submit(worker_func, worker_id, ops_per_journey)
                futures.append(future)

            # Wait for all workers to finish (they run until completion)
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[MacroTraffic] Worker failed: {e}")

        print(f"[MacroTraffic] All {concurrency} {journey_type} workers finished.")

    def run_for_duration(
        self,
        concurrency: int,
        duration_seconds: float,
        journey_type: str = "DAS",
        ops_per_journey: int = None,
    ) -> None:
        """
        Start `concurrency` worker threads that repeatedly run lifecycles for the given duration.

        Args:
            concurrency: Number of concurrent users/workers.
            duration_seconds: How long to keep generating traffic.
            journey_type: "DAS" or "2PC".
            ops_per_journey: Override default MACRO_OPS_PER_JOURNEY.
        """
        if ops_per_journey is None:
            ops_per_journey = MACRO_OPS_PER_JOURNEY

        if journey_type not in ("DAS", "2PC"):
            raise ValueError(f"Unknown journey_type: {journey_type}")

        worker_func = (
            self._worker_loop_das if journey_type == "DAS" else self._worker_loop_2pc
        )

        stop_time = time.time() + duration_seconds
        import threading
        stop_flag = threading.Event()

        def timed_worker(worker_id: int):
            while not stop_flag.is_set() and time.time() < stop_time:
                # Run one lifecycle
                worker_func(worker_id, ops_per_journey)
                # Optionally add a small gap between lifecycles
                time.sleep(MACRO_TX_INTERVAL * 2)

        # Use a thread pool to launch all workers
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for worker_id in range(concurrency):
                future = executor.submit(timed_worker, worker_id)
                futures.append(future)

            # Wait for duration
            time.sleep(duration_seconds)
            stop_flag.set()

            # Wait for workers to finish current iteration
            for future in futures:
                try:
                    future.result(timeout=5.0)
                except Exception as e:
                    print(f"[MacroTraffic] Worker timed out or failed: {e}")

        print(f"[MacroTraffic] Duration‑based traffic finished ({concurrency} {journey_type} workers).")