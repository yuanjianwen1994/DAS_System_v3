"""
Macro-benchmark traffic generator for Phase 4+.
Features: Task-Based execution, Raw Logging, Shard Distribution, and Simulated Jitter.
"""
import typing as t
import time
import random
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3
from web3.types import TxParams

from config_matrix import (
    MACRO_TX_TIMEOUT, 
    SIM_THINK_TIME_RANGE, 
    HTTP_RETRIES
)
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
    ) -> None:
        self.network = network_manager
        self.identity = identity_manager
        self.injector = injector
        self.registry = registry
        self.completed_journeys = []
        self.raw_logs = []  # RAW DATA LOGGING
        
        # Discover available shards for distribution
        # Assuming keys like 'shard_0', 'shard_1' exist in registry
        self.shard_ids = [
            int(k.split('_')[1]) for k in registry.keys() 
            if k.startswith('shard_') and k.split('_')[1].isdigit()
        ]
        if not self.shard_ids:
            self.shard_ids = [0] # Fallback
        self.shard_ids.sort()
        print(f"[Traffic] Load balancing across shards: {self.shard_ids}")

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
        # FIX: Handle Execution Shard (-1) for Withdrawals
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

    # ---------- Robust Send with Retry ----------
    def _send_and_wait(self, func_type, user_idx, **kwargs):
        # Retry Loop for Connection Stability
        for attempt in range(HTTP_RETRIES):
            try:
                start_time = time.time()
                
                # Copy kwargs to avoid mutation issues on retry
                call_kwargs = kwargs.copy()
                
                if "shard_id" not in call_kwargs:
                     raise ValueError(f"Missing 'shard_id' for {func_type}")
                shard_id = call_kwargs.pop("shard_id")

                contract_func = self._builders[func_type]

                # 1. Send
                tx_hashes = self.injector.send_batch(
                    shard_id,
                    users=[user_idx],
                    contract_func=contract_func,
                    **call_kwargs
                )
                
                if not tx_hashes:
                     raise Exception("No tx hash returned")
                
                tx_hash = tx_hashes[0]

                # 2. Wait
                node_name = "execution" if shard_id == -1 else f"shard_{shard_id}"
                web3 = self.network.get_web3(node_name)
                
                receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=MACRO_TX_TIMEOUT)
                if receipt.status != 1:
                    raise Exception(f"Tx {tx_hash} reverted")

                # 3. Log Raw Data
                duration = time.time() - start_time
                self.raw_logs.append({
                    "timestamp": time.time(),
                    "worker_id": user_idx,
                    "tx_type": func_type,
                    "latency_s": duration,
                    "gas_used": receipt['gasUsed'],
                    "block_number": receipt['blockNumber'],
                    "status": receipt['status']
                })
                
                return receipt
                
            except Exception as e:
                # Catch Connection errors and retry
                error_msg = str(e)
                is_conn_error = "Connection aborted" in error_msg or "Connection refused" in error_msg or "Available sockets" in error_msg
                
                if is_conn_error and attempt < HTTP_RETRIES - 1:
                    sleep_time = (attempt + 1) * 2
                    # print(f"[Traffic] Worker {user_idx} connection retry {attempt+1}/{HTTP_RETRIES}...") 
                    time.sleep(sleep_time)
                    continue
                else:
                    raise e

    def _sleep_random(self):
        """Inject simulation jitter."""
        time.sleep(random.uniform(*SIM_THINK_TIME_RANGE))

    # ---------- Worker Logic with Sharding ----------
    def _worker_loop_das_task(self, worker_id: int, ops_per_journey: int, target_journeys: int):
        user_idx = worker_id
        amount = 100
        
        # DISTRIBUTE USERS: Round-robin assignment to shards
        # Worker 0 -> Shard 0, Worker 1 -> Shard 1, etc.
        source_shard = self.shard_ids[worker_id % len(self.shard_ids)]
        
        journeys_done = 0
        while journeys_done < target_journeys:
            try:
                # 1. Deposit (Source Shard -> Execution)
                self._send_and_wait("das_burn", user_idx, shard_id=source_shard, amount=amount)
                self._sleep_random()
                
                self._send_and_wait("das_mint", user_idx, shard_id=-1, amount=amount)
                self._sleep_random()

                # 2. Work (Execution)
                for _ in range(ops_per_journey):
                    self._send_and_wait("das_work", user_idx, shard_id=-1, amount=amount)
                    self._sleep_random()

                # 3. Withdraw (Execution -> Source Shard)
                self._send_and_wait("das_burn", user_idx, shard_id=-1, amount=amount)
                self._sleep_random()
                
                self._send_and_wait("das_mint", user_idx, shard_id=source_shard, amount=amount)
                self._sleep_random()

                journeys_done += 1
                
                # Log completion for progress tracking (optional)
                # print(f"Worker {worker_id} finished journey {journeys_done}/{target_journeys}")

            except Exception as e:
                print(f"[Traffic] Worker {worker_id} failed: {e}")
                raise

    def run_task_based(
        self,
        concurrency: int,
        journeys_per_user: int,
        ops_per_journey: int,
        journey_type: str = "DAS"
    ) -> None:
        """
        Matrix Benchmark Entry: Runs until every user completes N journeys.
        """
        print(f"[MacroTraffic] Starting Matrix Task: {concurrency} users, {journeys_per_user} journeys each, q={ops_per_journey}")
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for i in range(concurrency):
                if journey_type == "DAS":
                    futures.append(executor.submit(self._worker_loop_das_task, i, ops_per_journey, journeys_per_user))
                # Add 2PC logic here if needed
            
            # Wait for all
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[Traffic] Critical worker failure: {e}")

    # ---------- Legacy Methods (Placeholders) ----------
    # Keep these to avoid breaking existing scripts, but they can be stubs.
    def start_concurrent(self, concurrency: int, journey_type: str = "DAS", ops_per_journey: int = None) -> None:
        """Legacy: Start concurrent workers (not journey‑limited)."""
        raise NotImplementedError("start_concurrent is deprecated; use run_task_based.")

    def run_for_duration(self, concurrency: int, duration_seconds: float, journey_type: str = "DAS", ops_per_journey: int = None) -> None:
        """Legacy: Run workers for a fixed duration."""
        raise NotImplementedError("run_for_duration is deprecated.")

    def _worker_loop_das(self, worker_id: int, ops_per_journey: int) -> None:
        """Legacy worker loop (single journey)."""
        # Redirect to task‑based loop with target_journeys=1
        self._worker_loop_das_task(worker_id, ops_per_journey, 1)

    def _worker_loop_2pc(self, worker_id: int, ops_per_journey: int) -> None:
        """Legacy 2PC loop."""
        raise NotImplementedError("2PC loop not implemented in this version.")

    def _repeating_worker(self, worker_id: int, journeys_per_user: int, worker_func, ops_per_journey: int):
        """Legacy internal helper."""
        for _ in range(journeys_per_user):
            worker_func(worker_id, ops_per_journey)