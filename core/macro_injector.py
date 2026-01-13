"""
Macro‑benchmark transaction injector for Phase 4.
Handles high‑throughput Legacy transactions and explicit node mapping WITHOUT touching the original injector.py.
"""
import typing as t
from concurrent.futures import ThreadPoolExecutor
from eth_account import Account
from web3 import Web3
from web3.types import TxParams

from config_macro import GAS_LIMIT, MACRO_TX_GAS_LIMIT, MACRO_GAS_PRICE
from .identity import UserManager
from .network import ConnectionManager


class MacroTransactionInjector:
    """
    High‑load injector that enforces Legacy transaction format and explicit shard mapping.
    """

    def __init__(
        self,
        network_manager: ConnectionManager,
        identity_manager: UserManager,
    ) -> None:
        """
        Args:
            network_manager: Provides Web3 connections to each shard.
            identity_manager: Provides deterministic accounts and nonces.
        """
        self.network = network_manager
        self.identity = identity_manager

    def send_batch(
        self,
        shard_id: int,
        users: t.List[int],
        contract_func: t.Callable[[Web3, str, int], TxParams],
        **kwargs,
    ) -> t.List[str]:
        """
        Submit a batch of transactions to the given shard.

        Args:
            shard_id: Settlement shard index (0‑based) or -1 for execution.
            users: List of user indices (as used by identity_manager).
            contract_func: Callable that returns the transaction parameters
                (to, data, value, …) given a Web3 instance, from address, and nonce.
            **kwargs: Additional keyword arguments passed to contract_func.

        Returns:
            List of transaction hashes (hex strings) in the same order as `users`.

        Performance:
            - Nonce assignment is sequential per user.
            - Sanitizes tx_params by removing EIP‑1559 fields.
            - Forces gasPrice and gas.
            - Raw transmission is parallelized with a thread pool.
        """
        # Map shard_id to node name
        if shard_id == -1:
            node_name = "execution"
        elif shard_id >= 0:
            node_name = f"shard_{shard_id}"
        else:
            node_name = "baseline"

        web3 = self.network.get_web3(node_name)

        # Determine scope for nonce tracking (matching send_and_wait mapping)
        if shard_id >= 0:
            scope = f"shard_{shard_id}"
        elif shard_id == -1:
            scope = "execution"
        else:
            scope = "baseline"

        # 1. Build all transactions sequentially (nonce safety)
        raw_txs: t.List[bytes] = []
        for user_idx in users:
            # Get account and nonce
            account = self.identity.get_user(user_idx)
            address = account.address
            nonce = self.identity.nonce_manager.get_and_increment(address, scope)

            # Obtain transaction parameters from the contract function
            # Pass shard_id to the contract function (used by builders)
            kwargs_with_shard = kwargs.copy()
            kwargs_with_shard["shard_id"] = shard_id
            tx_params = contract_func(web3, address, nonce, **kwargs_with_shard)

            # Sanitization: delete any EIP‑1559 fields
            tx_params.pop("maxFeePerGas", None)
            tx_params.pop("maxPriorityFeePerGas", None)

            # Fill mandatory fields
            tx_params.setdefault("gas", MACRO_TX_GAS_LIMIT)
            tx_params.setdefault("gasPrice", MACRO_GAS_PRICE)
            tx_params.setdefault("nonce", nonce)
            tx_params.setdefault("chainId", 1)  # Ganache ignores, but safe

            # Remove any extra fields that Web3 might reject
            filtered = {k: v for k, v in tx_params.items() if v is not None}

            # Sign locally
            signed = account.sign_transaction(filtered)
            raw_txs.append(signed.raw_transaction)

        # 2. Send raw transactions in parallel, preserving order
        tx_hashes: t.List[str] = []
        with ThreadPoolExecutor(max_workers=min(len(raw_txs), 20)) as executor:
            # Submit all tasks, keep futures in a list in the same order
            futures = []
            for raw in raw_txs:
                future = executor.submit(web3.eth.send_raw_transaction, raw)
                futures.append(future)

            # Collect results in order of submission
            for future in futures:
                try:
                    tx_hash = future.result()
                    tx_hashes.append(Web3.to_hex(tx_hash))
                except Exception as e:
                    # Log but keep going – fire‑and‑forget
                    print(f"[MacroInjector] Failed to send transaction: {e}")
                    tx_hashes.append("")  # placeholder for ordering

        return tx_hashes