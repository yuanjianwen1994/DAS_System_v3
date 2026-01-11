"""
Transaction injection engine for DAS System v3.
High‑concurrency fire‑and‑forget transaction submission.
"""
import typing as t
from concurrent.futures import ThreadPoolExecutor, as_completed
from eth_account import Account
from web3 import Web3
from web3.types import TxParams

from config_global import GAS_LIMIT, DEFAULT_GAS_PRICE
from .identity import UserManager
from .network import ConnectionManager


class TransactionInjector:
    """
    Handles raw transaction submission with local nonce management.

    Zero‑RPC principle: No RPC calls other than `send_raw_transaction`.
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
            shard_id: Settlement shard index (0‑based).
            users: List of user indices (as used by identity_manager).
            contract_func: Callable that returns the transaction parameters
                (to, data, value, …) given a Web3 instance, from address, and nonce.
            **kwargs: Additional keyword arguments passed to contract_func.

        Returns:
            List of transaction hashes (hex strings) in the same order as `users`.

        Performance:
            - Nonce assignment is sequential per user.
            - Transaction signing is done locally.
            - Raw transmission is parallelized with a thread pool.
        """
        shard_name = f"shard_{shard_id}"
        web3 = self.network.get_web3(shard_name)

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
            tx_params = contract_func(web3, address, nonce, **kwargs)

            # Fill mandatory fields
            tx_params.setdefault("gas", GAS_LIMIT)
            tx_params.setdefault("gasPrice", DEFAULT_GAS_PRICE)
            tx_params.setdefault("nonce", nonce)
            tx_params.setdefault("chainId", 1)  # Ganache ignores, but safe

            # Remove any extra fields that Web3 might reject
            filtered = {k: v for k, v in tx_params.items() if v is not None}

            # Sign locally
            signed = account.sign_transaction(filtered)
            raw_txs.append(signed.raw_transaction)

        # 2. Send raw transactions in parallel, preserving order
        tx_hashes: t.List[str] = []
        with ThreadPoolExecutor(max_workers=min(len(raw_txs), 10)) as executor:
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
                    print(f"Failed to send transaction: {e}")
                    tx_hashes.append("")  # placeholder for ordering

        return tx_hashes