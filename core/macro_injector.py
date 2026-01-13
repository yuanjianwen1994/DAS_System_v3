"""
Macro-Benchmark Injector.
Handles high-throughput Legacy transactions with explicit node mapping.
"""
import typing as t
import time
from web3 import Web3
from web3.types import TxParams
from eth_account.signers.local import LocalAccount
from concurrent.futures import ThreadPoolExecutor

from config_macro import GAS_LIMIT, MACRO_TX_GAS_LIMIT, MACRO_GAS_PRICE

class MacroTransactionInjector:
    def __init__(self, network_manager, identity_manager):
        self.network = network_manager
        self.identity = identity_manager
        # Use a thread pool for non-blocking broadcasting
        self.executor = ThreadPoolExecutor(max_workers=20)

    def send_batch(
        self,
        shard_id: int,
        users: t.List[int],
        contract_func: t.Callable[..., TxParams],
        **kwargs,
    ) -> t.List[str]:
        """
        Builds, signs, and sends transactions for the given users.
        """
        # 1. FIX: Explicit Node Mapping
        if shard_id == -1:
            node_name = "execution"
        else:
            node_name = f"shard_{shard_id}"

        try:
            web3 = self.network.get_web3(node_name)
        except Exception:
            # Fallback or re-raise with clear error
            raise ValueError(f"Unknown node for shard_id={shard_id} (mapped to {node_name})")

        tx_hashes = []

        for user_idx in users:
            account: LocalAccount = self.identity.get_user(user_idx)
            # Manage nonce locally or via web3 (for macro, web3.eth.get_transaction_count is safer but slower)
            # Optimization: Let's trust the network for the nonce to avoid collisions in long runs
            nonce = web3.eth.get_transaction_count(account.address, "pending")

            # Build params
            # Pass kwargs to the builder (e.g. amount, tpc_id)
            tx_params = contract_func(web3, account.address, nonce, **kwargs)

            # 2. FIX: Sanitize EIP-1559 fields (Force Legacy)
            if "maxFeePerGas" in tx_params:
                del tx_params["maxFeePerGas"]
            if "maxPriorityFeePerGas" in tx_params:
                del tx_params["maxPriorityFeePerGas"]

            # 3. FIX: Apply Macro Gas Limits (Prevent Insufficient Funds)
            # Use the smaller TX limit, not the Block limit
            tx_params["gas"] = MACRO_TX_GAS_LIMIT 
            tx_params["gasPrice"] = MACRO_GAS_PRICE
            tx_params["chainId"] = 1  # Ganache default

            # Sign
            signed_tx = account.sign_transaction(tx_params)
            
            # Send (Blocking or Async)
            # We use blocking here to return the hash reliably for the waiter
            tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hashes.append(web3.to_hex(tx_hash))

        return tx_hashes