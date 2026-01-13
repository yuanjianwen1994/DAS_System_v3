"""
Macro-Benchmark Injector.
Handles high-throughput Legacy transactions with explicit node mapping.
"""
import typing as t
from web3 import Web3
from web3.types import TxParams
from eth_account.signers.local import LocalAccount
from concurrent.futures import ThreadPoolExecutor

from config_macro import MACRO_TX_GAS_LIMIT, MACRO_GAS_PRICE

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
        # 1. Node Mapping
        if shard_id == -1:
            node_name = "execution"
        else:
            node_name = f"shard_{shard_id}"

        try:
            web3 = self.network.get_web3(node_name)
            # FIX: Dynamically fetch Chain ID to prevent "Invalid signature v value" error
            chain_id = web3.eth.chain_id
        except Exception as e:
            raise ValueError(f"Failed to connect to node for shard_id={shard_id} ({node_name}): {e}")

        tx_hashes = []

        for user_idx in users:
            account: LocalAccount = self.identity.get_user(user_idx)
            # Optimization: Getting nonce for every tx in a batch might be slow, 
            # but it ensures correctness.
            nonce = web3.eth.get_transaction_count(account.address, "pending")

            # 2. Re-inject shard_id into kwargs for the builder function
            builder_args = kwargs.copy()
            builder_args["shard_id"] = shard_id

            # Build params
            tx_params = contract_func(web3, account.address, nonce, **builder_args)

            # 3. Sanitize EIP-1559 fields (Force Legacy)
            if "maxFeePerGas" in tx_params:
                del tx_params["maxFeePerGas"]
            if "maxPriorityFeePerGas" in tx_params:
                del tx_params["maxPriorityFeePerGas"]

            # 4. Apply Macro Gas Limits & Dynamic Chain ID
            tx_params["gas"] = MACRO_TX_GAS_LIMIT 
            tx_params["gasPrice"] = MACRO_GAS_PRICE
            tx_params["chainId"] = chain_id  # <--- FIX HERE

            # Sign
            signed_tx = account.sign_transaction(tx_params)
            
            # Send
            tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hashes.append(web3.to_hex(tx_hash))

        return tx_hashes