"""
宏基准测试注入器。
处理具有显式节点映射的高吞吐量Legacy交易。
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
        # 使用线程池进行非阻塞广播
        self.executor = ThreadPoolExecutor(max_workers=20)

    def send_batch(
        self,
        shard_id: int,
        users: t.List[int],
        contract_func: t.Callable[..., TxParams],
        **kwargs,
    ) -> t.List[str]:
        """
        为给定用户构建、签名和发送交易。
        """
        # 1. 节点映射
        if shard_id == -1:
            node_name = "execution"
        else:
            node_name = f"shard_{shard_id}"

        try:
            web3 = self.network.get_web3(node_name)
            # 修复：动态获取Chain ID以防止"无效签名v值"错误
            chain_id = web3.eth.chain_id
        except Exception as e:
            raise ValueError(f"无法连接到shard_id={shard_id}的节点（{node_name}）：{e}")

        gas_price = kwargs.pop('gas_price', MACRO_GAS_PRICE)
        tx_hashes = []

        for user_idx in users:
            account: LocalAccount = self.identity.get_user(user_idx)
            # 优化：每次在批处理中获取nonce可能很慢，
            # 但它确保了正确性。
            nonce = web3.eth.get_transaction_count(account.address, "pending")

            # 2. 将shard_id重新注入kwargs用于构建函数
            builder_args = kwargs.copy()
            builder_args["shard_id"] = shard_id

            # 构建参数
            tx_params = contract_func(web3, account.address, nonce, **builder_args)

            # 3. 清理EIP-1559字段（强制Legacy）
            if "maxFeePerGas" in tx_params:
                del tx_params["maxFeePerGas"]
            if "maxPriorityFeePerGas" in tx_params:
                del tx_params["maxPriorityFeePerGas"]

            # 4. 应用宏Gas限制和动态Chain ID
            tx_params["gas"] = MACRO_TX_GAS_LIMIT
            tx_params["gasPrice"] = gas_price
            tx_params["chainId"] = chain_id  # 修复在这里

            # 签名
            signed_tx = account.sign_transaction(tx_params)
            
            # 发送
            tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hashes.append(web3.to_hex(tx_hash))

        return tx_hashes