"""
DAS System v3 交易注入引擎。
高并发 fire-and-forget 交易提交。
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
    处理原始交易提交，具有本地nonce管理。

    零RPC原则：除了`send_raw_transaction`外没有其他RPC调用。
    """

    def __init__(
        self,
        network_manager: ConnectionManager,
        identity_manager: UserManager,
    ) -> None:
        """
        参数:
            network_manager: 提供到每个分片的Web3连接。
            identity_manager: 提供确定性账户和nonce。
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
        提交一批交易到指定分片。

        参数:
            shard_id: 结算分片索引（从0开始）。
            users: 用户索引列表（由identity_manager使用）。
            contract_func: 给定Web3实例、from地址和nonce返回交易参数的可调用对象。
            **kwargs: 传递给contract_func的其他关键字参数。

        返回:
            交易哈希列表（十六进制字符串），顺序与`users`相同。

        性能:
            - Nonce分配按用户顺序进行。
            - 交易签名在本地完成。
            - 原始传输使用线程池并行化。
        """
        shard_name = f"shard_{shard_id}"
        web3 = self.network.get_web3(shard_name)

        # 确定nonce跟踪的范围（匹配send_and_wait映射）
        if shard_id >= 0:
            scope = f"shard_{shard_id}"
        elif shard_id == -1:
            scope = "execution"
        else:
            scope = "baseline"

        # 1. 按顺序构建所有交易（nonce安全）
        raw_txs: t.List[bytes] = []
        for user_idx in users:
            # 获取账户和nonce
            account = self.identity.get_user(user_idx)
            address = account.address
            nonce = self.identity.nonce_manager.get_and_increment(address, scope)

            # 从合约函数获取交易参数
            tx_params = contract_func(web3, address, nonce, **kwargs)

            # 填充必填字段
            tx_params.setdefault("gas", GAS_LIMIT)
            tx_params.setdefault("gasPrice", DEFAULT_GAS_PRICE)
            tx_params.setdefault("nonce", nonce)
            tx_params.setdefault("chainId", 1)  # Ganache忽略，但安全

            # 移除Web3可能拒绝的额外字段
            filtered = {k: v for k, v in tx_params.items() if v is not None}

            # 本地签名
            signed = account.sign_transaction(filtered)
            raw_txs.append(signed.raw_transaction)

        # 2. 并行发送原始交易，保持顺序
        tx_hashes: t.List[str] = []
        with ThreadPoolExecutor(max_workers=min(len(raw_txs), 10)) as executor:
            # 提交所有任务，在同一列表中保持futures顺序
            futures = []
            for raw in raw_txs:
                future = executor.submit(web3.eth.send_raw_transaction, raw)
                futures.append(future)

            # 按提交顺序收集结果
            for future in futures:
                try:
                    tx_hash = future.result()
                    tx_hashes.append(Web3.to_hex(tx_hash))
                except Exception as e:
                    # 记录但继续 - fire-and-forget
                    print(f"发送交易失败：{e}")
                    tx_hashes.append("")  # 占位符用于排序

        return tx_hashes