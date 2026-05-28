"""
DAS System v3 合约部署模块。
编译Solidity合约并部署到拓扑中。
"""
import json
import os
import typing as t
import time
from pathlib import Path

import solcx
from web3 import Web3
from web3.contract import Contract

from .identity import UserManager
from .network import ConnectionManager
from config_global import DEPLOYER_ACCOUNT_INDEX, TEST_USER_INDEX, GAS_LIMIT


class ContractDeployer:
    """
    管理实验合约的编译和部署。
    """

    SOLC_VERSION = "0.8.24"

    def __init__(
        self,
        network_manager: ConnectionManager,
        identity_manager: UserManager,
    ) -> None:
        self.network = network_manager
        self.identity = identity_manager
        self._compiled: t.Optional[t.Dict[str, t.Any]] = None

    def compile_all(self) -> t.Dict[str, t.Any]:
        """
        编译contracts/目录中的所有Solidity合约。

        返回:
            以合约名为键、ABI/字节码为值的字典。
        """
        contract_dir = Path(__file__).parent.parent / "contracts"
        source_files = [
            contract_dir / "DASEndpoint.sol",
            contract_dir / "TwoPhaseCommit.sol",
            contract_dir / "Workload.sol",
        ]
        # 确保solc已安装
        installed = solcx.get_installed_solc_versions()
        if self.SOLC_VERSION not in installed:
            solcx.install_solc(self.SOLC_VERSION)
        solcx.set_solc_version(self.SOLC_VERSION)
        # 编译
        compiled = solcx.compile_files(
            [str(f) for f in source_files],
            output_values=["abi", "bin"],
            solc_version=self.SOLC_VERSION,
        )
        # 简化结构
        result = {}
        for contract_name, data in compiled.items():
            # contract_name格式："path/to/file.sol:ContractName"
            simple_name = contract_name.split(":")[-1]
            result[simple_name] = {
                "abi": data["abi"],
                "bytecode": data["bin"],
            }
        self._compiled = result
        return result

    def deploy_infrastructure(
        self, topology: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Dict[str, t.Any]]:
        """
        将合约部署到拓扑中的所有节点。

        返回:
            注册表字典：
            {
                "shard_0": {"DAS": 地址, "2PC": 地址, "ABI": ...},
                "execution": {"DAS": 地址, "Workload": 地址, ...},
                "baseline": {"Workload": 地址, ...},
            }
        """
        if self._compiled is None:
            self.compile_all()

        registry: t.Dict[str, t.Dict[str, t.Any]] = {}

        # 部署单个合约的辅助函数
        def deploy_contract(
            web3: Web3, account, contract_name: str, args=None
        ) -> t.Tuple[Contract, str]:
            data = self._compiled[contract_name]
            contract = web3.eth.contract(
                abi=data["abi"], bytecode=data["bytecode"]
            )
            # === 唯一需要的修复 ===
            # 从链上获取最新nonce（处理重启/重置状态）
            # "pending"确保我们计算当前在mempool中的txs
            nonce = web3.eth.get_transaction_count(account.address, "pending")

            # === 恢复的健壮逻辑 ===
            # 使用自动检测的gas价格（与Anvil完美配合）
            # 让Web3隐式处理ChainID
            gas_price = web3.eth.gas_price
            chain_id = web3.eth.chain_id  # 保留用于日志
            print(f"[部署] 在链{chain_id}上部署{contract_name}，nonce {nonce}，gas价格 {gas_price}")
            print(f"[部署] 账户：{account.address}，余额：{web3.eth.get_balance(account.address)}")
            # 估算部署gas
            try:
                estimated = contract.constructor(*(args or ())).estimate_gas({'from': account.address})
                print(f"[部署] 估算的gas：{estimated}")
                gas = min(estimated + 100000, GAS_LIMIT)  # 添加缓冲但上限为全局限制
            except Exception as e:
                print(f"[部署] gas估算失败，使用默认{GAS_LIMIT}：{e}")
                gas = GAS_LIMIT
            # 确保gas不超过区块gas限制（anvil的限制）
            block_gas_limit = web3.eth.get_block('latest').gasLimit
            if gas > block_gas_limit:
                print(f"[部署] gas {gas}超过区块gas限制{block_gas_limit}，调整中")
                gas = block_gas_limit - 100000
            print(f"[部署] 使用gas：{gas}，区块gas限制：{block_gas_limit}")
            tx = contract.constructor(*(args or ())).build_transaction({
                "from": account.address,
                "gas": gas,
                "gasPrice": gas_price,
                "nonce": nonce,
                # "chainId": ... （已移除，让Web3自动检测）
                # "type": ...    （已移除）
            })
            signed = account.sign_transaction(tx)
            tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"[部署] 发送tx {tx_hash.hex()}，等待收据...")
            # 立即尝试获取交易以验证它在池中
            try:
                pending = web3.eth.get_transaction(tx_hash)
                print(f"[部署] 交易在池中：nonce {pending.nonce}，gas {pending.gas}，gasPrice {pending.gasPrice}")
            except Exception as e:
                print(f"[部署] 无法从池中获取交易：{e}")
            # 使用更长的超时等待收据（300秒）用于12秒区块时间
            # 首先，轮询区块进度
            start_block = web3.eth.block_number
            print(f"[部署] 当前区块：{start_block}")
            for i in range(30):
                time.sleep(1)
                current = web3.eth.block_number
                if current > start_block:
                    print(f"[部署] 区块进展到{current}")
                    start_block = current
                # 检查交易是否已挖出
                try:
                    receipt = web3.eth.get_transaction_receipt(tx_hash)
                    if receipt is not None:
                        print(f"[部署] 通过轮询找到收据：区块{receipt.blockNumber}")
                        break
                except:
                    pass
            else:
                # 如果30秒后仍未挖出，尝试强制出一个区块
                print("[部署] 交易30秒后未挖出，通过evm_mine强制出块...")
                try:
                    web3.provider.make_request('evm_mine', [])
                except Exception as e:
                    print(f"[部署] evm_mine失败：{e}")
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
            print(f"[部署] 收到收据：区块{receipt.blockNumber}，合约{receipt.contractAddress}")
            deployed = web3.eth.contract(
                address=receipt.contractAddress, abi=data["abi"]
            )
            return deployed, receipt.contractAddress

        # 使用专用部署者账户
        deployer_account = self.identity.get_user(DEPLOYER_ACCOUNT_INDEX)

        # 部署到每个分片
        shards = topology.get("shards", {})
        for shard_name, shard_cfg in shards.items():
            web3 = self.network.get_web3(shard_name)
            # 部署DASEndpoint和TwoPhaseCommit
            das_contract, das_addr = deploy_contract(
                web3, deployer_account, "DASEndpoint"
            )
            tpc_contract, tpc_addr = deploy_contract(
                web3, deployer_account, "TwoPhaseCommit"
            )
            registry[shard_name] = {
                "DAS": das_addr,
                "2PC": tpc_addr,
                "DAS_ABI": das_contract.abi,
                "2PC_ABI": tpc_contract.abi,
            }

        # 部署到执行节点
        if "execution" in topology:
            web3 = self.network.get_web3("execution")
            das_contract, das_addr = deploy_contract(
                web3, deployer_account, "DASEndpoint"
            )
            workload_contract, workload_addr = deploy_contract(
                web3, deployer_account, "Workload"
            )
            tpc_contract, tpc_addr = deploy_contract(
                web3, deployer_account, "TwoPhaseCommit"
            )
            registry["execution"] = {
                "DAS": das_addr,
                "Workload": workload_addr,
                "2PC": tpc_addr,
                "DAS_ABI": das_contract.abi,
                "Workload_ABI": workload_contract.abi,
                "2PC_ABI": tpc_contract.abi,
            }

        # 部署到基准节点
        if "baseline" in topology:
            web3 = self.network.get_web3("baseline")
            workload_contract, workload_addr = deploy_contract(
                web3, deployer_account, "Workload"
            )
            registry["baseline"] = {
                "Workload": workload_addr,
                "Workload_ABI": workload_contract.abi,
            }

        return registry