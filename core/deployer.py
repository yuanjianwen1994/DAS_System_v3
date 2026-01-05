"""
Contract deployment for DAS System v3.
Compiles Solidity contracts and deploys them across the topology.
"""
import json
import os
import typing as t
from pathlib import Path

import solcx
from web3 import Web3
from web3.contract import Contract

from .identity import UserManager
from .network import ConnectionManager
from config import DEPLOYER_ACCOUNT_INDEX, TEST_USER_INDEX, GAS_LIMIT


class ContractDeployer:
    """
    Manages compilation and deployment of the experiment contracts.
    """

    SOLC_VERSION = "0.8.21"

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
        Compile all Solidity contracts in the contracts/ directory.

        Returns:
            Dictionary with contract names as keys and ABI/bytecode as values.
        """
        contract_dir = Path(__file__).parent.parent / "contracts"
        source_files = [
            contract_dir / "DASEndpoint.sol",
            contract_dir / "TwoPhaseCommit.sol",
            contract_dir / "Workload.sol",
        ]
        # Ensure solc is installed
        installed = solcx.get_installed_solc_versions()
        if self.SOLC_VERSION not in installed:
            solcx.install_solc(self.SOLC_VERSION)
        solcx.set_solc_version(self.SOLC_VERSION)
        # Compile
        compiled = solcx.compile_files(
            [str(f) for f in source_files],
            output_values=["abi", "bin"],
            solc_version=self.SOLC_VERSION,
        )
        # Simplify the structure
        result = {}
        for contract_name, data in compiled.items():
            # contract_name format: "path/to/file.sol:ContractName"
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
        Deploy contracts to all nodes in the topology.

        Returns:
            Registry dict:
            {
                "shard_0": {"DAS": address, "2PC": address, "ABI": ...},
                "execution": {"DAS": address, "Workload": address, ...},
                "baseline": {"Workload": address, ...},
            }
        """
        if self._compiled is None:
            self.compile_all()

        registry: t.Dict[str, t.Dict[str, t.Any]] = {}

        # Helper to deploy a single contract
        def deploy_contract(
            web3: Web3, account, contract_name: str
        ) -> t.Tuple[Contract, str]:
            data = self._compiled[contract_name]
            contract = web3.eth.contract(
                abi=data["abi"], bytecode=data["bytecode"]
            )
            # Build transaction
            nonce = web3.eth.get_transaction_count(account.address)
            tx = contract.constructor().build_transaction({
                "from": account.address,
                "gas": GAS_LIMIT,
                "gasPrice": web3.to_wei(20, "gwei"),
                "nonce": nonce,
            })
            signed = account.sign_transaction(tx)
            tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
            # Wait for receipt
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
            deployed = web3.eth.contract(
                address=receipt.contractAddress, abi=data["abi"]
            )
            return deployed, receipt.contractAddress

        # Use dedicated deployer account
        deployer_account = self.identity.get_user(DEPLOYER_ACCOUNT_INDEX)

        # Deploy to each shard
        shards = topology.get("shards", {})
        for shard_name, shard_cfg in shards.items():
            web3 = self.network.get_web3(shard_name)
            # Deploy DASEndpoint and TwoPhaseCommit
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

        # Deploy to execution node
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

        # Deploy to baseline node
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