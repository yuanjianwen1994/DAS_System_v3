"""
Contract deployment for DAS System v3.
Compiles Solidity contracts and deploys them across the topology.
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
    Manages compilation and deployment of the experiment contracts.
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
            web3: Web3, account, contract_name: str, args=None
        ) -> t.Tuple[Contract, str]:
            data = self._compiled[contract_name]
            contract = web3.eth.contract(
                abi=data["abi"], bytecode=data["bytecode"]
            )
            # === THE ONLY FIX NEEDED ===
            # Fetch fresh nonce from chain (handling restart/reset state)
            # "pending" ensures we count txs currently in mempool
            nonce = web3.eth.get_transaction_count(account.address, "pending")

            # === RESTORED ROBUST LOGIC ===
            # Use auto-detected gas price (works perfectly with Anvil)
            # Let Web3 handle ChainID implicitly
            gas_price = web3.eth.gas_price
            chain_id = web3.eth.chain_id  # keep for logging
            print(f"[DEPLOY] Deploying {contract_name} on chain {chain_id}, nonce {nonce}, gas price {gas_price}")
            print(f"[DEPLOY] Account: {account.address}, balance: {web3.eth.get_balance(account.address)}")
            # Estimate gas for deployment
            try:
                estimated = contract.constructor(*(args or ())).estimate_gas({'from': account.address})
                print(f"[DEPLOY] Estimated gas: {estimated}")
                gas = min(estimated + 100000, GAS_LIMIT)  # Add a buffer but cap at global limit
            except Exception as e:
                print(f"[DEPLOY] Gas estimation failed, using default {GAS_LIMIT}: {e}")
                gas = GAS_LIMIT
            # Ensure gas does not exceed block gas limit (anvil's limit)
            block_gas_limit = web3.eth.get_block('latest').gasLimit
            if gas > block_gas_limit:
                print(f"[DEPLOY] Gas {gas} exceeds block gas limit {block_gas_limit}, adjusting")
                gas = block_gas_limit - 100000
            print(f"[DEPLOY] Using gas: {gas}, block gas limit: {block_gas_limit}")
            tx = contract.constructor(*(args or ())).build_transaction({
                "from": account.address,
                "gas": gas,
                "gasPrice": gas_price,
                "nonce": nonce,
                # "chainId": ... (Removed, let Web3 auto-detect)
                # "type": ...    (Removed)
            })
            signed = account.sign_transaction(tx)
            tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"[DEPLOY] Sent tx {tx_hash.hex()}, waiting for receipt...")
            # Immediately try to fetch the transaction to verify it's in pool
            try:
                pending = web3.eth.get_transaction(tx_hash)
                print(f"[DEPLOY] Transaction in pool: nonce {pending.nonce}, gas {pending.gas}, gasPrice {pending.gasPrice}")
            except Exception as e:
                print(f"[DEPLOY] Could not get transaction from pool: {e}")
            # Wait for receipt with longer timeout (300 seconds) for 12s block time
            # First, poll for block progression
            start_block = web3.eth.block_number
            print(f"[DEPLOY] Current block: {start_block}")
            for i in range(30):
                time.sleep(1)
                current = web3.eth.block_number
                if current > start_block:
                    print(f"[DEPLOY] Block advanced to {current}")
                    start_block = current
                # Check if transaction is already mined
                try:
                    receipt = web3.eth.get_transaction_receipt(tx_hash)
                    if receipt is not None:
                        print(f"[DEPLOY] Receipt found via polling: block {receipt.blockNumber}")
                        break
                except:
                    pass
            else:
                # If not mined after 30 seconds, try to force a block
                print("[DEPLOY] Transaction not mined after 30s, forcing a block via evm_mine...")
                try:
                    web3.provider.make_request('evm_mine', [])
                except Exception as e:
                    print(f"[DEPLOY] evm_mine failed: {e}")
            receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
            print(f"[DEPLOY] Receipt received: block {receipt.blockNumber}, contract {receipt.contractAddress}")
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