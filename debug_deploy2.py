#!/usr/bin/env python3
import subprocess
import time
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web3 import Web3
from eth_account import Account
from core.identity import UserManager
from config_global import MNEMONIC, DEPLOYER_ACCOUNT_INDEX

Account.enable_unaudited_hdwallet_features()

# Kill anvil
subprocess.run(["pkill", "-9", "anvil"], stderr=subprocess.DEVNULL)
time.sleep(2)

# Start single anvil on port 8580 (shard_0)
cmd = ["anvil", "--port=8580", "--host=127.0.0.1", "--block-time=12", f"--mnemonic={MNEMONIC}", "--accounts=6000", "--balance=10000", "--gas-limit=30000000", "--chain-id=31337"]
print("Starting:", " ".join(cmd))
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(3)

w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8580"))
print("Connected:", w3.is_connected())
print("Block:", w3.eth.block_number)

# Get deployer account
identity = UserManager(MNEMONIC)
account = identity.get_user(DEPLOYER_ACCOUNT_INDEX)
print("Deployer:", account.address)
print("Balance:", w3.eth.get_balance(account.address))

# Compile DASEndpoint
import solcx
solcx.set_solc_version("0.8.24")
contract_path = os.path.join(os.path.dirname(__file__), "contracts/DASEndpoint.sol")
compiled = solcx.compile_files([contract_path], output_values=["abi", "bin"], solc_version="0.8.24")
contract_name = list(compiled.keys())[0]
data = compiled[contract_name]
abi = data["abi"]
bytecode = data["bin"]
print("Compiled, bytecode length:", len(bytecode))

# Build contract
contract = w3.eth.contract(abi=abi, bytecode=bytecode)
nonce = w3.eth.get_transaction_count(account.address, "pending")
gas_price = 50_000_000_000  # 50 Gwei
print(f"Nonce: {nonce}, gas price: {gas_price}")
tx = contract.constructor().build_transaction({
    "from": account.address,
    "gas": 30_000_000,
    "gasPrice": gas_price,
    "nonce": nonce,
    "chainId": 31337
})
print("Transaction built")

# Sign and send
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"Tx hash: {tx_hash.hex()}")

# Immediately check transaction
try:
    pending_tx = w3.eth.get_transaction(tx_hash)
    print(f"Transaction retrieved: nonce {pending_tx.nonce}, gas {pending_tx.gas}, gasPrice {pending_tx.gasPrice}")
except Exception as e:
    print(f"Cannot get transaction: {e}")

# Wait for receipt with polling
print("Waiting for receipt...")
start = time.time()
while time.time() - start < 30:
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if receipt is not None:
            print(f"Receipt received at block {receipt.blockNumber}")
            print(f"Contract address: {receipt.contractAddress}")
            break
    except Exception as e:
        pass
    print(f"  Block {w3.eth.block_number}")
    time.sleep(1)
else:
    print("Timeout after 30 seconds")
    # Try to see if block advanced
    print(f"Final block: {w3.eth.block_number}")

# Kill
proc.terminate()
proc.wait()
print("Done")