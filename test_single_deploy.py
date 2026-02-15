#!/usr/bin/env python3
import subprocess
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web3 import Web3
from eth_account import Account

# Kill existing anvil
subprocess.run(["pkill", "-9", "anvil"], stderr=subprocess.DEVNULL)
time.sleep(2)

# Start single anvil
anvil = subprocess.Popen(
    ["anvil", "--port=8545", "--host=127.0.0.1", "--block-time=1", "--mnemonic=myth like bonus scare over problem client lizard pioneer submit female collect", "--accounts=1", "--balance=1000", "--chain-id=31337"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)
print("Anvil started, pid:", anvil.pid)
time.sleep(3)

# Connect
w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))
print("Connected:", w3.is_connected())
print("Block number:", w3.eth.block_number)

# Get first account
account = Account.from_mnemonic("myth like bonus scare over problem client lizard pioneer submit female collect", account_path="m/44'/60'/0'/0/0")
print("Account:", account.address)
print("Balance:", w3.eth.get_balance(account.address))

# Deploy a simple contract
simple_contract_bytecode = "0x6080604052348015600f57600080fd5b50603f80601d6000396000f3fe6080604052600080fdfea2646970667358221220c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c5a2a2c64736f6c63430008180033"
simple_contract_abi = []  # empty ABI for simplicity

nonce = w3.eth.get_transaction_count(account.address, "pending")
print("Nonce:", nonce)
gas_price = w3.eth.gas_price
print("Gas price:", gas_price)
tx = {
    "from": account.address,
    "gas": 2000000,
    "gasPrice": gas_price,
    "nonce": nonce,
    "chainId": 31337,
    "data": simple_contract_bytecode,
}
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print("Sent tx:", tx_hash.hex())

# Wait for receipt
print("Waiting for receipt...")
try:
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    print("Receipt:", receipt)
    print("Block:", receipt.blockNumber)
except Exception as e:
    print("Error waiting:", e)
    # Check pending transactions
    try:
        pending = w3.eth.get_transaction(tx_hash)
        print("Transaction pending:", pending)
    except Exception as e2:
        print("Cannot get transaction:", e2)

# Kill anvil
anvil.terminate()
anvil.wait()
print("Test done.")