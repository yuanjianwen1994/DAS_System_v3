#!/usr/bin/env python3
import subprocess
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web3 import Web3
from eth_account import Account
Account.enable_unaudited_hdwallet_features()

# Kill existing anvil
subprocess.run(["pkill", "-9", "anvil"], stderr=subprocess.DEVNULL)
time.sleep(2)

# Start anvil with block-time 12
cmd = ["anvil", "--port=8545", "--host=127.0.0.1", "--block-time=12", "--mnemonic=myth like bonus scare over problem client lizard pioneer submit female collect", "--accounts=1", "--balance=1000", "--chain-id=31337"]
print("Starting:", " ".join(cmd))
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(3)

w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8545"))
print("Connected:", w3.is_connected())
print("Block number:", w3.eth.block_number)
print("Gas price:", w3.eth.gas_price)

# Wait for a few seconds and see if block advances
for i in range(30):
    print(f"After {i}s: block {w3.eth.block_number}")
    time.sleep(1)

# Send a simple transaction
account = Account.from_mnemonic("myth like bonus scare over problem client lizard pioneer submit female collect", account_path="m/44'/60'/0'/0/0")
nonce = w3.eth.get_transaction_count(account.address, "pending")
tx = {
    "from": account.address,
    "to": account.address,
    "value": 0,
    "gas": 21000,
    "gasPrice": w3.eth.gas_price,
    "nonce": nonce,
    "chainId": 31337,
}
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print("Sent tx:", tx_hash.hex())

# Wait for receipt
print("Waiting for receipt...")
for i in range(30):
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if receipt is not None:
            print(f"Receipt at block {receipt.blockNumber}")
            break
    except:
        pass
    print(f"  {i}s: block {w3.eth.block_number}")
    time.sleep(1)
else:
    print("No receipt after 30 seconds")

# Kill
proc.terminate()
proc.wait()
print("Done")