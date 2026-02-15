#!/usr/bin/env python3
import subprocess
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web3 import Web3
from eth_account import Account
Account.enable_unaudited_hdwallet_features()

# Kill existing
subprocess.run(["pkill", "-9", "anvil"], stderr=subprocess.DEVNULL)
time.sleep(2)

# Start anvil with code-size-limit=0
cmd = ["anvil", "--port=8555", "--host=127.0.0.1", "--block-time=12", "--mnemonic=myth like bonus scare over problem client lizard pioneer submit female collect", "--accounts=1", "--balance=1000", "--gas-limit=30000000", "--code-size-limit=0", "--chain-id=31337"]
print("Starting:", " ".join(cmd))
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(3)

w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8555"))
print("Connected:", w3.is_connected())

# Get account
account = Account.from_mnemonic("myth like bonus scare over problem client lizard pioneer submit female collect", account_path="m/44'/60'/0'/0/0")
print("Account:", account.address)

# Deploy DASEndpoint
import solcx
solcx.set_solc_version("0.8.24")
compiled = solcx.compile_files(["contracts/DASEndpoint.sol"], output_values=["abi", "bin"])
contract_data = list(compiled.values())[0]
abi = contract_data["abi"]
bytecode = contract_data["bin"]
print(f"Bytecode length: {len(bytecode)}")

# Try estimate gas
contract = w3.eth.contract(abi=abi, bytecode=bytecode)
try:
    estimated = contract.constructor().estimate_gas({'from': account.address})
    print(f"Estimated gas: {estimated}")
except Exception as e:
    print(f"Estimate error: {e}")
    # Maybe still deploy
    estimated = 3_000_000

nonce = w3.eth.get_transaction_count(account.address, "pending")
print("Nonce:", nonce)
tx = contract.constructor().build_transaction({
    "from": account.address,
    "gas": estimated + 100000,
    "gasPrice": 50_000_000_000,
    "nonce": nonce,
    "chainId": 31337,
})
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"Tx hash: {tx_hash.hex()}")

# Wait for receipt
print("Waiting...")
for i in range(30):
    time.sleep(1)
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if receipt is not None:
            print(f"Receipt at block {receipt.blockNumber}, contract {receipt.contractAddress}")
            break
    except:
        pass
    print(f"  Block {w3.eth.block_number}")
else:
    print("Timeout")

proc.terminate()
proc.wait()
print("Done")