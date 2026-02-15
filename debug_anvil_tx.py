#!/usr/bin/env python3
import subprocess
import time
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web3 import Web3
from eth_account import Account
Account.enable_unaudited_hdwallet_features()

# Kill existing
subprocess.run(["pkill", "-9", "anvil"], stderr=subprocess.DEVNULL)
time.sleep(2)

# Start anvil with stdout/stderr piped
cmd = ["anvil", "--port=8555", "--host=127.0.0.1", "--block-time=12", "--mnemonic=myth like bonus scare over problem client lizard pioneer submit female collect", "--accounts=1", "--balance=1000", "--chain-id=31337", "--code-size-limit=0"]
print("Starting anvil...")
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(3)

# Read any initial output
import select
def read_output():
    lines = []
    while True:
        ready, _, _ = select.select([proc.stdout], [], [], 0.1)
        if ready:
            line = proc.stdout.readline()
            if line:
                print("ANVIL:", line.rstrip())
                lines.append(line)
            else:
                break
        else:
            break
    return lines

read_output()

w3 = Web3(Web3.HTTPProvider("http://127.0.0.1:8555"))
print("Connected:", w3.is_connected())
print("Block:", w3.eth.block_number)
print("Gas price:", w3.eth.gas_price)

# Get account
account = Account.from_mnemonic("myth like bonus scare over problem client lizard pioneer submit female collect", account_path="m/44'/60'/0'/0/0")
print("Account:", account.address)
print("Balance:", w3.eth.get_balance(account.address))

# Simple transfer to self
nonce = w3.eth.get_transaction_count(account.address, "pending")
print("Nonce:", nonce)
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
print("Sent transfer tx:", tx_hash.hex())

# Wait for receipt
print("Waiting...")
for i in range(20):
    time.sleep(1)
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        if receipt is not None:
            print(f"Receipt at block {receipt.blockNumber}")
            break
    except:
        pass
    print(f"  Block {w3.eth.block_number}")
    read_output()
else:
    print("Transfer timeout")

# Now try contract deployment
print("\n--- Contract deployment ---")
import solcx
solcx.set_solc_version("0.8.24")
compiled = solcx.compile_files(["contracts/DASEndpoint.sol"], output_values=["abi", "bin"])
contract_data = list(compiled.values())[0]
abi = contract_data["abi"]
bytecode = contract_data["bin"]
print(f"Bytecode length: {len(bytecode)}")
contract = w3.eth.contract(abi=abi, bytecode=bytecode)
nonce = w3.eth.get_transaction_count(account.address, "pending")
print("Nonce for deploy:", nonce)
gas_price = 50_000_000_000
try:
    estimated = contract.constructor().estimate_gas({'from': account.address})
    print(f"Estimated gas: {estimated}")
    gas = estimated + 100000
except Exception as e:
    print(f"Estimate failed: {e}")
    gas = 3_000_000
print(f"Using gas: {gas}")
tx = contract.constructor().build_transaction({
    "from": account.address,
    "gas": gas,
    "gasPrice": gas_price,
    "nonce": nonce,
    "chainId": 31337,
})
signed = account.sign_transaction(tx)
tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
print(f"Deploy tx hash: {tx_hash.hex()}")

# Try to get transaction from pool
try:
    pending = w3.eth.get_transaction(tx_hash)
    print(f"Transaction in pool: gas {pending.gas}, gasPrice {pending.gasPrice}")
except Exception as e:
    print(f"Cannot get transaction: {e}")

# Wait for receipt
print("Waiting for deploy receipt...")
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
    read_output()
else:
    print("Deploy timeout")

# If not mined, try evm_mine
print("Trying evm_mine...")
try:
    w3.provider.make_request('evm_mine', [])
except Exception as e:
    print(f"evm_mine error: {e}")

# Check again
time.sleep(2)
try:
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    if receipt is not None:
        print(f"Receipt after evm_mine: block {receipt.blockNumber}")
except:
    print("Still no receipt")

proc.terminate()
proc.wait()
print("Done")