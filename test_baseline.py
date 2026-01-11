#!/usr/bin/env python3
"""
Quick test to measure baseline node latency after fix.
"""
import time
from web3 import Web3
from config_global import get_topology, BLOCK_TIME, MNEMONIC
from core.network import GanacheManager, ConnectionManager
from core.identity import UserManager

def main():
    print("Testing baseline node latency...")
    print(f"BLOCK_TIME = {BLOCK_TIME}")
    topology = {
        "baseline": {"port": 9999, "type": "single_chain"}
    }
    print(f"Topology: {topology}")
    
    # Start network
    ganache = GanacheManager()
    print("Starting network...")
    ganache.start_network(topology)
    time.sleep(2)
    
    # Connect
    network = ConnectionManager(topology)
    web3 = network.get_web3("baseline")
    print(f"Connected to baseline node: {web3.is_connected()}")
    
    # Get user account
    identity = UserManager(MNEMONIC)
    user_account = identity.get_user(0)  # first account
    user_address = user_account.address
    print(f"Using account: {user_address}")
    
    # Send a simple transaction (transfer 0 ETH to self)
    nonce = web3.eth.get_transaction_count(user_address)
    tx = {
        'from': user_address,
        'to': user_address,
        'value': 0,
        'gas': 21000,
        'gasPrice': web3.to_wei(20, 'gwei'),
        'nonce': nonce,
    }
    signed = user_account.sign_transaction(tx)
    start_time = time.time()
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"Transaction sent: {tx_hash.hex()}")
    
    # Wait for receipt
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    end_time = time.time()
    latency = end_time - start_time
    print(f"Transaction mined in block {receipt.blockNumber}")
    print(f"Latency: {latency:.2f} seconds")
    
    # Verify block time
    block = web3.eth.get_block(receipt.blockNumber)
    print(f"Block timestamp: {block.timestamp}")
    
    # Stop network
    print("Stopping network...")
    ganache.stop_network()
    
    # Evaluate
    expected_min = BLOCK_TIME * 0.5  # rough lower bound
    expected_max = BLOCK_TIME * 1.5  # rough upper bound
    if expected_min <= latency <= expected_max:
        print(f"✅ Latency within expected range ({expected_min:.1f}-{expected_max:.1f}s)")
    else:
        print(f"⚠️  Latency outside expected range ({expected_min:.1f}-{expected_max:.1f}s)")
        print("   Baseline may still be in instant mining mode.")

if __name__ == "__main__":
    main()