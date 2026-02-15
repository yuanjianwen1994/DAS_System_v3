#!/usr/bin/env python3
"""
Debug script to test contract deployment.
"""
import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_global import get_topology, MNEMONIC
from core.network import AnvilManager, ConnectionManager
from core.identity import UserManager
from core.deployer import ContractDeployer

def main():
    print("=== Debug Deployment ===")
    
    # Kill existing anvil processes
    print("Killing existing anvil processes...")
    import subprocess
    try:
        subprocess.call(["pkill", "-f", "anvil"], stderr=subprocess.DEVNULL)
    except:
        pass
    time.sleep(2)
    
    # Start network
    topology = get_topology()
    print("Topology:", topology)
    
    anvil = AnvilManager()
    try:
        print("Starting network...")
        anvil.start_network(topology)
        print("Network started.")
    except Exception as e:
        print(f"Error starting network: {e}")
        sys.exit(1)
    
    # Wait a bit for nodes to be ready
    time.sleep(5)
    
    # Connect
    network = ConnectionManager(topology)
    identity = UserManager(MNEMONIC)
    
    # Test connectivity
    print("Testing connectivity...")
    for node in ["shard_0", "shard_1", "execution", "baseline"]:
        try:
            w3 = network.get_web3(node)
            if w3.is_connected():
                print(f"  {node}: connected, block number = {w3.eth.block_number}")
            else:
                print(f"  {node}: NOT connected")
        except Exception as e:
            print(f"  {node}: error - {e}")
    
    # Deployer
    deployer = ContractDeployer(network, identity)
    print("\nCompiling contracts...")
    try:
        compiled = deployer.compile_all()
        print(f"Compiled {len(compiled)} contracts: {list(compiled.keys())}")
    except Exception as e:
        print(f"Compilation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\nDeploying infrastructure...")
    try:
        registry = deployer.deploy_infrastructure(topology)
        print("Deployment succeeded!")
        for node, contracts in registry.items():
            print(f"  {node}:")
            for key, val in contracts.items():
                if 'ABI' not in key:
                    print(f"    {key}: {val}")
    except Exception as e:
        print(f"Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("\nStopping network...")
        anvil.stop_network()
        print("Debug done.")

if __name__ == "__main__":
    main()