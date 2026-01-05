#!/usr/bin/env python3
"""
Quick test for the updated GanacheManager.start_network.
"""
import sys
sys.path.insert(0, '.')

from core.network import GanacheManager

def test_start():
    manager = GanacheManager()
    # Use a topology with a single shard on a high port to avoid conflicts
    topology = {
        "shards": {
            "test_shard": {"port": 18545, "type": "settlement", "id": 0}
        },
        "execution": {"port": 18546, "type": "execution"},
        "baseline": {"port": 18547, "type": "single_chain"}
    }
    try:
        print("Testing start_network...")
        manager.start_network(topology)
        print("Network started successfully")
        # Wait a moment to see if processes stay alive
        import time
        time.sleep(1)
        for name, proc in manager.processes.items():
            if proc.poll() is None:
                print(f"   {name} is running (PID {proc.pid})")
            else:
                print(f"   {name} died unexpectedly")
        # Stop network
        manager.stop_network()
        print("Network stopped")
        # Check log files
        import os
        if os.path.exists("logs"):
            logs = os.listdir("logs")
            print(f"Log files created: {logs}")
        else:
            print("No logs directory created")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_start()