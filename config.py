"""
Configuration module for DAS System v3.
Single source of truth for topology & constants.
"""
import typing as t

# Constants
MNEMONIC: str = "myth like bonus scare over problem client lizard pioneer submit female collect"
BLOCK_TIME: int = 12  # seconds
GAS_LIMIT: int = 30_000_000
DEFAULT_GAS_PRICE: int = 20_000_000_000  # 20 Gwei in wei (20 * 10^9)
NUM_USERS: int = 50
DEPLOYER_ACCOUNT_INDEX: int = 99  # Use the last account for admin tasks
TEST_USER_INDEX: int = 0          # Use the first account for experiments
MICRO_BENCHMARK_ITERATIONS: int = 3  # Number of repetitions for micro‑benchmark

# Topology
NUM_SETTLEMENT_SHARDS: int = 2  # changeable

def get_topology() -> t.Dict[str, t.Any]:
    """
    Generate a deterministic topology for the simulation.

    Returns:
        Dictionary describing shards, execution, and baseline nodes.
    """
    topology = {
        "shards": {},
        "execution": {"port": 9000, "type": "execution"},
        "baseline": {"port": 9999, "type": "single_chain"}
    }

    for i in range(NUM_SETTLEMENT_SHARDS):
        shard_name = f"shard_{i}"
        topology["shards"][shard_name] = {
            "port": 8580 + i,
            "type": "settlement",
            "id": i
        }

    return topology