"""
Macro‑benchmark configuration for Phase 4.
Strictly separate from Phase 3 – overrides global constants for high‑load testing.
"""
from config_global import *

# === Macro Constraints ===
GAS_LIMIT = 6_000_000       # Strict bottleneck
BLOCK_TIME = 12
MACRO_TX_TIMEOUT = 180      # High tolerance for congestion

# === Load Parameters ===
MACRO_DURATION = 120
MACRO_WARMUP = 10
MACRO_CONCURRENCY_LEVELS = [50, 100, 150]  # High load levels
MACRO_OPS_PER_JOURNEY = 5

# Traffic Control
MACRO_TX_INTERVAL = 0.05    # Fast firing

# Override NUM_USERS for macro‑scale (need at least max concurrency + margin)
# We'll use 151 users (0‑150) to match the pre‑flight deposit loop.
NUM_USERS = 151

# Override topology? Keep same as global (2 shards + execution + baseline)
# Use get_topology() from config_global