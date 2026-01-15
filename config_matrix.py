"""
Matrix‑benchmark configuration for Phase 4 (Top‑Tier Paper quality).
Strictly for Task‑Based experiments with varying concurrency (N) and amortization factor (q).
"""
from config_global import *

# === Matrix Experiment Parameters ===
# N: Concurrency Levels (Users)
MATRIX_CONCURRENCY_LEVELS = [50, 100, 150]

# q: Amortization Factors (Ops per Journey)
MATRIX_AMORTIZATION_FACTORS = [1, 5, 10]

# n: Journeys per User (Target for Task‑Based Mode)
# Each user must complete this many journeys before the experiment ends.
MATRIX_JOURNEYS_PER_USER = 5

# === Gas & Network Limits (Same as Macro) ===
GAS_LIMIT = 6_000_000
MACRO_TX_GAS_LIMIT = 500_000
MACRO_GAS_PRICE = 1_000_000_000  # 1 Gwei
MACRO_TX_TIMEOUT = 180
MACRO_WARMUP = 10
MACRO_TX_INTERVAL = 0.05

# Override NUM_USERS to be at least max concurrency + margin
NUM_USERS = 200  # Must be greater than the maximum MATRIX_CONCURRENCY_LEVELS (150)

# Use the same topology as global (2 shards + execution + baseline)
# get_topology() is imported from config_global