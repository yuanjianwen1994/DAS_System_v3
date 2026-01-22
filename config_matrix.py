"""
Matrix‑benchmark configuration for Phase 4 (Top‑Tier Paper quality).
Strictly for Task‑Based experiments with varying concurrency (N) and amortization factor (q).
"""
from config_global import *

# === Matrix Experiment Parameters ===
# N: Concurrency Levels (Users)
MATRIX_CONCURRENCY_LEVELS = [100, 200, 300, 400, 500]

# q: Amortization Factors (Ops per Journey)
MATRIX_AMORTIZATION_FACTORS = [5]

# n: Journeys per User (Target for Task‑Based Mode)
# Each user must complete this many journeys before the experiment ends.
MATRIX_JOURNEYS_PER_USER = 5

# === Multiprocessing Config ===
# Number of CPU cores to use for traffic generation
# User has 20 cores, using 16 for traffic, leaving 4 for System/Ganache
MATRIX_PROCESSES = 16

# === Experiment Scenarios ===
# Matrix now iterates over these Journey Types too
MATRIX_SCENARIOS = ["2PC","BASELINE"]

# === Gas & Network Limits (Same as Macro) ===
GAS_LIMIT = 6_000_000
MACRO_TX_GAS_LIMIT = 500_000
MACRO_GAS_PRICE = 1_000_000_000  # 1 Gwei
MACRO_TX_TIMEOUT = 600
MACRO_WARMUP = 10
MACRO_TX_INTERVAL = 0.05

# Override NUM_USERS to be at least max concurrency + margin
NUM_USERS = 1200  # Must be greater than the maximum MATRIX_CONCURRENCY_LEVELS (1600)

# === NEW: Simulation Parameters ===
# Random delay between operations (min, max) in seconds
# Simulates "User Think Time" and Network Jitter
SIM_THINK_TIME_RANGE = (0.5, 2.0)

# HTTP Retry Settings (Fixes Connection Aborted)
HTTP_RETRIES = 5
HTTP_BACKOFF_FACTOR = 0.5

# Use the same topology as global (2 shards + execution + baseline)
# get_topology() is imported from config_global