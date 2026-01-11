"""
Configuration module for Amortized‑Cost Experiment.
Extends global infrastructure with experiment‑specific physics.
"""
from config_global import *

# --- Amortized Benchmark Specifics ---
AMORTIZED_OPS_COUNT: int = 20
AMORTIZED_ITERATIONS: int = 2

# Physics Assumptions (Can be same or different from Micro)
NETWORK_LATENCY_MEAN: float = 0.5
NETWORK_LATENCY_STD: float = 0.2
NETWORK_LATENCY_MIN: float = 0.05

USER_JITTER_MIN: float = 2.0
USER_JITTER_MAX: float = 14.0

# Additional experiment‑specific flags (optional)
AMORTIZED_ENABLE_DETAILED_LOG: bool = True  # Whether to write per‑transaction detailed CSV
AMORTIZED_JITTER_ENABLED: bool = True      # Whether to add random sleep between scenarios