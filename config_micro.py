"""
Configuration module for Micro‑Benchmark Experiment.
Extends global infrastructure with experiment‑specific physics.
"""
from config_global import *

# --- Micro-Benchmark Specifics ---
MICRO_BENCHMARK_ITERATIONS: int = 20

# Physics Assumptions (Latency & Jitter)
NETWORK_LATENCY_MEAN: float = 0.5
NETWORK_LATENCY_STD: float = 0.2
NETWORK_LATENCY_MIN: float = 0.05

USER_JITTER_MIN: float = 2.0
USER_JITTER_MAX: float = 14.0