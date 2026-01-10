"""
Configuration module for Amortized‑Cost Experiment.
Extends the base config with experiment‑specific constants.
"""
from config import *

# Experiment‑specific constants
AMORTIZED_OPS_COUNT: int = 20       # Number of consecutive operations for amortized experiment

# Optionally override any other base constants here
# e.g., MICRO_BENCHMARK_ITERATIONS = 1