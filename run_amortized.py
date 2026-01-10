"""
Launch script for Amortized‑Cost Experiment.
"""
import sys
import os
from scenarios import exp_amortized_cost

if __name__ == "__main__":
    print("Launching Amortized‑Cost Experiment...")
    exp_amortized_cost.run()