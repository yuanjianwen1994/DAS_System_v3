"""
Entry point for Phase 4+ Matrix Benchmark (Task-Based).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scenarios import exp_macro_matrix

if __name__ == "__main__":
    print(">>> Starting Phase 4+ Matrix Benchmark (Task-Based)...")
    exp_macro_matrix.main()