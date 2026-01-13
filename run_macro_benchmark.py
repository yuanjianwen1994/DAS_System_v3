#!/usr/bin/env python3
"""
Root launcher for the macro‑benchmark (Phase 4).
Imports the scenario module and calls its run() function.
"""
import sys
import os

# Ensure the current directory is in the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scenarios.exp_macro_capacity import run

if __name__ == "__main__":
    run()