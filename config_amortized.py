"""
Configuration module for Amortized‑Cost Experiment.
Extends global infrastructure with experiment‑specific physics.
"""
from config_global import *

# --- Amortized Benchmark Specifics ---
AMORTIZED_OPS_COUNT: int = 20
AMORTIZED_ITERATIONS: int = 2

# Physics Assumptions (Can be same or different from Micro)
# Log-normal 网络延迟参数
# 使用 random.lognormvariate(mu, sigma)，其中 mu/sigma 是底层正态分布的参数
# 期望值 E[X] = exp(mu + sigma²/2)，mu=-1.0, sigma=0.8 → 均值≈0.507
NETWORK_LATENCY_MU: float = -1.0       # 底层正态分布均值
NETWORK_LATENCY_SIGMA: float = 0.8     # 底层正态分布标准差
NETWORK_LATENCY_MIN: float = 0.05      # 下限 50ms
NETWORK_LATENCY_MAX: float = 4.0       # 上限 4 秒

USER_JITTER_MIN: float = 2.0
USER_JITTER_MAX: float = 14.0

# Additional experiment‑specific flags (optional)
AMORTIZED_ENABLE_DETAILED_LOG: bool = True  # Whether to write per‑transaction detailed CSV
AMORTIZED_JITTER_ENABLED: bool = True      # Whether to add random sleep between scenarios