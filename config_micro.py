"""
微基准测试配置模块。
在全局基础设施上扩展实验特定的物理参数。
"""
from config_global import *

# === 微基准测试特定参数 ===
MICRO_BENCHMARK_ITERATIONS: int = 20  # 迭代次数

# === 物理假设（延迟与抖动）===
# Log-normal 网络延迟参数
# 使用 random.lognormvariate(mu, sigma)，其中 mu/sigma 是底层正态分布的参数
# 期望值 E[X] = exp(mu + sigma²/2)，mu=-1.0, sigma=0.8 → 均值≈0.507
NETWORK_LATENCY_MU: float = -1.0       # 底层正态分布均值
NETWORK_LATENCY_SIGMA: float = 0.8     # 底层正态分布标准差
NETWORK_LATENCY_MIN: float = 0.05      # 下限 50ms
NETWORK_LATENCY_MAX: float = 4.0       # 上限 4 秒

# 用户思维时间抖动范围（秒）
USER_JITTER_MIN: float = 2.0  # 用户思考时间最小值
USER_JITTER_MAX: float = 14.0  # 用户思考时间最大值