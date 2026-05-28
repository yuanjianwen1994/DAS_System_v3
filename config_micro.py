"""
微基准测试配置模块。
在全局基础设施上扩展实验特定的物理参数。
"""
from config_global import *

# === 微基准测试特定参数 ===
MICRO_BENCHMARK_ITERATIONS: int = 20  # 迭代次数

# === 物理假设（延迟与抖动）===
# 网络延迟参数（用于模拟真实的网络条件）
# 使用高斯分布模拟：均值=0.5秒，标准差=0.2秒
# 关键参数位置：第11-13行定义网络延迟模型
NETWORK_LATENCY_MEAN: float = 0.5  # 网络延迟均值（秒）
NETWORK_LATENCY_STD: float = 0.2  # 网络延迟标准差（秒）
NETWORK_LATENCY_MIN: float = 0.05  # 最小延迟下限（秒），防止负值

# 用户思维时间抖动范围（秒）
USER_JITTER_MIN: float = 2.0  # 用户思考时间最小值
USER_JITTER_MAX: float = 14.0  # 用户思考时间最大值