"""
Phase 4 矩阵基准测试配置（顶级论文质量）。
专门用于任务型实验，变量包括并发数(N)和分摊因子(q)。
"""
from config_global import *

# === 矩阵实验参数 ===
# N: 并发级别（用户数量）
MATRIX_CONCURRENCY_LEVELS = [100]

# q: 分摊因子（每次旅程的操作数）
MATRIX_AMORTIZATION_FACTORS = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

# n: 每个用户的旅程数（任务型模式的目标）
# 每个用户必须在此实验结束前完成这么多旅程
MATRIX_JOURNEYS_PER_USER = 5

# === 多进程配置 ===
# 用于流量生成的CPU核心数
# 用户有20核，使用16核进行流量生成，留4核给系统/Ganache
MATRIX_PROCESSES = 20

# === 实验场景 ===
# 矩阵现在也迭代这些旅程类型 ["DAS", "2PC", "BASELINE"]
# TELEPORT / ATOMIC 为 R1 要求的 SOTA 对照（Teleport 式 burn-mint / Avalanche 式 export-import）
MATRIX_SCENARIOS = ["DAS", "2PC", "BASELINE", "TELEPORT", "ATOMIC"]

# === Gas与网络限制（与宏基准测试相同）===
GAS_LIMIT = 6_000_000
MACRO_TX_GAS_LIMIT = 500_000
MACRO_GAS_PRICE = 50_000_000_000  # 50 Gwei
MACRO_TX_TIMEOUT = 600
MACRO_WARMUP = 10
MACRO_TX_INTERVAL = 0.05

# === 覆盖NUM_USERS至少为最大并发+余量 ===
NUM_USERS = 1200  # 必须大于最大MATRIX_CONCURRENCY_LEVELS (1600)

# === 新增：模拟参数 ===
# 操作间的随机延迟（最小，最大）单位：秒
# 用于模拟"用户思考时间"和网络抖动
SIM_THINK_TIME_RANGE = (0.5, 2.0)

# === HTTP重试设置（修复连接中止问题）===
HTTP_RETRIES = 5  # 最大重试次数
HTTP_BACKOFF_FACTOR = 0.5  # 退避因子

# === 使用与全局相同的拓扑（2个分片 + 执行节点 + 基准节点）===
# get_topology()从config_global导入

# === Log-normal 网络延迟参数（与 config_micro 保持一致）===
# 使用 random.lognormvariate(mu, sigma)，mu=-1.0, sigma=0.8 → 均值≈0.507
NETWORK_LATENCY_MU: float = -1.0       # 底层正态分布均值
NETWORK_LATENCY_SIGMA: float = 0.8     # 底层正态分布标准差
NETWORK_LATENCY_MIN: float = 0.05      # 下限 50ms
NETWORK_LATENCY_MAX: float = 4.0       # 上限 4 秒