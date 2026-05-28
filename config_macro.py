"""
Phase 4 宏基准测试配置。
与Phase 3严格分离 - 为高负载测试覆盖全局常量。
"""
from config_global import *

# === 宏基准测试约束 ===
GAS_LIMIT = 6_000_000  # 严格的瓶颈限制
BLOCK_TIME = 12  # 区块时间（秒）
MACRO_TX_TIMEOUT = 120  # 完整生命周期需要时间
MACRO_TX_GAS_LIMIT = 500_000  # 单笔交易限制 - 解决资金不足问题
MACRO_GAS_PRICE = 1_000_000_000  # 1 Gwei - 低廉价格

# === 负载参数 ===
MACRO_DURATION = 120  # 持续时间（秒）
MACRO_WARMUP = 10  # 预热时间（秒）
# 高负载级别：50, 100, 150并发
MACRO_CONCURRENCY_LEVELS = [50, 100, 150]
MACRO_OPS_PER_JOURNEY = 5  # 每次旅程的操作数

# === 流量控制 ===
MACRO_TX_INTERVAL = 0.05  # 快速发射间隔

# === 覆盖NUM_USERS以适应宏规模（需要至少最大并发+余量）===
# 使用151个用户（0-150）以匹配预置存款循环
NUM_USERS = 200  # 必须大于最大MACRO_CONCURRENCY_LEVELS (150)

# === 覆盖拓扑？保持与全局相同（2个分片 + 执行节点 + 基准节点）===
# 使用config_global中的get_topology()