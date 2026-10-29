"""
DAS System v3 全局配置模块。
基础设施级常量和拓扑结构。
"""
import os
import typing as t

# === 基础常量 ===
MNEMONIC: str = "myth like bonus scare over problem client lizard pioneer submit female collect"
BLOCK_TIME: int = 12  # 区块时间（秒）
GAS_LIMIT: int = 30_000_000  # Gas上限
DEFAULT_GAS_PRICE: int = 20_000_000_000  # 默认Gas价格：20 Gwei (20 * 10^9)
NUM_USERS: int = 6000  # 用户数量，必须大于最大MACRO_CONCURRENCY_LEVELS (150)
DEPLOYER_ACCOUNT_INDEX: int = 99  # 使用最后一个账户执行管理员任务
TEST_USER_INDEX: int = 0  # 使用第一个账户进行实验

# === 账户余额配置（单位：wei）===
# 增加到10,000 ETH以防止压力测试时"资金不足"
ACCOUNT_BALANCE_ETH: int = 10000
ACCOUNT_BALANCE_WEI: int = ACCOUNT_BALANCE_ETH * 10**18

ACCOUNT: dict = {
    "balance_wei": ACCOUNT_BALANCE_WEI  # 10,000 ETH转换为wei
}

# === Ganache数据目录（重定向到E:盘以避免C:盘占满）===
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
GANACHE_DATA_DIR = "E:/ganache_data"

# === 网络拓扑 ===
# 分片数量设置
NUM_SETTLEMENT_SHARDS: int = 2

def get_topology() -> t.Dict[str, t.Any]:
    """
    生成用于模拟的确定性拓扑结构。

    返回:
        描述分片、执行节点和基准节点的字典。
    """
    topology = {
        "shards": {},
        "execution": {"port": 9000, "type": "execution"},
        "baseline": {"port": 9999, "type": "single_chain"}
    }

    for i in range(NUM_SETTLEMENT_SHARDS):
        shard_name = f"shard_{i}"
        topology["shards"][shard_name] = {
            "port": 8580 + i,
            "type": "settlement",
            "id": i
        }

    return topology