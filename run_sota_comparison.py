"""SOTA 对照实验的安全入口（R1 要求的 Teleport 式 / Avalanche 式对照）。

为什么需要这个文件而不是直接用 scenarios/exp_macro_matrix.py：

    exp_macro_matrix.main() 一开头会调用 kill_nodes()，其实现是 `pkill -9 -f anvil`，
    会杀掉**本机所有** anvil 进程（包括其它 agent 并行实验的那 16 个）。
    本脚本把 kill_nodes 换成空操作，并加一道负载闸门。

用法：
    export PATH="$HOME/.foundry/bin:$PATH"
    python run_sota_comparison.py                # 默认：θ=10, N=100..500, DAS/TELEPORT/ATOMIC
    SOTA_PROCESSES=4 python run_sota_comparison.py
    SOTA_MAX_LOAD=6 python run_sota_comparison.py     # 负载高于阈值则拒绝启动
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scenarios.exp_macro_matrix as drv  # noqa: E402

# ---------------------------------------------------------------- 安全补丁
def _safe_kill_nodes():
    print("[安全] 已跳过 kill_nodes：不执行 pkill，避免误杀本机其它实验的 anvil 进程。")
    print("[安全] 本脚本只会通过 AnvilManager 的进程句柄关闭自己启动的节点。")


drv.kill_nodes = _safe_kill_nodes

# ---------------------------------------------------------------- 矩阵规模
PROCESSES = int(os.environ.get("SOTA_PROCESSES", "4"))
MAX_LOAD = float(os.environ.get("SOTA_MAX_LOAD", "6"))

drv.AMORTIZATION_FACTORS = [int(x) for x in os.environ.get("SOTA_THETAS", "10").split(",")]
drv.CONCURRENCY_LEVELS = [int(x) for x in os.environ.get("SOTA_NS", "100,200,300,400,500").split(",")]
drv.SCENARIOS = os.environ.get("SOTA_SCENARIOS", "DAS,TELEPORT,ATOMIC").split(",")
drv.NUM_PROCESSES = PROCESSES

# 结算分片数 κ（超参数扫描用）：在 get_topology() 被调用前改写模块全局即可生效
_SHARDS = os.environ.get("SOTA_SHARDS")
if _SHARDS:
    import config_global
    config_global.NUM_SETTLEMENT_SHARDS = int(_SHARDS)
    print(f"[配置] 结算分片数 κ = {config_global.NUM_SETTLEMENT_SHARDS}")
    # 拓扑由 config_global.get_topology() 在 main() 内构造，届时读取该全局


def _load_ok() -> bool:
    load1 = os.getloadavg()[0]
    cores = os.cpu_count() or 1
    print(f"[负载闸门] 1 分钟负载 = {load1:.2f}，核心数 = {cores}，阈值 = {MAX_LOAD}")
    if load1 > MAX_LOAD:
        print(f"[负载闸门] 机器繁忙（可能另有实验在跑），本次不启动。")
        print(f"[负载闸门] 稍后重试，或显式放宽：SOTA_MAX_LOAD={load1 + 1:.0f} python run_sota_comparison.py")
        return False
    return True


def main() -> None:
    print("=" * 72)
    print("SOTA 对照实验（安全模式）")
    print(f"  θ = {drv.AMORTIZATION_FACTORS}")
    print(f"  N = {drv.CONCURRENCY_LEVELS}")
    print(f"  场景 = {drv.SCENARIOS}")
    print(f"  进程数 = {PROCESSES}（正式论文口径为 20，此处降载）")
    print("=" * 72)
    if not _load_ok():
        sys.exit(2)
    drv.main()


if __name__ == "__main__":
    main()
