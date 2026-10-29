"""SOTA 对照方案烟雾测试：小规模跑通 DAS / TELEPORT / ATOMIC 三条旅程。

目的：验证新增的合约与 worker loop 能正确发出交易、链上校验不误判（不 revert）、
      每笔交易都能被记入 gas，且三个方案的交易笔数一致。

**刻意与正式实验不同的两处（仅为省时省资源，不影响正确性验证）**：
  1. BLOCK_TIME 临时降到 1 秒（正式实验为 12 秒）；
  2. 账户数降到 200（正式实验 NUM_USERS=6000）。

**安全**：不使用 exp_macro_matrix.kill_nodes()——那会 `pkill -9 -f anvil`，
        把本机其它实验的 anvil 一并杀掉。这里只用 AnvilManager 自己的进程句柄收尾。

用法：
    export PATH="$HOME/.foundry/bin:$PATH"
    python smoke_test_sota.py
"""
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import core.network as net

# --- 仅本测试生效的降载设置 ---
net.NUM_USERS = 200        # 必须 > DEPLOYER_ACCOUNT_INDEX(99)
net.BLOCK_TIME = 1         # 正式实验为 12

import config_global  # noqa: E402
from core.deployer import ContractDeployer  # noqa: E402
from core.identity import UserManager  # noqa: E402
from core.macro_injector import MacroTransactionInjector  # noqa: E402
from core.macro_traffic import MacroTrafficGenerator  # noqa: E402
from core.network import AnvilManager, ConnectionManager  # noqa: E402

CONCURRENCY = 3
JOURNEYS_PER_USER = 1
THETA = 1


def main() -> None:
    topology = config_global.get_topology()
    mgr = AnvilManager()
    mgr.start_network(topology)
    try:
        conn = ConnectionManager(topology)
        ident = UserManager(config_global.MNEMONIC)
        deployer = ContractDeployer(conn, ident)
        deployer.compile_all()
        registry = deployer.deploy_infrastructure(topology)
        injector = MacroTransactionInjector(conn, ident)

        for journey_type in ["DAS", "TELEPORT", "ATOMIC"]:
            gen = MacroTrafficGenerator(
                network_manager=conn,
                identity_manager=ident,
                injector=injector,
                registry=registry,
                process_id=1,
                user_offset=0,
            )
            gen.run_task_based(
                concurrency=CONCURRENCY,
                journeys_per_user=JOURNEYS_PER_USER,
                ops_per_journey=THETA,
                journey_type=journey_type,
            )

            agg = collections.defaultdict(list)
            for row in gen.raw_logs:
                agg[row["tx_type"]].append((row["gas_used"], row["status"]))

            print(f"\n=== {journey_type} （每用户 {CONCURRENCY} 并发 × θ={THETA}）===")
            total_ok = 0
            for tx_type, rows in sorted(agg.items()):
                gases = [g for g, _ in rows]
                ok = sum(1 for _, s in rows if s == 1)
                total_ok += ok
                print(
                    f"  {tx_type:<12} 笔数={len(rows):3d} 成功={ok:3d} "
                    f"gas均值={sum(gases) / len(gases):>9,.0f}"
                )
            print(f"  合计成功交易：{total_ok}")
    finally:
        mgr.stop_network()


if __name__ == "__main__":
    main()
