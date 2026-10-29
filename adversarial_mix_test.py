"""异常混合实验：在正常迁移流量中混入少量异常交易，比较三种准入设计的处置。

异常类型（对称施加于三个端点）：
  1. 重放        —— 重复提交一个已消费的 receipt / 消息序号
  2. 跨目标提交  —— 把目的地为分片 A 的入账请求提交给分片 B

诚实边界：本实验只覆盖**绑定**与**重放**两条链上可执行的准入规则。
论文 §III-E 的第三条规则（最终性证明）在本原型中抽象化，不在链上，
因此本实验不涉及"伪造来源证明"这类需要证明系统的异常。

产出：
  - 每个端点对两类异常的接受/拒绝矩阵（安全性）
  - 正常交易的成功率与 gas（异常是否拖累正常流量）

用法：
    export PATH="$HOME/.foundry/bin:$PATH"
    python adversarial_mix_test.py
"""
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import core.network as net

net.NUM_USERS = 200        # 仅本测试：降载
net.BLOCK_TIME = 1

import config_global  # noqa: E402
from core.deployer import ContractDeployer  # noqa: E402
from core.identity import UserManager  # noqa: E402
from core.network import AnvilManager, ConnectionManager  # noqa: E402
from web3 import Web3  # noqa: E402

NORMAL_PAIRS = 8           # 每个端点先跑的正常迁移对数
ANOMALY_REPEATS = 3        # 每类异常重复次数


def domain_of(node_name: str) -> bytes:
    """端点部署时注入的域分隔符（不能用地址——模拟环境各节点地址相同）。"""
    return Web3.keccak(text=node_name)


def main() -> None:
    topology = config_global.get_topology()
    mgr = AnvilManager()
    mgr.start_network(topology)
    try:
        conn = ConnectionManager(topology)
        ident = UserManager(config_global.MNEMONIC)
        deployer = ContractDeployer(conn, ident)
        deployer.compile_all()
        reg = deployer.deploy_infrastructure(topology)

        user = ident.get_user(1)
        acct = user.address
        e0 = conn.get_web3("shard_0")      # 分片 0
        e1 = conn.get_web3("shard_1")      # 分片 1
        ee = conn.get_web3("execution")    # 执行节点

        from eth_account import Account

        def send(web3, addr, abi, fn, *args):
            """发送并返回 (status, gasUsed)；status=1 成功，0 为 revert。"""
            c = web3.eth.contract(address=addr, abi=abi)
            tx = c.functions[fn](*args).build_transaction(
                {
                    "from": acct,
                    "nonce": web3.eth.get_transaction_count(acct),
                    "gas": 300000,
                    "gasPrice": web3.eth.gas_price,
                }
            )
            signed = Account.sign_transaction(tx, user.key)
            h = web3.eth.send_raw_transaction(signed.raw_transaction)
            r = web3.eth.wait_for_transaction_receipt(h, timeout=90)
            return r.status, r.gasUsed

        results = collections.OrderedDict()
        seq = 1000

        for name, key in [("DAS", "DAS"), ("TELEPORT", "TELEPORT"), ("ATOMIC", "ATOMIC")]:
            row = {"正常": [0, 0], "重放": [0, 0], "跨目标": [0, 0], "gas": []}

            def endpoint(web3, which):
                return (reg[which][key], reg[which][f"{key}_ABI"])

            # ---------- 正常迁移：分片0 burn → 执行节点 mint ----------
            for _ in range(NORMAL_PAIRS):
                seq += 1
                # 出账（三个端点各自的出账函数名）
                if key == "ATOMIC":
                    send(e0, *endpoint(e0, "shard_0"), "exportTo", acct, 100)
                else:
                    send(e0, *endpoint(e0, "shard_0"), "burn", acct, 100, seq)
                # 入账
                if key == "DAS":
                    st, g = send(ee, *endpoint(ee, "execution"), "mint", acct, 100, seq,
                                 domain_of("execution"))
                elif key == "TELEPORT":
                    st, g = send(ee, *endpoint(ee, "execution"), "mint", acct, 100, seq)
                else:
                    st, g = send(ee, *endpoint(ee, "execution"), "importFrom", acct, 100)
                row["正常"][0] += st
                row["正常"][1] += 1
                row["gas"].append(g)

            # ---------- 异常 1：重放（复用已消费的序号/消息） ----------
            for _ in range(ANOMALY_REPEATS):
                if key == "DAS":
                    st, _ = send(ee, *endpoint(ee, "execution"), "mint", acct, 100, seq,
                                 domain_of("execution"))
                elif key == "TELEPORT":
                    st, _ = send(ee, *endpoint(ee, "execution"), "mint", acct, 100, seq)
                else:
                    st, _ = send(ee, *endpoint(ee, "execution"), "importFrom", acct, 100)
                row["重放"][0] += st
                row["重放"][1] += 1

            # ---------- 异常 2：跨目标提交（把本应发给执行节点的请求发给分片1） ----------
            for i in range(ANOMALY_REPEATS):
                seq += 1
                if key == "DAS":
                    # 绑定的是执行节点地址，却提交给分片1 → 应被绑定校验拒绝
                    st, _ = send(e1, *endpoint(e1, "shard_1"), "mint", acct, 100, seq,
                                 domain_of("execution"))
                elif key == "TELEPORT":
                    # 无绑定校验；给一个更大的序号即可通过
                    st, _ = send(e1, *endpoint(e1, "shard_1"), "mint", acct, 100, 900000 + i)
                else:
                    st, _ = send(e1, *endpoint(e1, "shard_1"), "importFrom", acct, 100)
                row["跨目标"][0] += st
                row["跨目标"][1] += 1

            results[name] = row

        print("\n" + "=" * 78)
        print("异常混合实验：接受 / 总数（接受 = 该异常被链上放行）")
        print("=" * 78)
        print(f"{'设计':<12}{'正常迁移':>16}{'重放攻击':>16}{'跨目标提交':>16}{'正常tx gas':>14}")
        print("-" * 78)
        for name, r in results.items():
            g = sum(r["gas"]) / len(r["gas"]) if r["gas"] else 0
            print(f"{name:<12}"
                  f"{str(r['正常'][0]) + '/' + str(r['正常'][1]):>16}"
                  f"{str(r['重放'][0]) + '/' + str(r['重放'][1]):>16}"
                  f"{str(r['跨目标'][0]) + '/' + str(r['跨目标'][1]):>16}"
                  f"{g:>14,.0f}")
        print()
        print("说明：正常迁移列应全通过；'重放'与'跨目标'两列中，被接受即为安全缺陷。")
    finally:
        mgr.stop_network()


if __name__ == "__main__":
    main()
