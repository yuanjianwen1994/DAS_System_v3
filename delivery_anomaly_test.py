"""交付异常实验：在迁移消息上施加投递异常，检验各设计的价值守恒。

三种设计共用同一套"恶意/不可靠交付层"：
  A. 正常        —— 两条迁移按序投递
  B. 重复投递    —— 第一条被投递两次
  C. 乱序投递    —— 先投递第二条，再投递第一条
  D. 丢包后重投  —— 第一条先不投，投完第二条之后再重投第一条

对每种情形测量"铸造总量 − 销毁总量"：
  0    = 价值守恒（正确）
  >0   = 超发（凭空多铸）
  <0   = 价值丢失（源侧已销毁、目的侧未入账，用户资金搁浅）

注意：三个端点共用的前提是"对方诚实交付"以外的部分完全对等——
同一批账户、同一金额、同一投递时序、同样的异常注入。
判定全部由链上合约的准入逻辑决定，本脚本只负责投递与计数。

用法：
    export PATH="$HOME/.foundry/bin:$PATH"
    python delivery_anomaly_test.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import core.network as net

net.NUM_USERS = 200
net.BLOCK_TIME = 1

import config_global  # noqa: E402
from core.deployer import ContractDeployer  # noqa: E402
from core.identity import UserManager  # noqa: E402
from core.network import AnvilManager, ConnectionManager  # noqa: E402
from eth_account import Account  # noqa: E402
from web3 import Web3  # noqa: E402

VALUE = 100


def domain_of(node_name: str) -> bytes:
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

        e0 = conn.get_web3("shard_0")     # 源分片
        ee = conn.get_web3("execution")   # 目的节点

        scenarios = [
            ("A 正常", "normal"),
            ("B 重复投递", "duplicate"),
            ("C 乱序投递", "reorder"),
            ("D 丢包后重投", "drop_retry"),
        ]

        print(f"\n{'='*82}")
        print("交付异常实验：价值守恒误差 = 铸造总量 − 销毁总量（0 为正确）")
        print(f"{'='*82}")
        print(f"{'设计':<10}" + "".join(f"{n:>17}" for n, _ in scenarios))
        print("-" * 82)

        for design in ["DAS", "TELEPORT", "ATOMIC"]:
            row = []
            for _, mode in scenarios:
                user = ident.get_user(100 + len(row) + 10 * ("DAS", "TELEPORT", "ATOMIC").index(design))
                acct = user.address
                base = 5_000_000 + 100_000 * ("DAS", "TELEPORT", "ATOMIC").index(design) + 10_000 * len(row)
                burned = minted = 0

                def send(web3, node, fn, *args):
                    c = web3.eth.contract(address=reg[node][design], abi=reg[node][f"{design}_ABI"])
                    tx = c.functions[fn](*args).build_transaction({
                        "from": acct,
                        "nonce": web3.eth.get_transaction_count(acct),
                        "gas": 300000,
                        "gasPrice": web3.eth.gas_price,
                    })
                    st = Account.sign_transaction(tx, user.key)
                    h = web3.eth.send_raw_transaction(st.raw_transaction)
                    return web3.eth.wait_for_transaction_receipt(h, timeout=90).status

                def burn(n):
                    """源侧销毁，返回该笔的标识（DAS/Teleport 用序号，ATOMIC 无）。"""
                    seq = base + n
                    if design == "DAS":
                        return send(e0, "shard_0", "burn", acct, VALUE, seq), seq
                    if design == "TELEPORT":
                        return send(e0, "shard_0", "burn", acct, VALUE, seq), seq
                    return send(e0, "shard_0", "exportTo", acct, VALUE), None

                def mint(seq):
                    """目的侧入账，返回是否被接受（1 = 入账成功）。"""
                    if design == "DAS":
                        return send(ee, "execution", "mint", acct, VALUE, seq, domain_of("execution"))
                    if design == "TELEPORT":
                        return send(ee, "execution", "mint", acct, VALUE, seq)
                    return send(ee, "execution", "importFrom", acct, VALUE)

                # 两笔迁移的销毁（源侧均成功）
                s1, id1 = burn(1)
                s2, id2 = burn(2)
                burned = VALUE * (s1 + s2)

                # 按情形投递
                if mode == "normal":
                    minted += VALUE * (mint(id1) + mint(id2))
                elif mode == "duplicate":
                    minted += VALUE * (mint(id1) + mint(id1) + mint(id2))   # 第一条投两次
                elif mode == "reorder":
                    minted += VALUE * (mint(id2) + mint(id1))               # 先二后一
                elif mode == "drop_retry":
                    minted += VALUE * (mint(id2) + mint(id1))               # 第一条延后重投

                row.append((minted - burned) // VALUE)

            print(f"{design:<10}" + "".join(f"{v:>+17d}" for v in row))

        print()
        print("说明：0 = 价值守恒；正数 = 超发（凭空铸造）；负数 = 价值丢失（销毁了却无法入账）")

        # ---------------- 规模化：20 笔迁移，15% 异常率 ----------------
        total_migrations = 20
        dup_at = {5, 12, 17}        # 重复投递
        reorder_at = {8, 14}        # 与下一笔互换顺序
        drop_at = {3, 19}           # 首次丢弃、其后重投
        print(f"\n{'='*82}")
        print(f"规模化：{total_migrations} 笔迁移 / 异常 {len(dup_at)+len(reorder_at)+len(drop_at)} 笔"
              f"（重复 {len(dup_at)}、乱序 {len(reorder_at)}、丢包重投 {len(drop_at)}）")
        print(f"{'='*82}")
        print(f"{'设计':<12}{'销毁总量':>10}{'铸造总量':>10}{'守恒误差':>12}{'占迁移量':>12}")
        print("-" * 82)

        for design in ["DAS", "TELEPORT", "ATOMIC"]:
            di = ("DAS", "TELEPORT", "ATOMIC").index(design)
            user = ident.get_user(150 + di)
            acct = user.address
            base = 7_000_000 + 100_000 * di
            burned = minted = 0

            def send2(web3, node, fn, *args):
                c = web3.eth.contract(address=reg[node][design], abi=reg[node][f"{design}_ABI"])
                tx = c.functions[fn](*args).build_transaction({
                    "from": acct,
                    "nonce": web3.eth.get_transaction_count(acct),
                    "gas": 300000,
                    "gasPrice": web3.eth.gas_price,
                })
                st = Account.sign_transaction(tx, user.key)
                h = web3.eth.send_raw_transaction(st.raw_transaction)
                return web3.eth.wait_for_transaction_receipt(h, timeout=90).status

            # 先全部销毁
            ids = []
            for k in range(1, total_migrations + 1):
                seq = base + k
                if design == "ATOMIC":
                    st = send2(e0, "shard_0", "exportTo", acct, VALUE)
                    ids.append(None)
                else:
                    st = send2(e0, "shard_0", "burn", acct, VALUE, seq)
                    ids.append(seq)
                burned += VALUE * st

            def deliver(k):
                seq = ids[k - 1]
                if design == "DAS":
                    return send2(ee, "execution", "mint", acct, VALUE, seq, domain_of("execution"))
                if design == "TELEPORT":
                    return send2(ee, "execution", "mint", acct, VALUE, seq)
                return send2(ee, "execution", "importFrom", acct, VALUE)

            k = 1
            while k <= total_migrations:
                if k in reorder_at and k + 1 <= total_migrations:
                    minted += VALUE * deliver(k + 1)
                    minted += VALUE * deliver(k)
                    k += 2
                    continue
                if k in drop_at:
                    k += 1                      # 首次丢弃（不投递）
                    minted += VALUE * deliver(k)  # 投下一笔
                    minted += VALUE * deliver(k - 1)  # 再重投被丢弃那笔
                    k += 1
                    continue
                minted += VALUE * deliver(k)
                if k in dup_at:
                    minted += VALUE * deliver(k)  # 重复投递同一笔
                k += 1

            err = minted - burned
            print(f"{design:<12}{burned:>10,}{minted:>10,}{err:>+12,}{err/burned*100:>11.1f}%")
        print()
    finally:
        mgr.stop_network()



if __name__ == "__main__":
    main()
