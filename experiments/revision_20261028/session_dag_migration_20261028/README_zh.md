# 实际 Stage DAG 与 session 迁移账本控制实验

## 目的与边界

本实验把既有 `DAGStage` 多合约级联与仓库中的 `MigrationHarness` 收据/余额状态机接入同一 EVM，观察完整 session 的迁入、执行和迁出耗时如何随每个 session 承载的 DAG 工作流数 θ 变化。`sourceDomain` 与 `destinationDomain` 只是同一 EVM 账本内的逻辑域标签，收据由可信本地 ECDSA 测试签名者授权。**它是受控组合机制，不是原生跨 VM/跨链桥测量。**

## 工作流与资金

- 每个 workflow 是 8 个独立部署的 `DAGStage` 合约组成的 d8 链；每个 stage 消耗前一 stage 的链上 checksum，合约事件/状态可按 workflow 和 node 审计。
- θ=1/2/4/8 个独立 workflow ID 和 seed 可以预先一起提交到一个 router 交易；不声称 workflow 之间有先后依赖。依赖发生在各 workflow 内部 8 个 Stage 之间。
- 每执行一个 Stage，router 必须从当前逻辑域的可用余额扣除 1 个单位，之后才会调用该 Stage。每 session 预充值 `8θ+1`，DAG 执行花费 `8θ`，最终余额 1。
- 同域组直接在 source 逻辑域执行，作为无迁移最佳情况。迁移组要求完整迁入收据已结算且目标余额足够后才能进入 session；退出只能在所有 workflow 均完成、sink 均存在、session 已关闭且目标剩余余额恰为 1 时开始。
- 迁入和迁出分别审计完整待处理 envelope、nonce、recipient、amount、payload hash、settled 状态、待处理清除和结算事件绑定。

## 运行流程与报告

正式汇总和每 session 原始记录见 `session_dag_migration_report.md`、`session_dag_migration_summary.csv`、`formal_1s_r*/session_dag_raw.json`。本轮 1 秒出块设置用于控制 pilot 的多重复比较，不代表公链时延或 production 性能。每次重复启动新 Anvil 和新账本；原始记录包含源码哈希、运行时版本、链上交易标识、内部/端到端时长、终态账本和 DAG 前驱绑定审计。

本实验不提供跨 VM 的执行域优势，也不应用于证明迁移能否 break-even。天然跨域收益需由原生跨 VM 机制及匹配工作量的结果另行验证，不能将本实验与 NEAR Sandbox 数字拼接成原生端到端结果。
