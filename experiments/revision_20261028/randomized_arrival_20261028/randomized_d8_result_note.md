# d8 随机到达：修复后完整重放与解释边界

## 结论先行

本实验测量的是一条**8 个有状态合约、7 条串行依赖边、每阶段 16 次操作**的特定工作流，不是“每增加一个合约就增加多少倍成本”的测量。当前观察到 NEAR sink 延迟约 9.6 s、EVM 约 0.6–0.7 s；这个较大的跨运行时差异必须按本地运行时配置和执行路径解释，不能包装成 DAS 的普遍性能优势，也不能据此推算 20 个合约会慢或快 20 倍。20 阶段链的关键路径、receipt/block 推进、流水重叠、争用和失败传播都可能改变，须用匹配的 20 阶段工作负载直接测量。

重放后的 goodput 几乎相同（约 0.47/1.00 workflows/s），所以本结果**没有显示吞吐或容量优势**。它显示的是这个工作负载下两个本地运行时的端到端 sink 延迟差异。现有数据不足以把差异因果归于 DAS 架构，也不足以声称完整系统优于既有方案。

## 修复后 seed 32 配对结果

延迟 p50/p95 统一按每个 phase 中**全部 fully audited 成功 workflow**计算，包含 warmup 工作流及 measurement 窗口结束后在 drain 阶段才完成的工作流；样本量为 385/817（seed 31 为 415/816）。goodput 则按 sink 在 780 s measurement 窗口内变为可查询的成功数除以 780 s。两项指标使用不同 cohort，表中已分列，不能把 goodput 的成功数当作 latency 百分位样本量。

| seed / 平台 | Offered（全 phase） | latency n（全 phase fully audited） | failure | pending | observation 窗口内 sink 完成数（goodput cohort） | Goodput (workflow/s) | sink p50（全 phase, s） | sink p95（全 phase, s） |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 20261032 / EVM（已有完整基线）λ=0.5 | 385 | 385 | 0 | 0 | 368 | 0.472 | 0.572 | 1.106 |
| 20261032 / NEAR（本次修复后重放）λ=0.5 | 385 | 385 | 0 | 0 | 369 | 0.473 | 9.614 | 10.075 |
| 20261032 / EVM（已有完整基线）λ=1.0 | 817 | 817 | 0 | 0 | 781 | 1.001 | 0.687 | 1.626 |
| 20261032 / NEAR（本次修复后重放）λ=1.0 | 817 | 817 | 0 | 0 | 789 | 1.012 | 9.655 | 10.089 |

此处 0.5/1.0 到达率对应 780 s measurement 窗口；goodput cohort 是 sink 在该窗口内可查询的成功 workflow，分别为 EVM 368/781、NEAR 369/789。百分位则使用全 phase 样本，EVM 与 NEAR 各为 385/817。全 phase 百分位不是 observation-window-only latency，也不应与只在窗口内完成的样本百分位混用。

EVM 是同一 schedule 的既有完整结果；**本轮只重放了 NEAR**。两端使用相同 schedule SHA-256 `9684e531a7523cd2672ef91abd2a951224472550304a8c49833cac0cde823166`。修复 runner SHA-256 为 `93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`；NEAR Sandbox 2.13.4 / protocol 86。

重放的 1,202/1,202 条 workflow 全部通过最终离线审计：0 failure、0 pending；每条均有交易 hash、8 个 stage 状态，且与 receipt events 和 sink 状态一致。完整轨迹中有 66 条 sink 早于 receipt tree 可查询（λ=0.5 为 22 条，λ=1.0 为 44 条），修复 runner 等到两种证据都齐全后才完成审计。`send_tx` 共 1,253 次 HTTP 尝试，51 次 HTTP 408 以同一 payload 重试；所有 workflow 最终都完成审计。NEAR 共记录 18,030 个 receipt 状态条目，平均每 workflow 15 个 receipt。

## 旧异常状态与修复后结果

| seed 32 NEAR 状态 | λ=0.5 | λ=1.0 | 解释 |
|---|---:|---:|---|
| 旧 runner：fully audited / failure label / pending | 353 / 10 / 22（共 385） | 767 / 16 / 34（共 817） | sink 与 8 个 stage state 已可查询，但 receipt/event 证据未齐；runner 在 sink 先到时提前离开 receipt 等待循环，并错误标记 timeout/pending。不能据此当作业务失败。 |
| 修复后完整 NEAR 重放 | 385 / 0 / 0（共 385） | 817 / 0 / 0（共 817） | 完整轨迹重放并逐条通过最终离线审计。 |

旧状态保留作审计历史，不与修复后数据混合计算，也不将其称为新的独立重复。seed 30 是与其他 NEAR 工作重叠的探索运行；seed 31 是另一条完整的 exclusive 配对运行；seed 32 的旧状态与修复后重放来自同一 schedule。它们不是“三次同条件的新鲜独立重复”。因此，对 latency 差异仍应保持描述性措辞。

## 计量口径与复现边界

- 工作负载：`dag-07-chain-d8-w1-theta1-nu1`，8 个独立有状态 stage、7 条串行边、每 stage 16 次操作；EVM 通过 router 做同步本地调用，NEAR 跨合约 receipt 执行。
- 每个到达率先预热 30 s，再观测 780 s；输入 schedule 与 workflow ID 配对。此设计测量的是本地 Anvil / NEAR Sandbox 下的端到端运行时行为，不是生产链吞吐或架构因果实验。
- EVM 每 workflow transaction gas（mean 约 1,124,771；p95 约 1,124,775）与 NEAR block-level gas sample（不能归因到单 workflow）分别报告；两者单位/归属口径不同，不直接相减或折算。
- 资源采样存在进程标度和采样间隔差异：`ps` CPU 是单逻辑 CPU 标度；EVM 约 2 s 采样、NEAR 约 1 s。结果不代表独占资源或严格可比的主机总 CPU。
- runner 退出码为 0，但 NEAR 清理状态为 `forced_after_timeout`：teardown 超过 15 s。后续确认进程已退出、Sandbox home 已移除、RPC 端口拒绝连接；不表述为 graceful cleanup。审计 gate 在 teardown 前通过；最终 tar 归档在 runner 结束后生成。因此“审计先于 teardown”成立，“最终 tar 先于 teardown”不成立。

## 可追踪证据

- [更新后的绝对延迟图](randomized_d8_main_figure.svg)
- [修复后 seed 32 全量报告](formal_seed20261032/auditfix_replay/full_replay_results/FINAL_RUN_REPORT.md)
- [最终离线审计](formal_seed20261032/auditfix_replay/full_replay_results/FINAL_OFFLINE_AUDIT.json)
- [逐阶段指标](formal_seed20261032/auditfix_replay/full_replay_results/FULL_METRICS.md)
- [旧状态分类记录](formal_seed20261032/audit_classification.csv)
- [证据归档及哈希清单](formal_seed20261032/auditfix_replay/full_replay_results/evidence_archive_manifest.json)
- [完整证据 tar.gz](formal_seed20261032/auditfix_replay/full_replay_results/seed32_full_evidence.tar.gz)
- [阶段综合结论：串行链、并行汇合与中间失败](../phase_conclusion_20261028.md)

归档 SHA-256：`30063239402389d3076fb8ccaa1e4968927be94aa807fa0d4d0c8a818ce58e75`；最终 NEAR raw SHA-256：`f29682cc217c8fdb603c253feab78821f49e268f8c32dae834dc0f5ef09b406e`。
