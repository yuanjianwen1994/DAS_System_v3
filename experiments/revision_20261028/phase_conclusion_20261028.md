# 阶段性实验结论：串行链、并行汇合与中间失败

## 目前可以说什么

这些结果说明，DAS 工作流的时延和故障结果受**依赖拓扑、执行模型和运行时边界**共同影响。它们不是一条可按合约数量线性外推的规律。当前证据来自三个彼此相关但测量目标不同的实验，不能把数字拼成单一的“DAS 加速倍数”。

### 1. 串行 8 阶段链：端到端延迟差异，不是架构胜负

[d8 随机到达结果](randomized_arrival_20261028/randomized_d8_result_note.md)测量 8 个有状态 stage、7 条串行依赖边、每 stage 16 次操作。修复后的 seed 32 NEAR 完整重放为 1,202/1,202 全审计成功；配对 EVM 是同 schedule 的既有完整基线，本轮只重放 NEAR。

延迟 p50/p95 统一使用每 phase **全部 fully audited 成功 workflow**（含 warmup 和观测结束后 drain 完成的 workflow；seed 31 样本量为 415/816，seed 32 为 385/817）。goodput 另按 sink 在 780 s measurement 窗口内可查询的成功数计算（seed 32：EVM 368/781、NEAR 369/789），不把 goodput cohort 数量误作 latency 分位数的样本量。NEAR 的 sink p50 约 9.6 s，EVM 约 0.6–0.7 s；但两端 goodput 几乎一致，约 0.47 和 1.00 workflow/s。较大的 wall-clock latency 差异是真实的**本地运行时观察**，但执行路径、receipt/block 推进和本地配置没有形成可以识别架构因果的控制实验。因此不将其表述为 DAS 的普遍优势，也不把它解释成每多一个合约就乘一个固定系数。

链深从 8 增至 20，关键路径、receipt 数与层间调度可能变化；并发、流水、资源争用和失败传播也会改变结果。现有数据既不能推出 20-stage 是 20 倍，也不能推出它会保持不变；需要在匹配的 20-stage 工作负载上直接测量。

### 2. 分支—并行—汇合：较早完成伴随更多消息与计算成本

[路线 C 正式拓扑报告](controller2_topology_20261028/formal_report.md)在 8 节点、每节点 16 次操作下，比较了 7 条边的串行链与 12 条边的“根→6 分支→汇合”。NEAR 三次独立 fresh runtime 的描述性中位数为：sink 首次可查询由 1,256 ms 降到 629 ms，完整 receipt tree 由 1,249 ms 降到 753 ms；与此同时 receipt 数由 15 增至 27，NEAR native gas 由 22.26 增至 37.50 Tgas。EVM 的 execution transaction wall time 两拓扑接近，native gas 增加约 27%。

这组数据支持一个有限结论：**这次 NEAR 分支汇合拓扑更早产出 sink 状态，但多付出 receipt 和 gas 成本**。它不是严格的只改深度对照，因为边数和依赖形状也不同；单工作流、三次重复也不支持生产吞吐或跨平台架构优劣结论。NEAR 的异步并行执行能力属于通用运行时机制；不能仅凭这组结果把它说成 DAS 独有能力。

### 3. 中间节点失败：两种执行边界下的状态保留不同

同一报告在汇合拓扑 node 4 注入失败。NEAR 观察到失败 receipt，节点 0、1、2、3、5、6 留有状态，node 4 失败且 sink 7 不存在，共 6/8 节点状态保留。EVM 的中间 `CALL` 失败使整笔执行交易回滚，合约状态均未保留；之后的 `abortSession` 是会话清理，不是业务补偿。

这证明的是**本实现和本交易边界中的故障语义差异**，不是 DAS 独占的原子性，也不是 NEAR 的协议漏洞。两端都没有实现或执行业务补偿；补偿交易数为 0，恢复时间未测，因此不能声称系统已经自动恢复或实现跨节点回滚。

## 对大修主张的建议边界

- 可报告具体拓扑、运行时版本、端到端延迟、goodput、原生 gas/receipt 数，以及失败后实际保留或回滚的状态。
- 将 NEAR 与 EVM 的差异表述为本地受控实现的观察；不把异单位 gas 相减、不把运行时差异直接归因于 DAS。
- 不把 d8 的延迟比值当作每合约倍率；尤其不对 20 个级联合约做线性外推。
- 不声称已证明完整 DAS 跨域性质、生产吞吐优势、跨平台优越性、业务补偿或普遍原子性。
- 将 seed 32 旧 runner 的 receipt 审计缺口留作历史异常；当前全量修复后 NEAR replay 与旧异常分别呈现。seed 30 并行运行、seed 31 exclusive、seed 32 修复后复跑不是三个同条件的新鲜独立重复。

## 相关证据

- [d8 更新图](randomized_arrival_20261028/randomized_d8_main_figure.svg) · [d8 结果与审计边界](randomized_arrival_20261028/randomized_d8_result_note.md)
- [路线 C 拓扑和失败控制正式报告](controller2_topology_20261028/formal_report.md) · [独立拓扑原始指标](controller2_topology_20261028/results/pilot/routeC_per_repeat_metrics.csv)
- [d8 修复后最终离线审计](randomized_arrival_20261028/formal_seed20261032/auditfix_replay/full_replay_results/FINAL_OFFLINE_AUDIT.json)
