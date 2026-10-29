## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: run
- Origin Date: 2026-10-28
- Verification Status: UNVERIFIED（本地派生数据自检通过；独立审计尚未完成）
- Version Label: exp_result_v1

## Experiment Result

- **ID**: routeC_topology_failure_20261028
- **Type**: generic
- **Status**: completed
- **Command**: 远端 `run_formal_remote.sh`（NEAR、EVM 各 3 个 fresh runtime；之后独立运行 `run_failure_control_remote.sh`）
- **Working Directory**: `/Users/yuanjianwen/ug_workspace/research/DAS_yjw_TSC_since_2026_April/DAS_System_v3/experiments/revision_20261028/controller2_topology_20261028`；远端目录 `/home/starshell/das_controller2_topology_20261028`
- **Duration**: 正式拓扑段 85 秒；另含独立失败控制段
- **Exit Code**: 正式 NEAR 0，正式 EVM 0；失败控制 NEAR 0，EVM 0

### 实验设计与运行边界

每个工作流均为 8 个有状态节点、每节点 16 次操作。链为 7 条依赖边；根→6 分支→汇合为 12 条边，故两者不是纯深度因果对照。每个平台、每种拓扑均运行 3 次独立 Sandbox/Anvil，workflow 状态和 checksum 审计均通过；结果按三个重复逐点呈现，样本量只支持描述性比较。本轮使用实际 Sandbox/Anvil 原生设置，独立成组，不与此前固定 1 秒区块条件下的结果合并。

NEAR 实际版本为 2.13.4、protocol 86，三次均使用同一 binary SHA-256 `4e89fbc0f2958634e84af44634f90e87c246288fa11b067d2121ce1394cba2c2`，单验证者、单 shard。Sandbox 未显式覆盖 block interval；工作流 receipt 所在连续 block 高度的 timestamp 间隔为链中位数 124.50 ms（123.08–126.16，n=24 个间隔），汇合拓扑中位数 125.02 ms（124.08–126.12，n=12）。这些间隔来自工作流内逐个连续 block 高度，非整个 Sandbox 空闲期的全时段采样。

EVM 使用 Anvil 1.8.6、automine 默认开启，未设置 `--block-time`；每次 fresh Anvil 的启动、RPC ready、停止请求和停止时间见 `routeC_summary.json`。Anvil 区块 timestamp 是整数秒的逻辑时间，不能视为实际 RPC wall time。远端容器有 20 个可用 CPU、无硬 CPU/内存 quota；运行软绑定 CPU 16–19。正式段资源采样 85 次，任务进程树峰值采样 RSS 428.1 MiB、CPU 峰值 97.5% of one core；独立失败段采样 19 次，RSS 峰值 370.7 MiB、CPU 峰值 89.5% of one core。容器内存和其他进程共享，RSS 可能重复计算共享映射，结果不代表独占资源。

运行边界时间均为 UTC：

| NEAR repeat | Sandbox 请求启动 | RPC ready | teardown 请求 | teardown 完成 |
|---|---|---|---|---|
| R1 | 2026-10-28T09:30:23.048Z | 2026-10-28T09:30:25.137Z | 2026-10-28T09:30:47.109Z | 2026-10-28T09:30:47.153Z |
| R2 | 2026-10-28T09:30:47.156Z | 2026-10-28T09:30:49.215Z | 2026-10-28T09:31:11.084Z | 2026-10-28T09:31:11.126Z |
| R3 | 2026-10-28T09:31:11.131Z | 2026-10-28T09:31:13.192Z | 2026-10-28T09:31:35.082Z | 2026-10-28T09:31:35.129Z |

| EVM repeat | Anvil 请求启动 | RPC ready | stop 请求 | stop 完成 |
|---|---|---|---|---|
| R1 | 2026-10-28T09:31:36Z | 2026-10-28T09:31:36Z | 2026-10-28T09:31:39Z | 2026-10-28T09:31:39Z |
| R2 | 2026-10-28T09:31:39Z | 2026-10-28T09:31:39Z | 2026-10-28T09:31:43Z | 2026-10-28T09:31:43Z |
| R3 | 2026-10-28T09:31:43Z | 2026-10-28T09:31:43Z | 2026-10-28T09:31:46Z | 2026-10-28T09:31:46Z |

### 拓扑延迟与原生执行成本

| 平台/指标 | 8 节点链，中位数（范围） | 根→6 分支→汇合，中位数（范围） |
|---|---:|---:|
| NEAR sink 首次成功 view 查询（ms） | 1256.29（1253.36–1256.75） | 628.55（627.25–677.88） |
| NEAR 完整 receipt tree 返回（ms） | 1248.58（1248.04–1249.28） | 752.65（751.86–754.11） |
| NEAR receipt tree 高度跨度（blocks） | 8 | 4 |
| NEAR 原生 gas（Tgas/workflow） | 22.2583（22.2420–22.2603） | 37.4995（37.4959–37.5008） |
| NEAR receipt 数（不含 originating tx） | 15 | 27 |
| EVM execution tx receipt（ms） | 112.37（112.18–112.72） | 112.82（112.67–112.89） |
| EVM 原生 gas（gas/workflow） | 1,124,775 | 1,430,761 |
| EVM 完整 session gas（entry+execute+exit，不含部署） | 1,249,892 | 1,555,878 |
| EVM 部署与注册 gas（每拓扑、每 Anvil） | 15,677,506 | 15,496,788 |
| EVM 真实 Stage 外部 CALL / edge events | 8 / 7 | 8 / 12 |

NEAR 汇合拓扑的 sink 首次成功轮询中位数较链快 50.0%，完整 receipt tree 中位数较链快 39.7%；同时 native gas 增加 68.5%，receipt 数增加 80.0%。EVM 两种拓扑的交易 wall time 接近（汇合中位数相对链 +0.40%），汇合拓扑 native gas 增加 27.2%。两个 VM 的 gas 单位和时间设置不同，跨平台绝对秒数及 gas 不直接作架构优劣比较。

NEAR 三次重复的 receipt 分类完全一致：链为 `start` 1、`receive_input` 7、`transfer_or_refund` 7；汇合为 `start` 1、`receive_input` 12、`on_fanout_complete` 1、`transfer_or_refund` 13。receipt 总数不是依赖轮数。sink 指标是每 50 ms 查询一次后首次读到值的时间；汇合拓扑三次均在完整 receipt tree 返回前读到 sink，链的三次均未早于 tree 返回被观测到。因此 sink 观测是轮询上界，不等于精确可见时刻。

NEAR 同 receiver 控制每个拓扑每次均为 1 个 receipt；完整 tree wall time 中位数约 337.4 ms（链）和 327.1 ms（汇合），native gas 中位数分别 2.8406 与 2.8933 Tgas。此控制把节点状态操作合并在同一接收合约，作用是边界参照，不与独立 Stage 拓扑混为同一执行模型。

### 中间分支失败控制

在汇合拓扑 node 4 注入失败。NEAR 完整 receipt tree 观察到失败 receipt（25 个 receipt、34.4938 Tgas）；节点 0、1、2、3、5、6 保留状态，节点 4 失败且 sink 7 不存在（6/8 节点有状态）。这表明独立异步调用可留下部分结果，不据此称为协议漏洞。当前实现没有业务补偿器，实际执行的补偿交易数为 0，恢复时间未测量。

EVM 中间 Stage CALL 失败，执行交易 receipt status 为 0，故障交易消耗 558,052 gas；已有 5 个 Stage CALL frame，所有合约状态在交易回滚后均不存在。这反映 EVM 单笔交易的回滚边界，不是 DAS 独占的原子性。随后执行 1 笔 `abortSession` 会话清理交易（status 1，27,479 gas），它不是业务补偿。两端均没有实现或执行业务补偿，不能声称已恢复。

### 图表与输出文件

- `routeC_topology_latency.svg`：三次逐点延迟；各平台单独面板，避免把运行时区块节奏差异误读成架构效应。
- `routeC_failure_outcome.svg`：NEAR 部分持久状态与 EVM 整笔回滚的节点级示意。
- `routeC_per_repeat_metrics.csv`：每个重复的 latency、native gas、receipt/CALL 数。
- `routeC_summary.json`：完整派生统计、运行窗口、区块配置、失败状态、资源边界。
- 原始结果：`near_topology_formal_raw.json`、`evm_topology_formal_raw.json`、`near_topology_failure_raw.json`、`evm_topology_failure_raw.json`；归档 SHA-256 `981160465ccdc3327bdeea9bbbf4985aaf3b38d6f0bc496bd9944a80302ae365`。

### 优势、代价与边界

- **优势**：在本次 NEAR Sandbox 条件下，分支汇合更早产生 sink 且完整 receipt tree 时间更短；运行状态与 checksum 审计通过。
- **代价**：分支汇合以更多依赖边、receipt 和 native gas 换取较低完成时间；EVM 交易时间没有观察到同样的收益。
- **边界**：仅 3 次 fresh runtime、受控单工作流；链和汇合边数不同、共享容器且 NEAR/EVM 出块设定不同，结果不支持跨平台架构因果或生产环境吞吐结论。

### 异常与审计说明

正式拓扑和失败控制退出码均为 0，成功工作流状态审计全部通过；失败控制中的 node 4 failure 是预期注入。此前的若干远端准备失败（工具缺项、错误目标路径、进程预检未命中）保留在 `near_*failure*` 与 startup 日志中，均发生在对应工作负载启动前，不计作工作流失败。
