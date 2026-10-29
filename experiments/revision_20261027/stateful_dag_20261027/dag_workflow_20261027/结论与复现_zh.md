# 状态化 DAG 实验：中文结论与复现

## 结论

在本次实际测量的独立多合约 DAG 工作流中，NEAR Sandbox 的终点合约随前驱 Promise 收据到达后执行。深度为 2、4、8 的链，其终点分别在发起交易后 2、4、8 个块高跨度完成；同一 DAG 有两种本地 EVM 实现：融合控制把节点循环放在一个合约中；独立 Stage 实现由 Router 对每个节点合约执行一次外部 `CALL`。两种实现都由一笔本地执行交易完成。这个结果支持一个限定结论：对所测的深依赖、独立多合约工作流，本地组合把 DAG 节点执行留在一次本地状态事务中，减少了异步依赖确认路径。它不是不同出块时间之间的延迟比，也不是端到端桥迁移性能结论。

块高统计区分了 DAG 终点完成与整个 Promise 收据树排空。宽度为 4 的 fork-join 和 mixed DAG，终点在 +4 块高执行完毕，完整回执树到 +6 块高才排空。后两块高来自分支回调与退款收据尾部；不能把总 receipt outcome 数当成串行依赖轮数。

Receipt 分类已通过 `EXPERIMENTAL_receipt` 查询动作载荷验证：

| 工作流 | 节点 FunctionCall | fanout barrier callback | `system` 退款 Transfer | 执行结果数 |
|---|---:|---:|---:|---:|
| Chain d2 | 2 | 0 | 1 | 3 |
| Chain d4 | 4 | 0 | 3 | 7 |
| Chain d8 | 8 | 0 | 7 | 15 |
| Fork-join d4, w4 | 25 | 5 | 29 | 59 |
| Mixed d4, w4 | 17 | 5 | 21 | 43 |

发起交易单独记录，不计入上述执行结果数。`FunctionCall` 包含 `start`、每条 DAG 边上的 `receive_input`，以及分支生产者的 `on_fanout_complete`。退款收据是 `predecessor_id=system` 的 `Transfer`。部署与 `new` 初始化发生在计时调用之外，不在这些数值中。

汇合节点完整性经过状态和 checksum 双重核对：每个工作流每个节点只执行一次；每条边只到达一次；节点储存的 `input_count` 等于配置前驱数量；节点 checksum 与同接收者 θ 批处理路径一致。宽度 4 的 fork-join 中，每个四输入汇合节点及终点都读取了全部 4 个父值后才计算。日志中的 `DAG_JOIN` 专指生产者侧 fanout barrier callback，不是 DAG 多父汇合节点本身。

同接收者 NEAR 对照把相同图、相同 θ 个工作流和相同终态放入一个合法调用/收据；84 个会话均为一条执行收据。所有正常 NEAR 工作流 354/354 完成，84 次故障试验均记录了失败收据和未完成终点。EVM 路径将 θ 个工作流放入一次本地组合执行调用；entry/exit 各另用一笔本地 commitment-storage 代理交易测量。代理不实现桥证明、认证、终局性或实际资产迁移。

实验覆盖单验证者 Sandbox、单分片配置，执行域边比例 `p=0`。这里没有跨分片边，因此结果不外推为跨分片实测。NEAR gas 与 EVM gas 使用不同计量体系，不做横向除法或费用比较。之前的 Avalanche-local / Teleport-local hash-chain 屏幕是独立机制正对照，未混入本 DAG 结果，也不冒充本 DAG 的独立实测。


## 独立 Stage 合约 EVM 实验

该实现为每个 DAG 节点部署一个独立 `DAGStage` 合约，并部署 `DASDistributedCompositionHarness` Router。Router 按拓扑顺序对各 Stage 发起 EVM 外部 `CALL`。每个 Stage 保存父节点输入并计算 checksum；会话结束后验证输入数量和所有节点 checksum。全矩阵共 42 个 profile-repeat、84 个会话，记录 84 笔 Router 执行交易、1,956 个 Stage `CALL` frame、1,956 个 `NodeExecuted` 事件和 2,274 个 `ParentInputConsumed` 事件；所有会话均通过 checksum 与输入状态核验。84 次跨合约故障探针均回滚了部分写入。

| 配置 | 融合单合约控制会话 gas | 独立 Stage 会话 gas（entry + Router + exit，不含部署） | 每个 profile 的部署和注册 gas | 每会话 Stage CALL 数 |
|---|---:|---:|---:|---:|
| Chain d2, θ=1 | 293,829 | 435,055 | 6,313,503 | 2 |
| Chain d2, θ=4 | 714,419 | 1,201,484 | 6,313,503 | 8 |
| Chain d4, θ=4 | 1,007,636 | 2,189,801 | 9,434,821 | 16 |
| Chain d8, θ=4 | 1,660,371 | 4,235,880 | 15,677,474 | 32 |
| Fork-join d4, w2, θ=4 | 1,376,807 | 3,883,587 | 12,527,376 | 24 |
| Fork-join d4, w4, θ=4 | 2,406,974 | 8,832,224 | 18,734,561 | 40 |
| Mixed d4, w2, θ=4 | 1,376,811 | 3,883,591 | 12,527,340 | 24 |
| Mixed d4, w4, θ=4 | 2,252,388 | 6,999,355 | 18,705,019 | 40 |
| Chain d4, θ=4, ν=4 | 1,007,634 | 2,189,799 | 9,434,821 | 16 |

均值按三个新建链、每条链两个会话计算。会话 gas 包含 entry 代理、Router 执行交易和 exit 代理。Stage frame gas 是 Router 交易内的嵌套消耗，属于诊断值，不能再加到总 gas。部署和节点注册每条新链每个 profile 执行一次，单列且不摊入会话 gas。此前表中 EVM 数值是融合单合约控制，不能称为独立 Stage 实验。两条 EVM 路径都是本地执行环境，不能说明跨分片执行性能。

- [EVM 融合与独立 Stage 对比图](dag_evm_composition_controls.png) · [PDF](dag_evm_composition_controls.pdf)
- [独立 Stage 原始记录](das_distributed_composition_raw.json) · [解析结果](das_distributed_composition_results.json)
- [独立 Stage Router 运行器](run_das_distributed_composition.py) · [合约源码目录](contracts/)

## 主要图表

- [DAG 结构图](dag_shapes.png) · [PDF](dag_shapes.pdf)
- [依赖完成与原生执行单位](dag_execution_comparison.png) · [PDF](dag_execution_comparison.pdf)
- [深度与会话参数趋势](dag_parameter_trends.png) · [PDF](dag_parameter_trends.pdf)
- [EVM 融合与独立 Stage 对比](dag_evm_composition_controls.png) · [PDF](dag_evm_composition_controls.pdf)

## 选定优势配置：d8 chain、θ=4

该配置每个工作流有 8 个节点、7 条依赖边，节点各做 16 次相同状态更新；每个会话有 4 个工作流。NEAR 多合约路径的终点在发起交易后 8 个块高跨度完成；每个工作流记录 15 个执行结果（8 个节点调用与 7 个 system 退款 Transfer）。NEAR 同接收者对照在一条执行收据中批处理相同 4 个工作流。融合控制和独立 Stage EVM 会话均将 4 个工作流放入一笔 DAG 执行交易，entry/exit 代理交易另列；独立 Stage 路径额外实际调用每个节点对应的 Stage 合约。

## 精确复现命令

在本实验独立目录下运行（依赖首次运行时安装到 `near_runtime_20261027/vendor/`，该目录由 Git 忽略）：

```sh
cd /Users/yuanjianwen/ug_workspace/research/DAS_yjw_TSC_since_2026_April/DAS_System_v3/experiments/revision_20261027/stateful_dag_20261027

(cd near_runtime_20261027 && \
  DAG_PROFILE_REGEX='^dag-08-chain-d8-w1-theta4-nu1$' \
  DAG_REPEATS=3 DAG_OUTPUT_TAG=near_d8_theta4 \
  bash run_macos_dag.sh)

python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r dag_workflow_20261027/requirements-experiment.txt

(cd dag_workflow_20261027 && \
  ANVIL_BIN=/private/tmp/DAS_System_v3-sota-mechanisms-20261027/experiments/revision_20261027/near_runtime_20261027/vendor/foundry/anvil \
  DAG_NEAR_RESULTS=../near_runtime_20261027/near_d8_theta4_results.json \
  DAG_PROFILE_REGEX='^dag-08-chain-d8-w1-theta4-nu1$' \
  DAG_REPEATS=3 DAG_OUTPUT_TAG=das_d8_theta4 \
  python run_das_composition.py)

(cd dag_workflow_20261027 && \
  MPLCONFIGDIR=/private/tmp/das-dag-mpl-cache \
  python generate_dag_figures.py)
```

以上命令只重跑 d8/θ4 的 NEAR 与融合 EVM 配置。独立 Stage EVM 运行器为 `python run_das_distributed_composition.py`，可使用同样的 `DAG_NEAR_RESULTS`、profile 和 repeats 环境变量。EVM 运行器要求 Anvil 1.8.6，可用 `ANVIL_BIN` 指定路径或从 `PATH` 查找；上面路径指向当前隔离运行时中保留的 Anvil，不复制进实验交付目录。若从本实验包重新生成图表，Python 3.12 环境会安装 `matplotlib==3.11.2`；已有交付图表基于完整 14 配置结果。EVM 全量矩阵在 Linux x86_64 远程主机上通过 Python 3.10.12、Anvil 1.8.6 和 Solidity 0.8.24 运行；源代码分支为 `codex/das-independent-stage-run`，提交 `1d6b6aa140374f4ce168eb629fe96c235ab61aa7`。单配置重跑不会取代完整矩阵结果。
