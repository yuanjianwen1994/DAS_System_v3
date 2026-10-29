# 压力试验异常调查

## 结论边界

此处存放的 NEAR 到达率试验是方法调试数据，不是论文性能结果。首轮 open-loop 数据含工作流 ID 冲突；几毫秒 sink 查询读数可能来自同一 Sandbox 中先前工作流的状态，不能计为新请求完成。交易和收据元数据当时也没有完整留存，无法用该批记录判定具体失败回执或 nonce 行为。

## 已确认的 ID 冲突

首轮脚本按阶段生成 `session_id`，再由 `workflow_id = session_id + i + 1` 派生。相邻 warmup/measurement 阶段的编号区间会重叠：例如 warmup 的 workflow ID 会与 measurement 阶段的 workflow ID 重合。由于两阶段共用一个 Sandbox，sink view 可在新请求完成前读到旧状态。首轮记录中有 `2.4–3.4 ms` 的假快速 sink 读数，而 `callRaw` 约 `2.45–3.02 s` 后报告交易/收据树失败。首轮 summary 的成功/失败计数不用于容量、延迟或吞吐结论。

- [`invalid_open_loop_pilot_v1/`](invalid_open_loop_pilot_v1/)：保留首轮压力试验的原始文件；明确作废。
- [`duplicate_id_diagnostic_v1/`](duplicate_id_diagnostic_v1/)：保留被重复 ID 检查提前终止的诊断记录；仅含一个成功 warmup 请求，不能说明并发行为。

## 唯一 ID 重叠诊断

修正后的诊断为每个请求分配唯一 session/workflow ID，并记录发送账户、交易哈希、回执结果、节点事件和各合约状态。1 个 warmup 请求及 3 个按每秒 1 次计划发出的并发测量请求都通过核验：ID 唯一；测量期由 3 个不同账户发送；每笔交易有 7 个成功回执；4 个 `DAG_NODE` 事件分别与对应状态及 checksum 匹配；没有失败回执。sink 首次可查时间约 6.02–6.04 秒，交易回执树完成时间约 6.01–6.03 秒。

- [`overlap_near_d4_v2/`](overlap_near_d4_v2/)：本地保留的完整诊断 raw、summary、stdout 和资源样本。
- [`overlap_near_d4_v3/`](overlap_near_d4_v3/)：显式验证每个请求提交前 sink 为 null，并记录确切 transaction nonce、公钥、signer/receiver、交易哈希及所有 receipt 状态。4/4 条记录通过：session/workflow/hash 唯一，7/7 receipts 成功，signer 与 sender 匹配，transaction hash 与 outcome 一致，节点状态/checksum 与事件匹配。

## 修正 ID 后的多速率试跑

[`unique_open_loop_pilot_v2/`](unique_open_loop_pilot_v2/) 的测量阶段分别发出 5、10、20、40 个工作流，对应 0.25、0.5、1、2 次/秒。四阶段均全部成功，sink 与四个节点状态/checksum 匹配，93 条记录中的 session ID、workflow ID、交易哈希均唯一；未见失败回执。最高负载阶段峰值并发约 13 个请求。

这版 raw 没有逐请求保存提交前 sink 与 transaction nonce，因此只作为 ID 修复后的内部压力试跑，不用于外部性能结论。带完整前置状态和 transaction 元数据的同一多速率复跑保存在 [`unique_open_loop_pilot_v3/`](unique_open_loop_pilot_v3/)。

完整记录复跑共 93 条（18 warmup、75 测量）：每个请求提交前 sink 都为空，session/workflow/hash 唯一；每笔交易的 nonce、公钥、signer 与 sender 均记录并核对；同一公钥的 nonce 连续递增且同一 key 没有并发调用；所有交易哈希与 outcome 匹配；93/93 工作流成功，7/7 receipts 全成功，节点事件与各 stage 状态及 checksum 一致。测量阶段 0.25/0.5/1/2 次每秒分别完成 5/5、10/10、20/20、40/40 个工作流；按调用区间计算的并发峰值为 2、4、7、13。sink 延迟中位数分别为 5.11、5.10、6.00、5.58 秒；观测范围分别为 5.10–6.12、5.09–6.11、5.08–6.11、5.06–6.10 秒。

这些数字是单一远程主机、NEAR Sandbox 2.14.0 / Protocol 88、4 节点链式工作流和约 1 秒区块周期下的短时压力试跑。最高只测到 2 次/秒，所有请求仍成功，所以它没有找到饱和点，也不能据此给出系统容量或与 EVM 的性能比较。

这些短诊断和压力试跑仅确认当前 Sandbox 路径下的执行一致性。样本与负载范围有限，不据此声称系统容量、饱和点或任何平台性能优势。
