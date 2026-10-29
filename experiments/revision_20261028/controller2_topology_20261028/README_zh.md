# 路线 C：8 节点分发—并行处理—汇合拓扑

## 目标与比较边界

首轮只跑一组远端配对 pilot：8 个独立状态节点，每节点 16 次相同计算。对照是 8 节点链（7 条依赖边）与 1 根节点分发给 6 个独立分支、最终汇合节点消费六个真实结果的拓扑（12 条边）。两者节点数、计算函数和输入生成规则一致，但边数和依赖形状不同；这不是纯深度因果比较。

NEAR 使用已验证的 Stage 源码：多个子 Promise 可并行执行，join 通过真实父节点结果去重并校验。每个图同时运行单接收器合约控制。EVM 使用独立 `DAGStage` 合约和真实外部 `CALL`，由一个 router 按拓扑顺序调用。NEAR 的异步并行能力可能缩小甚至逆转原链结果；保留实际结果。

## 运行顺序与收敛规则

1. 远端单次 pilot；实际 NEAR RPC 版本、protocol version、运行中二进制路径和 SHA-256 必须先核实并与预期匹配，随后才部署合约。
2. 核对两端每节点 checksum、每条父边输入、节点/边事件、sink 状态和完整 receipt tree；同接收器结果逐节点对齐。
3. 若拓扑确实改变路线间关系，再进行三个独立重复；否则报告 pilot 的无判别结果并停止扩展。
4. 拓扑结果收敛后，才做一次独立的中间节点失败控制。记录部分状态和失败语义；当前没有等价补偿实现时，明确写“未实现/未测”，不虚构补偿交易数或等待时间。

正式结果必须分列每个运行时原生 gas、receipt/边消息数、可测的 calldata/event 字节、节点存储写入、延迟、二进制及 RPC 版本。EVM gas 与 NEAR gas 不换算。吞吐不是单 workflow pilot 的估计目标。

## 远端资源边界

工作目录固定为 `/home/starshell/das_controller2_topology_20261028`；独立 `HOME`、`TMPDIR`、结果目录和 PID 文件。Anvil 动态分配端口。driver 和节点使用 CPU 16–19 软绑定；20 CPU 容器没有独占配额，重叠任务时按并行探索报告。只管理本目录启动的 PID，不清理其它任务。

## 静态审计记录

旧 burst NEAR runner 在普通审计路径的轮询条件同时要求“receipt tree 未结束”和“sink 未可见”。sink 若先可查询，循环会退出；下一行又在 receipt tree 尚未结束时写入 `receipt_audit_pending_timeout`。因此这个 timeout 标签不能单独证明业务失败或真实等满期限。本路线使用独立 DAG runner，等待 `callRaw` 的完整 receipt tree 后再做状态审计，并在 sandbox teardown 前落盘。
