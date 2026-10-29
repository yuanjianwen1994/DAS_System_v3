# NEAR 与 EVM 深度链工作流计时

## 目的与口径

本目录评估 d2、d4、d8 链式 DAG 的时间表现，和 `advantage_overview_20261027` 中的提交单元计数分开。计时从客户端开始提交执行调用，到首次通过状态查询接口读到 sink 节点结果；初始化、部署、入场/退场代理和故障注入不计入此区间。两端都配置约 1 秒的区块/出块周期。

样本只覆盖 `theta=1`，每个平台、每个深度计划使用 3 个全新运行时、每运行时 2 个会话。对照仅适用于本地沙箱设置和所测链式工作流，不能外推为一般的吞吐量或协议性能结论。

## 远程运行环境

- 单台远程 Linux 主机运行 NEAR Sandbox 与 Anvil。
- NEAR Sandbox 2.14.0，Protocol 88；Node.js 24.22.1；NEAR Sandbox JavaScript 包 0.3.4。
- Anvil 1.8.6；EVM 区块间隔设为 1 秒。
- 远程操作系统为 Ubuntu 22.04。NEAR Sandbox 二进制使用实验目录中的 glibc 2.39 动态加载器与兼容库启动；系统 glibc 未改动。这个运行时差异需要在解释性能时保留。

## 测量边界与限制

计时器分别在 NEAR `callRaw` 和 EVM `transact` 调用前启动。随后 NEAR 通过 near-workspaces 的 `stage_result` view 轮询 sink 状态，EVM 通过 Web3 对 Stage `nodeResult` 状态轮询。两者都记录提交开始时间、首次查询到 sink 的时间和单调时钟时长。轮询间隔为约 25 ms；观测到的时间包含 RPC/客户端调用开销和区块等待。

本计时不代表上述协议级提交单元数的倒数或比例。NEAR 的节点收据独立持久化；EVM 的 Stage 写入由一笔 Router 交易原子提交，失败时整体回滚。因此成功终态相同并不意味着失败语义相同。

`pilot/` 保存每个深度单次试跑的 summary 与 raw JSON。完整重复完成后会在此目录保存全量记录、运行脚本快照、聚合表和逐会话审计。
