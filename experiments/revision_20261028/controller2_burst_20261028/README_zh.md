# 控制器 2：d8 突发到达探索

本目录执行与控制器 1 不重叠的路线 B。输入 runner、合约、profile、轨迹和结果均保存在本目录；远端对应 `/home/starshell/das_controller2_burst_20261028`。远端共享的 Python/Node 运行时和依赖只读使用，NEAR `HOME`、`TMPDIR`、缓存、Anvil 工作目录和 PID 文件各自隔离。

## Pilot 设计

- 通用 d8、8 个独立 Stage 合约、每节点 16 次操作、沿用真实前驱 checksum、唯一 workflow/session ID；sink 预读为空，并在执行后核对节点状态与 receipt 事件。
- 每种到达轨迹 30 秒、共 30 个 workflow：10 秒连续暖机，随后 20 秒观测；两种轨迹总数、总时长和平均 offered rate 相同。
- `poisson_d8` 是固定样本数条件下的齐次 Poisson 到达（30 个均匀次序统计量），`burst_d8` 每 10 秒在 2 秒内以 5 workflow/s 投放 10 个，其余 8 秒不投放。测量窗包含两个完整突发并留出恢复时间。
- 两种轨迹共享随机种子与相对出块随机相位。EVM 独立 Stage、NEAR 独立合约和 NEAR 合法 same-receiver 控制各自使用相同 offset 列表及 SHA-256。每个观测按 1 秒分箱记录 offered、提交/接受代理指标、完成和队列。EVM 的 accepted 代理是交易 hash 返回，NEAR 的 accepted 代理是客户端已发出调用；二者不等同于链上成功接纳。
- 突发恢复时间以同一 runtime 的 Poisson 测量窗队列中位数作为正常在途基线：从一轮最后一次 offer 起，寻找队列代理回到该基线的首个 1 秒采样点，并注明是否早于下一轮突发；若无下一轮或采样窗结束仍未回基线，则如实标记。
- Pilot 样本量不足以支持 P99 或稳定尾部倍率；只有通过逐 workflow 状态及 receipt 审计后才纳入比较。失败、未完成和审计待定分别保留。

## 资源与边界

远端可见 20 CPU、无 cgroup CPU/内存硬配额、启动前约 27 GiB 可用内存。本 pilot 的 EVM/Anvil 或 NEAR 节点与压测器均通过 `taskset -c 16-19` 限定为 4 个 CPU；控制器 1 的亲和性不变。CPU 集合并非对控制器 1 的硬隔离，因此这批结果标为并行探索；有判别力的结果须在控制器 1 停止后独占复测。资源快照、`vmstat`、进程 PID、runner 日志和原始结果留在远端结果目录并拉回本地。

本实验是受控 sandbox 内部执行，不代表生产网络、原生跨 VM 迁移或吞吐优势。Anvil 仅使用内存状态，pilot 会将其状态 dump 到本任务专属目录；NEAR sandbox 从本任务的 home/TMP/cache 环境启动。端口由 runner 动态分配。

远端启动入口：`run_pilot_remote.sh`。本次生成的输入轨迹为 `input/burst_d8_seed20261028.json`。

## 三种子延长暖机筛选

正式脚本入口 `run_formal_remote.sh`，远端结果写入 `results/formal/`。每个 seed 的两种到达轨迹均有 90 个 workflow：连续前 30 秒暖机并持续投放，接着 60 秒测量，不在边界停投或排空。Poisson 轨迹仍是 90 个固定计数的均匀次序统计量；burst 轨迹每 10 秒一个周期，其中 2 秒内以 5 workflow/s 投放 10 个。测量窗包含 6 个 burst 周期；预期约 60 个测量样本/场景/路线。每个 seed/路线分别汇报，P95只作筛选，不称稳定尾分位数；每 seed 少于 100 个有效样本时不报 P99。

队列恢复按 1 秒 elapsed-time 分箱与同路线 Poisson 在同一秒的队列比较；连续两个采样点不高于匹配基线才算恢复。每周期结果以秒级精度汇总，单次时间采样误差约 ±1 秒。每轮周期和 seed 是汇总层级，不能将 workflow 当成独立重复。所有路线的调用偏移和轨迹哈希配对；EVM tx hash 返回与 NEAR call-issued 仍是不同的提交代理。

当前延长筛选在授权远端 `100.65.161.48:32777` 运行，容器有效 CPU 为 0–19；本任务 driver 和节点使用 `taskset -c 16-19`。控制器 1 的运行亲和性没有改动，亲和性仅提供软限制而非独占；与其重叠的正式结果标为并行探索，结束后对有判别力的点做独占复测。通过本机默认 SSH 端点误连到另一主机的启动失败日志保存在该端点的独立结果目录，未产生实验负载，也不纳入本路线分析。seed 20261103 的 EVM 首次启动遇到隔离 HOME 无 solc 缓存且下载请求 SSL EOF；该次退出码和日志保留，使用已有 solc 0.8.24 缓存单独重试，不计作工作流失败。
