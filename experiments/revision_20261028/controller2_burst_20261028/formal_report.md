# d8 突发到达多种子结果

## Material Passport

- ID：controller2_burst_d8_formal_20261028
- 类型：实验结果
- 状态：3 seed筛选完成；受并行资源限制，属于探索性结果。
- 执行：远端 `/home/starshell/das_controller2_burst_20261028/run_formal_remote.sh`；seed 20261103 EVM补跑使用 `run_seed3_evm_retry_remote.sh`。
- 输出：本目录的 `formal_burst_boundary.svg`、`formal_seed_summary.csv`、`formal_workflow_samples.csv` 与 `results/formal/` 原始记录。
- 异常：seed3 EVM首次在下载Solidity编译器时遇到SSL错误，未产生负载；使用已有0.8.24缓存重跑成功。原批处理在最后一个路线结束后有收尾shell错误；各路线退出码与原始数据已记录，最终资源快照在核对进程结束后补记，原日志保留。

正式重复数：3 个独立 seeds。每 seed、每场景测量窗约 60 个 workflow；P95 逐 seed 报告，P99 仅当该 seed 有至少 100 个有效测量样本时输出。Poisson 轨迹是在固定总数条件下生成的齐次 Poisson 次序统计量，不能解释为无条件 Poisson 计数过程。

## 筛选结果

| 路线 | 突发/Poisson P50比值，中位数（范围） | 探索性P95比值，中位数（范围） | 下一次突发前恢复的周期数 |
|---|---:|---:|---:|
| EVM独立Stage | 2.66 (2.42–2.81) | 1.86 (1.84–2.56) | 15/15 |
| NEAR独立合约 | 0.99 (0.99–1.00) | 0.99 (0.99–1.00) | 11/15 |
| NEAR同接收器控制 | 0.99 (0.99–1.00) | 1.00 (0.99–1.01) | 15/15 |

九个路线/seed数据单元的工作流失败数合计为 0，审计待定合计为 0。EVM seed 20261103 的首次启动失败发生在Solidity编译器缓存下载阶段，没有产生实验负载；随后用同版本缓存重跑成功。

EVM独立Stage路线在三个seed中都出现更高的burst中位时延；两条NEAR路线的P50/P95与Poisson接近，但NEAR独立合约路线只有11/15个存在后续burst的周期在下一次burst前回到同秒Poisson基线，seed3出现连续周期叠加后的延后恢复。NEAR同接收器控制为15/15周期在下一次burst前恢复。故结果支持EVM路线对突发更敏感，并显示NEAR独立路线可能积累跨周期队列；它不构成通用DAS吞吐结论。P95每条件约54–64个样本，仅是筛选指标。

不同 seeds 是重复单位；summary 保留每个 seed/路线/场景的P50/P95、queue、完成率和审计结果。正文不把跨 seeds 的 workflow 简单视作 iid 样本。NEAR接受率是调用发出率代理；EVM接受率是tx hash返回率。队列恢复逐观测秒与同路线、同elapsed-time的Poisson队列对比；需连续两个采样点不高于该匹配基线才记作恢复。每轮burst与周期统计按1秒采样精度报告，时间误差约±1秒，不给单次采样报伪精确小数。

资源边界：driver与节点使用4个CPU软绑定；实验与控制器1在同一远端容器内有重叠运行，未实现硬隔离。将这批结果视为并行探索，不能与独占条件结果混合，也不能据此声称资源独占下的精确性能。

## 图与数据

- `formal_burst_boundary.svg`：左侧展示每 seed 的配对 burst/Poisson P50 与探索性 P95 比值；右侧展示按同秒 Poisson 队列基线恢复的每 seed 周期中位数，误差线为1秒采样分辨率。
- `formal_seed_summary.csv`：每 seed/路线/场景结果与周期恢复计数。
- `formal_workflow_samples.csv`：逐 workflow 数据。
- `results/formal/`：远端原始结果、日志、资源样本和复现输入。
