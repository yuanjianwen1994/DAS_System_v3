# d8 突发到达 Pilot 结果

本报告由远端原始 JSON 生成。Pilot 只有一个 seed，每轨迹 30 个 workflow，其中测量窗约 20 个；P95 仅作探索性描述，P99 不报告。队列曲线是已提交/调用发出数减 sink 完成数的 1 秒分箱代理值。

## 汇总

| 平台路线 | 到达 | 有效审计样本 | P50 s | P95 s | 峰值队列 | 窗口 goodput/s | 失败 | 审计待定 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| EVM independent Stage | poisson_d8 | 21 | 0.722 | 1.026 | 2 | 1.050 | 0 | 0 |
| EVM independent Stage | burst_d8 | 20 | 1.534 | 2.933 | 6 | 1.000 | 0 | 0 |
| NEAR independent contracts | poisson_d8 | 21 | 9.585 | 9.948 | 12 | 0.950 | 0 | 0 |
| NEAR independent contracts | burst_d8 | 20 | 9.555 | 9.997 | 10 | 1.000 | 0 | 0 |
| NEAR same-receiver control | poisson_d8 | 21 | 2.536 | 2.880 | 4 | 0.900 | 0 | 0 |
| NEAR same-receiver control | burst_d8 | 20 | 2.500 | 2.936 | 10 | 1.000 | 0 | 0 |

## 图与数据

- `latency_ecdf.svg`：逐 workflow offer-to-sink ECDF。
- `burst_queue_recovery.svg`：减去 Poisson 中位队列后的 1 秒队列代理值。
- `pilot_summary.csv`：路线/场景汇总。
- `pilot_workflow_samples.csv`：逐 workflow 样本与审计状态。
- `results/pilot_seed20261028/`：未改写的远端原始结果、日志与资源记录。
