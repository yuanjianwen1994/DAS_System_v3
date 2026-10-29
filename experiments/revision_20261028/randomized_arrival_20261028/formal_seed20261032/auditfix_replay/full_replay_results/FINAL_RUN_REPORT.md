# Seed 20261032 全量复跑结果

## 范围与审计

- 原始 schedule：`poisson_d8`，seed 20261032，paired-exponential 到达，λ=0.5/1.0；两个平台使用相同 schedule SHA-256。EVM 使用已有同 schedule 完整基线（exit code 0），本次只补跑修复后的 NEAR。
- 修复 runner SHA-256：`93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`；原 schedule SHA-256：`9684e531a7523cd2672ef91abd2a951224472550304a8c49833cac0cde823166`。
- NEAR Sandbox 2.13.4 / protocol 86；每阶段 preheat 30 s、observation 780 s。
- 全量离线审计通过：385/385 + 817/817 = 1,202/1,202 成功；0 failure、0 pending。所有轨迹校验通过；每条记录有 transaction hash、8 个 stage 状态，并与 receipt events 和 sink 状态一致。
- 完整到达轨迹中有 66 条 sink 先于 receipt tree 可查询（λ=0.5: 22；λ=1: 44）；修复 runner 均等待两种证据，不再把先到的 sink 状态误当作 receipt 已完成。
- send_tx 共 1253 次 HTTP 尝试，其中 51 个 HTTP 408 触发同 payload 重试；所有 1,202 个 workflow 最终都关联到 transaction hash 并审计成功。
- NEAR 共 18030 个 receipt 状态条目，平均 15 receipts/workflow。

## 平台指标

表中 sink p50/p95 按全 phase 的 fully audited 成功 workflow 计算，包含 warmup 工作流和观测窗结束后在 drain 阶段完成的工作流；seed 32 每 phase 的延迟样本数为 385/817。`观测窗成功` 则仅计 sink 在 780 s measurement 窗口内变为可查询的 workflow，用于计算 goodput；两列的 cohort 不同。

| 平台 | λ | Offered | 成功 workflow | 观测窗成功 | Goodput (workflow/s) | Sink p50 | Sink p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| EVM | 0.5 | 385 | 385 | 368 | 0.472 | 0.572 s | 1.106 s |
| EVM | 1 | 817 | 817 | 781 | 1.001 | 0.687 s | 1.626 s |
| NEAR | 0.5 | 385 | 385 | 369 | 0.473 | 9.614 s | 10.075 s |
| NEAR | 1 | 817 | 817 | 789 | 1.012 | 9.655 s | 10.089 s |

## 平台原生资源指标

- EVM 每 workflow transaction gas：
  - λ=0.5: mean 1,124,771 gas，p95 1,124,775 gas。
  - λ=1: mean 1,124,771 gas，p95 1,124,775 gas。
- NEAR block-level gas（全阶段采样，非单 workflow 归属）：
  - λ=0.5: gas_used p95 20,312,720,172,452 / gas_limit 1,000,000,000,000,000，utilization p95 2.03%，max 3.16%。
  - λ=1: gas_used p95 37,225,349,350,548 / gas_limit 1,000,000,000,000,000，utilization p95 3.72%，max 5.26%。
- EVM 进程树：CPU mean/p95/max 7.6/9.9/58.5%（ps 单逻辑 CPU 标度）；RSS p95/max 498/584 MiB。
- EVM host vmstat：CPU busy p95/max 3.0/6.0%；I/O wait p95 0.0%。
- NEAR 进程树：CPU mean/p95/max 20.7/29.9/61.0%（ps 单逻辑 CPU 标度）；RSS p95/max 858/966 MiB。
- NEAR host vmstat：CPU busy p95/max 7.0/18.0%；I/O wait p95 3.0%。

EVM gas 是 transaction-level；NEAR 数值是 block-level gas samples，两个平台的 gas 口径不能直接相减或按 workflow 线性换算。该 8-stage 实验也不支持把结果外推成 20-stage 的线性倍率。

## Gate、清理与归档

- 远端保存了 offline audit request 和 gate decision；decision 为 `audit_status=passed`、`allow_sandbox_teardown=true`，其 report SHA-256 为 `146f54d9ad23daaba3e4e9062c25c61217a46dc26a5308b6ef2f3169a763cec1`。
- runner exit code 0，但最终 `cleanup_status=forced_after_timeout`：NEAR teardown 超过 15 s。后续核查确认对应 PID 已不存在、home 已移除、RPC 端口拒绝连接；因此不把 runner 状态表述为 graceful cleanup。
- 远端 evidence archive SHA-256：`30063239402389d3076fb8ccaa1e4968927be94aa807fa0d4d0c8a818ce58e75`；归档 UTC 起止：`2026-10-28T10:17:08Z` 至 `2026-10-28T10:17:08Z`。最终 raw SHA-256：`f29682cc217c8fdb603c253feab78821f49e268f8c32dae834dc0f5ef09b406e`。
- Final raw audit：通过；清理状态在 raw 中保留为 `forced_after_timeout`。

## 证据文件

- Full raw: `verified_archive_contents/results/near/d8_poisson_seed20261032_near_auditfix_full_near_raw.json`
- Audit checkpoint: `verified_archive_contents/results/near/d8_poisson_seed20261032_near_auditfix_full_near_audit_checkpoint.json`
- Gate request/decision/report: `verified_archive_contents/results/near/*offline_audit*`
- CPU/vmstat: `verified_archive_contents/results/near/process_resource_samples.txt`, `host_vmstat_samples.txt`
- Archive manifest: `evidence_archive_manifest.json`
- Verified archive: `seed32_full_evidence.tar.gz`
- Gate-time full audit report SHA-256: `146f54d9ad23daaba3e4e9062c25c61217a46dc26a5308b6ef2f3169a763cec1`
- Final raw SHA-256 check: `f29682cc217c8fdb603c253feab78821f49e268f8c32dae834dc0f5ef09b406e`
