# 状态化 DAG 实验交付包

这是 DAS 大修工作的独立实验交付目录，包含通用状态化 DAG 的 NEAR Sandbox 与两种本地 EVM 实验、原始记录、receipt 动作分类审计、可重绘图表和复现说明。

## 目录

- [`dag_workflow_20261027/`](dag_workflow_20261027/)：实验报告、参数矩阵、汇总 CSV/JSON、融合单合约控制、独立 Stage + Router 实验、图表、脚本和来源记录。
- [`near_runtime_20261027/`](near_runtime_20261027/)：NEAR Rust 合约源码、版本锁定、Sandbox 运行器、WASM 成品及完整记录。
- [`supplemental_hash_chain_control/`](supplemental_hash_chain_control/)：单独的 Avalanche-local / Teleport-local hash-chain 正对照材料，不属于通用 DAG 数据。
- [`SHA256SUMS.txt`](SHA256SUMS.txt)：本目录所有交付文件的 SHA-256 校验值；清单本身不包含在自身哈希列表中。

## 结果摘要

14 种 DAG 配置各有 3 次 fresh NEAR、融合 EVM 和独立 Stage EVM 重复。独立 Stage 实验共记录 84 笔 Router 执行交易、1,956 个真实外部 Stage `CALL` frame、1,956 个节点执行事件和 2,274 个父输入事件；所有会话通过 checksum 与节点输入状态校验，84 次跨合约故障探针均回滚。融合 EVM 结果保留为单合约控制组。

实验只测单分片 Sandbox 与本地 EVM harness；entry/exit 为本地 commitment-storage 代理，不含迁移证明；NEAR 与 EVM 原生 gas 单位不可横向换算。详见 [`英文实验报告`](dag_workflow_20261027/DAG_EXPERIMENT_RESULTS.md)、[`中文结论与复现说明`](dag_workflow_20261027/结论与复现_zh.md) 和 [`实验来源及校验信息`](dag_workflow_20261027/experiment_provenance.json)。

运行时依赖、编译缓存、Node modules 和 Python 虚拟环境未复制到交付目录。**本次更新仅写入此独立交付包；未暂存、提交或推送 Overleaf，也未提交实验结果到 Git。**
