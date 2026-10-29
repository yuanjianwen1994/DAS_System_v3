## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-10-28
- Verification Status: PARTIALLY VERIFIED（短验证与启动器静态审计通过；全量远端运行的最终离线审计决定未在本地材料中）
- Version Label: seed32_auditfix_independent_v1

## Scope and evidence identity

只读核对 C1 seed20261032 修复 runner、16/16 短验证 raw、短验证启动器及全量启动器。未改 C1 文件、结果或进程；未启动新运行。

- Runner: `DAS_System_v3/experiments/revision_20261028/randomized_arrival_20261028/formal_seed20261032/input/run_randomized_near_auditfix.mjs` — SHA-256 `93c51c2152fccabd229bbc7e0fe6ddabe1a319a8f34d11aa7dc9447c50fc68fc`。
- Short raw: `DAS_System_v3/experiments/revision_20261028/randomized_arrival_20261028/formal_seed20261032/auditfix_replay/staged_verify2_results/results/d8_seed20261032_auditfix_staged_verify2_near_raw.json` — SHA-256 `ab41abd14af84fa7713b30b135bf320a359b34a95c17a25f60b3b503e632e4e2`。
- Short manifest SHA-256: `96a9c00ac5359910117e514ca57bc668e88c07d6ef316be8a91b483b48557c11`。

## Findings

| 检查项 | 结果 | 证据与边界 |
|---|---|---|
| Sink 先可见时是否继续等 receipt | 通过 | 两个运行分支都在 `receiptTreeDoneAt === null || sinkQueryableAt === null` 时轮询，直到两者均有结果或 `receiptDeadlineEpochMs` 到期；参见 runner:371–389、391–409。短验证 16/16 的 `receipt_resolution_status=completed`、receipt tree 和 sink 均有记录；按时间戳复核，sink 先于 receipt tree 的记录为 0/16。 |
| 真正的 receipt deadline | 通过 | 每个 workflow 单独设置 `Date.now()+RECEIPT_TIMEOUT_MS`；sink 读取受剩余 deadline 限制，deadline 到期时标记 `deadline_expired`；参见 runner:352–365、371–409。短启动器设 120000 ms，见 `auditfix_replay/staging_package_v2/package/run_short_verify.sh`:46–47。 |
| HTTP、重试与交易关联 | 通过 | `send_tx` 记录 payload fingerprint、相同 payload attempt/retry count、HTTP 状态、RPC id、响应交易 hash 与错误；按交易 hash 回连请求；参见 runner:181–224、241–246、412–426、448–500。短 raw 中 16/16 workflow 均有匹配交易 hash；16 条关联的 `send_tx` HTTP 状态均为 200、same-payload attempt 均为 1；HTTP error ledger 为空。 |
| 逐节点状态与 receipt 事件 | 通过 | 短 raw 两阶段各 8 条，共 16/16 fully audited，pending 为 0；全部 workflow 有 8 个节点事件和 8 个 stage state，状态 checksum、operation_count、session/workflow、node id 与 input_count 检查通过。另抽两阶段首/中/末共 6 条，逐节点复核均 8/8 匹配。源码比较逻辑见 runner:427–447、584–617。 |
| 计时边界与轨迹 | 通过（限该短验证） | phase 起点、预热/观测起点及窗口终点分别记录，按单调时钟安排 offsets 并校验 planned timestamp；见 runner:521–566。raw 两阶段均为 20 秒观察窗，schedule trace validation 均通过，8/8 offset 进入每阶段。不能据此替代全量 780 秒窗口结果。 |
| 短验证的离线审计/清理顺序 | 范围缺口 | 短启动器没有设置 `STRESS_AUDIT_GATE_PATH`（见 `staging_package_v2/package/run_short_verify.sh`:40–48）；短 raw 的 `offline_audit_request_path`、`offline_audit_decision`、`preserved_sandbox` 都为空，`cleanup_status=complete`。runner 只有设置 gate 时才等外部决定，见 runner:748–774；随后清理 sandbox，见 runner:775–816。因此短验证证明了进程内的 receipt/state 审计通过，但没有证明外部离线审计归档在 sandbox teardown 前通过。 |
| 全量启动器是否启用审计门 | 静态配置通过；运行完成状态未核实 | `auditfix_replay/run_full_near_from_stable_stage.sh`:48–53 配置 30 秒预热、780 秒观测，并设置 `STRESS_AUDIT_GATE_PATH` 和 3600000 ms 等待；runner:748–773 会先写 gate request、等待决定，只有 `audit_status=passed` 且 `allow_sandbox_teardown=true` 才允许 sandbox teardown，否则保留 sandbox。该脚本的配置可验证；但对应全量远端结果目录不在本地工作区，故不能报告本次全量运行是否收到通过决定、是否完成外部归档或最终 sandbox 状态。 |

## Caveat

`boundedRead()` 用 `Promise.race` 实现读取超时，源码未给底层 `stage.view()` 传入取消信号（runner:98–124）。超时后调用方会继续重试/结束等待，但原 RPC view 操作不一定同步取消。短验证没有观察到由此产生的审计失败；在繁忙运行中是否积累未完成读取，需由运行记录确认。此点不是已证实的 seed32 数据错误。

## Audit conclusion

现有短验证支持修复状态机、交易关联和逐节点审计在 16/16 样本上按预期工作。它不具备外部审计 gate，故其 `cleanup_status=complete` 不能单独满足“外部审计归档通过后再 teardown”的证明要求。全量启动器已配置该 gate，源码也按通过决定控制 sandbox teardown；全量远端 gate request/decision、最终 raw 与状态文件必须回收后，才能把全量运行的审计归档和清理顺序标为已验证。
