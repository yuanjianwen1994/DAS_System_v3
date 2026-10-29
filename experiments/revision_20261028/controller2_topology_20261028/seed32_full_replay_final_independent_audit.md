## Material Passport

- Origin Skill: experiment-agent
- Origin Mode: validate
- Origin Date: 2026-10-28
- Verification Status: PARTIALLY VERIFIED（原始完整 raw、分母、逐 workflow 关联与顺序覆盖已独立核对；最终归档晚于 runner 收尾，离线报告未写入其输入 raw 的哈希）
- Version Label: seed32_full_replay_independent_audit_v1

## Scope

只读检查 seed20261032 完整 NEAR 复跑的最终离线审计、gate、archive manifest、gzip tar 内的 raw 和运行状态。所有逐行计算直接使用 tar 内 raw；`full_replay_results/results/near/` 下的 raw 是较早的截断传输副本，不作证据。未运行实验，未改 C1 程序或原始数据。本补充文件独立保存在 C2 目录。

## Archive and gate chain

| Artifact | UTC / SHA-256 | 核验结果 |
|---|---|---|
| Offline audit request | 2026-10-28 10:03:26.367Z；seed 20261032，两个 phase 均 `audit_status=complete`、pending=0，分别 385、817 条 | request 引用的 schedule 和 runner 哈希与 manifest 一致 |
| Offline audit report | tar member mtime 10:12:44Z；SHA-256 `146f54d9ad23daaba3e4e9062c25c61217a46dc26a5308b6ef2f3169a763cec1` | `audit=pass`，总计 audited_success=1202；gate 与 archive manifest 引用同一 report 哈希 |
| Gate decision | tar member mtime 10:13:38Z | `audit_status=passed`、`allow_sandbox_teardown=true`；晚于 report、早于 runner exit |
| Runner result/raw | 2026-10-28 10:13:53Z，exit code 0 | 最终 raw 的 `cleanup_status=forced_after_timeout`。审计获准后 runner 才结束，但 teardown 不是干净完成状态，不能仅凭 exit 0 写成 cleanup complete |
| Evidence archive | 2026-10-28 10:17:08Z（manifest start/finish）；archive SHA-256 `30063239402389d3076fb8ccaa1e4968927be94aa807fa0d4d0c8a818ce58e75` | tar 实际哈希等于 manifest。tar 内最终 raw SHA-256 `f29682cc217c8fdb603c253feab78821f49e268f8c32dae834dc0f5ef09b406e` 等于 manifest `final_raw_sha256`；`verified_archive_contents` 解包 raw 也相同 |

Request → offline report → gate pass 的顺序成立；gate pass 在 runner 结束前 15 秒左右写入。最终 tar 本身则在 runner 结束约 3 分钟后才生成，故若“归档后 teardown”指必须先形成最终 tar，这个顺序没有满足。实验原始 raw/checkpoint 已在 10:03:26Z 进入等待 gate 阶段，并由 10:12:44Z 的离线审计读取；因此“审计证据先于 teardown”成立，而“最终 tar 先于 teardown”不成立。两者应分开表述。

本地 `results/near/d8_poisson_seed20261032_near_auditfix_full_near_raw.json` 只有 6,009,344 bytes、SHA-256 `1adc0108e6acc3ed7cf75c011f0427779e1e8ce9c28d398cfab6f71cdac6285a`，与 tar 内 20,586,438-byte raw 不同，确为较早/截断副本；不能使用该文件替代归档 raw。

## Independent row-level audit of archived raw

以下计数由我直接读取 tar member `results/near/d8_poisson_seed20261032_near_auditfix_full_near_raw.json` 重新计算，并非照抄离线报告：

| Phase / offered rate | Rows | 成功且完整审计 | 每行收据数 | 交易 hash / send_tx 回连 | HTTP 重试 |
|---|---:|---:|---:|---:|---:|
| 0 / 0.5 workflow/s | 385/385 | 385/385 | 15（共 5,775） | 385/385 唯一交易 hash 匹配 outcome；关联 send_tx 返回 HTTP 200 且 request/response RPC id 相同 | 18 个 HTTP 408；18 个相同 payload 后续 attempt=2、HTTP 200 且有交易 hash |
| 1 / 1.0 workflow/s | 817/817 | 817/817 | 15（共 12,255） | 817/817 唯一交易 hash 匹配 outcome；关联 send_tx 返回 HTTP 200 且 request/response RPC id 相同 | 33 个 HTTP 408；33 个相同 payload 后续 attempt=2、HTTP 200 且有交易 hash |
| Total | 1,202/1,202 | 1,202/1,202；pending=0 | 18,030 | 1,202/1,202 workflow 均有唯一交易关联 | 1,253 send_tx ledger entries；51 次 HTTP 408 → 同 payload 成功重试 |

另外逐 workflow 检查了唯一 workflow id、receipt tree 已完成、sink 可查询、15 个 receipt id 与 15 个 receipt status、8 个 node event 与 8 个 stage state；逐节点验证 session/workflow/node、checksum、16 operations 和 input_count。两阶段全部 1,202 条均通过，没有发现对不上或缺失的行。

## Sink-before-receipt-tree ordering coverage

直接比较归档 raw 的 `sink_first_queryable_at` 与 `receipt_tree_completed_at`，并复核单调时钟耗时字段：

- Phase 0：22/385 条 sink 先可见；这些行之后均记录 `receipt_resolution_status=completed` 和 receipt tree 完成。receipt 相对 sink 的等待差为 1.366–477.280 ms。
- Phase 1：44/817 条 sink 先可见；这些行之后也均记录 receipt tree 完成。等待差为 0.514–505.105 ms。
- 合计 66 条真实 sink-first 样本均最终有完整 receipt tree、15 个收据、交易哈希关联及成功的节点状态审计；没有发现 sink-first 后提前返回的行。

这说明**完整运行本身真实覆盖了修复后的 sink-first 分支**。但在归档树和 C1 任务可见结果中，没有找到独立、确定性命名的“顺序回归测试”脚本/结果。现有 `offline_audit_seed32.py` 会检查成功行同时具有 receipt 完成和 sink 可查询状态（约 91–110 行），但不显式断言 sink 时间早于 receipt 时间，也不要求存在 sink-first 样本。因此它的 `audit=pass` 不是单独的顺序回归测试；66 条分支覆盖来自 full replay raw，本补充中的独立时间比较确认了这一事实。

## Remaining evidence limitations

1. Gate 中的 `report_sha256` 绑定了审计报告，但离线审计报告本身只列 schedule/runner SHA，不包含其输入 raw 的 SHA。最终 archive manifest 固定了最终 raw SHA；我对最终 tar raw 做了独立逐行核验并与报告的 phase/总数交叉对照，但现有元数据没有形成“gate 所审输入 raw 哈希 → 最终 raw 哈希”的直接字段链。
2. 最终审计 JSON 的 `near_cleanup_status_at_audit=forced_after_timeout` 是收尾后的最终状态；gate 前的 `offline_audit_report.json` 对应 runner 等待状态 `awaiting_offline_audit`。这与 gate 在 teardown 前通过不冲突，但 forced cleanup 应单独报告。
3. 未发现另一份独立顺序回归结果；若要求可重复的自动化回归证明，现有 66 条 full-run 运行时覆盖不能替代一个显式断言 `sink_first < receipt_tree_done` 且之后 receipt wait 未返回的回归夹具。

## Conclusion

完整归档的 seed32 数据身份、1202 条分母、receipt/state 覆盖与交易/HTTP 重试关联均通过独立核验。实际 full replay 有 66 条 sink-first 行且都等到 receipt tree 完成，足以证实这次真实负载中修复分支有效。审计报告/gate 在 runner 收尾前通过；最终 gzip evidence archive 则在收尾后生成，且 cleanup status 为 `forced_after_timeout`。独立 audit helper 没有时间先后断言，归档中也未找到单独顺序回归工具结果；这属于自动化回归证据缺口，不是否定本次 full-run 的真实分支覆盖。

## Reproduction pointers

- Full replay archive: `DAS_System_v3/experiments/revision_20261028/randomized_arrival_20261028/formal_seed20261032/auditfix_replay/full_replay_results/seed32_full_evidence.tar.gz`.
- Archive manifest and post-run audit summary: `.../full_replay_results/evidence_archive_manifest.json`, `.../full_replay_results/FINAL_OFFLINE_AUDIT.json`, and `.../full_replay_results/FULL_OFFLINE_AUDIT.json`.
- Gate request/decision and report are members under `results/near/` inside the tar. Their archived UTC mtimes are 10:03:26Z, 10:13:38Z, and 10:12:44Z respectively.
- Runner code for pre-gate raw/request persistence and gate-before-teardown ordering: `formal_seed20261032/input/run_randomized_near_auditfix.mjs`:748–773; teardown and final status writes: 775–816. The 120-second receipt wait condition is at 371–409.
- Offline auditor logic: `auditfix_replay/offline_audit_seed32.py`:59–119 checks schedule counts/trace, row classification, tx-hash ledger presence, 8-state receipt agreement, and both receipt/sink resolution. It contains no comparison of `sink_first_queryable_at` against `receipt_tree_completed_at`; its cleanup gate validation is at 121–124.
