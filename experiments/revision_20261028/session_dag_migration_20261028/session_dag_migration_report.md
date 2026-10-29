# 实际 d8 Stage DAG + session 迁移控制 pilot 汇总

三次独立远端重复，均为 1 秒 Anvil 出块设置；这是受控 pilot，不是公链时延或原生跨 VM 端到端结果。每轮使用新 Anvil 与新部署账本。两个逻辑域在同一 EVM 上，迁移由本地可信 ECDSA 测试签名者授权。θ 个独立 workflow 可同一交易预提交；每个 workflow 内部的 8 个 Stage 按前驱 checksum 依赖顺序执行。

每个 session 按 8θ+1 注资，每个 Stage 扣 1 个余额单位，最终保留 1。迁移组必须完成迁入 receipt settlement 才能进入 session；必须执行完全部 θ 个 workflow、审计所有 DAG sink 并关闭 session 后才能迁出最终余额。

| Arm | θ workflows/session | Repeats complete | Internal DAG median (s) | DAG tx gas median | Entry median (s) | Exit median (s) | Full journey median (s) | Journey / workflow (s) | Receipt binding checks | DAG nodes audited | Predecessor bindings | Terminal accounting | Fail / incomplete |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| migrated_dag | 1 | 3/3 | 1.036 | 1,117,008 | 1.994 | 1.990 | 7.016 | 7.016 | 6/6 | 24 | 21 | pass | 0 / 0 |
| migrated_dag | 2 | 3/3 | 1.038 | 2,063,031 | 1.989 | 1.989 | 7.113 | 3.557 | 6/6 | 48 | 42 | pass | 0 / 0 |
| migrated_dag | 4 | 3/3 | 1.044 | 3,955,180 | 1.990 | 1.989 | 7.111 | 1.778 | 6/6 | 96 | 84 | pass | 0 / 0 |
| migrated_dag | 8 | 3/3 | 1.056 | 7,739,818 | 1.989 | 1.989 | 7.011 | 0.876 | 6/6 | 192 | 168 | pass | 0 / 0 |
| same_domain_dag | 1 | 3/3 | 1.036 | 1,117,020 | 0.000 | 0.000 | 3.968 | 3.968 | 0/0 | 24 | 21 | pass | 0 / 0 |
| same_domain_dag | 2 | 3/3 | 0.938 | 2,063,055 | 0.000 | 0.000 | 4.072 | 2.036 | 0/0 | 48 | 42 | pass | 0 / 0 |
| same_domain_dag | 4 | 3/3 | 0.943 | 3,955,228 | 0.000 | 0.000 | 4.062 | 1.016 | 0/0 | 96 | 84 | pass | 0 / 0 |
| same_domain_dag | 8 | 3/3 | 1.058 | 7,739,914 | 0.000 | 0.000 | 4.051 | 0.506 | 0/0 | 192 | 168 | pass | 0 / 0 |

## 读数

24/24 sessions completed, failed=0, incomplete=0. Migrated sessions passed 24/24 receipt-envelope/settlement bindings. The 3 repeats audited 720 Stage results and 630 stored predecessor edges; terminal balance conservation passed for every session.

The direct same-domain best-case and migrated control both execute the same d8 chain inside one DAG transaction per session. At this 1-second local block setting, internal DAG phase remains about one block interval as θ grows, because θ independent workflows are submitted together. The DAG transaction gas rises from about 1.117M at θ=1 to 7.740M at θ=8 (about 6.93×), even though wall-clock time is near the one-second block-inclusion floor. The migration route adds entry and exit receipt transactions; its full journey therefore stays near seven one-second blocks while the direct route stays near four. Per-workflow journey time falls with θ through amortization of this fixed receipt overhead. This same-EVM control gives the destination no locality or execution advantage, so it cannot identify a migration break-even point.

Do not interpret θ=8 as an 8× performance gain: θ counts independent d8 workflows sharing a session, not a throughput speedup. This result does not test cross-contract execution across VMs or a native bridge. The separate NEAR internal-DAG results must remain a separate runtime arm; these values must not be concatenated into a synthetic native end-to-end result.

## Reproducibility

Source SHA-256 values are recorded in each raw JSON. Runtime/compiler versions and chain IDs are retained per repeat. `formal_1s_r1/` through `formal_1s_r3/` contain the unmodified per-session raw records, including transaction IDs, receipt IDs/envelopes, checksums, predecessor values, balances, and timing fields.

## Supplementary d20 pilot — separate from the d8 three-repeat matrix

One fresh-Anvil repeat at θ=1/2/4/8; descriptive pilot only and not pooled with the d8 results.

| Arm | θ workflows/session | Internal DAG (s) | DAG tx gas | Full journey (s) | Stage results audited | Predecessor bindings | Receipt bindings | Final accounting | Failed / incomplete |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| migrated_dag | 1 | 0.940 | 2,850,997 | 6.979 | 20 | 19 | 2/2 | pass | 0 / 0 |
| migrated_dag | 2 | 1.047 | 5,381,168 | 6.944 | 40 | 38 | 2/2 | pass | 0 / 0 |
| migrated_dag | 4 | 1.065 | 10,442,090 | 8.043 | 80 | 76 | 2/2 | pass | 0 / 0 |
| migrated_dag | 8 | 1.094 | 20,566,179 | 9.029 | 160 | 152 | 2/2 | pass | 0 / 0 |
| same_domain_dag | 1 | 0.938 | 2,851,009 | 4.034 | 20 | 19 | 0/0 | pass | 0 / 0 |
| same_domain_dag | 2 | 1.049 | 5,381,192 | 3.990 | 40 | 38 | 0/0 | pass | 0 / 0 |
| same_domain_dag | 4 | 1.064 | 10,442,138 | 5.061 | 80 | 76 | 0/0 | pass | 0 / 0 |
| same_domain_dag | 8 | 1.097 | 20,566,275 | 6.046 | 160 | 152 | 0/0 | pass | 0 / 0 |

### Matched-depth descriptive comparison

| θ | d8 same-domain journey median (s) | d20 same-domain (s, 1 run) | d20/d8 same-domain DAG gas | d8 migrated journey median (s) | d20 migrated (s, 1 run) | d20/d8 migrated DAG gas |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3.968 | 4.034 | 2.55× | 7.016 | 6.979 | 2.55× |
| 2 | 4.072 | 3.990 | 2.61× | 7.113 | 6.944 | 2.61× |
| 4 | 4.062 | 5.061 | 2.64× | 7.111 | 8.043 | 2.64× |
| 8 | 4.051 | 6.046 | 2.66× | 7.011 | 9.029 | 2.66× |

The d20 pilot completed 8/8 sessions; 600 Stage results and 570 predecessor edges were audited; all receipt binding and final-accounting checks passed: yes.

At d20, θ=8 represents 160 dependent Stage executions in the session, not a 20× speedup. For θ=1→8, DAG execution gas rises from 2.851M to 20.566M (7.21×), and the measured full session grows from about 4.03 to 6.05 seconds in the same-domain arm and 6.98 to 9.03 seconds in the migrated arm. At matched θ, moving from d8 to d20 increases Stage count by 2.5× and DAG transaction gas by about 2.55× at θ=1 and 2.66× at θ=8. The full-session times do not increase by 20× in this controlled EVM run. Since d20 has only one repeat, treat this as a pilot observation rather than a general scaling law.

The d20 row values are one repeat each, not medians. They quantify a same-EVM trusted-attestor composition. They do not measure a native cross-VM migration or establish production scalability.

Raw input: `pilot_d20_raw.json`; supplemental rows: `d20_pilot_summary.csv`.
