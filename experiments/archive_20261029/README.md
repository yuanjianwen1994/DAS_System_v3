# DAS experimental archive through 2026-10-29

This index covers the current experimental sources, raw data, derived tables,
figures and documents. It does not claim that every historical experiment is
valid or that the full DAS system outperforms production SOTA systems.

## Start here

| Material | Entry point |
|---|---|
| Full experiment summary and manuscript-figure replacement proposals | [Summary](../revision_20261028/figure_revision_20261028/实验总结与论文图表替换方案.md) |
| Latest dense point plot and SOTA argument/evidence review | [Evidence review](../revision_20261029/figure_density_and_sota_20261029/DAS_SOTA优势论证与图表补证.md) |
| Original experiment baseline and scope | [Baseline](../revision_20261027/BASELINE_AND_PLAN.md) |

## Raw results, audit records and analyses

| Experiment family | Directory / report |
|---|---|
| Receipt binding, balances, abstract SOTA controls, two-Anvil journeys | [2026-10-27 archive](../revision_20261027/) |
| Stateful independent-stage DAG and original runtime comparisons | [Stateful DAG](../revision_20261027/stateful_dag_20261027/), [timing and load profiles](../revision_20261027/timing_comparison_20261027/) |
| Randomized arrivals, unique workflow IDs, original failures and repaired replay | [Randomized arrival archive](../revision_20261028/randomized_arrival_20261028/randomized_d8_result_note.md) |
| Two new complete paired chain repeats, seeds 20261033/34 | [Validation summary](../revision_20261028/figure_validation_chain_20261028/validation_summary.md), [original 81-file manifest](../revision_20261028/figure_validation_chain_20261028/archive_manifest_sha256.txt) |
| Independent audit of those paired repeats | [Original audit](../revision_20261028/figure_validation_topology_20261028/chain_fresh_independent_audit.md), [recovered original provenance](../revision_20261028/figure_validation_topology_20261028/chain_fresh_supplement/), [supplement audit](../revision_20261028/figure_validation_topology_20261028/chain_fresh_supplement/independent_provenance_audit.md) |
| Three-seed burst/conditional-random comparison | [Burst report](../revision_20261028/controller2_burst_20261028/formal_report.md) |
| Original chain/fanout-join and node-4 failure controls | [Topology report](../revision_20261028/controller2_topology_20261028/formal_report.md) |
| Additional five topology repeats and three failure controls | [Additional batch audit](../revision_20261028/figure_validation_topology_20261028/C2_added_batch_independent_audit.md), [topology rows](../revision_20261028/figure_validation_topology_20261028/topology5_per_repeat_audit.csv), [failure rows](../revision_20261028/figure_validation_topology_20261028/failure3_per_repeat_audit.csv) |
| Same-EVM migration/session amortization controls | [Balance/session control](../revision_20261028/session_amortization_20261028/session_amortization_report.md), [actual DAG/session control](../revision_20261028/session_dag_migration_20261028/session_dag_migration_report.md) |
| Historical benchmark output | [Logs](../../logs/), [legacy log archive](../../logs.zip) |

All other files under the dated directories are retained, including pilots,
invalid runs, audit-pending original results, recovery logs and input bundles.
Use the associated report to determine whether a dataset supports a claim.
Names such as `formal` do not override the report's caveats.

## Figure candidates

- [Completion latency and window completion rate](../revision_20261028/figure_revision_20261028/candidate_fig4_measured_outcomes.pdf)
- [Distributions and burst sensitivity](../revision_20261028/figure_revision_20261028/candidate_fig5_distribution_robustness.pdf)
- [Topology costs, same-receiver control and failure states](../revision_20261028/figure_revision_20261028/candidate_fig6_topology_failure.pdf)
- [Dense per-workflow measurements with explicit zooms](../revision_20261029/figure_density_and_sota_20261029/dense_workflow_observations.pdf)

Each figure directory includes its plotting source, input hashes and/or plotting
CSV, plus vector SVG/PDF and PNG previews. These remain proposals; manuscript
sources and the original manuscript figures are not replaced by this upload.

## Interpretation boundaries

- The new d8 chain repeats have 688 observation-window offers per runtime nested
  within two independent seed pairs. Window completions/s include any warmup
  offers completing in the window; cohort latency follows window offers through
  drain. These are different denominators.
- Chain latency results use one-second blocks; native topology controls use
  approximately 125-ms NEAR blocks and Anvil automine. Do not pool these settings.
- The paired chain runs used NEAR Sandbox 2.13.4 / protocol 86. Recovered original
  launch manifests establish provenance; older missing/unknown version records
  remain separately labelled. There is no production multi-shard deployment.
- NEAR and EVM native gas units are not directly comparable. A Stage event and a
  receipt are not the same unit. Submission/stage counts are not throughput gains.
- Same-EVM trusted-attestor and two-Anvil controls do not implement production
  finality proofs, an XCM executor or AvalancheGo. Archive auditing is not an
  independent execution reproduction.
- NEAR teardown timeouts, transport retries and business failures are separate.
  Later PID probes do not establish historical process-exit times.
- The observed internal-execution advantage does not establish whole-system,
  migration-inclusive or saturation-capacity superiority over all SOTA systems.

## Publication inventory

`uploaded_source_inventory.csv` records paths, byte counts and SHA-256 digests of
the included source snapshot. `publication_scope.json` records inclusion and
exclusion counts, scans and total size. These two generated publication metadata
files are excluded from their own inventory to avoid recursive hashes.

Only Git metadata, dependencies/build caches, Python bytecode, editor/OS state and
empty directories are omitted. Raw measurements, serialized experiment states,
logs, compressed evidence bundles, source code, lockfiles, plots and reports are
included. No raw result is rewritten for publication.

Original local repositories, their pre-existing staging state, and their remote
configuration are preserved. GitHub publication uses a separate checkout; the
Overleaf repository is not pushed.
