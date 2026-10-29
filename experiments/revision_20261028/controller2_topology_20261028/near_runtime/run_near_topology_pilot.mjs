import { readFile, writeFile, realpath } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { Sandbox, DEFAULT_ACCOUNT_ID, DEFAULT_PRIVATE_KEY } from 'near-sandbox';
import { Worker } from 'near-workspaces';
import { KeyPair, keyStores } from 'near-api-js';

const ROOT = new URL('.', import.meta.url);
const PROFILE_PATH = new URL('../profile_manifest.json', ROOT);
const PROFILE_MANIFEST = JSON.parse(await readFile(PROFILE_PATH, 'utf8'));
const TAG = process.env.DAG_OUTPUT_TAG ?? 'near_dag';
const OUT = new URL(`../results/pilot/${TAG}_results.json`, ROOT);
const RAW = new URL(`../results/pilot/${TAG}_raw.json`, ROOT);
const VERSION = process.env.NEARCORE_VERSION ?? '2.13.4';
const EXPECTED_PROTOCOL = Number(process.env.NEAR_EXPECTED_PROTOCOL ?? '86');
const RUN_FAILURE_PROBE = process.env.DAG_RUN_FAILURE_PROBE === '1';
const RECEIPT_AUDIT_TIMEOUT_MS = Number(process.env.DAG_RECEIPT_AUDIT_TIMEOUT_MS ?? '120000');
const GAS = 300n * 10n ** 12n;
const REPEATS = Number(process.env.DAG_REPEATS ?? '3');
const FILTER = process.env.DAG_PROFILE_REGEX ? new RegExp(process.env.DAG_PROFILE_REGEX) : null;
const OPERATIONS = Number(process.env.DAG_OPERATIONS_OVERRIDE ?? '16');
const profiles = PROFILE_MANIFEST.profiles.filter(p => !FILTER || FILTER.test(p.profile_id));
const rawRepeatResults = [];
let activePartial = null;
const sleep = ms => new Promise(resolveSleep => setTimeout(resolveSleep, ms));

async function persistRawCheckpoint() {
  const document = { experiment: PROFILE_MANIFEST.experiment, near_sandbox: '0.3.4', near_workspaces: '5.0.0',
    requested_nearcore: VERSION, repeats: rawRepeatResults };
  if (activePartial) document.current_partial = activePartial;
  await writeFile(RAW, `${JSON.stringify(document, null, 2)}\n`);
}

async function rpc(rpcUrl, method, params) {
  const response = await fetch(`${rpcUrl}/`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }),
  });
  const body = await response.json();
  if (body.error) throw new Error(JSON.stringify(body.error));
  return body.result;
}

async function runtimeIdentity(rpcUrl) {
  const status = await fetch(`${rpcUrl}/status`).then(response => response.json());
  const version = status.version?.version ?? status.version ?? null;
  if (version !== VERSION || Number(status.protocol_version) !== EXPECTED_PROTOCOL) {
    throw new Error(`runtime mismatch before workload: requested=${VERSION}, rpc=${version}, protocol=${status.protocol_version}`);
  }
  const launcher = process.env.NEAR_SANDBOX_BIN_PATH;
  const configuredBinary = process.env.NEAR_SANDBOX_REAL_BIN;
  if (!launcher || !configuredBinary) throw new Error('NEAR Sandbox launcher and actual binary paths must be pinned');
  const launcherPath = await realpath(launcher);
  const binaryPath = await realpath(configuredBinary);
  const launcherText = await readFile(launcherPath, 'utf8');
  if (!launcherText.includes(binaryPath)) throw new Error('configured launcher does not reference the pinned NEAR Sandbox binary');
  const binaryVersion = execFileSync(launcherPath, ['--version'], { encoding: 'utf8' }).trim();
  if (!binaryVersion.includes(`release ${VERSION}`) || !binaryVersion.includes(`protocol ${EXPECTED_PROTOCOL}`)) {
    throw new Error(`binary version mismatch: ${binaryVersion}`);
  }
  const hash = createHash('sha256');
  await new Promise((resolveHash, rejectHash) => {
    const stream = createReadStream(binaryPath);
    stream.on('data', chunk => hash.update(chunk));
    stream.on('error', rejectHash);
    stream.on('end', resolveHash);
  });
  let genesisConfig = null;
  let genesisConfigError = null;
  try { genesisConfig = await rpc(rpcUrl, 'EXPERIMENTAL_genesis_config', {}); }
  catch (error) { genesisConfigError = String(error); }
  return {
    rpc_version: version,
    protocol_version: Number(status.protocol_version),
    chain_id: status.chain_id,
    node_key: status.node_key ?? null,
    latest_block_height: status.sync_info?.latest_block_height ?? null,
    genesis_config: genesisConfig,
    genesis_config_rpc_error: genesisConfigError,
    sandbox_launcher_path: launcherPath,
    sandbox_binary_path: binaryPath,
    sandbox_binary_version: binaryVersion,
    sandbox_binary_sha256: hash.digest('hex'),
  };
}

async function blockDetails(rpcUrl, hash) {
  const header = (await rpc(rpcUrl, 'block', { block_id: hash })).header;
  return { height: header.height, timestamp_nanosec: header.timestamp_nanosec };
}

function classifyNearReceipt(details) {
  const receipt = details?.receipt;
  if (!receipt) return { receipt_class: 'details_unavailable', action_methods: [], args_bytes: 0, action_types: [] };
  if (receipt.Action) {
    const methods = [];
    const types = [];
    let argsBytes = 0;
    for (const action of receipt.Action.actions ?? []) {
      const [type] = Object.keys(action);
      if (type) types.push(type);
      if (action.FunctionCall) {
        methods.push(action.FunctionCall.method_name);
        argsBytes += Buffer.from(action.FunctionCall.args ?? '', 'base64').length;
      }
    }
    const uniqueMethods = [...new Set(methods)];
    const receiptClass = uniqueMethods.length ? `function_call:${uniqueMethods.join('+')}`
      : types.includes('Transfer') ? 'transfer_or_refund'
      : types.length ? `action:${types.join('+')}` : 'action_no_payload';
    return { receipt_class: receiptClass, action_methods: uniqueMethods, args_bytes: argsBytes, action_types: types };
  }
  if (receipt.Data) return { receipt_class: 'data_receipt', action_methods: [], args_bytes: 0, action_types: ['Data'] };
  return { receipt_class: Object.keys(receipt)[0] ?? 'unknown_receipt', action_methods: [], args_bytes: 0, action_types: Object.keys(receipt) };
}

function parseResult(result) {
  try { return result.parseResult?.() ?? null; }
  catch (error) { return { parse_error: String(error) }; }
}

async function summarizeExecution(rpcUrl, result) {
  const raw = result.result;
  const tx = raw.transaction_outcome;
  const receipts = raw.receipts_outcome ?? [];
  const records = [{
    kind: 'originating_transaction', outcome_id: tx.id, block_hash: tx.block_hash,
    executor_id: tx.outcome.executor_id, gas_burnt: tx.outcome.gas_burnt,
    status_kind: Object.keys(tx.outcome.status ?? {})[0] ?? null,
    logs: tx.outcome.logs ?? [],
  }, ...receipts.map((r, i) => ({
    kind: 'receipt', receipt_index: i, outcome_id: r.id, block_hash: r.block_hash,
    executor_id: r.outcome.executor_id, gas_burnt: r.outcome.gas_burnt,
    status_kind: Object.keys(r.outcome.status ?? {})[0] ?? null,
    failure: r.outcome.status?.Failure ?? null, receipt_ids: r.outcome.receipt_ids ?? [], logs: r.outcome.logs ?? [],
  }))];
  if (process.env.DAG_INCLUDE_RECEIPT_DETAILS === '1') {
    for (const item of records.slice(1)) {
      try {
        item.receipt_details = await rpc(rpcUrl, 'EXPERIMENTAL_receipt', { receipt_id: item.outcome_id });
        Object.assign(item, classifyNearReceipt(item.receipt_details));
      } catch (error) {
        item.receipt_detail_error = String(error);
        Object.assign(item, classifyNearReceipt(null));
      }
    }
  }
  for (const item of records) {
    const block = await blockDetails(rpcUrl, item.block_hash);
    item.block_height = block.height;
    item.block_timestamp_nanosec = block.timestamp_nanosec;
  }
  const txHeight = records[0].block_height;
  const laterHeights = records.slice(1).map(x => x.block_height);
  const logEvents = [];
  for (const record of records) {
    for (const log of record.logs) {
      if (log.startsWith('DAG_NODE|')) {
        const [, session, workflow, node, checksum, operations] = log.split('|');
        logEvents.push({ type: 'node_execution', session_id: session, workflow_id: workflow, node_id: Number(node), checksum, operations: Number(operations), receipt_id: record.outcome_id, executor_id: record.executor_id, block_height: record.block_height });
      } else if (log.startsWith('DAG_EDGE|')) {
        const [, session, workflow, node, parent, checksum] = log.split('|');
        logEvents.push({ type: 'edge_receipt', session_id: session, workflow_id: workflow, node_id: Number(node), parent_node_id: Number(parent), checksum, receipt_id: record.outcome_id, executor_id: record.executor_id, block_height: record.block_height });
      } else if (log.startsWith('DAG_JOIN|')) {
        const [, session, workflow, node] = log.split('|');
        logEvents.push({ type: 'fanout_barrier_callback', session_id: session, workflow_id: workflow, node_id: Number(node), receipt_id: record.outcome_id, executor_id: record.executor_id, block_height: record.block_height });
      }
    }
  }
  return {
    succeeded: result.succeeded,
    failed: result.failed,
    value: parseResult(result),
    gas_burnt: String(result.gas_burnt),
    receipt_count: receipts.length,
    tx_block_height: txHeight,
    completion_block_height: laterHeights.length ? Math.max(...laterHeights) : txHeight,
    completion_lag_blocks: laterHeights.length ? Math.max(...laterHeights) - txHeight : 0,
    duration_ms_sdk_diagnostic_only: result.durationMs,
    records,
    log_events: logEvents,
  };
}

function addDagSinkCompletion(run, profile) {
  const sinkNodes = profile.successors.map((children, node) => children.length === 0 ? node : null).filter(node => node !== null);
  const sinkEvents = run.log_events.filter(event => event.type === 'node_execution' && sinkNodes.includes(event.node_id));
  if (sinkEvents.length !== sinkNodes.length) throw new Error(`sink execution evidence missing for ${profile.profile_id}`);
  const sinkHeight = Math.max(...sinkEvents.map(event => event.block_height));
  run.sink_nodes = sinkNodes;
  run.dag_sink_completion_block_height = sinkHeight;
  run.dag_sink_completion_lag_blocks = sinkHeight - run.tx_block_height;
  run.receipt_tree_drain_lag_blocks = run.completion_lag_blocks;
  return run;
}

function nodeGasNeeds(successors, baseTgas = 7, joinCallbackTgas = 2) {
  const needs = Array(successors.length).fill(baseTgas);
  for (let node = successors.length - 1; node >= 0; node--) {
    needs[node] = baseTgas + successors[node].reduce((sum, child) => sum + needs[child], 0) + (successors[node].length > 1 ? joinCallbackTgas : 0);
  }
  for (const node of successors.keys()) {
    if (nodeGasNeeds.childrenGas(needs, successors[node]) > 300) throw new Error(`graph needs >300 Tgas to fan out at node ${node}`);
  }
  return needs;
}
nodeGasNeeds.childrenGas = (needs, children) => children.reduce((sum, child) => sum + needs[child], 0);

async function createAccount(root) { return root.devCreateAccount(); }

async function runProfile(root, sandbox, repeat, profile, profileIndex, pinnedRuntime) {
  const sessionNodes = profile.node_count;
  const gasNeeds = nodeGasNeeds(profile.successors);
  const faultWorkflowId = BigInt(repeat) * 10_000_000_000n + BigInt(profileIndex) * 1_000_000n + 999_999n;
  const faultNodeId = Math.floor(sessionNodes / 2);
  const stageAccounts = [];
  for (let node = 0; node < sessionNodes; node++) {
    const account = await createAccount(root);
    await account.deploy(await readFile(new URL('./artifacts/stage_contract.wasm', ROOT)));
    stageAccounts.push(account);
  }
  for (let node = 0; node < sessionNodes; node++) {
    const successors = profile.successors[node].map(child => [stageAccounts[child].accountId, child, gasNeeds[child]]);
    await root.call(stageAccounts[node], 'new', {
      node_id: node,
      fail_workflow_id: node === faultNodeId ? faultWorkflowId.toString() : null,
      parents: profile.parents[node],
      successors,
      operations: OPERATIONS,
    }, { gas: GAS });
  }
  const sameReceiver = await createAccount(root);
  await sameReceiver.deploy(await readFile(new URL('./artifacts/same_receiver_contract.wasm', ROOT)));
  await root.call(sameReceiver, 'new', { operations_per_node: OPERATIONS }, { gas: GAS });

  const caseSessions = [];
  const sessionCount = Number(process.env.DAG_SESSIONS_OVERRIDE ?? profile.sessions_per_profile);
  for (let s = 0; s < sessionCount; s++) {
    const sessionId = BigInt(repeat) * 100_000_000_000n + BigInt(profileIndex) * 1_000_000_000n + BigInt(s + 1) * 1_000_000n;
    const flows = [];
    const drivers = [];
    for (let k = 0; k < profile.workflows_per_session_theta; k++) {
      const workflowId = sessionId + BigInt(k + 1);
      const seed = 0x123456789abcn ^ (workflowId * 0x9e3779b97f4a7c15n & ((1n << 64n) - 1n));
      flows.push({ session_id: sessionId.toString(), workflow_id: workflowId.toString(), seed: seed.toString() });
      drivers.push(await createAccount(root));
    }

    const multiContractRuns = [];
    for (let offset = 0; offset < flows.length; offset += profile.concurrent_workflows_nu) {
      const chunk = flows.slice(offset, offset + profile.concurrent_workflows_nu);
      const results = await Promise.all(chunk.map(async (flow, i) => {
        const driver = drivers[offset + i];
        const wallStart = performance.now();
        let receiptTreeCompletedAt = null;
        const sinkNode = profile.successors.findIndex(children => children.length === 0);
        let sinkReadError = null;
        const receiptTreePromise = driver.callRaw(stageAccounts[0], 'start', {
          session_id: flow.session_id,
          workflow_id: flow.workflow_id,
          seed: flow.seed,
        }, { gas: GAS }).then(raw => {
          receiptTreeCompletedAt = performance.now();
          return { raw };
        }).catch(error => {
          receiptTreeCompletedAt = performance.now();
          return { error: String(error) };
        });
        const sinkPromise = (async () => {
          const deadline = Date.now() + RECEIPT_AUDIT_TIMEOUT_MS;
          while (Date.now() < deadline) {
            try {
              const value = await stageAccounts[sinkNode].view('stage_result', { workflow_id: flow.workflow_id });
              if (value) return { value, first_queryable_ms: performance.now() - wallStart,
                queried_before_receipt_tree: receiptTreeCompletedAt === null };
            } catch (error) { sinkReadError = String(error); }
            await sleep(50);
          }
          return { value: null, first_queryable_ms: null, queried_before_receipt_tree: null, timeout_ms: RECEIPT_AUDIT_TIMEOUT_MS };
        })();
        const [receiptOutcome, sinkAudit] = await Promise.all([receiptTreePromise, sinkPromise]);
        if (receiptOutcome.error) {
          activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
            session_id: sessionId.toString(), evidence_stage: 'receipt_tree_error', workflow: flow,
            receipt_tree_error: receiptOutcome.error, sink_audit: sinkAudit };
          await persistRawCheckpoint();
          throw new Error(`receipt tree failed for workflow ${flow.workflow_id}: ${receiptOutcome.error}`);
        }
        const raw = receiptOutcome.raw;
        const receiptTreeWallMs = receiptTreeCompletedAt - wallStart;
        const observed = addDagSinkCompletion({ workflow: flow, ...(await summarizeExecution(sandbox.rpcUrl, raw)) }, profile);
        observed.sink_first_queryable_ms = sinkAudit.first_queryable_ms;
        observed.sink_first_queryable_before_receipt_tree = sinkAudit.queried_before_receipt_tree;
        observed.sink_view_first_value = sinkAudit.value;
        observed.sink_view_error = sinkReadError;
        observed.client_wall_ms_to_receipt_tree = receiptTreeWallMs;
        return observed;
      }));
      multiContractRuns.push(...results);
      activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
        session_id: sessionId.toString(), evidence_stage: 'multi_contract_receipt_trees_archived', multi_contract_runs: multiContractRuns };
      await persistRawCheckpoint();
    }

    const compositeStarted = performance.now();
    const compositeRaw = await drivers[0].callRaw(sameReceiver, 'run_session', {
      session_id: sessionId.toString(),
      workflows: flows,
      graph: { parents: profile.parents },
    }, { gas: GAS });
    const compositeReceiptTreeMs = performance.now() - compositeStarted;
    const compositeRun = await summarizeExecution(sandbox.rpcUrl, compositeRaw);
    const compositeSinkStarted = performance.now();
    const compositeSink = await sameReceiver.view('workflow_result', { workflow_id: flows[0].workflow_id }).catch(error => ({ query_error: String(error) }));
    compositeRun.client_wall_ms_to_receipt_tree = compositeReceiptTreeMs;
    compositeRun.sink_observed_in_post_receipt_tree_view_ms = compositeReceiptTreeMs + (performance.now() - compositeSinkStarted);
    compositeRun.sink_view_first_value = compositeSink;
    const compositeReturned = compositeRun.value;
    activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
      session_id: sessionId.toString(), evidence_stage: 'same_receiver_receipt_tree_archived',
      multi_contract_runs: multiContractRuns, same_receiver_run: compositeRun };
    await persistRawCheckpoint();

    const stageStates = [];
    for (const flow of flows) {
      const state = await Promise.all(stageAccounts.map(async (account, node) => ({
        node_id: node,
        result: await account.view('stage_result', { workflow_id: flow.workflow_id }),
      })));
      const control = Array.isArray(compositeReturned) ? compositeReturned.find(x => x.workflow_id === flow.workflow_id) : null;
      const checksums = state.map(x => x.result?.checksum ?? null);
      const nodesMatchControl = control && checksums.every((value, node) => value === control.node_checksums[node]);
      const allNodesPresent = state.every((x, node) => x.result && x.result.input_count === profile.parents[node].length && x.result.operation_count === OPERATIONS);
      stageStates.push({ workflow: flow, nodes: state, composite_result: control, node_checksums: checksums, all_nodes_present: allNodesPresent, nodes_match_same_receiver: nodesMatchControl });
    }
    activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
      session_id: sessionId.toString(), evidence_stage: 'post_completion_state_audit', multi_contract_runs: multiContractRuns,
      same_receiver_run: compositeRun, same_nodes_and_checksum: stageStates };
    await persistRawCheckpoint();

    const workflowsVerified = multiContractRuns.every(x => x.succeeded && !x.failed) &&
      compositeRun.succeeded && stageStates.every(x => x.all_nodes_present && x.nodes_match_same_receiver);
    let failureEvidence = { not_run: true, reason: 'separate failure scenario is scheduled after topology pilot' };
    if (RUN_FAILURE_PROBE) {
      const faultDriver = await createAccount(root);
      const faultRaw = await faultDriver.callRaw(stageAccounts[0], 'start', {
        session_id: sessionId.toString(), workflow_id: faultWorkflowId.toString(),
        seed: (0x6a09e667f3bcc909n ^ faultWorkflowId).toString(),
      }, { gas: GAS });
      const faultExecution = await summarizeExecution(sandbox.rpcUrl, faultRaw);
      const faultStoredNodes = await Promise.all(stageAccounts.map(async (account, node) => ({
        node_id: node,
        result: await account.view('stage_result', { workflow_id: faultWorkflowId.toString() }),
      })));
      activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
        session_id: sessionId.toString(), evidence_stage: 'failure_multicontract_observed',
        failure_workflow_id: faultWorkflowId.toString(), multi_contract: faultExecution,
        multi_contract_stored_nodes: faultStoredNodes };
      await persistRawCheckpoint();
      const localFaultRaw = await faultDriver.callRaw(sameReceiver, 'probe_failure', {
        workflow_id: faultWorkflowId.toString(), seed: (0x6a09e667f3bcc909n ^ faultWorkflowId).toString(),
        graph: { parents: profile.parents }, fail_at_node: faultNodeId,
      }, { gas: GAS });
      const localFault = await summarizeExecution(sandbox.rpcUrl, localFaultRaw);
      const failureReceiptObserved = faultExecution.records.some(x => x.kind === 'receipt' && x.status_kind === 'Failure');
      const failureVerified = failureReceiptObserved && (localFault.failed || !localFault.succeeded) &&
        faultStoredNodes.some(x => x.result) && !faultStoredNodes.every(x => x.result) &&
        !profile.successors.map((out, node) => out.length === 0 ? node : null).filter(x => x !== null).every(node => Boolean(faultStoredNodes[node].result));
      failureEvidence = {
        workflow_id: faultWorkflowId.toString(), fail_at_node: faultNodeId,
        multi_contract: faultExecution, failed_receipt_observed: failureReceiptObserved,
        same_receiver: localFault, multi_contract_stored_nodes: faultStoredNodes,
        multi_contract_unfinished: !faultStoredNodes.every(x => x.result),
        multi_contract_completed_sinks: profile.successors.map((out, node) => out.length === 0 ? node : null).filter(x => x !== null).every(node => Boolean(faultStoredNodes[node].result)),
        verified: failureVerified,
      };
      activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
        session_id: sessionId.toString(), evidence_stage: 'failure_probe_audited',
        multi_contract: faultExecution, multi_contract_stored_nodes: faultStoredNodes,
        failure_evidence: failureEvidence };
      await persistRawCheckpoint();
      if (!failureVerified) throw new Error(`failure probe invariant failed for ${profile.profile_id}: ${JSON.stringify(failureEvidence)}`);
    }
    if (!workflowsVerified) throw new Error(`workflow DAG invariant failed for ${profile.profile_id}: ${JSON.stringify({workflowsVerified})}`);

    const startHeights = multiContractRuns.map(x => x.tx_block_height);
    const completionHeights = multiContractRuns.map(x => x.completion_block_height);
    const session = {
      session_id: sessionId.toString(),
      theta_workflows: flows,
      multi_contract: {
        runs: multiContractRuns,
        receipt_graph: multiContractRuns.map(x => ({ workflow_id: x.workflow.workflow_id, edges: x.log_events.filter(e => e.type === 'edge_receipt'), node_executions: x.log_events.filter(e => e.type === 'node_execution') })),
        session_block_window: Math.max(...completionHeights) - Math.min(...startHeights),
      },
      same_receiver_best_control: {
        run: compositeRun,
        receipt_count_for_theta_session: compositeRun.receipt_count,
        block_window: compositeRun.completion_block_height - compositeRun.tx_block_height,
      },
      same_nodes_and_checksum: stageStates,
      injected_failure: failureEvidence,
    };
    caseSessions.push(session);
    activePartial = { repeat, profile_id: profile.profile_id, stages: stageAccounts.map(x => x.accountId),
      session_id: sessionId.toString(), evidence_stage: 'session_audited', sessions: caseSessions };
    await persistRawCheckpoint();
  }

  return {
    profile: { ...profile, operations_per_node: OPERATIONS },
    nearcore: { ...pinnedRuntime, requested_version: VERSION, expected_protocol_version: EXPECTED_PROTOCOL, single_validator_sandbox: true },
    stages: stageAccounts.map(x => x.accountId),
    same_receiver_contract: sameReceiver.accountId,
    gas_budget_tgas: { top_level: 300, per_stage_node: gasNeeds },
    sessions: caseSessions,
  };
}

async function runRepeat(repeat) {
  const startRequestedAt = new Date().toISOString();
  const sandbox = await Sandbox.start({ version: VERSION });
  const sandboxReadyAt = new Date().toISOString();
  let worker;
  let runtime = null;
  const completedProfiles = [];
  const repeatRaw = { repeat, sandbox_start_requested_at: startRequestedAt, sandbox_ready_at: sandboxReadyAt,
    sandbox_start_options: { version: VERSION, explicit_block_interval_override: null }, chain_id: null, profiles: completedProfiles };
  rawRepeatResults.push(repeatRaw);
  try {
    runtime = await runtimeIdentity(sandbox.rpcUrl);
    repeatRaw.runtime = runtime;
    const keyStore = new keyStores.InMemoryKeyStore();
    await keyStore.setKey('custom', DEFAULT_ACCOUNT_ID, KeyPair.fromString(DEFAULT_PRIVATE_KEY));
    worker = await Worker.init({ network: 'custom', rpcAddr: sandbox.rpcUrl, rootAccountId: DEFAULT_ACCOUNT_ID, keyStore });
    const root = worker.rootAccount;
    for (const profile of profiles) {
      const manifestIndex = PROFILE_MANIFEST.profiles.findIndex(item => item.profile_id === profile.profile_id);
      if (manifestIndex < 0) throw new Error(`profile missing from manifest: ${profile.profile_id}`);
      const result = await runProfile(root, sandbox, repeat, profile, manifestIndex + 1, runtime);
      completedProfiles.push(result);
      repeatRaw.chain_id = result.nearcore.chain_id;
      activePartial = null;
      await persistRawCheckpoint();
      console.log(`repeat ${repeat}: finished ${result.profile.profile_id}`);
    }
    return { repeat, chain_id: completedProfiles[0]?.nearcore.chain_id, runtime, profiles: completedProfiles };
  } finally {
    if (worker) await worker.tearDown();
    repeatRaw.sandbox_tear_down_requested_at = new Date().toISOString();
    await persistRawCheckpoint();
    await sandbox.tearDown();
    repeatRaw.sandbox_tear_down_completed_at = new Date().toISOString();
    await persistRawCheckpoint();
  }
}

const result = {
  experiment: PROFILE_MANIFEST.experiment,
  design: { profile_count_per_repeat: profiles.length, fresh_repeats: REPEATS, session_count_per_profile: profiles[0]?.sessions_per_profile ?? null, graph_source: PROFILE_PATH.pathname, metrics: 'raw receipt IDs, edge/node logs, success/failure outcomes, receipt block-height spans; SDK duration diagnostic only' },
  runtime: { near_sandbox: '0.3.4', near_workspaces: '5.0.0', node: process.version, requested_nearcore: VERSION, expected_protocol_version: EXPECTED_PROTOCOL, wasm_stage: resolve(new URL('./artifacts/stage_contract.wasm', ROOT).pathname), wasm_same_receiver: resolve(new URL('./artifacts/same_receiver_contract.wasm', ROOT).pathname) },
  started_at: new Date().toISOString(),
  completed_repeats: [],
};

try {
  for (let repeat = 1; repeat <= REPEATS; repeat++) {
    result.completed_repeats.push(await runRepeat(repeat));
    result.updated_at = new Date().toISOString();
    await writeFile(OUT, `${JSON.stringify(result, null, 2)}\n`);
  }
} catch (error) {
  result.error = String(error?.stack ?? error);
} finally {
  result.finished_at = new Date().toISOString();
  await writeFile(OUT, `${JSON.stringify(result, null, 2)}\n`);
}

if (result.error) {
  console.error(result.error);
  process.exitCode = 1;
} else {
  console.log(`Completed ${result.completed_repeats.length} fresh nearcore Sandboxes and ${result.completed_repeats.reduce((n, r) => n + r.profiles.length, 0)} profile repeats.`);
}
