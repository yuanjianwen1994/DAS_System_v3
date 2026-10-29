import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { Sandbox, DEFAULT_ACCOUNT_ID, DEFAULT_PRIVATE_KEY } from 'near-sandbox';
import { Worker } from 'near-workspaces';
import { KeyPair, keyStores } from 'near-api-js';

const ROOT = new URL('.', import.meta.url);
const PROFILE_PATH = new URL('../dag_workflow_20261027/profile_manifest.json', ROOT);
const PROFILE_MANIFEST = JSON.parse(await readFile(PROFILE_PATH, 'utf8'));
const TAG = process.env.DAG_OUTPUT_TAG ?? 'near_dag';
const OUT = new URL(`./${TAG}_results.json`, ROOT);
const RAW = new URL(`./${TAG}_raw.json`, ROOT);
const VERSION = '2.14.0';
const GAS = 300n * 10n ** 12n;
const REPEATS = Number(process.env.DAG_REPEATS ?? '3');
const FILTER = process.env.DAG_PROFILE_REGEX ? new RegExp(process.env.DAG_PROFILE_REGEX) : null;
const OPERATIONS = Number(process.env.DAG_OPERATIONS_OVERRIDE ?? '16');
const profiles = PROFILE_MANIFEST.profiles.filter(p => !FILTER || FILTER.test(p.profile_id));
const rawRepeatResults = [];

async function rpc(rpcUrl, method, params) {
  const response = await fetch(`${rpcUrl}/`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }),
  });
  const body = await response.json();
  if (body.error) throw new Error(JSON.stringify(body.error));
  return body.result;
}

async function blockHeight(rpcUrl, hash) {
  return (await rpc(rpcUrl, 'block', { block_id: hash })).header.height;
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
      const result = await rpc(rpcUrl, 'EXPERIMENTAL_receipt', { receipt_id: item.outcome_id });
      item.receipt_details = result;
    }
  }
  for (const item of records) item.block_height = await blockHeight(rpcUrl, item.block_hash);
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

async function runProfile(root, sandbox, repeat, profile, profileIndex) {
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
        const sinkNodes = profile.successors.map((children, node) => children.length === 0 ? node : null).filter(node => node !== null);
        const sinkNode = sinkNodes[0];
        const apiCallStartedAt = new Date().toISOString();
        const startedNs = process.hrtime.bigint();
        let callSettled = false;
        const rawPromise = driver.callRaw(stageAccounts[0], 'start', {
          session_id: flow.session_id,
          workflow_id: flow.workflow_id,
          seed: flow.seed,
        }, { gas: GAS }).finally(() => { callSettled = true; });
        let sinkTiming = null;
        while (!callSettled && !sinkTiming) {
          const state = await stageAccounts[sinkNode].view('stage_result', { workflow_id: flow.workflow_id }).catch(() => null);
          if (state) sinkTiming = {
            first_sink_queryable_at: new Date().toISOString(),
            api_to_sink_queryable_ms: Number(process.hrtime.bigint() - startedNs) / 1e6,
            sink_node: sinkNode,
          };
          if (!sinkTiming) await new Promise(resolve => setTimeout(resolve, 25));
        }
        const raw = await rawPromise;
        if (!sinkTiming) {
          const state = await stageAccounts[sinkNode].view('stage_result', { workflow_id: flow.workflow_id }).catch(() => null);
          if (state) sinkTiming = {
            first_sink_queryable_at: new Date().toISOString(),
            api_to_sink_queryable_ms: Number(process.hrtime.bigint() - startedNs) / 1e6,
            sink_node: sinkNode,
          };
        }
        return addDagSinkCompletion({ workflow: flow, api_call_started_at: apiCallStartedAt, sink_visibility: sinkTiming, ...(await summarizeExecution(sandbox.rpcUrl, raw)) }, profile);
      }));
      multiContractRuns.push(...results);
    }

    const compositeRaw = await drivers[0].callRaw(sameReceiver, 'run_session', {
      session_id: sessionId.toString(),
      workflows: flows,
      graph: { parents: profile.parents },
    }, { gas: GAS });
    const compositeRun = await summarizeExecution(sandbox.rpcUrl, compositeRaw);
    const compositeReturned = compositeRun.value;

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

    const faultDriver = await createAccount(root);
    const faultRaw = await faultDriver.callRaw(stageAccounts[0], 'start', {
      session_id: sessionId.toString(),
      workflow_id: faultWorkflowId.toString(),
      seed: (0x6a09e667f3bcc909n ^ faultWorkflowId).toString(),
    }, { gas: GAS });
    const faultExecution = await summarizeExecution(sandbox.rpcUrl, faultRaw);
    const faultStoredNodes = await Promise.all(stageAccounts.map(async (account, node) => ({
      node_id: node,
      result: await account.view('stage_result', { workflow_id: faultWorkflowId.toString() }),
    })));
    const localFaultRaw = await faultDriver.callRaw(sameReceiver, 'probe_failure', {
      workflow_id: faultWorkflowId.toString(),
      seed: (0x6a09e667f3bcc909n ^ faultWorkflowId).toString(),
      graph: { parents: profile.parents },
      fail_at_node: faultNodeId,
    }, { gas: GAS });
    const localFault = await summarizeExecution(sandbox.rpcUrl, localFaultRaw);
    const workflowsVerified = multiContractRuns.every(x => x.succeeded && !x.failed) &&
      compositeRun.succeeded && stageStates.every(x => x.all_nodes_present && x.nodes_match_same_receiver);
    const failureReceiptObserved = faultExecution.records.some(x => x.kind === 'receipt' && x.status_kind === 'Failure');
    const failureVerified = failureReceiptObserved && (localFault.failed || !localFault.succeeded) &&
      faultStoredNodes.some(x => x.result) && !faultStoredNodes.every(x => x.result) &&
      !profile.successors.map((out, node) => out.length === 0 ? node : null).filter(x => x !== null).every(node => Boolean(faultStoredNodes[node].result));
    if (!workflowsVerified || !failureVerified) {
      throw new Error(`workflow DAG invariant failed for ${profile.profile_id}: ${JSON.stringify({workflowsVerified, failureVerified, fault_execution: faultExecution, local_fault: localFault, fault_stored_nodes: faultStoredNodes.map(x => Boolean(x.result))})}`);
    }

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
      injected_failure: {
        workflow_id: faultWorkflowId.toString(),
        fail_at_node: faultNodeId,
        multi_contract: faultExecution,
        failed_receipt_observed: failureReceiptObserved,
        same_receiver: localFault,
        multi_contract_stored_nodes: faultStoredNodes,
        multi_contract_unfinished: !faultStoredNodes.every(x => x.result),
        multi_contract_completed_sinks: profile.successors.map((out, node) => out.length === 0 ? node : null).filter(x => x !== null).every(node => Boolean(faultStoredNodes[node].result)),
      },
    };
    caseSessions.push(session);
  }

  return {
    profile: { ...profile, operations_per_node: OPERATIONS },
    nearcore: { version: '2.14.0', protocol_version: 88, chain_id: (await (await fetch(`${sandbox.rpcUrl}/status`)).json()).chain_id, single_validator_sandbox: true },
    stages: stageAccounts.map(x => x.accountId),
    same_receiver_contract: sameReceiver.accountId,
    gas_budget_tgas: { top_level: 300, per_stage_node: gasNeeds },
    sessions: caseSessions,
  };
}

async function runRepeat(repeat) {
  const sandbox = await Sandbox.start({ version: VERSION, config: { additionalConfig: { consensus: { min_block_production_delay: { secs: 1, nanos: 0 }, max_block_production_delay: { secs: 1, nanos: 0 }, produce_empty_blocks: true } } } });
  let worker;
  try {
    const keyStore = new keyStores.InMemoryKeyStore();
    await keyStore.setKey('custom', DEFAULT_ACCOUNT_ID, KeyPair.fromString(DEFAULT_PRIVATE_KEY));
    worker = await Worker.init({ network: 'custom', rpcAddr: sandbox.rpcUrl, rootAccountId: DEFAULT_ACCOUNT_ID, keyStore });
    const root = worker.rootAccount;
    const completedProfiles = [];
    const repeatRaw = { repeat, chain_id: null, profiles: completedProfiles };
    rawRepeatResults.push(repeatRaw);
    for (const profile of profiles) {
      const manifestIndex = PROFILE_MANIFEST.profiles.findIndex(item => item.profile_id === profile.profile_id);
      if (manifestIndex < 0) throw new Error(`profile missing from manifest: ${profile.profile_id}`);
      const result = await runProfile(root, sandbox, repeat, profile, manifestIndex + 1);
      completedProfiles.push(result);
      const partial = { experiment: PROFILE_MANIFEST.experiment, near_sandbox: '0.3.4', near_workspaces: '5.0.0', requested_nearcore: VERSION, repeat, completed_profiles: completedProfiles };
      repeatRaw.chain_id = result.nearcore.chain_id;
      await writeFile(RAW, `${JSON.stringify({ experiment: PROFILE_MANIFEST.experiment, near_sandbox: '0.3.4', near_workspaces: '5.0.0', requested_nearcore: VERSION, repeats: rawRepeatResults, current_repeat_partial: partial }, null, 2)}\n`);
      console.log(`repeat ${repeat}: finished ${result.profile.profile_id}`);
    }
    return { repeat, chain_id: completedProfiles[0]?.nearcore.chain_id, profiles: completedProfiles };
  } finally {
    if (worker) await worker.tearDown();
    await sandbox.tearDown();
  }
}

const result = {
  experiment: PROFILE_MANIFEST.experiment,
  design: { profile_count_per_repeat: profiles.length, fresh_repeats: REPEATS, session_count_per_profile: profiles[0]?.sessions_per_profile ?? null, graph_source: PROFILE_PATH.pathname, metrics: 'raw receipt IDs, edge/node logs, success/failure outcomes, receipt block-height spans, API-call-to-first-queryable-sink latency and timestamp; SDK duration diagnostic only', near_block_production_delay_sec: 1 },
  runtime: { near_sandbox: '0.3.4', near_workspaces: '5.0.0', node: process.version, requested_nearcore: VERSION, wasm_stage: resolve(new URL('./artifacts/stage_contract.wasm', ROOT).pathname), wasm_same_receiver: resolve(new URL('./artifacts/same_receiver_contract.wasm', ROOT).pathname) },
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
