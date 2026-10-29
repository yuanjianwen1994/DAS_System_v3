import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { createServer } from 'node:http';
import { Sandbox, DEFAULT_ACCOUNT_ID, DEFAULT_PRIVATE_KEY } from 'near-sandbox';
import { Worker } from 'near-workspaces';
import { KeyPair, keyStores } from 'near-api-js';

const ROOT = new URL('.', import.meta.url);
const MANIFEST_PATH = process.env.STRESS_PROFILE_MANIFEST_PATH ?? new URL('../dag_workflow_20261027/profile_manifest.json', import.meta.url);
const MANIFEST = JSON.parse(await readFile(MANIFEST_PATH, 'utf8'));
const PROFILE_ID = process.env.STRESS_PROFILE_ID ?? 'dag-04-chain-d4-w1-theta1-nu1';
const PROFILE = MANIFEST.profiles.find(p => p.profile_id === PROFILE_ID);
if (!PROFILE) throw new Error(`unknown profile ${PROFILE_ID}`);
const SCHEDULE_PATH = process.env.STRESS_SCHEDULE_PATH ?? new URL('../timing_comparison_20261027/paired_load_schedule.json', import.meta.url);
const SCHEDULE_TEXT = await readFile(SCHEDULE_PATH, 'utf8');
const SCHEDULE = JSON.parse(SCHEDULE_TEXT);
const SCENARIO_NAMES = Object.keys(SCHEDULE.scenarios);
const REQUESTED_SCENARIOS = (process.env.STRESS_SCHEDULE_SCENARIOS ?? process.env.STRESS_SCHEDULE_SCENARIO ?? 'matched_low').split(',').map(x => x.trim()).filter(Boolean);
for (const scenario of REQUESTED_SCENARIOS) if (!SCHEDULE.scenarios[scenario]) throw new Error(`unknown schedule scenario ${scenario}`);
const REPEAT = BigInt(process.env.STRESS_REPEAT ?? '1');
const TAG = process.env.STRESS_TAG ?? `paired_${REQUESTED_SCENARIOS.join('_')}_r${REPEAT}`;
const OUT_DIR = process.env.STRESS_OUTPUT_DIR ? new URL(`file://${process.env.STRESS_OUTPUT_DIR.replace(/\/$/, '')}/`) : new URL('./', import.meta.url);
const OUT = new URL(`${TAG}_near_results.json`, OUT_DIR);
const RAW = new URL(`${TAG}_near_raw.json`, OUT_DIR);
const WORKERS = Number(process.env.STRESS_WORKERS ?? SCHEDULE.resource_policy.sender_accounts_per_platform);
const DIAGNOSTIC_LIGHT = process.env.STRESS_DIAGNOSTIC_LIGHT === '1';
const OPERATIONS = SCHEDULE.operations_per_node;
const GAS = 300n * 10n ** 12n;
const raw = { experiment: 'paired-open-loop-independent-stage-dag', platform: 'NEAR Sandbox', profile_id: PROFILE.profile_id,
  schedule_scenarios: REQUESTED_SCENARIOS, repeat: Number(REPEAT), schedule_sha256: createHash('sha256').update(SCHEDULE_TEXT).digest('hex'),
  block_period_sec: SCHEDULE.block_period_sec, theta: 1, operations_per_node: OPERATIONS,
  worker_accounts: WORKERS, diagnostic_light: DIAGNOSTIC_LIGHT,
  diagnostic_overrides: { rates_per_sec: process.env.STRESS_RATE_OVERRIDE ?? null,
    warmup_sec: process.env.STRESS_WARMUP_OVERRIDE_SEC ?? null, measurement_sec: process.env.STRESS_MEASUREMENT_OVERRIDE_SEC ?? null }, phases: [] };

function gasNeeds(successors, base = 7, callback = 2) {
  const needs = Array(successors.length).fill(base);
  for (let node = successors.length - 1; node >= 0; node--) needs[node] = base + successors[node].reduce((sum, child) => sum + needs[child], 0) + (successors[node].length > 1 ? callback : 0);
  return needs;
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function rpc(rpcUrl, method, params) {
  const response = await fetch(`${rpcUrl}/`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }) });
  const body = await response.json();
  if (body.error) throw new Error(JSON.stringify(body.error));
  return body.result;
}
async function createRpcProxy(upstreamUrl) {
  const counts = new Map();
  const durations = new Map();
  const errors = new Map();
  const record = (method, elapsed, failed) => {
    counts.set(method, (counts.get(method) ?? 0) + 1);
    durations.set(method, [...(durations.get(method) ?? []), elapsed]);
    if (failed) errors.set(method, (errors.get(method) ?? 0) + 1);
  };
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const bodyText = Buffer.concat(chunks).toString('utf8');
    let methods = [];
    try {
      const parsed = JSON.parse(bodyText);
      methods = (Array.isArray(parsed) ? parsed : [parsed]).map(x => x.method ?? 'unknown');
    } catch { methods = ['invalid_json']; }
    const started = performance.now();
    try {
      const upstream = await fetch(upstreamUrl, { method: request.method, headers: { 'content-type': 'application/json' }, body: bodyText });
      const responseText = await upstream.text();
      for (const method of methods) record(method, performance.now() - started, !upstream.ok);
      response.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type') ?? 'application/json' });
      response.end(responseText);
    } catch (error) {
      for (const method of methods) record(method, performance.now() - started, true);
      response.writeHead(502, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: String(error) }));
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  return {
    rpcUrl: `http://127.0.0.1:${address.port}`,
    snapshot() {
      return { counts: Object.fromEntries(counts), errors: Object.fromEntries(errors),
        durations_ms: Object.fromEntries([...durations].map(([k,v]) => [k, [...v]])) };
    },
    async close() { await new Promise(resolve => server.close(resolve)); },
  };
}
function rpcDelta(before, after) {
  const output = {};
  const methods = new Set([...Object.keys(before.counts), ...Object.keys(after.counts)]);
  for (const method of [...methods].sort()) {
    const count0 = before.counts[method] ?? 0, count1 = after.counts[method] ?? 0;
    const values = (after.durations_ms[method] ?? []).slice(count0);
    const sorted = [...values].sort((a,b) => a-b);
    output[method] = { calls: count1-count0, errors: (after.errors[method] ?? 0) - (before.errors[method] ?? 0),
      total_ms: values.reduce((a,b) => a+b,0), mean_ms: values.length ? values.reduce((a,b) => a+b,0)/values.length : null,
      p95_ms: values.length ? sorted[Math.ceil(.95*values.length)-1] : null };
  }
  return output;
}
async function mapLimit(values, limit, fn) {
  const output = Array(values.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, values.length) }, async () => {
    while (next < values.length) { const index = next++; output[index] = await fn(values[index]); }
  }));
  return output;
}

const result = { experiment: raw.experiment, started_at: new Date().toISOString(), error: null };
let sandbox, worker, rpcProxy;
try {
  sandbox = await Sandbox.start({ version: '2.14.0', config: { additionalConfig: { consensus: { min_block_production_delay: { secs: 1, nanos: 0 }, max_block_production_delay: { secs: 1, nanos: 0 }, produce_empty_blocks: true } } } });
  rpcProxy = await createRpcProxy(sandbox.rpcUrl);
  const keyStore = new keyStores.InMemoryKeyStore();
  await keyStore.setKey('custom', DEFAULT_ACCOUNT_ID, KeyPair.fromString(DEFAULT_PRIVATE_KEY));
  worker = await Worker.init({ network: 'custom', rpcAddr: rpcProxy.rpcUrl, rootAccountId: DEFAULT_ACCOUNT_ID, keyStore });
  const root = worker.rootAccount;
  const stages = [];
  const needs = gasNeeds(PROFILE.successors);
  for (let node = 0; node < PROFILE.node_count; node++) {
    const account = await root.devCreateAccount();
    await account.deploy(await readFile(new URL('artifacts/stage_contract.wasm', ROOT)));
    stages.push(account);
  }
  for (let node = 0; node < PROFILE.node_count; node++) {
    const successors = PROFILE.successors[node].map(child => [stages[child].accountId, child, needs[child]]);
    await root.call(stages[node], 'new', { node_id: node, fail_workflow_id: '999999999999999', parents: PROFILE.parents[node], successors, operations: OPERATIONS }, { gas: GAS });
  }
  const drivers = [];
  for (let i = 0; i < WORKERS; i++) drivers.push(await root.devCreateAccount());
  const tails = drivers.map(() => Promise.resolve());
  const slotDepth = Array(WORKERS).fill(0);
  let maxQueued = 0, maxInFlight = 0, inFlight = 0, serial = 0;
  const sinkNode = PROFILE.node_count - 1;
  const observations = [];
  const usedSessionIds = new Set();
  const usedWorkflowIds = new Set();

  async function runOne(job) {
    const preSubmitSink = await stages[sinkNode].view('stage_result', { workflow_id: job.workflowId });
    if (preSubmitSink !== null) throw new Error(`sink is not null before submit for workflow ${job.workflowId}`);
    const clientStartedAt = new Date().toISOString();
    const clientStarted = performance.now();
    const scheduledLagMs = Math.max(0, clientStarted - job.offeredMonoMs);
    inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
    let receiptTreeDoneAt = null, receiptTreeDoneWall = null, receiptTree = null, callError = null;
    const callPromise = drivers[job.slot].callRaw(stages[0], 'start', { session_id: job.sessionId, workflow_id: job.workflowId, seed: job.seed }, { gas: GAS })
      .then(value => { receiptTree = value; receiptTreeDoneAt = performance.now(); receiptTreeDoneWall = new Date().toISOString(); })
      .catch(error => { callError = String(error); receiptTreeDoneAt = performance.now(); receiptTreeDoneWall = new Date().toISOString(); });
    let sinkQueryableAt = null, sinkQueryableWall = null, sinkValue = null;
    if (DIAGNOSTIC_LIGHT) {
      await callPromise;
      try {
        sinkValue = await stages[sinkNode].view('stage_result', { workflow_id: job.workflowId });
        if (sinkValue) { sinkQueryableAt = performance.now(); sinkQueryableWall = new Date().toISOString(); }
      } catch {}
    } else {
      while (receiptTreeDoneAt === null && sinkQueryableAt === null) {
        try {
          sinkValue = await stages[sinkNode].view('stage_result', { workflow_id: job.workflowId });
          if (sinkValue) { sinkQueryableAt = performance.now(); sinkQueryableWall = new Date().toISOString(); }
        } catch {}
        if (sinkQueryableAt === null && receiptTreeDoneAt === null) await sleep(100);
      }
      await callPromise;
      if (sinkQueryableAt === null) {
        try {
          sinkValue = await stages[sinkNode].view('stage_result', { workflow_id: job.workflowId });
          if (sinkValue) { sinkQueryableAt = performance.now(); sinkQueryableWall = new Date().toISOString(); }
        } catch {}
      }
    }
    const sinkStateMatchesWorkflow = Boolean(sinkValue && String(sinkValue.session_id) === String(job.sessionId) && Number(sinkValue.node_id) === sinkNode);
    const rawResult = receiptTree?.result;
    const transaction = rawResult?.transaction ?? null;
    const transactionHash = transaction?.hash ?? rawResult?.transaction_outcome?.id ?? null;
    const receiptStatuses = rawResult?.receipts_outcome?.map(item => ({
      outcome_id: item.id, executor_id: item.outcome.executor_id,
      block_hash: item.block_hash ?? null,
      status_kind: Object.keys(item.outcome.status ?? {})[0] ?? null,
      failure: item.outcome.status?.Failure ?? null,
      logs: item.outcome.logs ?? [],
    })) ?? [];
    const allLogs = [...(rawResult?.transaction_outcome?.outcome?.logs ?? []), ...receiptStatuses.flatMap(item => item.logs)];
    const nodeEvents = allLogs.filter(log => log.startsWith('DAG_NODE|')).map(log => {
      const [, sessionId, workflowId, nodeId, checksum, operations] = log.split('|');
      return { session_id: sessionId, workflow_id: workflowId, node_id: Number(nodeId), checksum, operations: Number(operations) };
    }).filter(event => event.session_id === job.sessionId && event.workflow_id === job.workflowId);
    const allStageStates = DIAGNOSTIC_LIGHT ? [] : await Promise.all(stages.map(async (stage, nodeId) => ({
      node_id: nodeId,
      state: await stage.view('stage_result', { workflow_id: job.workflowId }).catch(() => null),
    })));
    let statesMatchEvents = DIAGNOSTIC_LIGHT
      ? nodeEvents.length === PROFILE.node_count && new Set(nodeEvents.map(x => x.node_id)).size === PROFILE.node_count
      : allStageStates.every(item => {
      const state = item.state;
      const event = nodeEvents.find(x => x.node_id === item.node_id && x.session_id === job.sessionId);
      return state && event && String(state.session_id) === job.sessionId && Number(state.node_id) === item.node_id &&
        String(state.checksum) === event.checksum && Number(state.input_count) === (item.node_id === 0 ? 0 : 1) &&
        Number(state.operation_count) === OPERATIONS && event.operations === OPERATIONS;
    });
    const succeeded = Boolean(receiptTree?.succeeded && !receiptTree?.failed && sinkStateMatchesWorkflow &&
      nodeEvents.length === PROFILE.node_count && (DIAGNOSTIC_LIGHT || statesMatchEvents) && transactionHash === rawResult?.transaction_outcome?.id &&
      transaction?.signer_id === drivers[job.slot].accountId && Number.isSafeInteger(transaction?.nonce) && transaction?.public_key);
    const row = {
      phase: job.phase, warmup: job.warmup, workflow_id: job.workflowId, driver_slot: job.slot,
      session_id: job.sessionId, sender_account_id: drivers[job.slot].accountId, seed: job.seed,
      pre_submit_sink_state: preSubmitSink,
      planned_at: job.plannedAt, offered_at: job.offeredAt, call_issued_at: clientStartedAt,
      receipt_tree_completed_at: receiptTreeDoneWall,
      sink_first_queryable_at: sinkQueryableWall,
      client_queue_delay_ms: scheduledLagMs,
      generator_schedule_lag_ms: job.offeredMonoMs - job.plannedMonoMs,
      call_to_receipt_tree_ms: receiptTreeDoneAt === null ? null : receiptTreeDoneAt - clientStarted,
      call_to_sink_queryable_ms: sinkQueryableAt === null ? null : sinkQueryableAt - clientStarted,
      offered_to_sink_queryable_ms: sinkQueryableAt === null ? null : (clientStarted - job.offeredMonoMs) + (sinkQueryableAt - clientStarted),
      planned_to_sink_queryable_ms: sinkQueryableAt === null ? null : sinkQueryableAt - job.plannedMonoMs,
      receipt_count: receiptTree?.result?.receipts_outcome?.length ?? null,
      transaction_hash: transactionHash,
      transaction_nonce: transaction?.nonce ?? null,
      transaction_public_key: transaction?.public_key?.toString?.() ?? transaction?.public_key ?? null,
      transaction_signer_id: transaction?.signer_id ?? null,
      transaction_receiver_id: transaction?.receiver_id ?? null,
      transaction_hash_matches_outcome: transactionHash === (rawResult?.transaction_outcome?.id ?? null),
      transaction_outcome_block_hash: rawResult?.transaction_outcome?.block_hash ?? null,
      block_hashes: [...new Set([rawResult?.transaction_outcome?.block_hash, ...receiptStatuses.map(x => x.block_hash)].filter(Boolean))],
      transaction_block_hash: rawResult?.transaction_outcome?.block_hash ?? null,
      receipt_ids: receiptStatuses.map(x => x.outcome_id),
      sink_state_at_first_query: sinkValue,
      sink_state_matches_workflow: sinkStateMatchesWorkflow,
      transaction_outcome_status: rawResult?.transaction_outcome?.outcome?.status ?? null,
      receipt_statuses: receiptStatuses,
      node_events: nodeEvents,
      all_stage_states: allStageStates,
      states_match_receipt_events: statesMatchEvents,
      succeeded, failure: callError ?? (receiptTree?.failed ? 'transaction_or_receipt_failed' : null),
    };
    observations.push(row);
    inFlight--; return row;
  }

  async function phase(rate, seconds, warmup, phaseIndex, rateIndex) {
    maxQueued = 0;
    maxInFlight = 0;
    if (inFlight !== 0) throw new Error(`phase ${phaseIndex} began with ${inFlight} in-flight calls`);
    const count = Math.max(1, Math.floor(rate * seconds));
    const warmupOffset = warmup ? 0n : 50000000n;
    const idBase = 500000000000n + REPEAT * 10000000000n + BigInt(phaseIndex) * 100000000n + warmupOffset;
    const sessionId = String(idBase);
    const workflowBase = 800000000000n + REPEAT * 10000000000n + BigInt(phaseIndex) * 100000000n + warmupOffset;
    if (usedSessionIds.has(sessionId)) throw new Error(`duplicate phase session id ${sessionId}`);
    usedSessionIds.add(sessionId);
    const phaseStartedAt = new Date().toISOString();
    const phaseWindowEndAt = new Date(Date.now() + seconds * 1000).toISOString();
    const baseMono = performance.now();
    const rpcLoadStart = rpcProxy.snapshot();
    const jobs = [];
    const schedulers = [];
    for (let i = 0; i < count; i++) {
      const offeredMonoMs = baseMono + i * (1000 / rate);
      const workflowId = String(workflowBase + BigInt(i + 1));
      if (usedWorkflowIds.has(workflowId)) throw new Error(`duplicate workflow id ${workflowId}`);
      usedWorkflowIds.add(workflowId);
      if (BigInt(sessionId) > BigInt(Number.MAX_SAFE_INTEGER) || BigInt(workflowId) > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('session/workflow id exceeds JS safe integer range');
      const plannedMonoMs = offeredMonoMs;
      const plannedAt = new Date(Date.now() + (plannedMonoMs - performance.now())).toISOString();
      const slot = serial++ % WORKERS;
      schedulers.push((async () => {
        await sleep(Math.max(0, plannedMonoMs - performance.now()));
        const actualOfferedMonoMs = performance.now();
        const job = { phase: phaseIndex, warmup, sessionId, workflowId, slot,
          plannedMonoMs, plannedAt, offeredMonoMs: actualOfferedMonoMs, offeredAt: new Date().toISOString(),
          seed: String(0x123456789abcn ^ (BigInt(workflowId) * 0x9e3779b97f4a7c15n & ((1n << 64n) - 1n))) };
        slotDepth[slot]++; maxQueued = Math.max(maxQueued, slotDepth[slot]);
        const task = tails[slot].then(() => { slotDepth[slot]--; return runOne(job); });
        tails[slot] = task.then(() => undefined, () => undefined);
        jobs.push(task);
      })());
    }
    await Promise.all(schedulers);
    const generationEndedAt = new Date().toISOString();
    const generationEndedMonoMs = performance.now();
    const completed = await Promise.race([Promise.all(jobs), sleep(120000).then(() => null)]);
    const phaseRows = observations.filter(row => row.phase === phaseIndex && row.warmup === warmup);
    const rpcLoadEnd = rpcProxy.snapshot();
    if (DIAGNOSTIC_LIGHT) {
      await mapLimit(phaseRows, 32, async row => {
        const wid = row.workflow_id;
        const states = await Promise.all(stages.map(async (stage, nodeId) => ({
          node_id: nodeId,
          state: await stage.view('stage_result', { workflow_id: wid }).catch(() => null),
        })));
        const events = row.node_events;
        const match = states.every(item => {
          const state = item.state;
          const event = events.find(x => x.node_id === item.node_id && x.session_id === row.session_id && x.workflow_id === wid);
          return state && event && String(state.session_id) === row.session_id && Number(state.node_id) === item.node_id &&
            String(state.checksum) === event.checksum && Number(state.input_count) === (item.node_id === 0 ? 0 : 1) &&
            Number(state.operation_count) === OPERATIONS && event.operations === OPERATIONS;
        });
        row.all_stage_states = states;
        row.states_match_receipt_events = match;
        row.succeeded = Boolean(row.succeeded && match);
        row.failure = row.succeeded ? null : (row.failure ?? 'post_drain_state_event_audit_failed');
        return row;
      });
    }
    const rpcPostAuditEnd = rpcProxy.snapshot();
    const windowEndMs = Date.parse(phaseWindowEndAt);
    const attemptedInWindow = phaseRows.filter(row => row.call_issued_at && Date.parse(row.call_issued_at) <= windowEndMs);
    const successfulInWindow = phaseRows.filter(row => row.succeeded && row.sink_first_queryable_at && Date.parse(row.sink_first_queryable_at) <= windowEndMs).length;
    const inFlightAtWindowEnd = phaseRows.filter(row => row.call_issued_at && row.receipt_tree_completed_at &&
      Date.parse(row.call_issued_at) <= windowEndMs && Date.parse(row.receipt_tree_completed_at) > windowEndMs).length;
    const blockHashes = [...new Set(phaseRows.flatMap(row => row.block_hashes ?? []))];
    const blockUsage = await mapLimit(blockHashes, 8, async blockHash => {
      try {
        const block = await rpc(sandbox.rpcUrl, 'block', { block_id: blockHash });
        const chunks = block.chunks ?? [];
        const gasUsed = chunks.reduce((sum, chunk) => sum + BigInt(chunk.gas_used ?? 0), 0n);
        const gasLimit = chunks.reduce((sum, chunk) => sum + BigInt(chunk.gas_limit ?? 0), 0n);
        return { block_hash: blockHash, height: block.header?.height ?? null,
          gas_used: gasUsed.toString(), gas_limit: gasLimit.toString(),
          utilization: gasLimit > 0n ? Number(gasUsed) / Number(gasLimit) : null,
          chunk_count: chunks.length, timestamp_nanosec: block.header?.timestamp_nanosec ?? null };
      } catch (error) { return { block_hash: blockHash, error: String(error) }; }
    });
    const completedTimes = phaseRows.map(row => row.sink_first_queryable_at ? Date.parse(row.sink_first_queryable_at) : null).filter(Number.isFinite);
    const drainTimeAfterWindowMs = completedTimes.length ? Math.max(0, Math.max(...completedTimes) - windowEndMs) : null;
    const summary = {
      phase: phaseIndex, rate_index: rateIndex, warmup, session_id: sessionId, offered_rate_per_sec: rate, offered: count,
      phase_started_at: phaseStartedAt, measurement_window_end_at: phaseWindowEndAt, generation_ended_at: generationEndedAt,
      drained: completed !== null, completed: phaseRows.filter(row => row.call_to_receipt_tree_ms !== null).length,
      successful_sink_completions: phaseRows.filter(row => row.succeeded).length,
      submission_attempts_in_measurement_window: attemptedInWindow.length,
      offered_call_rate_in_measurement_window: attemptedInWindow.length / seconds,
      successful_completions_in_measurement_window: successfulInWindow,
      measured_window_goodput_per_sec: successfulInWindow / seconds,
      failures: phaseRows.filter(row => !row.succeeded && row.call_to_receipt_tree_ms !== null).length,
      unfinished_at_120s: completed === null ? count - phaseRows.filter(row => row.call_to_receipt_tree_ms !== null).length : 0,
      in_flight_at_measurement_window_end: inFlightAtWindowEnd,
      drain_time_after_window_ms: drainTimeAfterWindowMs,
      offered_generator_rate_per_sec: count / ((generationEndedMonoMs - baseMono + 1000 / rate) / 1000),
      mean_generator_schedule_lag_ms: phaseRows.length ? phaseRows.reduce((a, x) => a + x.generator_schedule_lag_ms, 0) / phaseRows.length : null,
      drained_successes_per_measurement_second: phaseRows.filter(row => row.succeeded).length / seconds,
      client_queue_depth_high_water: maxQueued, client_inflight_high_water: maxInFlight,
      mean_client_queue_delay_ms: phaseRows.length ? phaseRows.reduce((a, x) => a + x.client_queue_delay_ms, 0) / phaseRows.length : null,
      rpc_load_path_by_method: rpcDelta(rpcLoadStart, rpcLoadEnd),
      rpc_post_drain_audit_by_method: rpcDelta(rpcLoadEnd, rpcPostAuditEnd),
      receipt_tree_waiting_method_counts: Object.fromEntries(Object.entries(rpcDelta(rpcLoadStart, rpcLoadEnd)).filter(([method]) => /broadcast_tx|tx_status|query/.test(method))),
      block_usage_samples: blockUsage,
      rows: phaseRows,
    };
    raw.phases.push(summary);
    await writeFile(RAW, `${JSON.stringify(raw, null, 2)}\n`);
    await writeFile(OUT, `${JSON.stringify({ ...result, finished_at: new Date().toISOString(), phases: raw.phases.map(({ rows, ...x }) => x) }, null, 2)}\n`);
    if (completed === null) throw new Error(`phase ${phaseIndex} failed to drain in 120 seconds`);
  }

  for (const scenario of REQUESTED_SCENARIOS) {
    const scenarioIndex = SCENARIO_NAMES.indexOf(scenario);
    const config = SCHEDULE.scenarios[scenario];
    const rates = process.env.STRESS_RATE_OVERRIDE
      ? process.env.STRESS_RATE_OVERRIDE.split(',').map(Number)
      : config.rates_per_sec.map(Number);
    const warmupSeconds = Number(process.env.STRESS_WARMUP_OVERRIDE_SEC ?? config.warmup_sec);
    const measurementSeconds = Number(process.env.STRESS_MEASUREMENT_OVERRIDE_SEC ?? config.measurement_sec);
    for (let i = 0; i < rates.length; i++) {
      const phaseIndex = scenarioIndex * 10 + i;
      await phase(rates[i], warmupSeconds, true, phaseIndex, i);
      await phase(rates[i], measurementSeconds, false, phaseIndex, i);
    }
  }
} catch (error) {
  result.error = String(error?.stack ?? error);
} finally {
  result.finished_at = new Date().toISOString();
  await writeFile(OUT, `${JSON.stringify({ ...result, phases: raw.phases.map(({ rows, ...x }) => x) }, null, 2)}\n`);
  await writeFile(RAW, `${JSON.stringify(raw, null, 2)}\n`);
  if (worker) await worker.tearDown();
  if (rpcProxy) await rpcProxy.close();
  if (sandbox) await sandbox.tearDown();
}
if (result.error) { console.error(result.error); process.exitCode = 1; }
