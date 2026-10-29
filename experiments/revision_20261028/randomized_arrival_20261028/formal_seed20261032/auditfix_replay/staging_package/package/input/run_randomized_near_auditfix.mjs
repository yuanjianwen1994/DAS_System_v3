import { readFile, writeFile, rm, unlink } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { createServer } from 'node:http';
import { Sandbox, DEFAULT_ACCOUNT_ID, DEFAULT_PRIVATE_KEY } from 'near-sandbox';
import { Worker } from 'near-workspaces';
import { KeyPair, keyStores } from 'near-api-js';

const ROOT = new URL('.', import.meta.url);
const RUNNER_SHA256 = createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex');
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

function validateRandomizedScenario(config) {
  if (!config.arrival_seed || config.arrival_rule !== 'paired_exponential') throw new Error('randomized schedule requires a seed and paired_exponential arrival_rule');
  for (const rate of config.rates_per_sec ?? []) {
    const matches = Object.entries(config.arrival_offsets_sec_by_rate ?? {}).filter(([key]) => Number(key) === Number(rate));
    if (matches.length !== 1 || !Array.isArray(matches[0][1]) || matches[0][1].length === 0) throw new Error(`missing or ambiguous randomized trajectory for lambda=${rate}`);
    const offsets = matches[0][1].map(Number), phase = Number(config.phase_random_offset_sec);
    const end = phase + Number(config.warmup_sec) + Number(config.measurement_sec);
    if (offsets.some((x,i) => !Number.isFinite(x) || (i > 0 && x < offsets[i-1])) || Math.abs(offsets[0]-phase) > 1e-8 || offsets.at(-1) >= end) throw new Error(`invalid trajectory bounds/order for lambda=${rate}`);
  }
}
function offsetsForRate(config, rate) {
  const matches = Object.entries(config.arrival_offsets_sec_by_rate ?? {}).filter(([key]) => Number(key) === Number(rate));
  if (matches.length !== 1) throw new Error(`missing_or_ambiguous_offset_trace_for_rate_${rate}`);
  return matches[0][1].map(Number);
}
for (const scenario of REQUESTED_SCENARIOS) validateRandomizedScenario(SCHEDULE.scenarios[scenario]);
const REPEAT = BigInt(process.env.STRESS_REPEAT ?? '1');
const TAG = process.env.STRESS_TAG ?? `paired_${REQUESTED_SCENARIOS.join('_')}_r${REPEAT}`;
const OUT_DIR = process.env.STRESS_OUTPUT_DIR ? new URL(`file://${process.env.STRESS_OUTPUT_DIR.replace(/\/$/, '')}/`) : new URL('./', import.meta.url);
const OUT = new URL(`${TAG}_near_results.json`, OUT_DIR);
const RAW = new URL(`${TAG}_near_raw.json`, OUT_DIR);
const WORKERS = Number(process.env.STRESS_WORKERS ?? SCHEDULE.resource_policy.sender_accounts_per_platform);
const DIAGNOSTIC_LIGHT = process.env.STRESS_DIAGNOSTIC_LIGHT === '1';
const PIPELINE_PREHEAT_SEC = Number(process.env.STRESS_PIPELINE_PREHEAT_SEC ?? SCHEDULE.scenarios[REQUESTED_SCENARIOS[0]].warmup_sec);
const PIPELINE_OBSERVATION_SEC = Number(process.env.STRESS_PIPELINE_OBSERVATION_SEC ?? SCHEDULE.scenarios[REQUESTED_SCENARIOS[0]].measurement_sec);
const STARTUP_ASSERTIONS = {
  pipeline_mode_is_1: process.env.STRESS_PIPELINE_MODE === '1',
  rate_override_absent: !process.env.STRESS_RATE_OVERRIDE,
  warmup_override_absent: !process.env.STRESS_WARMUP_OVERRIDE_SEC,
  measurement_override_absent: !process.env.STRESS_MEASUREMENT_OVERRIDE_SEC,
  schedule_is_paired_exponential: REQUESTED_SCENARIOS.every(name => SCHEDULE.scenarios[name].arrival_rule === 'paired_exponential'),
  schedule_seed_present: REQUESTED_SCENARIOS.every(name => Number.isInteger(Number(SCHEDULE.scenarios[name].arrival_seed))),
};
if (Object.values(STARTUP_ASSERTIONS).some(value => !value)) throw new Error(`startup_config_assertion_failed: ${JSON.stringify(STARTUP_ASSERTIONS)}`);
const OPERATIONS = SCHEDULE.operations_per_node;
const GAS = 300n * 10n ** 12n;
const READ_TIMEOUT_MS = Number(process.env.STRESS_READ_TIMEOUT_MS ?? 15000);
const READ_ATTEMPTS = Number(process.env.STRESS_READ_ATTEMPTS ?? 3);
const RECEIPT_TIMEOUT_MS = Number(process.env.STRESS_RECEIPT_TIMEOUT_MS ?? 120000);
const AUDIT_CHECKPOINT_EVERY = Number(process.env.STRESS_AUDIT_CHECKPOINT_EVERY ?? 10);
const SINK_POLL_INTERVAL_MS = Number(process.env.STRESS_SINK_POLL_MS ?? 50);
const raw = { experiment: 'paired-open-loop-continuous-pipeline', platform: 'NEAR Sandbox', runner_sha256: RUNNER_SHA256, route: 'multi_contract', profile_id: PROFILE.profile_id,
  schedule_scenarios: REQUESTED_SCENARIOS, repeat: Number(REPEAT), schedule_sha256: createHash('sha256').update(SCHEDULE_TEXT).digest('hex'),
  block_period_sec: SCHEDULE.block_period_sec, theta: 1, operations_per_node: OPERATIONS,
  worker_accounts: WORKERS, diagnostic_light: DIAGNOSTIC_LIGHT,
  diagnostic_overrides: { rates_per_sec: process.env.STRESS_RATE_OVERRIDE ?? null,
    warmup_sec: process.env.STRESS_WARMUP_OVERRIDE_SEC ?? null, measurement_sec: process.env.STRESS_MEASUREMENT_OVERRIDE_SEC ?? null }, phases: [] };
const effectiveConfigStartup = {
  captured_at_utc: new Date().toISOString(), seed: REQUESTED_SCENARIOS.map(name => SCHEDULE.scenarios[name].arrival_seed),
  profile_id: PROFILE.profile_id, scenarios: REQUESTED_SCENARIOS,
  pipeline_mode: process.env.STRESS_PIPELINE_MODE, preheat_sec: PIPELINE_PREHEAT_SEC, observation_sec: PIPELINE_OBSERVATION_SEC,
  receipt_timeout_ms: RECEIPT_TIMEOUT_MS, read_timeout_ms: READ_TIMEOUT_MS, read_attempts: READ_ATTEMPTS,
  rpc_proxy_timeout_ms: Number(process.env.STRESS_RPC_PROXY_TIMEOUT_MS ?? 60000),
  diagnostic_light: DIAGNOSTIC_LIGHT, rates_per_sec: REQUESTED_SCENARIOS.flatMap(name => SCHEDULE.scenarios[name].rates_per_sec.map(Number)),
  schedule_path: String(SCHEDULE_PATH), schedule_sha256: raw.schedule_sha256, runner_sha256: RUNNER_SHA256,
  per_rate_expected_counts: Object.fromEntries(REQUESTED_SCENARIOS.flatMap(name => {
    const config = SCHEDULE.scenarios[name];
    return config.rates_per_sec.map(rate => [String(rate), {
      expected_total: offsetsForRate(config, rate).length,
      phase_offset_sec: config.phase_random_offset_sec,
      offsets_sha256: createHash('sha256').update(offsetsForRate(config, rate).map(x => Number(x).toFixed(9)).join(',')).digest('hex'),
    }]);
  })),
  effective_environment_subset: Object.fromEntries(['STRESS_PIPELINE_MODE','STRESS_PIPELINE_PREHEAT_SEC','STRESS_PIPELINE_OBSERVATION_SEC',
    'STRESS_PROFILE_ID','STRESS_SCHEDULE_SCENARIO','STRESS_REPEAT','STRESS_TAG','STRESS_WORKERS','STRESS_RECEIPT_TIMEOUT_MS',
    'STRESS_RPC_PROXY_TIMEOUT_MS','STRESS_DIAGNOSTIC_LIGHT'].map(key => [key, process.env[key] ?? null])),
  startup_assertions: STARTUP_ASSERTIONS, startup_assertions_passed: true,
};

function gasNeeds(successors, base = 7, callback = 2) {
  const needs = Array(successors.length).fill(base);
  for (let node = successors.length - 1; node >= 0; node--) needs[node] = base + successors[node].reduce((sum, child) => sum + needs[child], 0) + (successors[node].length > 1 ? callback : 0);
  return needs;
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function boundedRead(label, operation, deadlineEpochMs = null) {
  let lastError = null;
  for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt++) {
    let timer;
    const remainingMs = deadlineEpochMs === null ? READ_TIMEOUT_MS : Math.min(READ_TIMEOUT_MS, deadlineEpochMs - Date.now());
    if (remainingMs <= 0) return { ok: false, value: null, attempts: attempt - 1, deadline_expired: true, error: `${label}_deadline_expired` };
    try {
      const result = await Promise.race([
        Promise.resolve().then(operation),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label}_timeout_${remainingMs}ms`)), remainingMs); }),
      ]);
      clearTimeout(timer);
      return { ok: true, value: result, attempts: attempt, deadline_expired: false, error: null };
    } catch (error) {
      clearTimeout(timer);
      lastError = String(error);
      if (attempt < READ_ATTEMPTS) {
        const backoff = Math.min(2000, 250 * 2 ** (attempt - 1));
        if (deadlineEpochMs !== null && Date.now() + backoff >= deadlineEpochMs) break;
        await sleep(backoff);
      }
    }
  }
    const deadlineExpired = deadlineEpochMs !== null && Date.now() >= deadlineEpochMs;
    return { ok: false, value: null, attempts: Math.min(READ_ATTEMPTS, attempt - 1), deadline_expired: deadlineExpired,
      error: deadlineExpired ? `${label}_deadline_expired_after:${lastError}` : lastError };
}
async function settleWithin(label, operation, timeoutMs = 15000) {
  let timer;
  try {
    const value = await Promise.race([
      Promise.resolve().then(operation).then(value => ({ done: true, value })),
      new Promise(resolve => { timer = setTimeout(() => resolve({ done: false, value: null }), timeoutMs); }),
    ]);
    if (!value.done) console.error(`CLEANUP_PENDING|${label}|timeout_ms=${timeoutMs}`);
    return value;
  } catch (error) {
    console.error(`CLEANUP_ERROR|${label}|${String(error)}`);
    return { done: false, value: null };
  } finally { clearTimeout(timer); }
}
async function rpc(rpcUrl, method, params) {
  let lastError = null;
  for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), READ_TIMEOUT_MS);
    try {
      const response = await fetch(`${rpcUrl}/`, { method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }), signal: controller.signal });
      const body = await response.json();
      if (body.error) throw new Error(JSON.stringify(body.error));
      return body.result;
    } catch (error) {
      lastError = String(error);
      if (attempt < READ_ATTEMPTS) await sleep(Math.min(2000, 250 * 2 ** (attempt - 1)));
    } finally { clearTimeout(timer); }
  }
  throw new Error(`${method}_read_failed_after_${READ_ATTEMPTS}_attempts: ${lastError}`);
}
async function createRpcProxy(upstreamUrl) {
  const counts = new Map();
  const durations = new Map();
  const errors = new Map();
  const sendTxLedger = [];
  const httpErrorLedger = [];
  const payloadAttempts = new Map();
  let requestSequence = 0;
  const record = (method, elapsed, failed) => {
    counts.set(method, (counts.get(method) ?? 0) + 1);
    durations.set(method, [...(durations.get(method) ?? []), elapsed]);
    if (failed) errors.set(method, (errors.get(method) ?? 0) + 1);
  };
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const bodyText = Buffer.concat(chunks).toString('utf8');
    let rpcRequests = [];
    try {
      const parsed = JSON.parse(bodyText);
      rpcRequests = (Array.isArray(parsed) ? parsed : [parsed]).map(x => ({ id: x.id ?? null, method: x.method ?? 'unknown', params: x.params ?? null }));
    } catch { rpcRequests = [{ id: null, method: 'invalid_json', params: null }]; }
    const methods = rpcRequests.map(x => x.method);
    const proxyRequestSequence = ++requestSequence;
    const sendTxMeta = rpcRequests.filter(x => x.method === 'send_tx').map(x => {
      const payload = Array.isArray(x.params) ? x.params[0] : x.params?.signed_tx_base64 ?? x.params;
      const fingerprint = payload == null ? null : createHash('sha256').update(String(payload)).digest('hex');
      const attempt = fingerprint ? (payloadAttempts.get(fingerprint) ?? 0) + 1 : null;
      if (fingerprint) payloadAttempts.set(fingerprint, attempt);
      return { proxy_request_sequence: proxyRequestSequence, rpc_request_id: x.id,
        payload_fingerprint_sha256: fingerprint, same_payload_attempt: attempt,
        same_payload_retry_count: attempt == null ? null : attempt - 1 };
    });
    const started = performance.now();
    try {
      const controller = new AbortController();
      const proxyTimeoutMs = Number(process.env.STRESS_RPC_PROXY_TIMEOUT_MS ?? 60000);
      const proxyTimer = setTimeout(() => controller.abort(new Error(`upstream_rpc_timeout_${proxyTimeoutMs}ms`)), proxyTimeoutMs);
      let upstream;
      try { upstream = await fetch(upstreamUrl, { method: request.method, headers: { 'content-type': 'application/json' }, body: bodyText, signal: controller.signal }); }
      finally { clearTimeout(proxyTimer); }
      const responseText = await upstream.text();
      for (const method of methods) record(method, performance.now() - started, !upstream.ok);
      let responseJson = null;
      try { responseJson = JSON.parse(responseText); } catch {}
      const responseEntries = Array.isArray(responseJson) ? responseJson : (responseJson ? [responseJson] : []);
      for (const meta of sendTxMeta) {
        const matched = responseEntries.find(x => x.id === meta.rpc_request_id) ?? responseEntries[0] ?? null;
        const rpcError = matched?.error ?? null;
        sendTxLedger.push({ ...meta, observed_at: new Date().toISOString(), http_status: upstream.status,
          http_ok: upstream.ok, duration_ms: performance.now() - started,
          response_rpc_id: matched?.id ?? null,
          transaction_hash: matched?.result?.transaction_outcome?.id ?? matched?.result?.transaction?.hash ?? null,
          rpc_error: rpcError == null ? null : JSON.stringify(rpcError).slice(0, 4096) });
      }
      if (!upstream.ok || responseEntries.some(x => x.error)) httpErrorLedger.push({ proxy_request_sequence: proxyRequestSequence, observed_at: new Date().toISOString(),
        http_status: upstream.status, methods, response_excerpt: responseText.slice(0, 4096), duration_ms: performance.now() - started });
      response.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type') ?? 'application/json' });
      response.end(responseText);
    } catch (error) {
      for (const method of methods) record(method, performance.now() - started, true);
      const errorText = String(error?.stack ?? error).slice(0, 4096);
      for (const meta of sendTxMeta) sendTxLedger.push({ ...meta, observed_at: new Date().toISOString(), http_status: null,
        http_ok: false, duration_ms: performance.now() - started, response_rpc_id: null, transaction_hash: null,
        rpc_error: errorText, transport_error: errorText });
      httpErrorLedger.push({ proxy_request_sequence: proxyRequestSequence, observed_at: new Date().toISOString(), http_status: null, methods,
        response_excerpt: errorText, duration_ms: performance.now() - started });
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
        durations_ms: Object.fromEntries([...durations].map(([k,v]) => [k, [...v]])),
        send_tx_request_ledger: sendTxLedger.map(x => ({ ...x })), http_error_ledger: httpErrorLedger.map(x => ({ ...x })) };
    },
    async close() {
      server.closeAllConnections?.();
      await new Promise(resolve => server.close(resolve));
    },
    requestsForTransaction(transactionHash) {
      if (!transactionHash) return [];
      return sendTxLedger.filter(x => x.transaction_hash === transactionHash).map(x => ({
        proxy_request_sequence: x.proxy_request_sequence, rpc_request_id: x.rpc_request_id, response_rpc_id: x.response_rpc_id,
        http_status: x.http_status, same_payload_attempt: x.same_payload_attempt,
      }));
    },
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
const CHECKPOINT = new URL(`${TAG}_near_audit_checkpoint.json`, OUT_DIR);
const PROGRESS = new URL(`${TAG}_near_audit_progress.json`, OUT_DIR);
const EFFECTIVE_CONFIG = new URL(`${TAG}_effective_config_startup.json`, OUT_DIR);
const AUDIT_GATE_REQUEST = new URL(`${TAG}_offline_audit_request.json`, OUT_DIR);
const AUDIT_GATE_PATH = process.env.STRESS_AUDIT_GATE_PATH ?? null;
const AUDIT_GATE_TIMEOUT_MS = Number(process.env.STRESS_AUDIT_GATE_TIMEOUT_MS ?? 3600000);
raw.effective_config_startup_path = EFFECTIVE_CONFIG.pathname;
raw.offline_audit_request_path = AUDIT_GATE_PATH ? AUDIT_GATE_REQUEST.pathname : null;
await writeFile(EFFECTIVE_CONFIG, `${JSON.stringify(effectiveConfigStartup, null, 2)}\n`);
let auditProgress = { stage: 'initializing', pending: 0, completed: 0, total: 0, updated_at: new Date().toISOString() };
let auditProgressWrite = Promise.resolve();
function writeAuditProgress(extra = {}) {
  auditProgress = { ...auditProgress, ...extra, updated_at: new Date().toISOString() };
  auditProgressWrite = auditProgressWrite.then(() => writeFile(PROGRESS, `${JSON.stringify(auditProgress, null, 2)}\n`));
  return auditProgressWrite;
}
let sandbox, worker, rpcProxy;
const blockUsageCache = new Map();
function ensureRecentBlockSample(blockHash) {
  if (!blockUsageCache.has(blockHash)) {
    const sample = rpc(sandbox.rpcUrl, 'block', { block_id: blockHash }).then(block => {
      const chunks = block.chunks ?? [];
      const gasUsed = chunks.reduce((sum, chunk) => sum + BigInt(chunk.gas_used ?? 0), 0n);
      const gasLimit = chunks.reduce((sum, chunk) => sum + BigInt(chunk.gas_limit ?? 0), 0n);
      return { block_hash: blockHash, height: block.header?.height ?? null,
        gas_used: gasUsed.toString(), gas_limit: gasLimit.toString(),
        utilization: gasLimit > 0n ? Number(gasUsed) / Number(gasLimit) : null,
        chunk_count: chunks.length, timestamp_nanosec: block.header?.timestamp_nanosec ?? null,
        sampled_at: new Date().toISOString(), sample_method: 'on_workflow_receipt_completion' };
    }).catch(error => ({ block_hash: blockHash, error: String(error), audit_pending: true,
      sampled_at: new Date().toISOString(), sample_method: 'on_workflow_receipt_completion' }));
    blockUsageCache.set(blockHash, sample);
  }
  return blockUsageCache.get(blockHash);
}
async function captureRecentBlocks(blockHashes) {
  await Promise.all([...new Set(blockHashes)].map(ensureRecentBlockSample));
}
try {
  sandbox = await Sandbox.start({ version: '2.13.4', config: { additionalConfig: { consensus: { min_block_production_delay: { secs: 1, nanos: 0 }, max_block_production_delay: { secs: 1, nanos: 0 }, produce_empty_blocks: true } } } });
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
    const preRead = await boundedRead(`pre_submit_view_${job.workflowId}`, () => stages[sinkNode].view('stage_result', { workflow_id: job.workflowId }));
    if (!preRead.ok) {
      const row = { phase: job.phase, warmup: job.warmup, workflow_id: job.workflowId, driver_slot: job.slot,
        session_id: job.sessionId, sender_account_id: drivers[job.slot].accountId, seed: job.seed,
        planned_at: job.plannedAt, offered_at: job.offeredAt, call_issued_at: null,
        execution_status: 'not_submitted', audit_pending: true, audit_status: 'audit_pending',
        audit_error: preRead.error, audit_attempts: preRead.attempts, succeeded: false, failure: null };
      observations.push(row);
      return row;
    }
    const preSubmitSink = preRead.value;
    if (preSubmitSink !== null) throw new Error(`sink is not null before submit for workflow ${job.workflowId}`);
    const clientStartedAt = new Date().toISOString();
    const clientStarted = performance.now();
    const scheduledLagMs = Math.max(0, clientStarted - job.offeredMonoMs);
    inFlight++; maxInFlight = Math.max(maxInFlight, inFlight);
    const receiptWaitStartedWall = new Date().toISOString();
    const receiptDeadlineEpochMs = Date.now() + RECEIPT_TIMEOUT_MS;
    const receiptDeadlineWall = new Date(receiptDeadlineEpochMs).toISOString();
    let receiptTreeDoneAt = null, receiptTreeDoneWall = null, receiptTree = null, callError = null;
    let receiptResolutionStatus = 'waiting';
    const callPromise = drivers[job.slot].callRaw(stages[0], 'start', { session_id: job.sessionId, workflow_id: job.workflowId, seed: job.seed }, { gas: GAS })
      .then(value => { receiptTree = value; receiptTreeDoneAt = performance.now(); receiptTreeDoneWall = new Date().toISOString();
        receiptResolutionStatus = value?.failed || !value?.succeeded ? 'completed_with_execution_failure' : 'completed'; })
      .catch(error => { callError = String(error?.stack ?? error); receiptTreeDoneAt = performance.now(); receiptTreeDoneWall = new Date().toISOString();
        receiptResolutionStatus = 'rpc_or_client_error_outcome_unknown'; });
    let sinkQueryableAt = null, sinkQueryableWall = null, sinkValue = null;
    let sinkAuditError = null;
    let sinkPollLastMissWall = null, sinkPollCount = 0, sinkPollMaxReadMs = 0;
    let sinkReadAttemptsTotal = 0, sinkReadFailureCount = 0;
    if (DIAGNOSTIC_LIGHT) {
      while ((receiptTreeDoneAt === null || sinkQueryableAt === null) && Date.now() < receiptDeadlineEpochMs) {
        if (sinkQueryableAt === null) {
          const pollStarted = performance.now();
          const sinkRead = await boundedRead(`sink_view_${job.workflowId}`, () => stages[sinkNode].view('stage_result', { workflow_id: job.workflowId }), receiptDeadlineEpochMs);
          const pollFinished = performance.now(); sinkPollCount++; sinkPollMaxReadMs = Math.max(sinkPollMaxReadMs, pollFinished-pollStarted);
          sinkReadAttemptsTotal += sinkRead.attempts ?? 0;
          if (sinkRead.ok) {
            sinkValue = sinkRead.value;
            if (sinkValue) { sinkQueryableAt = pollFinished; sinkQueryableWall = new Date().toISOString(); }
            else sinkPollLastMissWall = new Date().toISOString();
          } else {
            if (!sinkRead.deadline_expired) { sinkAuditError = sinkRead.error; sinkReadFailureCount++; }
            sinkPollLastMissWall = new Date().toISOString();
          }
        }
        if (receiptTreeDoneAt === null) await Promise.race([callPromise.then(() => null), sleep(SINK_POLL_INTERVAL_MS)]);
        else if (sinkQueryableAt === null) await sleep(SINK_POLL_INTERVAL_MS);
      }
      if (receiptTreeDoneAt === null) receiptResolutionStatus = 'deadline_expired';
    } else {
      while ((receiptTreeDoneAt === null || sinkQueryableAt === null) && Date.now() < receiptDeadlineEpochMs) {
        if (sinkQueryableAt === null) {
          const sinkRead = await boundedRead(`sink_view_${job.workflowId}`, () => stages[sinkNode].view('stage_result', { workflow_id: job.workflowId }), receiptDeadlineEpochMs);
          sinkPollCount++; sinkReadAttemptsTotal += sinkRead.attempts ?? 0;
          if (sinkRead.ok) {
            sinkValue = sinkRead.value;
            if (sinkValue) { sinkQueryableAt = performance.now(); sinkQueryableWall = new Date().toISOString(); }
            else sinkPollLastMissWall = new Date().toISOString();
          } else {
            if (!sinkRead.deadline_expired) { sinkAuditError = sinkRead.error; sinkReadFailureCount++; }
            sinkPollLastMissWall = new Date().toISOString();
          }
        }
        if ((sinkQueryableAt === null || receiptTreeDoneAt === null) && Date.now() < receiptDeadlineEpochMs) await sleep(100);
      }
      if (receiptTreeDoneAt === null) receiptResolutionStatus = 'deadline_expired';
    }
    if (receiptTreeDoneAt === null && receiptResolutionStatus === 'deadline_expired') callError = callError ?? `receipt_deadline_expired_after_${RECEIPT_TIMEOUT_MS}ms`;
    const sinkResolutionStatus = sinkQueryableAt !== null ? 'queryable' : (sinkAuditError ? 'read_error_at_deadline' : 'not_queryable_at_deadline');
    const sinkStateMatchesWorkflow = Boolean(sinkValue && String(sinkValue.session_id) === String(job.sessionId) && Number(sinkValue.node_id) === sinkNode);
    const rawResult = receiptTree?.result;
    const transaction = rawResult?.transaction ?? null;
    const transactionHash = transaction?.hash ?? rawResult?.transaction_outcome?.id ?? null;
    const requestsForTransaction = rpcProxy?.requestsForTransaction(transactionHash) ?? [];
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
    const stateReads = DIAGNOSTIC_LIGHT ? [] : await Promise.all(stages.map(async (stage, nodeId) => {
      const read = await boundedRead(`state_view_${job.workflowId}_${nodeId}`, () => stage.view('stage_result', { workflow_id: job.workflowId }));
      return { node_id: nodeId, state: read.value, audit_error: read.error, audit_attempts: read.attempts };
    }));
    const allStageStates = stateReads.map(({ node_id, state }) => ({ node_id, state }));
    const receiptEvidenceUnresolved = !receiptTree && ['deadline_expired', 'rpc_or_client_error_outcome_unknown'].includes(receiptResolutionStatus);
    const workflowAuditPending = Boolean(receiptEvidenceUnresolved || sinkQueryableAt === null || sinkAuditError ||
      stateReads.some(item => !item.state && item.audit_error));
    let statesMatchEvents = DIAGNOSTIC_LIGHT
      ? nodeEvents.length === PROFILE.node_count && new Set(nodeEvents.map(x => x.node_id)).size === PROFILE.node_count
      : allStageStates.every(item => {
      const state = item.state;
      const event = nodeEvents.find(x => x.node_id === item.node_id && x.session_id === job.sessionId);
      return state && event && String(state.session_id) === job.sessionId && Number(state.node_id) === item.node_id &&
        String(state.checksum) === event.checksum && Number(state.input_count) === (item.node_id === 0 ? 0 : 1) &&
        Number(state.operation_count) === OPERATIONS && event.operations === OPERATIONS;
    });
    const executionSucceeded = Boolean(receiptTree?.succeeded && !receiptTree?.failed && nodeEvents.length === PROFILE.node_count &&
      transactionHash === rawResult?.transaction_outcome?.id && transaction?.signer_id === drivers[job.slot].accountId &&
      Number.isSafeInteger(transaction?.nonce) && transaction?.public_key);
    const succeeded = Boolean(executionSucceeded && sinkStateMatchesWorkflow && (DIAGNOSTIC_LIGHT || statesMatchEvents));
    const row = {
      phase: job.phase, warmup: job.warmup, workflow_id: job.workflowId, driver_slot: job.slot,
      session_id: job.sessionId, sender_account_id: drivers[job.slot].accountId, seed: job.seed,
      pre_submit_sink_state: preSubmitSink,
      planned_at: job.plannedAt, offered_at: job.offeredAt, call_issued_at: clientStartedAt,
      receipt_tree_completed_at: receiptTreeDoneWall,
      receipt_wait_started_at: receiptWaitStartedWall, receipt_deadline_at: receiptDeadlineWall,
      receipt_wait_duration_ms: receiptTreeDoneAt === null ? Math.max(0, Date.now() - (receiptDeadlineEpochMs - RECEIPT_TIMEOUT_MS)) : receiptTreeDoneAt - clientStarted,
      receipt_resolution_status: receiptResolutionStatus,
      sink_first_queryable_at: sinkQueryableWall,
      sink_resolution_status: sinkResolutionStatus, sink_read_attempts_total: sinkReadAttemptsTotal,
      sink_read_failure_count: sinkReadFailureCount, sink_last_read_error: sinkAuditError,
      client_queue_delay_ms: scheduledLagMs,
      generator_schedule_lag_ms: job.offeredMonoMs - job.plannedMonoMs,
      call_to_receipt_tree_ms: receiptTreeDoneAt === null ? null : receiptTreeDoneAt - clientStarted,
      call_to_sink_queryable_ms: sinkQueryableAt === null ? null : sinkQueryableAt - clientStarted,
      offered_to_sink_queryable_ms: sinkQueryableAt === null ? null : (clientStarted - job.offeredMonoMs) + (sinkQueryableAt - clientStarted),
      planned_to_sink_queryable_ms: sinkQueryableAt === null ? null : sinkQueryableAt - job.plannedMonoMs,
      sink_poll_interval_ms: DIAGNOSTIC_LIGHT ? SINK_POLL_INTERVAL_MS : 100,
      sink_poll_count: DIAGNOSTIC_LIGHT ? sinkPollCount : null,
      sink_poll_last_miss_at: sinkPollLastMissWall,
      sink_poll_max_read_ms: DIAGNOSTIC_LIGHT ? sinkPollMaxReadMs : null,
      sink_poll_detection_bracket_ms: sinkQueryableAt !== null && sinkPollLastMissWall ?
        Math.max(0, Date.parse(sinkQueryableWall)-Date.parse(sinkPollLastMissWall)) : null,
      receipt_count: receiptTree?.result?.receipts_outcome?.length ?? null,
      transaction_hash: transactionHash,
      rpc_send_tx_requests_linked_by_transaction_hash: requestsForTransaction,
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
      execution_succeeded: executionSucceeded,
      execution_status: receiptTree ? ((receiptTree.failed || !receiptTree.succeeded) ? 'execution_failed' : 'completed') :
        (receiptResolutionStatus === 'rpc_or_client_error_outcome_unknown' ? 'rpc_error_outcome_unknown' : 'receipt_evidence_pending'),
      audit_pending: workflowAuditPending,
      audit_status: workflowAuditPending ? 'audit_pending' : 'complete',
      audit_error: [sinkAuditError, ...stateReads.map(item => item.audit_error).filter(Boolean)].filter(Boolean).join('; ') || null,
      audit_attempts: Math.max(1, ...stateReads.map(item => item.audit_attempts ?? 1)),
      succeeded: succeeded && !workflowAuditPending,
      failure: receiptTree && (receiptTree.failed || !receiptTree.succeeded) ? (callError ?? 'transaction_or_receipt_failed') : (workflowAuditPending ? null :
        (callError ?? (!succeeded ? 'sink_state_or_receipt_event_mismatch' : null))),
    };
    await captureRecentBlocks(row.block_hashes);
    observations.push(row);
    inFlight--; return row;
  }

  async function phase(rate, seconds, warmup, phaseIndex, rateIndex, preheatSeconds = 0,
    arrivalOffsets = null, phaseRandomOffsetSec = 0, arrivalRule = 'fixed_interarrival') {
    maxQueued = 0;
    maxInFlight = 0;
    if (inFlight !== 0) throw new Error(`phase ${phaseIndex} began with ${inFlight} in-flight calls`);
    const plannedOffsets = Array.isArray(arrivalOffsets) ? arrivalOffsets.map(Number) :
      Array.from({length: Math.max(1, Math.floor(rate * seconds))}, (_,i) => i/rate);
    const count = plannedOffsets.length;
    const warmupOffset = warmup ? 0n : 50000000n;
    const idBase = 500000000000n + REPEAT * 10000000000n + BigInt(phaseIndex) * 100000000n + warmupOffset;
    const sessionId = String(idBase);
    const workflowBase = 800000000000n + REPEAT * 10000000000n + BigInt(phaseIndex) * 100000000n + warmupOffset;
    if (usedSessionIds.has(sessionId)) throw new Error(`duplicate phase session id ${sessionId}`);
    usedSessionIds.add(sessionId);
    const phaseStartedAt = new Date().toISOString();
    const observationStartAt = new Date(Date.now() + preheatSeconds * 1000).toISOString();
    const phaseWindowEndAt = new Date(Date.now() + seconds * 1000).toISOString();
    const baseMono = performance.now();
    const rpcLoadStart = rpcProxy.snapshot();
    const jobs = [];
    const schedulers = [];
    for (let i = 0; i < count; i++) {
      const offeredMonoMs = baseMono + plannedOffsets[i] * 1000;
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
        const jobWarmup = plannedMonoMs < baseMono + preheatSeconds * 1000;
        const job = { phase: phaseIndex, warmup: jobWarmup, sessionId, workflowId, slot,
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
    const phaseRows = observations.filter(row => row.phase === phaseIndex);
    const scenarioForPhase = SCHEDULE.scenarios[REQUESTED_SCENARIOS[Math.floor(phaseIndex / 10)]];
    const expectedOffsets = offsetsForRate(scenarioForPhase, rate);
    const exactOffsetsMatch = plannedOffsets.length === expectedOffsets.length && plannedOffsets.every((value, index) => value === expectedOffsets[index]);
    const orderedPhaseRows = [...phaseRows].sort((a,b) => BigInt(a.workflow_id) < BigInt(b.workflow_id) ? -1 : 1);
    const consumedOffsets = orderedPhaseRows.map(row => (Date.parse(row.planned_at) - Date.parse(phaseStartedAt)) / 1000);
    const offsetDeltas = consumedOffsets.map((value,index) => Math.abs(value - (expectedOffsets[index] ?? Number.NaN)));
    const maxRowOffsetDeviationSec = offsetDeltas.length ? Math.max(...offsetDeltas) : null;
    const traceValidation = { expected_count: expectedOffsets.length, recorded_count: phaseRows.length,
      exact_offsets_passed_to_scheduler: exactOffsetsMatch, row_schedule_timestamps_match_offsets: phaseRows.length === expectedOffsets.length &&
        offsetDeltas.every(Number.isFinite) && maxRowOffsetDeviationSec <= 0.02,
      max_row_offset_deviation_sec: maxRowOffsetDeviationSec, expected_offsets_sec: expectedOffsets,
      consumed_planned_offsets_sec: consumedOffsets };
    traceValidation.passed = traceValidation.exact_offsets_passed_to_scheduler && traceValidation.row_schedule_timestamps_match_offsets;
    const rpcLoadEnd = rpcProxy.snapshot();
    const phaseSendTxLedger = rpcLoadEnd.send_tx_request_ledger.slice(rpcLoadStart.send_tx_request_ledger.length);
    const phaseHttpErrorLedger = rpcLoadEnd.http_error_ledger.slice(rpcLoadStart.http_error_ledger.length);
    await writeFile(CHECKPOINT, `${JSON.stringify({ tag: TAG, profile_id: PROFILE.profile_id, repeat: Number(REPEAT), phase: phaseIndex,
      audit_status: 'audit_pending', audit_stage: 'post_drain_state_audit', created_at: new Date().toISOString(),
      schedule_sha256: raw.schedule_sha256, runner_sha256: RUNNER_SHA256, trace_validation: traceValidation,
      expected_offered: count, recorded_rows: phaseRows.length, receipt_completed: phaseRows.filter(row => row.receipt_tree_completed_at).length,
      receipt_pending: phaseRows.filter(row => !row.receipt_tree_completed_at).length,
      workflow_execution_successes: phaseRows.filter(row => row.execution_succeeded).length,
      rpc_send_tx_request_ledger: phaseSendTxLedger, rpc_http_error_ledger: phaseHttpErrorLedger, rows: phaseRows }, null, 2)}\n`);
    if (!traceValidation.passed) throw new Error(`consumed_schedule_trace_mismatch_phase_${phaseIndex}: ${JSON.stringify({
      expected_count: traceValidation.expected_count, recorded_count: traceValidation.recorded_count,
      exact_offsets_passed_to_scheduler: traceValidation.exact_offsets_passed_to_scheduler,
      max_row_offset_deviation_sec: traceValidation.max_row_offset_deviation_sec })}`);
    await writeAuditProgress({ tag: TAG, phase: phaseIndex, stage: 'post_drain_state_audit', pending: phaseRows.length,
      completed: 0, total: phaseRows.length, expected_offered: count });
    if (DIAGNOSTIC_LIGHT) {
      let workflowAuditCompleted = 0;
      const workflowAuditTotal = phaseRows.length;
      await writeAuditProgress({ tag: TAG, phase: phaseIndex, stage: 'post_drain_state_audit', pending: workflowAuditTotal,
        completed: 0, total: workflowAuditTotal });
      await mapLimit(phaseRows, 32, async row => {
        const wid = row.workflow_id;
        const states = await Promise.all(stages.map(async (stage, nodeId) => {
          const read = await boundedRead(`post_drain_view_${wid}_${nodeId}`, () => stage.view('stage_result', { workflow_id: wid }));
          return { node_id: nodeId, state: read.value, audit_error: read.error, audit_attempts: read.attempts };
        }));
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
        row.audit_pending = Boolean(row.audit_pending || states.some(item => !item.state && item.audit_error));
        row.audit_status = row.audit_pending ? 'audit_pending' : 'complete';
        row.audit_error = [row.audit_error, ...states.map(item => item.audit_error).filter(Boolean)].filter(Boolean).join('; ') || null;
        row.succeeded = Boolean(row.succeeded && match && !row.audit_pending);
        row.failure = row.audit_pending || row.succeeded ? (row.failure ?? null) : (row.failure ?? 'post_drain_state_event_audit_failed');
        workflowAuditCompleted++;
        if (workflowAuditCompleted % AUDIT_CHECKPOINT_EVERY === 0 || workflowAuditCompleted === workflowAuditTotal) {
          await writeAuditProgress({ pending: workflowAuditTotal - workflowAuditCompleted, completed: workflowAuditCompleted,
            total: workflowAuditTotal, last_workflow_id: wid });
        }
        return row;
      });
    }
    const rpcPostAuditEnd = rpcProxy.snapshot();
    const completePhaseHttpErrorLedger = rpcPostAuditEnd.http_error_ledger.slice(rpcLoadStart.http_error_ledger.length);
    const windowEndMs = Date.parse(phaseWindowEndAt);
    const windowStartMs = Date.parse(observationStartAt);
    const observationSec = Math.max(0.001, seconds - preheatSeconds);
    const attemptedInWindow = phaseRows.filter(row => row.call_issued_at && Date.parse(row.call_issued_at) >= windowStartMs && Date.parse(row.call_issued_at) <= windowEndMs);
    const successfulInWindow = phaseRows.filter(row => row.execution_succeeded && row.sink_first_queryable_at && Date.parse(row.sink_first_queryable_at) >= windowStartMs && Date.parse(row.sink_first_queryable_at) <= windowEndMs).length;
    const inFlightAtWindowEnd = phaseRows.filter(row => row.call_issued_at && row.receipt_tree_completed_at &&
      Date.parse(row.call_issued_at) <= windowEndMs && Date.parse(row.receipt_tree_completed_at) > windowEndMs).length;
    const blockHashes = [...new Set(phaseRows.flatMap(row => row.block_hashes ?? []))];
    const blockUsageResults = [];
    await writeFile(CHECKPOINT, `${JSON.stringify({ tag: TAG, profile_id: PROFILE.profile_id, repeat: Number(REPEAT), phase: phaseIndex,
      audit_status: 'audit_pending', audit_stage: 'block_usage', created_at: new Date().toISOString(), offered: count,
      receipt_completed: phaseRows.filter(row => row.receipt_tree_completed_at).length,
      receipt_pending: phaseRows.filter(row => !row.receipt_tree_completed_at).length,
      workflow_successes: phaseRows.filter(row => row.succeeded).length,
      workflow_failures: phaseRows.filter(row => !row.audit_pending && !row.succeeded).length,
      workflow_audit_pending: phaseRows.filter(row => row.audit_pending).length,
      block_hashes_total: blockHashes.length, block_hashes_completed: 0,
      rpc_send_tx_request_ledger: phaseSendTxLedger, rpc_http_error_ledger: phaseHttpErrorLedger, rows: phaseRows }, null, 2)}\n`);
    await writeAuditProgress({ tag: TAG, phase: phaseIndex, stage: 'block_usage', pending: blockHashes.length, completed: 0, total: blockHashes.length });
    const blockUsage = await mapLimit(blockHashes, 8, async blockHash => {
      const sample = await ensureRecentBlockSample(blockHash);
      blockUsageResults.push(sample);
      const complete = blockUsageResults.length;
      if (complete % AUDIT_CHECKPOINT_EVERY === 0 || complete === blockHashes.length) {
        await writeAuditProgress({ pending: blockHashes.length - complete, completed: complete, total: blockHashes.length, last_block_hash: blockHash });
      }
      return sample;
    });
    const completedTimes = phaseRows.map(row => row.sink_first_queryable_at ? Date.parse(row.sink_first_queryable_at) : null).filter(Number.isFinite);
    const drainTimeAfterWindowMs = completedTimes.length ? Math.max(0, Math.max(...completedTimes) - windowEndMs) : null;
    const timeBins = [];
    for (let lo = 0; lo < observationSec; lo += 10) {
      const hi = Math.min(observationSec, lo + 10), wallLo = windowStartMs + lo * 1000, wallHi = windowStartMs + hi * 1000;
      const offered = phaseRows.filter(row => row.offered_at && Date.parse(row.offered_at) >= wallLo && Date.parse(row.offered_at) < wallHi).length;
      const accepted = phaseRows.filter(row => row.call_issued_at && Date.parse(row.call_issued_at) >= wallLo && Date.parse(row.call_issued_at) < wallHi).length;
      const completedInBin = phaseRows.filter(row => row.execution_succeeded && row.sink_first_queryable_at && Date.parse(row.sink_first_queryable_at) >= wallLo && Date.parse(row.sink_first_queryable_at) < wallHi).length;
      const acceptedCum = phaseRows.filter(row => row.call_issued_at && Date.parse(row.call_issued_at) <= wallHi).length;
      const completedCum = phaseRows.filter(row => row.execution_succeeded && row.sink_first_queryable_at && Date.parse(row.sink_first_queryable_at) <= wallHi).length;
      timeBins.push({ bin_start_sec: lo, bin_end_sec: hi, offered, accepted, completed: completedInBin,
        queue_at_bin_end: Math.max(0, acceptedCum - completedCum) });
    }
    const intervals=plannedOffsets.slice(1).map((x,i)=>x-plannedOffsets[i]);
    const intervalMean=intervals.length?intervals.reduce((a,b)=>a+b,0)/intervals.length:null;
    const intervalVariance=intervals.length>1?intervals.reduce((a,b)=>a+(b-intervalMean)**2,0)/intervals.length:0;
    const summary = {
      phase: phaseIndex, rate_index: rateIndex, warmup, pipeline: process.env.STRESS_PIPELINE_MODE === '1', preheat_sec: preheatSeconds,
      warmup_duration_sec: Math.max(0,preheatSeconds-phaseRandomOffsetSec), phase_random_offset_sec: phaseRandomOffsetSec,
      arrival_rule: arrivalRule, arrival_seed: SCHEDULE.scenarios[REQUESTED_SCENARIOS[Math.floor(phaseIndex/10)]].arrival_seed, planned_arrival_offsets_sec: plannedOffsets,
      arrival_trace_sha256: createHash('sha256').update(`${Number(rate)}|${SCHEDULE.scenarios[REQUESTED_SCENARIOS[Math.floor(phaseIndex/10)]].arrival_seed}|${Number(phaseRandomOffsetSec).toFixed(12)}|${plannedOffsets.map(x=>Number(x).toFixed(9)).join(',')}`).digest('hex'),
      trace_validation: traceValidation,
      planned_interarrival_mean_sec: intervalMean, planned_interarrival_cv: intervalMean && intervals.length>1?Math.sqrt(intervalVariance)/intervalMean:null,
      observation_start_at: observationStartAt, observation_duration_sec: observationSec, time_bins_10s: timeBins,
      session_id: sessionId, offered_rate_per_sec: rate, offered: count,
      phase_started_at: phaseStartedAt, measurement_window_end_at: phaseWindowEndAt, generation_ended_at: generationEndedAt,
      drained: completed !== null, completed: phaseRows.filter(row => row.call_to_receipt_tree_ms !== null).length,
      successful_sink_completions: phaseRows.filter(row => row.execution_succeeded && row.sink_first_queryable_at).length,
      fully_audited_successes: phaseRows.filter(row => row.succeeded && !row.audit_pending).length,
      submission_attempts_in_measurement_window: attemptedInWindow.length,
      offered_call_rate_in_measurement_window: attemptedInWindow.length / observationSec,
      successful_completions_in_measurement_window: successfulInWindow,
      measured_window_goodput_per_sec: successfulInWindow / observationSec,
      failures: phaseRows.filter(row => !row.audit_pending && !row.execution_succeeded && row.call_to_receipt_tree_ms !== null).length,
      audit_pending_count: phaseRows.filter(row => row.audit_pending).length,
      unfinished_at_120s: completed === null ? count - phaseRows.filter(row => row.call_to_receipt_tree_ms !== null).length : 0,
      in_flight_at_measurement_window_end: inFlightAtWindowEnd,
      drain_time_after_window_ms: drainTimeAfterWindowMs,
      offered_generator_rate_per_sec: count / ((generationEndedMonoMs - baseMono + 1000 / rate) / 1000),
      mean_generator_schedule_lag_ms: phaseRows.length ? phaseRows.reduce((a, x) => a + x.generator_schedule_lag_ms, 0) / phaseRows.length : null,
      drained_successes_per_measurement_second: phaseRows.filter(row => row.execution_succeeded && row.sink_first_queryable_at).length / observationSec,
      client_queue_depth_high_water: maxQueued, client_inflight_high_water: maxInFlight,
      mean_client_queue_delay_ms: phaseRows.length ? phaseRows.reduce((a, x) => a + x.client_queue_delay_ms, 0) / phaseRows.length : null,
      rpc_load_path_by_method: rpcDelta(rpcLoadStart, rpcLoadEnd),
      rpc_post_drain_audit_by_method: rpcDelta(rpcLoadEnd, rpcPostAuditEnd),
      rpc_send_tx_request_ledger: phaseSendTxLedger, rpc_http_error_ledger: completePhaseHttpErrorLedger,
      receipt_tree_waiting_method_counts: Object.fromEntries(Object.entries(rpcDelta(rpcLoadStart, rpcLoadEnd)).filter(([method]) => /broadcast_tx|tx_status|query/.test(method))),
      sink_poll_interval_ms: DIAGNOSTIC_LIGHT ? SINK_POLL_INTERVAL_MS : 100,
      block_usage_samples: blockUsage,
      audit_pending: phaseRows.some(row => row.audit_pending) || blockUsage.some(item => item.audit_pending),
      audit_status: phaseRows.some(row => row.audit_pending) || blockUsage.some(item => item.audit_pending) ? 'audit_pending' : 'complete',
      audit_pending_count: phaseRows.filter(row => row.audit_pending).length + blockUsage.filter(item => item.audit_pending).length,
      audit_checkpoint: CHECKPOINT.pathname,
      rows: phaseRows,
    };
    await writeFile(CHECKPOINT, `${JSON.stringify({ tag: TAG, profile_id: PROFILE.profile_id, repeat: Number(REPEAT), phase: phaseIndex,
      audit_status: summary.audit_status, audit_stage: 'complete', finished_at: new Date().toISOString(), summary: { ...summary, rows: undefined },
      trace_validation: traceValidation,
      rpc_send_tx_request_ledger: phaseSendTxLedger, rpc_http_error_ledger: completePhaseHttpErrorLedger,
      block_usage_samples: blockUsage, rows: phaseRows }, null, 2)}\n`);
    await writeAuditProgress({ stage: 'phase_complete', pending: summary.audit_pending_count, completed: blockHashes.length, total: blockHashes.length,
      audit_status: summary.audit_status });
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
      if (process.env.STRESS_PIPELINE_MODE === '1') {
        const preheat = Number(process.env.STRESS_PIPELINE_PREHEAT_SEC ?? warmupSeconds);
        const observation = Number(process.env.STRESS_PIPELINE_OBSERVATION_SEC ?? measurementSeconds);
        const phaseOffset=Number(config.phase_random_offset_sec ?? 0);
        const arrivalKey=Object.keys(config.arrival_offsets_sec_by_rate ?? {}).find(key => Number(key) === Number(rates[i]));
        const arrivalOffsets=arrivalKey === undefined ? null : config.arrival_offsets_sec_by_rate[arrivalKey];
        await phase(rates[i], preheat+observation+phaseOffset, false, phaseIndex, i,
          preheat+phaseOffset, arrivalOffsets, phaseOffset, config.arrival_rule ?? 'fixed_interarrival');
      } else {
        await phase(rates[i], warmupSeconds, true, phaseIndex, i);
        await phase(rates[i], measurementSeconds, false, phaseIndex, i);
      }
    }
  }
} catch (error) {
  result.error = String(error?.stack ?? error);
} finally {
  result.finished_at = new Date().toISOString();
  console.log(`CLEANUP_START|${new Date().toISOString()}|phases=${raw.phases.length}`);
  let cleanupPending = false;
  let preserveSandbox = false;
  let auditGateDecision = null;
  if (AUDIT_GATE_PATH && sandbox) {
    result.cleanup_status = 'awaiting_offline_audit';
    raw.cleanup_status = result.cleanup_status;
    raw.sandbox_rpc_addr = sandbox.rpcUrl;
    raw.sandbox_home_dir = sandbox.homeDir;
    raw.sandbox_pid = sandbox.childProcess?.pid ?? null;
    await writeFile(RAW, `${JSON.stringify(raw, null, 2)}\n`);
    await writeFile(OUT, `${JSON.stringify({ ...result, phases: raw.phases.map(({ rows, ...x }) => x) }, null, 2)}\n`);
    await writeFile(AUDIT_GATE_REQUEST, `${JSON.stringify({ tag: TAG, seed: SCHEDULE.seed,
      schedule_sha256: raw.schedule_sha256, runner_sha256: RUNNER_SHA256,
      expected_phases: REQUESTED_SCENARIOS.flatMap(name => SCHEDULE.scenarios[name].rates_per_sec.map(Number)).length,
      phase_status: raw.phases.map(phase => ({ phase: phase.phase, rate: phase.offered_rate_per_sec,
        offered: phase.offered, audit_status: phase.audit_status, audit_pending_count: phase.audit_pending_count })),
      sandbox: { pid: sandbox.childProcess?.pid ?? null, rpc_addr: sandbox.rpcUrl, home_dir: sandbox.homeDir },
      gate_path: AUDIT_GATE_PATH, created_at: new Date().toISOString() }, null, 2)}\n`);
    await writeAuditProgress({ stage: 'awaiting_offline_audit', pending: raw.phases.filter(phase => phase.audit_pending).length,
      completed: raw.phases.length, total: REQUESTED_SCENARIOS.flatMap(name => SCHEDULE.scenarios[name].rates_per_sec).length,
      audit_gate_request_path: AUDIT_GATE_REQUEST.pathname });
    const gateStarted = Date.now();
    while (Date.now() - gateStarted < AUDIT_GATE_TIMEOUT_MS) {
      try { auditGateDecision = JSON.parse(await readFile(AUDIT_GATE_PATH, 'utf8')); break; }
      catch { await sleep(1000); }
    }
    const gateCanCleanup = auditGateDecision?.audit_status === 'passed' && auditGateDecision?.allow_sandbox_teardown === true;
    preserveSandbox = !gateCanCleanup;
    raw.offline_audit_decision = auditGateDecision ?? { audit_status: 'gate_timeout', allow_sandbox_teardown: false };
  }
  if (worker) {
    const stopped = await settleWithin('worker_teardown', () => worker.tearDown());
    if (!stopped.done) {
      cleanupPending = true;
      try { worker.server?.subprocess?.kill('SIGKILL'); } catch {}
    }
  }
  if (rpcProxy) {
    const closed = await settleWithin('rpc_proxy_close', () => rpcProxy.close(), 5000);
    cleanupPending ||= !closed.done;
  }
  if (sandbox && !preserveSandbox) {
    const stopped = await settleWithin('near_sandbox_teardown', () => sandbox.tearDown());
    if (!stopped.done) {
      cleanupPending = true;
      try { sandbox.childProcess?.kill('SIGKILL'); } catch {}
      await settleWithin('near_sandbox_child_exit', () => new Promise(resolve => {
        if (sandbox.childProcess?.exitCode !== null) return resolve();
        sandbox.childProcess?.once('exit', resolve);
      }), 5000);
      await Promise.all([
        sandbox.homeDir ? rm(sandbox.homeDir, { recursive: true, force: true }).catch(() => {}) : Promise.resolve(),
        sandbox.rpcPortLockPath ? unlink(sandbox.rpcPortLockPath).catch(() => {}) : Promise.resolve(),
        sandbox.netPortLockPath ? unlink(sandbox.netPortLockPath).catch(() => {}) : Promise.resolve(),
      ]);
    }
  }
  if (preserveSandbox && sandbox) {
    try { sandbox.childProcess?.unref?.(); } catch {}
    try { sandbox.childProcess?.stdout?.unref?.(); } catch {}
    try { sandbox.childProcess?.stderr?.unref?.(); } catch {}
    raw.preserved_sandbox = { pid: sandbox.childProcess?.pid ?? null, rpc_addr: sandbox.rpcUrl,
      home_dir: sandbox.homeDir, rpc_port_lock_path: sandbox.rpcPortLockPath, net_port_lock_path: sandbox.netPortLockPath,
      reason: auditGateDecision ? 'offline_audit_not_passed_or_teardown_not_authorized' : 'offline_audit_gate_timeout' };
  }
  result.cleanup_status = preserveSandbox ? 'preserved_for_supplemental_audit' : (cleanupPending ? 'forced_after_timeout' : 'complete');
  raw.cleanup_status = result.cleanup_status;
  raw.audit_progress_path = PROGRESS.pathname;
  await writeAuditProgress({ stage: 'finished', pending: 0, completed: auditProgress.completed, total: auditProgress.total, cleanup_status: result.cleanup_status });
  await writeFile(OUT, `${JSON.stringify({ ...result, phases: raw.phases.map(({ rows, ...x }) => x) }, null, 2)}\n`);
  await writeFile(RAW, `${JSON.stringify(raw, null, 2)}\n`);
  console.log(`CLEANUP_FINISH|${new Date().toISOString()}|status=${result.cleanup_status}`);
  if (result.error) console.error(result.error);
  await new Promise(resolve => process.stdout.write('', resolve));
  process.exit(result.error ? 1 : 0);
}
if (result.error) { console.error(result.error); process.exitCode = 1; }
