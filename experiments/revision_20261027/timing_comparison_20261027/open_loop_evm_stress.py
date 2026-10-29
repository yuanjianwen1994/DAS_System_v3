"""Open-loop local-Anvil load pilot for the d4 independent-Stage DAG."""
from __future__ import annotations

import concurrent.futures
import collections
import hashlib
import json
import os
import signal
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

from web3 import Web3
from web3.datastructures import AttributeDict
from web3.exceptions import TransactionNotFound
from web3.providers import HTTPProvider
from hexbytes import HexBytes
from solcx import compile_source, get_installed_solc_versions, install_solc

import run_das_distributed_composition as base

ROOT = Path(__file__).resolve().parent
BASE_DIR = ROOT.parent
MANIFEST_PATH = Path(os.environ.get("STRESS_PROFILE_MANIFEST_PATH", str(BASE_DIR / "dag_workflow_20261027/profile_manifest.json")))
MANIFEST = json.loads(MANIFEST_PATH.read_text())
PROFILE_ID = os.environ.get("STRESS_PROFILE_ID", "dag-04-chain-d4-w1-theta1-nu1")
PROFILE = next((p for p in MANIFEST["profiles"] if p["profile_id"] == PROFILE_ID), None)
if PROFILE is None:
    raise ValueError(f"unknown profile {PROFILE_ID}")
SCHEDULE_PATH = Path(os.environ.get("STRESS_SCHEDULE_PATH", str(ROOT / "paired_load_schedule.json")))
SCHEDULE_TEXT = SCHEDULE_PATH.read_text()
SCHEDULE = json.loads(SCHEDULE_TEXT)
SCENARIO_NAMES = list(SCHEDULE["scenarios"])
REQUESTED_SCENARIOS = [x.strip() for x in os.environ.get("STRESS_SCHEDULE_SCENARIOS", os.environ.get("STRESS_SCHEDULE_SCENARIO", "matched_low")).split(",") if x.strip()]
for scenario in REQUESTED_SCENARIOS:
    if scenario not in SCHEDULE["scenarios"]:
        raise ValueError(f"unknown schedule scenario {scenario}")
REPEAT = int(os.environ.get("STRESS_REPEAT", "1"))
TAG = os.environ.get("STRESS_TAG", f"paired_{'_'.join(REQUESTED_SCENARIOS)}_r{REPEAT}")
WORKERS = int(os.environ.get("STRESS_WORKERS", SCHEDULE["resource_policy"]["sender_accounts_per_platform"]))
BLOCK_TIME = float(os.environ.get("DAG_ANVIL_BLOCK_TIME_SEC", SCHEDULE["block_period_sec"]))
OPERATIONS = int(SCHEDULE["operations_per_node"])
OUT_DIR = Path(os.environ.get("STRESS_OUTPUT_DIR", str(ROOT)))
OUT = OUT_DIR / f"{TAG}_evm_results.json"
RAW = OUT_DIR / f"{TAG}_evm_raw.json"
MASK64 = (1 << 64) - 1
MULTIPLIER = 0x9E3779B97F4A7C15
DIAGNOSTIC_LIGHT = os.environ.get("STRESS_DIAGNOSTIC_LIGHT", "0") == "1"
RECEIPT_POLL_SEC = float(os.environ.get("STRESS_RECEIPT_POLL_SEC", "0.2" if DIAGNOSTIC_LIGHT else "0.1"))
RECEIPT_BATCH_SIZE = int(os.environ.get("STRESS_RECEIPT_BATCH_SIZE", "128"))
ANVIL_MAX_TRANSACTIONS = int(os.environ.get("ANVIL_MAX_TRANSACTIONS", "1000"))


class RpcMeter:
    def __init__(self):
        self.lock = __import__("threading").Lock()
        self.counts = collections.Counter()
        self.errors = collections.Counter()
        self.durations = collections.defaultdict(list)

    def record(self, method: str, duration_ms: float, failed: bool) -> None:
        with self.lock:
            self.counts[method] += 1
            self.durations[method].append(duration_ms)
            if failed:
                self.errors[method] += 1

    def snapshot(self) -> dict:
        with self.lock:
            return {"counts": dict(self.counts), "errors": dict(self.errors),
                    "durations_ms": {k: list(v) for k, v in self.durations.items()}}

    @staticmethod
    def delta(before: dict, after: dict) -> dict:
        result = {}
        for method in sorted(set(before["counts"]) | set(after["counts"])):
            n0, n1 = before["counts"].get(method, 0), after["counts"].get(method, 0)
            samples = after["durations_ms"].get(method, [])[n0:]
            result[method] = {"calls": n1-n0, "errors": after["errors"].get(method,0)-before["errors"].get(method,0),
                              "total_ms": round(sum(samples), 3),
                              "mean_ms": round(sum(samples)/len(samples), 3) if samples else None,
                              "p95_ms": round(sorted(samples)[int(.95*(len(samples)-1))], 3) if samples else None}
        return result


class MeteredHTTPProvider(HTTPProvider):
    def __init__(self, endpoint_uri: str, meter: RpcMeter, **kwargs):
        super().__init__(endpoint_uri=endpoint_uri, **kwargs)
        self.meter = meter

    def make_request(self, method, params):
        start = time.perf_counter()
        failed = False
        try:
            response = super().make_request(method, params)
            failed = "error" in response
            return response
        except Exception:
            failed = True
            raise
        finally:
            self.meter.record(method, (time.perf_counter()-start)*1000, failed)

    def make_batch_request(self, batch_requests):
        start = time.perf_counter()
        elapsed_ms = 0.0
        try:
            response = super().make_batch_request(batch_requests)
            elapsed_ms = (time.perf_counter() - start) * 1000
            responses = response if isinstance(response, list) else [response]
            methods = {str(method) for method, _ in batch_requests}
            for method in methods:
                items = [responses[index] if index < len(responses) else {"error": {"message": "missing batch response"}}
                         for index, (request_method, _) in enumerate(batch_requests) if str(request_method) == method]
                for item in items:
                    self.meter.record(method, elapsed_ms, "error" in item)
                self.meter.record(f"{method}_batch", elapsed_ms, any("error" in item for item in items))
            return response
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            for method in {str(method) for method, _ in batch_requests}:
                for _ in (item for item in batch_requests if str(item[0]) == method):
                    self.meter.record(method, elapsed_ms, True)
                self.meter.record(f"{method}_batch", elapsed_ms, True)
            raise


class ReceiptTicket:
    def __init__(self, key: str):
        self.key = key
        self.event = __import__("threading").Event()
        self.receipt = None
        self.observed_wall = None


class ReceiptBatchPoller:
    def __init__(self, provider, interval_sec: float, batch_size: int):
        self.provider = provider
        self.interval_sec = interval_sec
        self.batch_size = batch_size
        self.lock = __import__("threading").Lock()
        self.stop_event = __import__("threading").Event()
        self.pending = {}
        self.batch_calls = 0
        self.max_batch_latency_ms = 0.0
        self.thread = __import__("threading").Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def watch(self, tx_hash):
        key = Web3.to_hex(tx_hash).lower()
        ticket = ReceiptTicket(key)
        with self.lock:
            self.pending[key] = ticket
        return ticket

    def discard(self, ticket):
        if ticket is not None:
            with self.lock:
                self.pending.pop(ticket.key, None)

    def _format_receipt(self, result):
        quantity_fields = ("blockNumber", "transactionIndex", "cumulativeGasUsed", "gasUsed", "status", "type")
        formatted = dict(result)
        for field in quantity_fields:
            if formatted.get(field) is not None:
                formatted[field] = int(formatted[field], 16) if isinstance(formatted[field], str) else int(formatted[field])
        for field in ("transactionHash", "blockHash"):
            if formatted.get(field) is not None:
                formatted[field] = HexBytes(formatted[field])
        logs = []
        for item in formatted.get("logs", []):
            log = dict(item)
            log["topics"] = [HexBytes(topic) for topic in log.get("topics", [])]
            if log.get("data") is not None:
                log["data"] = HexBytes(log["data"])
            for field in ("blockNumber", "transactionIndex", "logIndex", "type"):
                if log.get(field) is not None:
                    log[field] = int(log[field], 16) if isinstance(log[field], str) else int(log[field])
            for field in ("transactionHash", "blockHash"):
                if log.get(field) is not None:
                    log[field] = HexBytes(log[field])
            logs.append(AttributeDict(log))
        formatted["logs"] = logs
        return AttributeDict(formatted)

    def _run(self):
        while not self.stop_event.is_set():
            started = time.perf_counter()
            with self.lock:
                pending = list(self.pending.values())
            for offset in range(0, len(pending), self.batch_size):
                batch = pending[offset:offset + self.batch_size]
                if not batch:
                    continue
                requests = [("eth_getTransactionReceipt", [ticket.key]) for ticket in batch]
                batch_started = time.perf_counter()
                try:
                    responses = self.provider.make_batch_request(requests)
                    if not isinstance(responses, list):
                        responses = [responses]
                    self.batch_calls += 1
                    elapsed = (time.perf_counter() - batch_started) * 1000
                    self.max_batch_latency_ms = max(self.max_batch_latency_ms, elapsed)
                    for ticket, response in zip(batch, responses):
                        result = response.get("result")
                        if result is not None:
                            ticket.receipt = self._format_receipt(result)
                            ticket.observed_wall = now_iso()
                            ticket.event.set()
                            self.discard(ticket)
                except Exception:
                    elapsed = (time.perf_counter() - batch_started) * 1000
                    self.max_batch_latency_ms = max(self.max_batch_latency_ms, elapsed)
            elapsed_sec = time.perf_counter() - started
            self.stop_event.wait(max(0, self.interval_sec - elapsed_sec))

    def snapshot(self):
        with self.lock:
            pending = len(self.pending)
        return {"batch_http_calls": self.batch_calls, "pending_receipts": pending,
                "max_batch_latency_ms": round(self.max_batch_latency_ms, 3),
                "batch_size": self.batch_size, "poll_interval_sec": self.interval_sec}

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=max(2.0, self.interval_sec * 2))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def wait_until(deadline: float) -> None:
    delay = deadline - time.perf_counter()
    if delay > 0:
        time.sleep(delay)


if "0.8.24" not in {str(v) for v in get_installed_solc_versions()}:
    install_solc("0.8.24")
compiled = compile_source(base.SOURCE.read_text(), solc_version="0.8.24", output_values=["abi", "bin"])
artifacts = {}
for name in ("DAGStage", "DASDistributedCompositionHarness"):
    item = next(value for key, value in compiled.items() if key.endswith(f":{name}"))
    artifacts[name] = item

configured_anvil = os.environ.get("ANVIL_BIN")
anvil = Path(configured_anvil) if configured_anvil else Path(os.popen("command -v anvil").read().strip())
if not anvil.is_file():
    raise FileNotFoundError("Anvil 1.8.6 executable not found")
version = subprocess.check_output([str(anvil), "--version"], text=True).splitlines()[0]
if "1.8.6" not in version:
    raise RuntimeError(f"expected Anvil 1.8.6, got {version}")

port = base.free_port()
process = subprocess.Popen([str(anvil), "--host", "127.0.0.1", "--port", str(port), "--chain-id", "31337",
    "--block-time", str(BLOCK_TIME), "--max-transactions", str(ANVIL_MAX_TRANSACTIONS),
    "--accounts", str(WORKERS + 2), "--silent"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
rpc_meter = RpcMeter()
raw = {"experiment": "paired-open-loop-independent-stage-dag", "platform": "EVM / Anvil", "profile_id": PROFILE["profile_id"],
    "schedule_scenarios": REQUESTED_SCENARIOS, "repeat": REPEAT, "schedule_sha256": hashlib.sha256(SCHEDULE_TEXT.encode()).hexdigest(),
    "runtime": {"anvil": version, "block_time_sec": BLOCK_TIME, "solidity": "0.8.24", "workers": WORKERS,
                "block_gas_limit": 30_000_000, "operations_per_node": OPERATIONS,
                "diagnostic_light": DIAGNOSTIC_LIGHT, "receipt_poll_interval_sec": RECEIPT_POLL_SEC,
                "receipt_sampling_bound_ms": RECEIPT_POLL_SEC * 1000,
                "receipt_polling_strategy": "centralized_json_rpc_batch" if DIAGNOSTIC_LIGHT else "per_workflow_polling",
                "receipt_batch_size": RECEIPT_BATCH_SIZE if DIAGNOSTIC_LIGHT else None,
                "anvil_max_transactions_per_block": ANVIL_MAX_TRANSACTIONS},
    "diagnostic_overrides": {"rates_per_sec": os.environ.get("STRESS_RATE_OVERRIDE"),
                             "warmup_sec": os.environ.get("STRESS_WARMUP_OVERRIDE_SEC"),
                             "measurement_sec": os.environ.get("STRESS_MEASUREMENT_OVERRIDE_SEC")},
    "phases": []}
summary = {"experiment": raw["experiment"], "started_at": now_iso(), "error": None}
receipt_poller = None

try:
    url = f"http://127.0.0.1:{port}"
    w3 = Web3(MeteredHTTPProvider(url, rpc_meter, request_kwargs={"timeout": 10}))
    rpc_deadline = time.time() + 20
    while time.time() < rpc_deadline and process.poll() is None and not w3.is_connected():
        time.sleep(0.1)
    if not w3.is_connected():
        raise TimeoutError("Anvil RPC did not become ready")
    w3.eth.default_account = w3.eth.accounts[0]
    raw["runtime"]["actual_block_gas_limit"] = int(w3.eth.get_block("latest").gasLimit)
    router, stage_rows, setup_gas = base.deployment_bundle(w3, artifacts, PROFILE, 999999999999, 2)
    stages = [w3.eth.contract(address=row["address"], abi=artifacts["DAGStage"]["abi"]) for row in stage_rows]
    sink_node = PROFILE["node_count"] - 1
    worker_accounts = w3.eth.accounts[1:1 + WORKERS]
    worker_w3 = [Web3(MeteredHTTPProvider(url, rpc_meter, request_kwargs={"timeout": 10})) for _ in range(WORKERS)]
    worker_routers = [x.eth.contract(address=router.address, abi=artifacts["DASDistributedCompositionHarness"]["abi"]) for x in worker_w3]
    if DIAGNOSTIC_LIGHT:
        receipt_poller = ReceiptBatchPoller(w3.provider, RECEIPT_POLL_SEC, RECEIPT_BATCH_SIZE)
        receipt_poller.start()
    observations = []
    sender_locks = [__import__("threading").Lock() for _ in range(WORKERS)]
    stage_address_to_node = {row["address"].lower(): int(row["node_id"]) for row in stage_rows}

    def pending_tx_count(value):
        if isinstance(value, dict):
            if "hash" in value:
                return 1
            return sum(pending_tx_count(child) for child in value.values())
        if isinstance(value, list):
            return sum(pending_tx_count(child) for child in value)
        return 0

    def phase(rate: float, seconds: float, warmup: bool, phase_index: int, rate_index: int) -> None:
        count = max(1, int(rate * seconds))
        serial = 0
        inflight_global = 0
        warmup_offset = 0 if warmup else 50_000_000
        id_base = 500_000_000_000 + REPEAT * 10_000_000_000 + phase_index * 100_000_000 + warmup_offset
        session_id = id_base
        workflow_base = 800_000_000_000 + REPEAT * 10_000_000_000 + phase_index * 100_000_000 + warmup_offset
        workflow_ids = [workflow_base + i + 1 for i in range(count)]
        if len(set(workflow_ids)) != count or any(x > 2**53 - 1 for x in [session_id, *workflow_ids]):
            raise RuntimeError("paired schedule contains duplicate or unsafe IDs")
        phase_rpc_start = rpc_meter.snapshot()
        phase_receipt_poll_start = receipt_poller.snapshot() if receipt_poller is not None else None
        commitment = Web3.keccak(text=f"open-loop|{phase_index}|{warmup}|{count}")
        opened, entry_receipt = base.send(w3, router.functions.enterSession(session_id, count, commitment), gas=2_000_000)
        if opened["status"] != 1:
            raise RuntimeError("session entry failed")
        phase_start_block = int(entry_receipt.blockNumber)
        generation_started_wall = now_iso()
        generation_started = time.perf_counter()
        generation_started_perf = generation_started
        measurement_window_end_wall = datetime.fromtimestamp(time.time() + seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        lock = __import__("threading").Lock()
        queued_high = [0]
        inflight_high = [0]
        jobs: list[dict] = []
        mempool_stop = __import__("threading").Event()
        mempool_samples = []

        def sample_mempool() -> None:
            supported_method = "anvil_getTransactionPool"
            while not mempool_stop.wait(1.0):
                started = time.perf_counter()
                response = w3.provider.make_request(supported_method, [])
                if "error" in response and supported_method == "anvil_getTransactionPool":
                    supported_method = "eth_pendingTransactions"
                    response = w3.provider.make_request(supported_method, [])
                result = response.get("result")
                mempool_samples.append({"sampled_at": now_iso(), "method": supported_method,
                    "pending_count": pending_tx_count(result) if result is not None else None,
                    "error": response.get("error"), "elapsed_ms": (time.perf_counter()-started)*1000})

        mempool_thread = __import__("threading").Thread(target=sample_mempool, daemon=True)
        if DIAGNOSTIC_LIGHT:
            mempool_thread.start()

        def run_one(index: int, slot: int, planned_wall: str, planned_mono: float, offered_wall: str, offered_mono: float) -> dict:
            nonlocal inflight_global
            worker = worker_w3[slot]
            account = worker_accounts[slot]
            wid = workflow_ids[index]
            seed = 0x123456789ABC ^ ((wid * MULTIPLIER) & MASK64)
            call_started_wall = None
            call_started = None
            tx_hash = None
            nonce_before = None
            transaction = None
            receipt = None
            receipt_ticket = None
            receipt_mined_wall = None
            sink_wall = None
            sink_mono = None
            failure = None
            accepted_wall = None
            pre_submit_sink = None
            pre_submit_sink_check_passed = False
            node_events, edge_events, all_stage_states = [], [], []
            state_event_match = False
            transaction_match = False
            with lock:
                inflight_global += 1
                inflight_high[0] = max(inflight_high[0], inflight_global)
            try:
                function = worker_routers[slot].functions.executeWorkflows(session_id, [wid], [seed], PROFILE["operations_per_node"])
                pre_submit_sink = worker_routers[slot].functions.nodeResult(session_id, wid, sink_node).call()
                pre_submit_sink_check_passed = not bool(pre_submit_sink[0])
                if bool(pre_submit_sink[0]):
                    raise RuntimeError(f"pre-submit sink already exists: session={session_id} workflow={wid}")
                with sender_locks[slot]:
                    call_started_wall = now_iso()
                    call_started = time.perf_counter()
                    nonce_before = worker.eth.get_transaction_count(account, "pending")
                    tx_hash = function.transact({"from": account, "gas": 28_000_000})
                    accepted_wall = now_iso()
                    if DIAGNOSTIC_LIGHT and receipt_poller is not None:
                        receipt_ticket = receipt_poller.watch(tx_hash)
                    if not DIAGNOSTIC_LIGHT:
                        transaction = worker.eth.get_transaction(tx_hash)
                while receipt is None or (not DIAGNOSTIC_LIGHT and sink_wall is None):
                    if receipt is None:
                        if DIAGNOSTIC_LIGHT and receipt_ticket is not None:
                            receipt_ticket.event.wait(timeout=RECEIPT_POLL_SEC)
                            if receipt_ticket.receipt is not None:
                                receipt = receipt_ticket.receipt
                                receipt_mined_wall = receipt_ticket.observed_wall
                        else:
                            try:
                                receipt = worker.eth.get_transaction_receipt(tx_hash)
                                receipt_mined_wall = now_iso()
                            except TransactionNotFound:
                                pass
                    if DIAGNOSTIC_LIGHT and receipt is not None and int(receipt.status) == 1:
                        state = worker_routers[slot].functions.nodeResult(session_id, wid, sink_node).call()
                        if bool(state[0]):
                            sink_wall = now_iso()
                            sink_mono = time.perf_counter()
                        else:
                            failure = "receipt_success_but_sink_not_queryable"
                        break
                    if not DIAGNOSTIC_LIGHT and sink_wall is None:
                        try:
                            state = worker_routers[slot].functions.nodeResult(session_id, wid, sink_node).call()
                            if bool(state[0]):
                                sink_wall = now_iso()
                                sink_mono = time.perf_counter()
                        except Exception:
                            pass
                    if receipt is not None and int(receipt.status) == 0:
                        break
                    if receipt is not None and sink_wall is not None:
                        break
                    if time.perf_counter() - call_started > 120:
                        failure = "receipt_or_sink_timeout_120s"
                        break
                    time.sleep(RECEIPT_POLL_SEC if DIAGNOSTIC_LIGHT else 0.1)
                status = None if receipt is None else int(receipt.status)
                if status != 1 or sink_wall is None:
                    failure = "transaction_failed_or_sink_missing"
                node_events, edge_events = base.decode_logs(receipt, stage_address_to_node) if receipt is not None and status == 1 else ([], [])
                if not DIAGNOSTIC_LIGHT:
                    all_stage_states = [worker_routers[slot].functions.nodeResult(session_id, wid, node).call()
                                        for node in range(PROFILE["node_count"])] if status == 1 else []
                else:
                    all_stage_states = []
                state_event_match = (len(node_events) == PROFILE["node_count"] and len(all_stage_states) == PROFILE["node_count"])
                if DIAGNOSTIC_LIGHT and status == 1:
                    state_event_match = (len(node_events) == PROFILE["node_count"] and
                        {int(event["node_id"]) for event in node_events} == set(range(PROFILE["node_count"])) and
                        all(int(event["session_id"]) == session_id and int(event["workflow_id"]) == wid for event in node_events))
                if state_event_match:
                    events_by_node = {int(event["node_id"]): event for event in node_events}
                    state_event_match = set(events_by_node) == set(range(PROFILE["node_count"])) and all(
                        int(event["session_id"]) == session_id and int(event["workflow_id"]) == wid for event in node_events)
                    for node, state in enumerate(all_stage_states):
                        event = events_by_node.get(node)
                        if not event or not bool(state[0]) or int(state[1]) != session_id or int(state[2]) != int(event["checksum"]) or \
                                int(state[3]) != int(event["input_count"]) or int(state[4]) != int(event["operation_count"]) or \
                                int(state[3]) != (0 if node == 0 else 1) or int(state[4]) != OPERATIONS:
                            state_event_match = False
                            break
                transaction_match = bool(transaction and tx_hash is not None and transaction["hash"] == tx_hash and
                    transaction["from"].lower() == account.lower() and int(transaction["nonce"]) == nonce_before)
                if not state_event_match and not DIAGNOSTIC_LIGHT:
                    failure = failure or "node_state_event_mismatch"
                if not transaction_match and not DIAGNOSTIC_LIGHT:
                    failure = failure or "transaction_identity_or_nonce_mismatch"
            except Exception as exc:
                accepted_wall = now_iso() if tx_hash is not None else None
                status = None if receipt is None else int(receipt.status)
                sink_mono = None
                failure = repr(exc)
            finally:
                if receipt_poller is not None:
                    receipt_poller.discard(receipt_ticket)
                with lock:
                    inflight_global = max(0, inflight_global - 1)
            status = None if receipt is None else int(receipt.status)
            row = {"phase": phase_index, "warmup": warmup, "workflow_id": str(wid), "session_id": str(session_id),
                "sender_slot": slot, "sender_account": account, "seed": str(seed), "planned_at": planned_wall, "offered_at": offered_wall,
                "pre_submit_sink_state": None if pre_submit_sink is None else list(pre_submit_sink),
                "pre_submit_sink_check_passed": pre_submit_sink_check_passed,
                "tx_submit_started_at": call_started_wall,
                "tx_hash_returned_at": accepted_wall, "receipt_mined_at": receipt_mined_wall,
                "sink_first_queryable_at": sink_wall,
                "transaction_hash": None if tx_hash is None else tx_hash.hex(),
                "transaction_nonce_before": nonce_before,
                "transaction_nonce": None if transaction is None else int(transaction["nonce"]),
                "transaction_signer": None if transaction is None else transaction["from"],
                "transaction_hash_matches_outcome": bool(transaction and tx_hash and transaction["hash"] == tx_hash),
                "generator_schedule_lag_ms": max(0, (offered_mono - planned_mono) * 1000),
                "offered_to_submit_ms": None if call_started is None else max(0, (call_started - offered_mono) * 1000),
                "submit_to_sink_ms": None if sink_wall is None or call_started is None else (sink_mono - call_started) * 1000,
                "offered_to_sink_ms": None if sink_wall is None else (sink_mono - offered_mono) * 1000,
                "planned_to_sink_ms": None if sink_wall is None else (sink_mono - planned_mono) * 1000,
                "receipt_status": status, "receipt_block": None if receipt is None else int(receipt.blockNumber),
                "gas_used": None if receipt is None else int(receipt.gasUsed),
                "node_events": node_events,
                "edge_events": edge_events,
                "all_stage_states": [list(x) for x in all_stage_states],
                "states_match_receipt_events": bool(state_event_match),
                "transaction_identity_nonce_match": bool(transaction_match),
                "succeeded": status == 1 and sink_wall is not None and pre_submit_sink_check_passed and
                    (DIAGNOSTIC_LIGHT or (bool(state_event_match) and bool(transaction_match))),
                "failure": failure if failure else ("post_drain_audit_pending" if DIAGNOSTIC_LIGHT and status == 1 and sink_wall else None)}
            observations.append(row)
            return row

        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = []
            for index in range(count):
                wait_until(generation_started + index / rate)
                planned_mono = generation_started + index / rate
                planned_wall = now_iso()
                offered = now_iso()
                offered_mono = time.perf_counter()
                slot = serial % WORKERS
                serial += 1
                futures.append(pool.submit(run_one, index, slot, planned_wall, planned_mono, offered, offered_mono))
                queued_high[0] = max(queued_high[0], pool._work_queue.qsize())
            generation_ended_wall = now_iso()
            generation_ended_perf = time.perf_counter()
            rows = [future.result(timeout=180) for future in futures]
        if DIAGNOSTIC_LIGHT:
            mempool_stop.set()
            mempool_thread.join(timeout=2)
        phase_rpc_load_end = rpc_meter.snapshot()
        post_audit_rpc_start = phase_rpc_load_end
        phase_receipt_poll_end = receipt_poller.snapshot() if receipt_poller is not None else None
        if DIAGNOSTIC_LIGHT:
            def audit_row(row):
                if row["receipt_status"] != 1:
                    row["states_match_receipt_events"] = False
                    row["transaction_identity_nonce_match"] = False
                    row["succeeded"] = False
                    return row
                slot = int(row["sender_slot"])
                worker = worker_w3[slot]
                account = worker_accounts[slot]
                transaction = worker.eth.get_transaction(row["transaction_hash"])
                row["transaction_nonce"] = int(transaction["nonce"])
                row["transaction_signer"] = transaction["from"]
                row["transaction_hash_matches_outcome"] = (
                    Web3.to_hex(transaction["hash"]).removeprefix("0x").lower() ==
                    row["transaction_hash"].removeprefix("0x").lower()
                )
                tx_match = (row["transaction_hash_matches_outcome"] and transaction["from"].lower() == account.lower() and
                            int(transaction["nonce"]) == row["transaction_nonce_before"])
                states = [worker_routers[slot].functions.nodeResult(int(row["session_id"]), int(row["workflow_id"]), node).call()
                          for node in range(PROFILE["node_count"])]
                events = row["node_events"]
                by_node = {int(event["node_id"]): event for event in events}
                states_match = set(by_node) == set(range(PROFILE["node_count"])) and all(
                    int(event["session_id"]) == int(row["session_id"]) and int(event["workflow_id"]) == int(row["workflow_id"])
                    for event in events)
                if states_match:
                    for node, state in enumerate(states):
                        event = by_node[node]
                        if not bool(state[0]) or int(state[1]) != int(row["session_id"]) or int(state[2]) != int(event["checksum"]) or \
                                int(state[3]) != int(event["input_count"]) or int(state[4]) != int(event["operation_count"]) or \
                                int(state[3]) != (0 if node == 0 else 1) or int(state[4]) != OPERATIONS:
                            states_match = False
                            break
                row["all_stage_states"] = [list(x) for x in states]
                row["states_match_receipt_events"] = states_match
                row["transaction_identity_nonce_match"] = tx_match
                row["succeeded"] = bool(row["pre_submit_sink_check_passed"] and row["receipt_status"] == 1 and
                                         row["sink_first_queryable_at"] and states_match and tx_match)
                row["failure"] = None if row["succeeded"] else ("post_drain_state_or_transaction_audit_failed")
                return row
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(64, WORKERS)) as audit_pool:
                rows = list(audit_pool.map(audit_row, rows))
        post_audit_rpc_end = rpc_meter.snapshot()
        successes = [x for x in rows if x["succeeded"]]
        phase_rows = [x for x in observations if x["phase"] == phase_index and x["warmup"] == warmup]
        if len(successes) == count:
            closed, close_receipt = base.send(w3, router.functions.exitSession(session_id, Web3.keccak(text=f"exit|{session_id}")), gas=1_000_000)
        else:
            closed, close_receipt = base.send(w3, router.functions.abortSession(session_id), gas=1_000_000)
        window_end = datetime.fromisoformat(measurement_window_end_wall.replace("Z", "+00:00")).timestamp()
        completed_in_window = [x for x in phase_rows if x["succeeded"] and x["sink_first_queryable_at"] and
            datetime.fromisoformat(x["sink_first_queryable_at"].replace("Z", "+00:00")).timestamp() <= window_end]
        accepted_in_window = [x for x in phase_rows if x["tx_hash_returned_at"] and
            datetime.fromisoformat(x["tx_hash_returned_at"].replace("Z", "+00:00")).timestamp() <= window_end]
        inflight_at_window_end = [x for x in phase_rows if x["tx_submit_started_at"] and x["receipt_mined_at"] and
            datetime.fromisoformat(x["tx_submit_started_at"].replace("Z", "+00:00")).timestamp() <= window_end and
            datetime.fromisoformat(x["receipt_mined_at"].replace("Z", "+00:00")).timestamp() > window_end]
        completed_wall = [datetime.fromisoformat(x["sink_first_queryable_at"].replace("Z", "+00:00")).timestamp()
                          for x in phase_rows if x["sink_first_queryable_at"]]
        drain_after_window_ms = max(0.0, (max(completed_wall) - window_end) * 1000) if completed_wall else None
        last_block = int(close_receipt.blockNumber)
        block_usage = []
        for block_number in range(phase_start_block + 1, last_block + 1):
            block = w3.eth.get_block(block_number)
            gas_used = int(block.gasUsed)
            gas_limit = int(block.gasLimit)
            block_usage.append({"block_number": block_number, "timestamp": int(block.timestamp),
                "gas_used": gas_used, "gas_limit": gas_limit,
                "utilization": gas_used / gas_limit if gas_limit else None,
                "transaction_count": len(block.transactions)})
        summary_row = {"phase": phase_index, "rate_index": rate_index, "warmup": warmup, "session_id": str(session_id),
            "offered_rate_per_sec": rate, "offered": count,
            "phase_started_at": generation_started_wall, "measurement_window_end_at": measurement_window_end_wall,
            "generation_ended_at": generation_ended_wall,
            "drained": True, "completed": len(rows), "successful_sink_completions": len(successes),
            "failures": len(rows) - len(successes), "unfinished_at_drain": count - len(rows),
            "successful_completions_in_measurement_window": len(completed_in_window),
            "measured_window_goodput_per_sec": len(completed_in_window) / seconds,
            "transaction_accepts_in_measurement_window": len(accepted_in_window),
            "measured_window_accept_rate_per_sec": len(accepted_in_window) / seconds,
            "in_flight_at_measurement_window_end": len(inflight_at_window_end),
            "drain_time_after_window_ms": drain_after_window_ms,
            "offered_generator_rate_per_sec": count / max(0.001, (generation_ended_perf - generation_started_perf + 1 / rate)),
            "drained_successes_per_measurement_second": len(successes) / seconds,
            "client_task_queue_depth_high_water": max(queued_high), "client_inflight_high_water": inflight_high[0],
            "mean_client_queue_delay_ms": statistics.mean(x["offered_to_submit_ms"] for x in rows if x["offered_to_submit_ms"] is not None) if any(x["offered_to_submit_ms"] is not None for x in rows) else None,
            "mean_generator_schedule_lag_ms": statistics.mean(x["generator_schedule_lag_ms"] for x in rows if x["generator_schedule_lag_ms"] is not None) if any(x["generator_schedule_lag_ms"] is not None for x in rows) else None,
            "receipt_poll_interval_sec": RECEIPT_POLL_SEC if DIAGNOSTIC_LIGHT else 0.1,
            "receipt_polling_strategy": "centralized_json_rpc_batch" if DIAGNOSTIC_LIGHT else "per_workflow_polling",
            "receipt_batch_size": RECEIPT_BATCH_SIZE if DIAGNOSTIC_LIGHT else None,
            "receipt_batch_http_calls_in_phase": (phase_receipt_poll_end["batch_http_calls"] - phase_receipt_poll_start["batch_http_calls"])
                if phase_receipt_poll_end and phase_receipt_poll_start else None,
            "receipt_poll_pending_at_phase_end": phase_receipt_poll_end["pending_receipts"] if phase_receipt_poll_end else None,
            "receipt_poll_max_batch_latency_ms": phase_receipt_poll_end["max_batch_latency_ms"] if phase_receipt_poll_end else None,
            "receipt_sampling_error_bound_ms": ((RECEIPT_POLL_SEC * 1000 + phase_receipt_poll_end["max_batch_latency_ms"])
                if phase_receipt_poll_end else (0.1 * 1000)),
            "rpc_load_path_by_method": RpcMeter.delta(phase_rpc_start, phase_rpc_load_end),
            "rpc_post_drain_audit_by_method": RpcMeter.delta(post_audit_rpc_start, post_audit_rpc_end),
            "mempool_samples": mempool_samples,
            "phase_start_block": phase_start_block, "phase_end_block": last_block,
            "session_entry_tx_hash": entry_receipt.transactionHash.hex(),
            "session_close_status": closed["status"], "session_close_tx_hash": close_receipt.transactionHash.hex(),
            "block_usage_samples": block_usage, "rows": phase_rows}
        raw["phases"].append(summary_row)
        OUT.write_text(json.dumps({**summary, "finished_at": now_iso(), "phases": [{k: v for k, v in x.items() if k != "rows"} for x in raw["phases"]]}, indent=2) + "\n")
        RAW.write_text(json.dumps(raw, indent=2) + "\n")

    for scenario in REQUESTED_SCENARIOS:
        scenario_index = SCENARIO_NAMES.index(scenario)
        config = SCHEDULE["scenarios"][scenario]
        rate_override = os.environ.get("STRESS_RATE_OVERRIDE")
        rates = [float(x) for x in rate_override.split(",")] if rate_override else [float(x) for x in config["rates_per_sec"]]
        warmup_seconds = float(os.environ.get("STRESS_WARMUP_OVERRIDE_SEC", config["warmup_sec"]))
        measurement_seconds = float(os.environ.get("STRESS_MEASUREMENT_OVERRIDE_SEC", config["measurement_sec"]))
        for i, rate in enumerate(rates):
            phase_index = scenario_index * 10 + i
            phase(rate, warmup_seconds, True, phase_index, i)
            phase(rate, measurement_seconds, False, phase_index, i)
    if receipt_poller is not None:
        receipt_poller.stop()
        raw["runtime"]["receipt_batch_poller"] = receipt_poller.snapshot()
    summary["setup_gas"] = setup_gas
    summary["completed"] = True
except Exception as exc:
    summary["error"] = repr(exc)
finally:
    if receipt_poller is not None:
        receipt_poller.stop()
    summary["finished_at"] = now_iso()
    OUT.write_text(json.dumps({**summary, "phases": [{k: v for k, v in x.items() if k != "rows"} for x in raw["phases"]]}, indent=2) + "\n")
    RAW.write_text(json.dumps(raw, indent=2) + "\n")
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
