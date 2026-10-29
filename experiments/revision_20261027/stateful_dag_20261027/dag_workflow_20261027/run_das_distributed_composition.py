#!/usr/bin/env python3
"""Run a router plus one independently deployed EVM Stage contract per DAG node."""
from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import time
from collections import Counter
from pathlib import Path

from web3 import Web3
from solcx import compile_source, get_installed_solc_versions, install_solc

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
NEAR_DIR = ROOT / "near_runtime_20261027"
MANIFEST_PATH = HERE / "profile_manifest.json"
SOURCE = HERE / "contracts" / "DASDistributedCompositionHarness.sol"
NEAR_RESULTS = Path(os.environ.get("DAG_NEAR_RESULTS", str(NEAR_DIR / "near_dag_full_results.json")))
TAG = os.environ.get("DAG_OUTPUT_TAG", "das_distributed_composition")
OUT = HERE / f"{TAG}_results.json"
RAW = HERE / f"{TAG}_raw.json"
REPEATS = int(os.environ.get("DAG_REPEATS", "3"))
FILTER = re.compile(os.environ["DAG_PROFILE_REGEX"]) if os.environ.get("DAG_PROFILE_REGEX") else None

MASK64 = (1 << 64) - 1
WORKFLOW_MULTIPLIER = 0x9E3779B97F4A7C15


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_web3(url: str, process: subprocess.Popen, timeout: float = 20) -> Web3:
    deadline = time.time() + timeout
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Anvil exited early with code {process.returncode}")
        if w3.is_connected():
            return w3
        time.sleep(0.1)
    raise TimeoutError("Anvil RPC did not become ready")


def receipt_row(receipt) -> dict:
    return {
        "tx_hash": receipt.transactionHash.hex(),
        "status": int(receipt.status),
        "gas_used": int(receipt.gasUsed),
        "block_number": int(receipt.blockNumber),
        "transaction_index": int(receipt.transactionIndex),
    }


def send(w3: Web3, function, *, gas: int = 28_000_000) -> tuple[dict, object]:
    tx_hash = function.transact({"from": w3.eth.default_account, "gas": gas})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
    return receipt_row(receipt), receipt


def deploy(w3: Web3, artifact: dict, constructor_args: tuple = (), gas: int = 8_000_000) -> tuple[object, dict]:
    factory = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["bin"])
    tx_hash = factory.constructor(*constructor_args).transact({"from": w3.eth.default_account, "gas": gas})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
    row = receipt_row(receipt)
    if row["status"] != 1 or not receipt.contractAddress:
        raise RuntimeError("contract deployment failed")
    contract = w3.eth.contract(address=receipt.contractAddress, abi=artifact["abi"])
    return contract, {**row, "contract_address": receipt.contractAddress}


def load_near_results() -> dict:
    if not NEAR_RESULTS.exists():
        raise FileNotFoundError(f"completed NEAR results required: {NEAR_RESULTS}")
    data = json.loads(NEAR_RESULTS.read_text())
    if data.get("error") or not data.get("completed_repeats"):
        raise RuntimeError("NEAR DAG results are incomplete")
    return data


def expected_near_index(data: dict) -> dict:
    result = {}
    for rep in data["completed_repeats"]:
        for profile in rep["profiles"]:
            for session in profile["sessions"]:
                for flow in session["same_nodes_and_checksum"]:
                    result[(rep["repeat"], profile["profile"]["profile_id"], flow["workflow"]["workflow_id"])] = flow["node_checksums"]
    return result


def address_key(address: str) -> str:
    return Web3.to_checksum_address(address).lower()


def flatten_call_trace(frame: dict, stage_addresses: set[str], rows: list[dict], depth: int = 0) -> None:
    target = frame.get("to")
    if target and address_key(target) in stage_addresses:
        rows.append({
            "depth": depth,
            "type": frame.get("type"),
            "from": frame.get("from"),
            "to": Web3.to_checksum_address(target),
            "gas_used": int(frame.get("gasUsed", "0x0"), 16),
            "error": frame.get("error"),
        })
    for child in frame.get("calls", []) or []:
        flatten_call_trace(child, stage_addresses, rows, depth + 1)


def trace_stage_calls(w3: Web3, tx_hash: str, stage_addresses: set[str]) -> dict:
    response = w3.provider.make_request("debug_traceTransaction", [tx_hash, {"tracer": "callTracer"}])
    if response.get("error"):
        raise RuntimeError(f"callTracer RPC failed: {response['error']}")
    root = response.get("result")
    if not isinstance(root, dict):
        raise RuntimeError(f"unexpected callTracer result: {type(root).__name__}")
    rows: list[dict] = []
    flatten_call_trace(root, stage_addresses, rows)
    return {"stage_call_frames": rows, "stage_call_frame_count": len(rows), "stage_frame_gas_used_sum": sum(x["gas_used"] for x in rows)}


def event_topic(signature: str) -> str:
    return Web3.keccak(text=signature).hex().lower()


def decode_logs(receipt, stage_address_to_node: dict[str, int]) -> tuple[list[dict], list[dict]]:
    node_topic = event_topic("NodeExecuted(uint64,uint64,uint32,uint64,uint32,uint32)")
    edge_topic = event_topic("ParentInputConsumed(uint64,uint64,uint32,uint32,uint64)")
    node_events, edge_events = [], []
    for log in receipt.logs:
        topics = [x.hex().lower() for x in log["topics"]]
        address = Web3.to_checksum_address(log["address"])
        if not topics:
            continue
        row = {"contract_address": address, "topic0": topics[0], "topics": topics, "data": log["data"].hex()}
        if address.lower() in stage_address_to_node and topics[0] == node_topic:
            row["stage_node_id"] = stage_address_to_node[address.lower()]
            row["session_id"] = int(topics[1], 16)
            row["workflow_id"] = int(topics[2], 16)
            row["node_id"] = int(topics[3], 16)
            row["checksum"], row["input_count"], row["operation_count"] = Web3().codec.decode(
                ["uint64", "uint32", "uint32"], bytes(log["data"])
            )
            node_events.append(row)
        elif address.lower() in stage_address_to_node and topics[0] == edge_topic:
            row["stage_node_id"] = stage_address_to_node[address.lower()]
            row["session_id"] = int(topics[1], 16)
            row["workflow_id"] = int(topics[2], 16)
            row["node_id"] = int(topics[3], 16)
            row["parent_node_id"], row["value"] = Web3().codec.decode(
                ["uint32", "uint64"], bytes(log["data"])
            )
            edge_events.append(row)
    return node_events, edge_events


def deployment_bundle(w3: Web3, artifacts: dict, profile: dict, fail_workflow_id: int, fail_node: int) -> tuple[object, list[dict], int]:
    router, router_deploy = deploy(w3, artifacts["DASDistributedCompositionHarness"], (profile["parents"],), gas=10_000_000)
    stage_contracts, stage_rows = [], []
    for node in range(profile["node_count"]):
        stage, deployment = deploy(
            w3,
            artifacts["DAGStage"],
            (router.address, node, profile["parents"][node], fail_workflow_id, fail_node),
            gas=6_000_000,
        )
        stage_contracts.append(stage)
        stage_rows.append({"node_id": node, "address": stage.address, "deployment": deployment})
    configured, configured_receipt = send(w3, router.functions.setStages([stage.address for stage in stage_contracts]), gas=2_000_000)
    if configured["status"] != 1:
        raise RuntimeError("router stage registration failed")
    setup = [router_deploy, *[x["deployment"] for x in stage_rows], configured]
    return router, stage_rows, sum(x["gas_used"] for x in setup)


def main() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    profiles = [p for p in manifest["profiles"] if not FILTER or FILTER.search(p["profile_id"])]
    near = load_near_results()
    expected = expected_near_index(near)
    if "0.8.24" not in {str(v) for v in get_installed_solc_versions()}:
        install_solc("0.8.24")
    compiled = compile_source(SOURCE.read_text(), solc_version="0.8.24", output_values=["abi", "bin"])
    artifacts = {}
    for name in ("DAGStage", "DASDistributedCompositionHarness"):
        item = next(value for key, value in compiled.items() if key.endswith(f":{name}"))
        artifacts[name] = item
        (HERE / f"{name}.abi.json").write_text(json.dumps(item["abi"], indent=2) + "\n")
        (HERE / f"{name}.bin").write_text(item["bin"])

    configured_anvil = os.environ.get("ANVIL_BIN")
    anvil = Path(configured_anvil) if configured_anvil else (Path(shutil.which("anvil")) if shutil.which("anvil") else NEAR_DIR / "vendor" / "foundry" / "anvil")
    if not anvil.is_file() or not os.access(anvil, os.X_OK):
        raise FileNotFoundError("Anvil 1.8.6 executable not found; set ANVIL_BIN or install anvil on PATH")
    anvil_version = subprocess.check_output([str(anvil), "--version"], text=True).splitlines()[0]
    if "1.8.6" not in anvil_version:
        raise RuntimeError(f"expected Anvil 1.8.6, found: {anvil_version}")

    summary = {
        "experiment": "independent-stage same-domain EVM DAG composition",
        "runtime": {"anvil": anvil_version, "solidity": "0.8.24", "backend": "local EVM", "contract_model": "one independently deployed DAGStage per graph node; router invokes each by external CALL"},
        "design": {
            "profile_count_per_repeat": len(profiles),
            "fresh_anvil_repeats": REPEATS,
            "entry_execution_exit_are_separate_transactions": True,
            "theta_workflows_per_execution_transaction": True,
            "setup_deployment_gas_separate": True,
            "migration_boundary_note": "entry/exit are local session-storage proxies, not bridge proof or production migration",
        },
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "completed_repeats": [],
    }
    OUT.write_text(json.dumps(summary, indent=2) + "\n")
    raw_rows = []

    for repeat in range(1, REPEATS + 1):
        port = free_port()
        process = subprocess.Popen(
            [str(anvil), "--host", "127.0.0.1", "--port", str(port), "--chain-id", "31337", "--silent"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            w3 = wait_web3(f"http://127.0.0.1:{port}", process)
            w3.eth.default_account = w3.eth.accounts[0]
            rows = []
            for profile in profiles:
                profile_index = next(i for i, x in enumerate(manifest["profiles"], 1) if x["profile_id"] == profile["profile_id"])
                fail_workflow_id = repeat * 10_000_000_000 + profile_index * 1_000_000 + 999_999
                fail_node = profile["node_count"] // 2
                router, stage_rows, setup_gas = deployment_bundle(w3, artifacts, profile, fail_workflow_id, fail_node)
                stage_address_to_node = {x["address"].lower(): x["node_id"] for x in stage_rows}
                stage_address_set = set(stage_address_to_node)
                case_sessions = []

                for session_index in range(profile["sessions_per_profile"]):
                    session_id = repeat * 100_000_000_000 + profile_index * 1_000_000_000 + (session_index + 1) * 1_000_000
                    workflow_ids = [session_id + k + 1 for k in range(profile["workflows_per_session_theta"])]
                    seeds = [0x123456789ABC ^ ((wid * WORKFLOW_MULTIPLIER) & MASK64) for wid in workflow_ids]
                    fail_session_id = session_id + 900_000_000

                    entry_commitment = Web3.keccak(text=f"entry|{repeat}|{profile['profile_id']}|{session_id}")
                    exit_commitment = Web3.keccak(text=f"exit|{repeat}|{profile['profile_id']}|{session_id}")
                    entry, _ = send(w3, router.functions.enterSession(session_id, len(workflow_ids), entry_commitment), gas=2_000_000)
                    execute, execution_receipt = send(w3, router.functions.executeWorkflows(session_id, workflow_ids, seeds, profile["operations_per_node"]), gas=28_000_000)
                    exit_row, _ = send(w3, router.functions.exitSession(session_id, exit_commitment), gas=1_000_000)
                    if not (entry["status"] == execute["status"] == exit_row["status"] == 1):
                        raise AssertionError(f"successful session transaction failed for {profile['profile_id']}: entry={entry}, execute={execute}, exit={exit_row}")

                    call_trace = trace_stage_calls(w3, execute["tx_hash"], stage_address_set)
                    node_events, edge_events = decode_logs(execution_receipt, stage_address_to_node)
                    expected_node_calls = len(workflow_ids) * profile["node_count"]
                    expected_edge_calls = len(workflow_ids) * profile["edge_count"]
                    calls_per_stage = Counter(x["stage_node_id"] for x in node_events)
                    edge_inputs_per_stage = Counter(x["stage_node_id"] for x in edge_events)
                    expected_calls_per_stage = {node: len(workflow_ids) for node in range(profile["node_count"])}
                    expected_edges_per_stage = {node: len(workflow_ids) * len(profile["parents"][node]) for node in range(profile["node_count"])}
                    expected_edges_per_active_stage = {node: count for node, count in expected_edges_per_stage.items() if count}
                    call_frames_per_stage = Counter(stage_address_to_node[x["to"].lower()] for x in call_trace["stage_call_frames"])
                    if len(node_events) != expected_node_calls or dict(calls_per_stage) != expected_calls_per_stage:
                        raise AssertionError(f"stage execution event counts mismatch: {profile['profile_id']} {dict(calls_per_stage)}")
                    if len(edge_events) != expected_edge_calls or dict(edge_inputs_per_stage) != expected_edges_per_active_stage:
                        raise AssertionError(f"edge input event counts mismatch: {profile['profile_id']} {dict(edge_inputs_per_stage)}")
                    if call_trace["stage_call_frame_count"] != expected_node_calls or dict(call_frames_per_stage) != expected_calls_per_stage:
                        raise AssertionError(f"external CALL trace mismatch: {profile['profile_id']} {dict(call_frames_per_stage)}")

                    expected_node_records, observed_node_records = set(), set()
                    expected_edge_records, observed_edge_records = set(), set()
                    for event in node_events:
                        observed_node_records.add((event["session_id"], event["workflow_id"], event["node_id"], event["checksum"], event["input_count"], event["operation_count"]))
                    for event in edge_events:
                        observed_edge_records.add((event["session_id"], event["workflow_id"], event["node_id"], event["parent_node_id"], event["value"]))
                    for wid in workflow_ids:
                        expected_nodes = expected[(repeat, profile["profile_id"], str(wid))]
                        for node in range(profile["node_count"]):
                            parents = profile["parents"][node]
                            expected_node_records.add((session_id, wid, node, int(expected_nodes[node]), len(parents), profile["operations_per_node"]))
                            for parent in parents:
                                expected_edge_records.add((session_id, wid, node, parent, int(expected_nodes[parent])))
                    if observed_node_records != expected_node_records:
                        raise AssertionError(f"stage event payload mismatch: {profile['profile_id']}")
                    if observed_edge_records != expected_edge_records:
                        raise AssertionError(f"parent event payload mismatch: {profile['profile_id']}")

                    per_workflow = []
                    for wid in workflow_ids:
                        expected_nodes = expected[(repeat, profile["profile_id"], str(wid))]
                        actual_nodes = [str(router.functions.nodeChecksums(session_id, wid, node).call()) for node in range(profile["node_count"])]
                        stage_nodes = []
                        inputs_match = True
                        for node, stage in enumerate([w3.eth.contract(address=x["address"], abi=artifacts["DAGStage"]["abi"]) for x in stage_rows]):
                            state = stage.functions.nodeResult(session_id, wid).call()
                            input_rows = []
                            for parent in profile["parents"][node]:
                                present, value = router.functions.nodeInput(session_id, wid, node, parent).call()
                                expected_parent = int(expected_nodes[parent])
                                inputs_match = inputs_match and present and int(value) == expected_parent
                                input_rows.append({"parent_node_id": parent, "present": bool(present), "value": str(value), "expected_value": str(expected_parent)})
                            stage_nodes.append({
                                "node_id": node,
                                "stage_address": stage_rows[node]["address"],
                                "result_exists": bool(state[0]),
                                "session_id": int(state[1]),
                                "checksum": str(state[2]),
                                "input_count": int(state[3]),
                                "operation_count": int(state[4]),
                                "inputs": input_rows,
                            })
                        checksums_match_near = actual_nodes == expected_nodes
                        all_stage_state_valid = all(
                            item["result_exists"] and item["input_count"] == len(profile["parents"][item["node_id"]])
                            and item["operation_count"] == profile["operations_per_node"]
                            and item["checksum"] == expected_nodes[item["node_id"]]
                            for item in stage_nodes
                        )
                        per_workflow.append({
                            "workflow_id": str(wid),
                            "node_checksums": actual_nodes,
                            "near_node_checksums": expected_nodes,
                            "checksums_match_near_same_receiver": checksums_match_near,
                            "all_stage_storage_and_input_values_verified": all_stage_state_valid and inputs_match,
                            "stage_nodes": stage_nodes,
                        })

                    router_state = router.functions.sessionState(session_id).call()
                    session_state_valid = int(router_state[2]) == len(workflow_ids) and not bool(router_state[0])
                    success_valid = all(x["checksums_match_near_same_receiver"] and x["all_stage_storage_and_input_values_verified"] for x in per_workflow)

                    failure_entry, _ = send(w3, router.functions.enterSession(fail_session_id, 1, Web3.keccak(text=f"fault-entry|{fail_session_id}")), gas=2_000_000)
                    failure_wid = fail_workflow_id
                    failure_seed = 0x6A09E667F3BCC909 ^ failure_wid
                    failure_receipt, _ = send(w3, router.functions.executeWorkflows(fail_session_id, [failure_wid], [failure_seed], profile["operations_per_node"]), gas=28_000_000)
                    failed_trace = trace_stage_calls(w3, failure_receipt["tx_hash"], stage_address_set)
                    router_after_failure = router.functions.sessionState(fail_session_id).call()
                    failure_stage_state = []
                    for stage_meta in stage_rows:
                        stage = w3.eth.contract(address=stage_meta["address"], abi=artifacts["DAGStage"]["abi"])
                        result_state = stage.functions.nodeResult(fail_session_id, failure_wid).call()
                        stage_inputs = [
                            bool(stage.functions.hasInput(fail_session_id, failure_wid, parent).call())
                            for parent in profile["parents"][stage_meta["node_id"]]
                        ]
                        failure_stage_state.append({
                            "node_id": stage_meta["node_id"],
                            "stage_address": stage_meta["address"],
                            "result_exists": bool(result_state[0]),
                            "parent_inputs_present": stage_inputs,
                        })
                    rollback_verified = (
                        failure_receipt["status"] == 0
                        and all(not x["result_exists"] and not any(x["parent_inputs_present"]) for x in failure_stage_state)
                        and router.functions.workflowChecksums(fail_session_id, failure_wid).call() == 0
                        and not router.functions.workflowOperationCounts(fail_session_id, failure_wid).call()
                        and bool(router_after_failure[0]) and int(router_after_failure[2]) == 0
                        and len(failed_trace["stage_call_frames"]) == fail_node + 1
                    )
                    cleanup, _ = send(w3, router.functions.abortSession(fail_session_id), gas=1_000_000)
                    if not (success_valid and session_state_valid and rollback_verified and cleanup["status"] == 1):
                        raise AssertionError(f"distributed composition invariant failed for {profile['profile_id']}: success={success_valid}, session={session_state_valid}, rollback={rollback_verified}, cleanup={cleanup}, failure_status={failure_receipt['status']}, failure_frames={failed_trace['stage_call_frames']}, failure_states={failure_stage_state}, router={router_after_failure}")

                    session_row = {
                        "session_id": str(session_id),
                        "theta_workflows": [str(x) for x in workflow_ids],
                        "entry_proxy": entry,
                        "router_execution": {
                            **execute,
                            "transaction_count": 1,
                            "external_stage_call_frames": call_trace["stage_call_frames"],
                            "external_stage_call_frame_count": call_trace["stage_call_frame_count"],
                            "external_stage_call_gas_used_sum": call_trace["stage_frame_gas_used_sum"],
                            "node_events": node_events,
                            "edge_input_events": edge_events,
                            "node_event_count": len(node_events),
                            "edge_input_event_count": len(edge_events),
                            "workflows": per_workflow,
                        },
                        "exit_proxy": exit_row,
                        "stage_setup": {
                            "router_address": router.address,
                            "stage_addresses": stage_rows,
                            "deployment_and_registration_gas": setup_gas,
                        },
                        "complete_session_gas_excluding_setup": entry["gas_used"] + execute["gas_used"] + exit_row["gas_used"],
                        "local_execution_gas": execute["gas_used"],
                        "session_state_valid": session_state_valid,
                        "checksums_match_near_and_stage_inputs_valid": success_valid,
                        "failure_probe": {
                            "entry": failure_entry,
                            "workflow_id": str(failure_wid),
                            "fail_node": fail_node,
                            **failure_receipt,
                            "external_stage_call_frames": failed_trace["stage_call_frames"],
                            "stage_state_after_revert": failure_stage_state,
                            "router_state_after_revert": {
                                "active": bool(router_after_failure[0]),
                                "expected_workflows": int(router_after_failure[1]),
                                "executed_workflows": int(router_after_failure[2]),
                                "workflow_checksum": int(router.functions.workflowChecksums(fail_session_id, failure_wid).call()),
                                "operation_count": int(router.functions.workflowOperationCounts(fail_session_id, failure_wid).call()),
                            },
                            "cleanup": cleanup,
                            "all_contract_state_rolled_back": rollback_verified,
                        },
                    }
                    case_sessions.append(session_row)

                rows.append({
                    "repeat": repeat,
                    "profile": profile,
                    "runtime": {"chain_id": int(w3.eth.chain_id), "block_number_after_profile": int(w3.eth.block_number)},
                    "per_node_stage_contracts": stage_rows,
                    "deployment_and_registration_gas_per_profile": setup_gas,
                    "independent_stage_sessions": case_sessions,
                })
                raw_rows.append(rows[-1])
                RAW.write_text(json.dumps({"experiment": summary["experiment"], "completed_profile_repeats": raw_rows}, indent=2) + "\n")
                print(f"repeat {repeat}: finished {profile['profile_id']}")

            summary["completed_repeats"].append({"repeat": repeat, "profiles": rows})
            summary["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            OUT.write_text(json.dumps(summary, indent=2) + "\n")
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    OUT.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Completed {len(summary['completed_repeats'])} fresh Anvil repeats.")


if __name__ == "__main__":
    main()
