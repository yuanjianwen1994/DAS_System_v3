#!/usr/bin/env python3
"""Execute the parameterized DAG in a local Type-II-state composition harness."""
from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import time
import shutil
from pathlib import Path

from web3 import Web3
from solcx import compile_source, get_installed_solc_versions, install_solc

ROOT = Path(__file__).resolve().parents[1]
NEAR_DIR = ROOT / "near_runtime_20261027"
MANIFEST_PATH = Path(__file__).with_name("profile_manifest.json")
SOURCE = Path(__file__).resolve().parent / "contracts" / "DASLocalCompositionHarness.sol"
NEAR_RESULTS = Path(os.environ.get("DAG_NEAR_RESULTS", str(NEAR_DIR / "near_dag_results.json")))
TAG = os.environ.get("DAG_OUTPUT_TAG", "das_composition")
OUT = Path(__file__).with_name(f"{TAG}_results.json")
RAW = Path(__file__).with_name(f"{TAG}_raw.json")
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
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 3}))
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Anvil exited early with code {process.returncode}")
        try:
            if w3.is_connected():
                return w3
        except Exception:
            pass
        time.sleep(0.1)
    raise TimeoutError("Anvil RPC did not become ready")


def send(w3: Web3, function, *, gas: int = 25_000_000) -> dict:
    tx_hash = function.transact({"from": w3.eth.default_account, "gas": gas})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    return {
        "tx_hash": tx_hash.hex(),
        "status": int(receipt.status),
        "gas_used": int(receipt.gasUsed),
        "block_number": int(receipt.blockNumber),
        "transaction_index": int(receipt.transactionIndex),
    }


def read_near_results() -> dict:
    if not NEAR_RESULTS.exists():
        raise FileNotFoundError(f"NEAR DAG results must be complete first: {NEAR_RESULTS}")
    data = json.loads(NEAR_RESULTS.read_text())
    if data.get("error") or not data.get("completed_repeats"):
        raise RuntimeError("NEAR DAG results are incomplete")
    return data


def near_expected_index(near_data: dict) -> dict:
    index = {}
    for repeat in near_data["completed_repeats"]:
        for profile in repeat["profiles"]:
            for session in profile["sessions"]:
                for row in session["same_nodes_and_checksum"]:
                    flow_id = row["workflow"]["workflow_id"]
                    index[(repeat["repeat"], profile["profile"]["profile_id"], flow_id)] = row["node_checksums"]
    return index


def main() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())
    profiles = [p for p in manifest["profiles"] if not FILTER or FILTER.search(p["profile_id"])]
    near_data = read_near_results()
    expected = near_expected_index(near_data)
    source = SOURCE.read_text()
    if "0.8.24" not in {str(v) for v in get_installed_solc_versions()}:
        install_solc("0.8.24")
    compiled = compile_source(source, solc_version="0.8.24", output_values=["abi", "bin"])
    artifact = next(value for key, value in compiled.items() if key.endswith(":DASLocalCompositionHarness"))
    full_binary = Path(__file__).parent / "DASLocalCompositionHarness.bin"
    full_abi = Path(__file__).parent / "DASLocalCompositionHarness.abi.json"
    full_binary.write_text(artifact["bin"])
    full_abi.write_text(json.dumps(artifact["abi"], indent=2) + "\n")

    configured_anvil = os.environ.get("ANVIL_BIN")
    anvil_binary = Path(configured_anvil) if configured_anvil else (Path(shutil.which("anvil")) if shutil.which("anvil") else NEAR_DIR / "vendor" / "foundry" / "anvil")
    if not anvil_binary.is_file() or not os.access(anvil_binary, os.X_OK):
        raise FileNotFoundError("Anvil 1.8.6 executable not found; set ANVIL_BIN or install anvil on PATH")
    anvil_version = subprocess.check_output([str(anvil_binary), "--version"], text=True).splitlines()[0]
    if "1.8.6" not in anvil_version:
        raise RuntimeError(f"expected Anvil 1.8.6, found: {anvil_version}")
    raw_repeats = []
    summary = {
        "experiment": "DAS-local parameterized stateful DAG composition",
        "runtime": {"anvil": "1.8.6", "solidity": "0.8.24", "backend": "local EVM", "node_modules_or_compiler_caches_are_not_in_git": True},
        "design": {"profile_count_per_repeat": len(profiles), "fresh_anvil_repeats": REPEATS, "entry_workflow_exit_are_separate_transactions": True, "migration_boundary_note": "measured session/storage commitments, no production bridge proof or network migration included"},
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "completed_repeats": [],
    }
    OUT.write_text(json.dumps(summary, indent=2) + "\n")

    for repeat in range(1, REPEATS + 1):
        port = free_port()
        process = subprocess.Popen(
            [str(anvil_binary), "--host", "127.0.0.1", "--port", str(port), "--chain-id", "31337", "--silent"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            w3 = wait_web3(f"http://127.0.0.1:{port}", process)
            w3.eth.default_account = w3.eth.accounts[0]
            factory = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["bin"])
            deploy_tx = factory.constructor().transact({"from": w3.eth.default_account, "gas": 6_000_000})
            deploy_receipt = w3.eth.wait_for_transaction_receipt(deploy_tx)
            if deploy_receipt.status != 1:
                raise RuntimeError("local composition contract deployment failed")
            contract = w3.eth.contract(address=deploy_receipt.contractAddress, abi=artifact["abi"])
            rows = []
            for profile in profiles:
                profile_index = next(i for i, item in enumerate(manifest["profiles"], 1) if item["profile_id"] == profile["profile_id"])
                case_sessions = []
                for session_index in range(profile["sessions_per_profile"]):
                    session_id = repeat * 100_000_000_000 + profile_index * 1_000_000_000 + (session_index + 1) * 1_000_000
                    workflow_ids = [session_id + k + 1 for k in range(profile["workflows_per_session_theta"])]
                    seeds = [0x123456789ABC ^ ((wid * WORKFLOW_MULTIPLIER) & MASK64) for wid in workflow_ids]
                    fail_workflow_id = repeat * 10_000_000_000 + profile_index * 1_000_000 + 999_999
                    fail_session_id = session_id + 900_000_000
                    fail_node = profile["node_count"] // 2
                    entry_commitment = Web3.keccak(text=f"entry|{repeat}|{profile['profile_id']}|{session_id}")
                    exit_commitment = Web3.keccak(text=f"exit|{repeat}|{profile['profile_id']}|{session_id}")

                    migration_in = send(w3, contract.functions.enterSession(session_id, len(workflow_ids), entry_commitment), gas=2_000_000)
                    local_execution = send(w3, contract.functions.executeWorkflows(
                        session_id, workflow_ids, seeds, profile["parents"], profile["operations_per_node"]
                    ), gas=20_000_000)
                    migration_out = send(w3, contract.functions.exitSession(session_id, exit_commitment), gas=1_000_000)
                    session_state = contract.functions.sessionState(session_id).call()
                    checksums = [int(contract.functions.workflowChecksums(session_id, wid).call()) for wid in workflow_ids]
                    expected_checksums = [expected[(repeat, profile["profile_id"], str(wid))] for wid in workflow_ids]
                    computed_node_checksums = []
                    for wid in workflow_ids:
                        computed_node_checksums.append([
                            str(contract.functions.nodeChecksums(session_id, wid, node).call())
                            for node in range(profile["node_count"])
                        ])
                    checksums_match_near = all(
                        computed_node_checksums[i] == expected_checksums[i]
                        for i in range(len(workflow_ids))
                    )

                    failure_entry = send(w3, contract.functions.enterSession(fail_session_id, 1, Web3.keccak(text=f"fault-entry|{fail_session_id}")), gas=2_000_000)
                    failure_receipt = send(w3, contract.functions.probeFailure(
                        fail_session_id, fail_workflow_id, 0x6A09E667F3BCC909 ^ fail_workflow_id,
                        profile["parents"], profile["operations_per_node"], fail_node,
                    ), gas=20_000_000)
                    failure_cleanup = send(w3, contract.functions.abortSession(fail_session_id), gas=1_000_000)
                    failure_state = contract.functions.sessionState(fail_session_id).call()
                    failure_unfinished = failure_receipt["status"] == 0 and contract.functions.workflowChecksums(fail_session_id, fail_workflow_id).call() == 0 and not failure_state[0]
                    session_row = {
                        "session_id": str(session_id),
                        "workflow_ids": [str(x) for x in workflow_ids],
                        "migration_in": migration_in,
                        "workflow_execution": local_execution,
                        "migration_out": migration_out,
                        "complete_session_gas": migration_in["gas_used"] + local_execution["gas_used"] + migration_out["gas_used"],
                        "gas_per_workflow_within_batched_execution": local_execution["gas_used"] / len(workflow_ids),
                        "session_block_window": migration_out["block_number"] - migration_in["block_number"],
                        "session_state": {
                            "active": session_state[0],
                            "expected_workflows": session_state[1],
                            "executed_workflows": session_state[2],
                            "aggregate": str(session_state[3]),
                        },
                        "workflow_checksums": [str(x) for x in checksums],
                        "node_checksums": computed_node_checksums,
                        "checksums_match_near_same_receiver": checksums_match_near,
                        "failure_probe": {
                            "workflow_id": str(fail_workflow_id), "fail_at_node": fail_node,
                            "separate_probe_session": str(fail_session_id),
                            "probe_migration_in": failure_entry,
                            **failure_receipt,
                            "abort_cleanup": failure_cleanup,
                            "unfinished_and_rolled_back": failure_unfinished,
                        },
                    }
                    if not (migration_in["status"] == local_execution["status"] == migration_out["status"] == 1 and session_state[2] == len(workflow_ids) and checksums_match_near and failure_unfinished):
                        raise AssertionError(f"state/checksum/failure invariant failed for {profile['profile_id']}")
                    case_sessions.append(session_row)

                row = {
                    "repeat": repeat,
                    "profile": profile,
                    "runtime": {"chain_id": w3.eth.chain_id, "block_number_after_profile": w3.eth.block_number},
                    "local_type_ii_session_mode": case_sessions,
                }
                rows.append(row)
                raw_repeats.append(row)
                RAW.write_text(json.dumps({"experiment": summary["experiment"], "completed_profile_repeats": raw_repeats}, indent=2) + "\n")
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
