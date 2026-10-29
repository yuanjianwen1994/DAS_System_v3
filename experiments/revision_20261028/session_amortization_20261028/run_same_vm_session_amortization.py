#!/usr/bin/env python3
"""Same-EVM session migration pilot/formal matrix using MigrationHarness.

Models logical domains inside one Anvil/EVM with a trusted ECDSA attestor.
It measures ledger mechanics and amortization only; it is not a bridge or
cross-VM latency benchmark.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import solcx
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
from web3.logs import DISCARD

ROOT = Path(__file__).resolve().parents[3]
EXP = Path(__file__).resolve().parent
DEV_MNEMONIC = "test test test test test test test test test test test junk"

SOLC_VERSION = "0.8.24"
THEtas = (1, 2, 4, 8)
ARMS = ("same_domain", "migrated_unbatched", "migrated_batch")
SRC = Web3.keccak(text="session-source-domain")
DST = Web3.keccak(text="session-destination-domain")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compile_artifacts():
    if SOLC_VERSION not in {str(v) for v in solcx.get_installed_solc_versions()}:
        solcx.install_solc(SOLC_VERSION)
    solcx.set_solc_version(SOLC_VERSION)
    migration_path = ROOT / "contracts" / "MigrationHarness.sol"
    batch_path = EXP / "BatchBusinessExecutor.sol"
    sources = {
        "contracts/MigrationHarness.sol": {"content": migration_path.read_text()},
        "experiments/revision_20261028/session_amortization_20261028/BatchBusinessExecutor.sol": {"content": batch_path.read_text()},
    }
    output = solcx.compile_standard({
        "language": "Solidity", "sources": sources,
        "settings": {
            "optimizer": {"enabled": True, "runs": 200},
            "viaIR": True,
            "outputSelection": {"*": {"*": ["abi", "evm.bytecode.object"]}},
        },
    })["contracts"]
    return {
        "MigrationHarness": output["contracts/MigrationHarness.sol"]["MigrationHarness"],
        "BatchBusinessExecutor": output["experiments/revision_20261028/session_amortization_20261028/BatchBusinessExecutor.sol"]["BatchBusinessExecutor"],
    }


def start_anvil(block_time: int, chain_id: int, mnemonic: str):
    binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    if not binary:
        raise RuntimeError("anvil was not found; configure ANVIL_BIN or PATH on the remote host")
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    process = subprocess.Popen(
        [binary, "--silent", "--accounts", "10", "--mnemonic", mnemonic,
         "--chain-id", str(chain_id), "--port", str(port), "--block-time", str(block_time)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}", request_kwargs={"timeout": 30}))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Anvil exited with code {process.returncode}")
        try:
            if w3.is_connected() and w3.eth.chain_id == chain_id:
                return process, w3, binary
        except Exception:
            pass
        time.sleep(0.2)
    process.terminate()
    process.wait(timeout=10)
    raise TimeoutError("Anvil did not become ready")


def stop_anvil(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def deploy(w3, artifact, sender, args=()):
    contract = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["evm"]["bytecode"]["object"])
    tx_hash = contract.constructor(*args).transact({"from": sender, "gas": 8_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    if receipt.status != 1:
        raise RuntimeError("deployment transaction reverted")
    return w3.eth.contract(address=receipt.contractAddress, abi=artifact["abi"]), receipt


def wait_ok(w3, tx_hash):
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    if receipt.status != 1:
        raise RuntimeError(f"transaction reverted: {tx_hash.hex()}")
    return receipt


def sign_receipt(attestor, receipt_id):
    return bytes(Account.sign_message(encode_defunct(primitive=bytes(receipt_id)), attestor.key).signature)


def transfer(w3, ledger, attestor, submit_begin, begin_sender_expected,
             source_domain, destination_domain, asset, recipient, amount, payload, relay):
    started = time.monotonic()
    begin_receipt = wait_ok(w3, submit_begin())
    events = ledger.events.MigrationStarted().process_receipt(begin_receipt, errors=DISCARD)
    if len(events) != 1:
        raise AssertionError(f"expected exactly one MigrationStarted event; got {len(events)}")
    args = events[0]["args"]
    receipt_id = args["receiptId"]
    pending = ledger.functions.pendingReceipt(receipt_id).call()
    envelope_matches = (
        bytes(pending[0]) == bytes(source_domain) and bytes(pending[1]) == bytes(destination_domain)
        and bytes(pending[2]) == bytes(asset) and int(pending[3]) == amount
        and args["sender"].lower() == begin_sender_expected.lower()
        and args["recipient"].lower() == recipient.lower()
        and bytes(pending[7]) == payload and bool(pending[8])
        and bytes(args["payloadHash"]) == Web3.keccak(payload)
    )
    if not envelope_matches:
        raise AssertionError("pending receipt did not match the requested complete envelope")
    signature = sign_receipt(attestor, receipt_id)
    settle_receipt = wait_ok(w3, ledger.functions.settleMigration(receipt_id, signature).transact({"from": relay, "gas": 1_000_000}))
    settled_events = ledger.events.MigrationSettled().process_receipt(settle_receipt, errors=DISCARD)
    settled = bool(ledger.functions.settled(receipt_id).call())
    pending_cleared = not bool(ledger.functions.pendingReceipt(receipt_id).call()[-1])
    event_matches = len(settled_events) == 1 and (
        settled_events[0]["args"]["receiptId"] == receipt_id
        and settled_events[0]["args"]["recipient"].lower() == recipient.lower()
        and int(settled_events[0]["args"]["amount"]) == amount
        and bytes(settled_events[0]["args"]["payloadHash"]) == Web3.keccak(payload)
    )
    if not (settled and pending_cleared and event_matches):
        raise AssertionError("settlement or receipt binding audit failed")
    return {
        "seconds": time.monotonic() - started,
        "receipt_id": receipt_id.hex(),
        "amount": amount,
        "nonce": int(args["nonce"]),
        "source_domain": source_domain.hex(),
        "destination_domain": destination_domain.hex(),
        "sender": args["sender"], "recipient": args["recipient"],
        "payload_hash": bytes(args["payloadHash"]).hex(),
        "binding_verified": True,
        "source_tx_gas": begin_receipt.gasUsed,
        "settlement_tx_gas": settle_receipt.gasUsed,
    }


def business_actions(w3, ledger, executor, arm, domain, asset, sender, count, session_id):
    started = time.monotonic()
    receipts = []
    if arm == "migrated_batch":
        tx = executor.functions.executeBusinessBatch(domain, asset, count, session_id).transact({"from": sender, "gas": 3_000_000})
        receipts.append(wait_ok(w3, tx))
    else:
        base_nonce = w3.eth.get_transaction_count(sender, "pending")
        tx_hashes = []
        for i in range(count):
            payload = Web3.keccak(text=f"{session_id.hex()}|action|{i}")
            tx_hashes.append(ledger.functions.executeBusiness(domain, asset, 1, payload).transact(
                {"from": sender, "nonce": base_nonce + i, "gas": 500_000}))
        receipts.extend(wait_ok(w3, tx_hash) for tx_hash in tx_hashes)
    if any(receipt.status != 1 for receipt in receipts):
        raise RuntimeError("one or more business transactions failed")
    return {
        "seconds": time.monotonic() - started,
        "transaction_count": len(receipts),
        "gas_used": sum(receipt.gasUsed for receipt in receipts),
        "successful_business_transactions": count,
    }


def run_session(w3, ledger, executor, attestor, accounts, arm, theta, repeat):
    sender, relay = accounts[2], accounts[0]
    asset = Web3.keccak(text=f"session-amortization|repeat={repeat}|theta={theta}|arm={arm}")
    session_id = Web3.keccak(text=f"session-id|repeat={repeat}|theta={theta}|arm={arm}")
    amount = theta + 1
    record = {
        "repeat": repeat, "theta": theta, "arm": arm,
        "model_boundary": "same_evm_two_logical_domains_trusted_attestor",
        "status": "running", "failure": None, "incomplete": True,
        "receipt_bindings": [],
    }
    stage = "fund"
    try:
        wait_ok(w3, ledger.functions.fund(SRC, asset, sender, amount).transact({"from": accounts[0], "gas": 500_000}))
        session_started = time.monotonic()
        if arm == "same_domain":
            stage = "same_domain_execution"
            internal = business_actions(w3, ledger, executor, arm, SRC, asset, sender, theta, session_id)
            journey_seconds = time.monotonic() - session_started
            entry, exit_ = None, None
        else:
            recipient = executor.address if arm == "migrated_batch" else sender
            entry_payload = Web3.keccak(text=f"entry|{session_id.hex()}|theta={theta}")
            stage = "migration_in"
            entry = transfer(
                w3, ledger, attestor,
                lambda: ledger.functions.beginMigration(SRC, DST, asset, recipient, amount, entry_payload).transact({"from": sender, "gas": 1_000_000}),
                sender, SRC, DST, asset, recipient, amount, entry_payload, relay,
            )
            record["receipt_bindings"].append({"leg": "in", **entry})
            stage = "destination_business_execution"
            internal = business_actions(w3, ledger, executor, arm, DST, asset, sender, theta, session_id)
            stage = "migration_out"
            exit_payload = Web3.keccak(text=f"exit|{session_id.hex()}|theta={theta}")
            if arm == "migrated_batch":
                submit_exit = lambda: executor.functions.beginExit(DST, SRC, asset, sender, 1, exit_payload).transact({"from": sender, "gas": 1_000_000})
                exit_sender = executor.address
            else:
                submit_exit = lambda: ledger.functions.beginMigration(DST, SRC, asset, sender, 1, exit_payload).transact({"from": sender, "gas": 1_000_000})
                exit_sender = sender
            exit_ = transfer(
                w3, ledger, attestor,
                submit_exit, exit_sender, DST, SRC, asset, sender, 1, exit_payload, relay,
            )
            record["receipt_bindings"].append({"leg": "out", **exit_})
            journey_seconds = time.monotonic() - session_started
        stage = "final_audit"
        source_balance = ledger.functions.balanceOf(SRC, asset, sender).call()
        destination_balance = ledger.functions.balanceOf(DST, asset, sender).call()
        executor_destination_balance = ledger.functions.balanceOf(DST, asset, executor.address).call()
        pending = ledger.functions.pendingByAsset(asset).call()
        total_balance = ledger.functions.totalBalanceByAsset(asset).call()
        spent = ledger.functions.spentByAsset(asset).call()
        funded = ledger.functions.fundedByAsset(asset).call()
        invariant = ledger.functions.checkInvariant(asset).call()
        if (source_balance, destination_balance, executor_destination_balance, pending, total_balance, spent, funded, invariant) != (1, 0, 0, 0, 1, theta, amount, True):
            raise AssertionError("final balances or conservation invariant do not match the session")
        record.update({
            "status": "completed", "incomplete": False, "internal_business_seconds": internal["seconds"],
            "internal_business_tx_count": internal["transaction_count"], "internal_business_gas": internal["gas_used"],
            "entry_seconds": entry["seconds"] if entry else 0.0, "exit_seconds": exit_["seconds"] if exit_ else 0.0,
            "end_to_end_seconds": journey_seconds,
            "source_final_balance": int(source_balance), "destination_final_balance": int(destination_balance),
            "batch_executor_final_balance": int(executor_destination_balance),
            "pending_final_balance": int(pending), "business_spent": int(spent), "funded": int(funded),
            "total_balance": int(total_balance), "conservation_invariant": bool(invariant),
            "failure_stage": None,
        })
    except Exception as error:
        record.update({"status": "failed", "failure": f"{type(error).__name__}: {error}", "failure_stage": stage})
        record["incomplete"] = True
    return record


def summarize(records):
    output = []
    for arm in ARMS:
        for theta in THEtas:
            group = [r for r in records if r["arm"] == arm and r["theta"] == theta]
            successful = [r for r in group if r["status"] == "completed"]
            output.append({
                "arm": arm, "theta": theta, "planned_repeats": len(group),
                "completed_sessions": len(successful), "failed_sessions": sum(r["status"] == "failed" for r in group),
                "incomplete_sessions": sum(bool(r["incomplete"]) for r in group),
                "median_internal_business_seconds": median([r["internal_business_seconds"] for r in successful]) if successful else None,
                "median_end_to_end_seconds": median([r["end_to_end_seconds"] for r in successful]) if successful else None,
                "median_end_to_end_per_operation_seconds": median([r["end_to_end_seconds"] / theta for r in successful]) if successful else None,
                "receipt_binding_passes": sum(sum(1 for b in r["receipt_bindings"] if b.get("binding_verified")) for r in successful),
                "receipt_binding_expected": 0 if arm == "same_domain" else 2 * len(successful),
            })
    return output


def save(outdir, metadata, records):
    outdir.mkdir(parents=True, exist_ok=True)
    data = {"metadata": metadata, "sessions": records, "summary": summarize(records)}
    temp = outdir / "session_amortization_raw.json.tmp"
    temp.write_text(json.dumps(data, indent=2, default=str) + "\n")
    temp.replace(outdir / "session_amortization_raw.json")
    with (outdir / "session_amortization_summary.csv").open("w", newline="") as f:
        cols = list(data["summary"][0].keys()) if data["summary"] else []
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(data["summary"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("pilot", "formal"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--block-time", type=int)
    parser.add_argument("--repeats", type=int)
    args = parser.parse_args()
    block_time = args.block_time or (1 if args.mode == "pilot" else 12)
    repeat_count = args.repeats or (1 if args.mode == "pilot" else 3)
    outdir = args.output.resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    artifacts = compile_artifacts()
    Account.enable_unaudited_hdwallet_features()
    mnemonic = DEV_MNEMONIC
    attestor = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/1")
    source_files = [ROOT / "contracts" / "MigrationHarness.sol", EXP / "BatchBusinessExecutor.sol", Path(__file__)]
    base_commit = os.environ.get("SESSION_SOURCE_COMMIT") or subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    records = []
    anvil_binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    anvil_version = subprocess.run([anvil_binary, "--version"], capture_output=True, text=True).stdout.strip()
    metadata = {
        "created_utc": datetime.now(timezone.utc).isoformat(), "mode": args.mode,
        "execution_model": "one fresh Anvil EVM per repeat; two logical domains; trusted ECDSA attestor; not a cross-VM bridge",
        "block_time_seconds": block_time, "repeats": repeat_count, "thetas": list(THEtas),
        "arms": list(ARMS), "chain_id": 31337, "anvil_version": anvil_version,
        "solc_version": SOLC_VERSION, "web3_version": importlib.metadata.version("web3"),
        "source_commit": base_commit, "source_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in source_files},
        "deployments_by_repeat": [],
        "elapsed_interval_excludes_deployment_and_funding": True,
        "same_domain_control_is_best_case_without_migration": True,
        "batch_control_migrates_once_in_and_out_then_batches_theta_business_spends": True,
    }
    save(outdir, metadata, records)
    for repeat in range(1, repeat_count + 1):
        anvil, w3, _ = start_anvil(block_time, 31337, mnemonic)
        try:
            accounts = [Web3.to_checksum_address(x) for x in w3.eth.accounts]
            ledger, ledger_deploy = deploy(w3, artifacts["MigrationHarness"], accounts[0], (attestor.address,))
            executor, executor_deploy = deploy(w3, artifacts["BatchBusinessExecutor"], accounts[0], (ledger.address,))
            metadata["deployments_by_repeat"].append({
                "repeat": repeat, "ledger_address": ledger.address, "executor_address": executor.address,
                "ledger_deployment_gas": ledger_deploy.gasUsed, "executor_deployment_gas": executor_deploy.gasUsed,
            })
            save(outdir, metadata, records)
            for theta in THEtas:
                for arm in ARMS:
                    item = run_session(w3, ledger, executor, attestor, accounts, arm, theta, repeat)
                    records.append(item)
                    save(outdir, metadata, records)
                    print(f"repeat={repeat} theta={theta} arm={arm} status={item['status']} full_s={item.get('end_to_end_seconds')}", flush=True)
        finally:
            stop_anvil(anvil)
    print(f"output={outdir / 'session_amortization_raw.json'}", flush=True)


if __name__ == "__main__":
    main()
