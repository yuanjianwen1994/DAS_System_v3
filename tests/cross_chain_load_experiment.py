"""Two-Anvil cross-chain receipt and bounded-load experiment.

The source and destination use separate EVM processes and chain IDs. A local
attestor signs the source event envelope; it is an authentication abstraction,
not a validator/light-client or finality proof.
"""
from __future__ import annotations

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

from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
import solcx
from web3.logs import DISCARD

ROOT = Path(__file__).resolve().parents[1]
SOLC_VERSION = "0.8.24"
SRC_CHAIN = 31337
DST_CHAIN = 31338
BLOCK_TIME = 12
REPEATS = [2026102701, 2026102702, 2026102703]
LOAD_LEVELS = [1, 3]  # concurrent complete journeys per model
THINK_SECONDS = 0.5
MODEL_NAMES = ["DAS", "TELEPORT_ABSTRACT", "ATOMIC_UTXO_ABSTRACT"]
MODEL_KIND = {name: i for i, name in enumerate(MODEL_NAMES)}


def compile_contracts():
    installed = {str(v) for v in solcx.get_installed_solc_versions()}
    if SOLC_VERSION not in installed:
        solcx.install_solc(SOLC_VERSION)
    solcx.set_solc_version(SOLC_VERSION)
    source = (ROOT / "contracts" / "CrossChainPairHarness.sol").read_text()
    output = solcx.compile_standard({
        "language": "Solidity",
        "sources": {"CrossChainPairHarness.sol": {"content": source}},
        "settings": {
            "optimizer": {"enabled": True, "runs": 200},
            "viaIR": True,
            "outputSelection": {"*": {"*": ["abi", "evm.bytecode.object"]}},
        },
    })["contracts"]["CrossChainPairHarness.sol"]
    return {
        name: {"abi": item["abi"], "bin": item["evm"]["bytecode"]["object"]}
        for name, item in output.items()
    }


def start_anvil(chain_id: int, mnemonic: str):
    binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    if not binary:
        raise RuntimeError("anvil was not found; set ANVIL_BIN or add it to PATH")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    proc = subprocess.Popen(
        [binary, "--silent", "--accounts", "10", "--mnemonic", mnemonic,
         "--chain-id", str(chain_id), "--port", str(port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}", request_kwargs={"timeout": 10}))
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"anvil {chain_id} exited with code {proc.returncode}")
        if w3.is_connected():
            if w3.eth.chain_id != chain_id:
                raise AssertionError("Anvil returned an unexpected chain id")
            return proc, w3
        time.sleep(0.1)
    proc.terminate()
    proc.wait(timeout=5)
    raise TimeoutError(f"anvil {chain_id} did not become ready")


def stop_anvil(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def deploy(w3, artifact, sender, args=()):
    factory = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["bin"])
    tx = factory.constructor(*args).transact({"from": sender, "gas": 5_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx, timeout=60)
    if receipt.status != 1:
        raise RuntimeError("contract deployment failed")
    return w3.eth.contract(address=receipt.contractAddress, abi=artifact["abi"]), receipt.gasUsed


def wait_receipts(w3, pending):
    out = {}
    for label, tx_hash, submitted in pending:
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=BLOCK_TIME + 90)
        if receipt.status != 1:
            raise RuntimeError(f"transaction reverted: {label}")
        out[label] = (receipt, time.monotonic(), submitted)
    return out


def submit_phase(w3, transactions):
    pending = []
    for label, fn, sender in transactions:
        started = time.monotonic()
        tx_hash = fn.transact({"from": sender, "gas": 2_000_000})
        pending.append((label, tx_hash, started))
    return wait_receipts(w3, pending)


def parse_export(contract, receipt):
    event = contract.events.ExportStarted().process_receipt(receipt, errors=DISCARD)
    if len(event) != 1:
        raise AssertionError(f"expected one ExportStarted event, got {len(event)}")
    return dict(event[0]["args"])


def sign_digest(account, digest):
    return account.sign_message(encode_defunct(primitive=bytes(digest))).signature


def assert_reverts(fn, sender, label):
    try:
        fn.call({"from": sender})
    except Exception:
        return
    raise AssertionError(f"expected rejection: {label}")


def source_accounting(w3, source, asset):
    assert source.functions.checkInvariant(asset).call(), "source chain invariant failed"
    return {
        "funded": source.functions.fundedByAsset(asset).call(),
        "balance": source.functions.totalBalanceByAsset(asset).call(),
        "pending": source.functions.pendingByAsset(asset).call(),
        "spent": source.functions.spentByAsset(asset).call(),
    }


def global_accounting(w3s, w3d, source, destination, asset, receipt_ids):
    src = source_accounting(w3s, source, asset)
    assert destination.functions.checkInvariant(asset).call(), "destination chain invariant failed"
    dest_balance = destination.functions.totalBalanceByAsset(asset).call()
    dest_spent = destination.functions.spentByAsset(asset).call()
    unmatched_pending = 0
    for receipt_id in receipt_ids:
        record = source.functions.exports(receipt_id).call()
        imported = destination.functions.importedReceipt(receipt_id).call()
        if record[-1] and not imported:
            unmatched_pending += record[3]
    global_total = src["balance"] + src["spent"] + dest_balance + dest_spent + unmatched_pending
    if src["funded"] != global_total:
        raise AssertionError(f"cross-chain conservation failed: funded={src['funded']} accounted={global_total}")
    return {
        "source_funded": src["funded"],
        "source_spendable": src["balance"],
        "source_pending": src["pending"],
        "source_export_records_pending": src["pending"],
        "destination_imported": destination.functions.importedByAsset(asset).call(),
        "destination_spendable": dest_balance,
        "destination_business_spent": dest_spent,
        "unmatched_source_pending": unmatched_pending,
        "conserved": True,
    }


def run_trial(seed, concurrency, block_time, artifacts, mnemonic, adversarial=False):
    src_proc, w3s = start_anvil(SRC_CHAIN, mnemonic)
    dst_proc = None
    try:
        dst_proc, w3d = start_anvil(DST_CHAIN, mnemonic)
        admin_s, admin_d = w3s.eth.accounts[0], w3d.eth.accounts[0]
        senders = w3s.eth.accounts[1:1 + len(MODEL_NAMES) * concurrency]
        recipients = w3d.eth.accounts[1:]
        Account.enable_unaudited_hdwallet_features()
        attestor = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/20")
        attestor_digest = attestor

        endpoints = {}
        setup_gas = {"source_deploy": {}, "destination_deploy": {}, "teleport_trust": 0, "source_funding_by_model": {}}
        assets = {}
        for model_i, model in enumerate(MODEL_NAMES):
            source_domain = Web3.keccak(text=f"{model}-source-{seed}-{concurrency}")
            destination_domain = Web3.keccak(text=f"{model}-destination-{seed}-{concurrency}")
            asset = Web3.keccak(text=f"asset-{model}-{seed}-{concurrency}")
            src, gas_src = deploy(w3s, artifacts["source"], admin_s)
            dst, gas_dst = deploy(w3d, artifacts["destination"], admin_d, (attestor.address, MODEL_KIND[model], destination_domain))
            endpoints[model] = (src, dst, source_domain, destination_domain)
            assets[model] = asset
            setup_gas["source_deploy"][model] = gas_src
            setup_gas["destination_deploy"][model] = gas_dst
            if model == "TELEPORT_ABSTRACT":
                tx = dst.functions.setTrustedSource(SRC_CHAIN, src.address, source_domain, True).transact({"from": admin_d, "gas": 200_000})
                trust_receipt = w3d.eth.wait_for_transaction_receipt(tx, timeout=30)
                if trust_receipt.status != 1:
                    raise RuntimeError("teleport source trust configuration failed")
                setup_gas["teleport_trust"] = trust_receipt.gasUsed

        cases = []
        account_i = 0
        effective_journeys = concurrency + 1 if adversarial else concurrency
        for model_i, model in enumerate(MODEL_NAMES):
            src, dst, source_domain, destination_domain = endpoints[model]
            asset = assets[model]
            journey_count = effective_journeys
            setup_gas["source_funding_by_model"][model] = 0
            for journey_i in range(journey_count):
                if adversarial:
                    sender = w3s.eth.accounts[1 + model_i]
                    recipient = recipients[3 + model_i * journey_count + journey_i]
                else:
                    sender = senders[account_i]
                    recipient = sender
                    account_i += 1
                payload_hash = Web3.keccak(text=f"seed={seed};model={model};load={concurrency};journey={journey_i}")
                cases.append({
                    "model": model, "journey_index": journey_i, "source": src, "destination": dst,
                    "source_domain": source_domain, "destination_domain": destination_domain,
                    "asset": asset, "sender": sender, "recipient": recipient,
                    "amount": 10, "payload_hash": payload_hash,
                })
                tx = src.functions.fund(asset, sender, 10).transact({"from": admin_s, "gas": 200_000})
                funding_receipt = w3s.eth.wait_for_transaction_receipt(tx, timeout=30)
                if funding_receipt.status != 1:
                    raise RuntimeError("source funding failed")
                setup_gas["source_funding_by_model"][model] += funding_receipt.gasUsed

        if block_time:
            for w3 in (w3s, w3d):
                result = w3.provider.make_request("anvil_setIntervalMining", [block_time])
                if "error" in result:
                    raise RuntimeError("could not set interval mining: " + str(result["error"]))

        run_started = time.monotonic()

        begin_txs = []
        for i, case in enumerate(cases):
            begin_txs.append((f"begin-{i}", case["source"].functions.beginExport(
                DST_CHAIN, case["destination"].address, case["source_domain"],
                case["destination_domain"], case["asset"], case["recipient"],
                case["amount"], case["payload_hash"]), case["sender"]))
        if adversarial:
            begin_receipts = {}
            for label, fn, sender in begin_txs:
                submitted = time.monotonic()
                tx_hash = fn.transact({"from": sender, "gas": 2_000_000})
                rc = w3s.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                if rc.status != 1:
                    raise RuntimeError(f"transaction reverted: {label}")
                begin_receipts[label] = (rc, time.monotonic(), submitted)
        else:
            begin_receipts = submit_phase(w3s, begin_txs)
        receipt_ids = []
        for i, case in enumerate(cases):
            receipt, completed_at, submitted_at = begin_receipts[f"begin-{i}"]
            event = parse_export(case["source"], receipt)
            case["receipt"] = event
            case["receipt_id"] = event["receiptId"]
            case["begin_gas"] = receipt.gasUsed
            case["begin_seconds"] = completed_at - submitted_at
            case["journey_started"] = submitted_at
            receipt_ids.append(event["receiptId"])
        # Funds are separated by asset/model. Check the debit-to-pending state
        # before any import is accepted on the destination chain.
        post_begin = {}
        for model in MODEL_NAMES:
            src, dst, _, dst_domain = endpoints[model]
            asset = assets[model]
            model_ids = [c["receipt_id"] for c in cases if c["model"] == model]
            src_state = source_accounting(w3s, src, asset)
            if src_state["pending"] != 10 * effective_journeys:
                raise AssertionError("source debit did not enter pending state")
            model_ids = [c["receipt_id"] for c in cases if c["model"] == model]
            post_begin[model] = global_accounting(w3s, w3d, src, dst, asset, model_ids)
            for c in cases:
                if c["model"] == model:
                    assert_reverts(dst.functions.executeBusiness(dst_domain, asset, 1), c["sender"], "business spend before import")

        # Short smoke attack checks use calls only; they do not add transactions.
        if adversarial:
            c = cases[0]
            e = c["receipt"]
            signature = sign_digest(attestor_digest, e["receiptId"])
            assert_reverts(c["destination"].functions.importReceipt(
                e["receiptId"], e["sourceChainId"] + 1, e["sourceEndpoint"], e["sourceDomain"],
                e["destinationDomain"], e["asset"], e["amount"], e["sender"], e["recipient"],
                e["payloadHash"], e["nonce"], signature), c["sender"], "wrong source chain")
            assert_reverts(c["destination"].functions.importReceipt(
                e["receiptId"], e["sourceChainId"], e["sourceEndpoint"], e["sourceDomain"],
                e["destinationDomain"], e["asset"], e["amount"], e["sender"], e["recipient"],
                Web3.keccak(text="tampered payload"), e["nonce"], signature), c["sender"], "tampered payload")

        import_txs = []
        # Reverse order to show receipt settlement does not depend on nonce order.
        for i, case in reversed(list(enumerate(cases))):
            e = case["receipt"]
            sig = sign_digest(attestor_digest, e["receiptId"])
            case["receipt_signature"] = sig
            import_txs.append((f"import-{i}", case["destination"].functions.importReceipt(
                e["receiptId"], e["sourceChainId"], e["sourceEndpoint"], e["sourceDomain"],
                e["destinationDomain"], e["asset"], e["amount"], e["sender"], e["recipient"],
                e["payloadHash"], e["nonce"], sig), case["sender"]))
        import_receipts = submit_phase(w3d, import_txs)
        for i, case in enumerate(cases):
            receipt, completed_at, submitted_at = import_receipts[f"import-{i}"]
            case["import_gas"] = receipt.gasUsed
            case["import_seconds"] = completed_at - submitted_at
            case["imported_at"] = completed_at
        post_import = {}
        for model in MODEL_NAMES:
            src, dst, _, _ = endpoints[model]
            ids = [c["receipt_id"] for c in cases if c["model"] == model]
            post_import[model] = global_accounting(w3s, w3d, src, dst, assets[model], ids)
        if adversarial:
            c = cases[0]
            e = c["receipt"]
            assert_reverts(c["destination"].functions.importReceipt(
                e["receiptId"], e["sourceChainId"], e["sourceEndpoint"], e["sourceDomain"],
                e["destinationDomain"], e["asset"], e["amount"], e["sender"], e["recipient"],
                e["payloadHash"], e["nonce"], c["receipt_signature"]), c["sender"], "duplicate import")

        before_business = {}
        for model in MODEL_NAMES:
            src, dst, _, _ = endpoints[model]
            asset = assets[model]
            model_cases = [c for c in cases if c["model"] == model]
            ids = [c["receipt_id"] for c in model_cases]
            audit = global_accounting(w3s, w3d, src, dst, asset, ids)
            if audit["source_pending"] != 10 * effective_journeys or audit["unmatched_source_pending"] != 0:
                raise AssertionError("source export records and destination imports did not reconcile")
            if audit["destination_imported"] != 10 * effective_journeys:
                raise AssertionError("destination imported total mismatch")
            before_business[model] = audit

        # A fixed user think interval is modeled once per wave and recorded
        # separately from chain/RPC waiting time.
        think_started = time.monotonic()
        time.sleep(THINK_SECONDS)
        think_elapsed = time.monotonic() - think_started
        business_txs = []
        for i, case in enumerate(cases):
            business_txs.append((f"business-{i}", case["destination"].functions.executeBusiness(
                case["destination_domain"], case["asset"], 1), case["recipient"]))
        business_receipts = submit_phase(w3d, business_txs)
        finished = max(v[1] for v in business_receipts.values())
        for i, case in enumerate(cases):
            receipt, completed_at, submitted_at = business_receipts[f"business-{i}"]
            case["business_gas"] = receipt.gasUsed
            case["business_seconds"] = completed_at - submitted_at
            case["completed_at"] = completed_at
            case["journey_seconds"] = completed_at - case["journey_started"]
            case["think_seconds"] = think_elapsed
            case["chain_and_rpc_seconds_excluding_think"] = max(0.0, case["journey_seconds"] - think_elapsed)
            case["completed"] = True

        final_audits = {}
        for model in MODEL_NAMES:
            src, dst, _, _ = endpoints[model]
            asset = assets[model]
            model_cases = [c for c in cases if c["model"] == model]
            ids = [c["receipt_id"] for c in model_cases]
            audit = global_accounting(w3s, w3d, src, dst, asset, ids)
            if audit["destination_spendable"] != 9 * effective_journeys or audit["destination_business_spent"] != effective_journeys:
                raise AssertionError("destination business state mismatch")
            final_audits[model] = audit

        return {
            "seed": seed,
            "concurrent_journeys_per_model": concurrency,
            "block_time_seconds": block_time,
            "source_chain_id": w3s.eth.chain_id,
            "destination_chain_id": w3d.eth.chain_id,
            "trial_wall_seconds": round(finished - run_started, 4),
            "think_interval_seconds": round(think_elapsed, 4),
            "completed_journeys": {m: sum(1 for c in cases if c["model"] == m and c["completed"]) for m in MODEL_NAMES},
            "failed_journeys": {m: 0 for m in MODEL_NAMES},
            "incomplete_journeys": {m: 0 for m in MODEL_NAMES},
            "setup_gas": setup_gas,
            "pre_business_audits": before_business,
            "post_export_audits": post_begin,
            "post_import_audits": post_import,
            "final_audits": final_audits,
            "journeys": [{k: v for k, v in c.items() if k not in ("source", "destination", "receipt", "receipt_signature")} for c in cases],
            "security_checks": {"wrong_source_chain_rejected": adversarial, "tampered_payload_rejected": adversarial, "duplicate_import_rejected": adversarial, "reordered_receipts_settled": True, "business_before_import_rejected": True},
        }
    finally:
        stop_anvil(src_proc)
        if dst_proc is not None:
            stop_anvil(dst_proc)


def summarize(trials):
    summary = {}
    for concurrency in LOAD_LEVELS:
        group = [trial for trial in trials if trial["concurrent_journeys_per_model"] == concurrency]
        group_seconds = sum(t["trial_wall_seconds"] for t in group)
        for model in MODEL_NAMES:
            journeys = [j for t in group for j in t["journeys"] if j["model"] == model and j["completed"]]
            latencies = sorted(j["journey_seconds"] for j in journeys)
            transfer_txs = 2 * len(journeys)  # source export and destination import
            business_txs = len(journeys)
            summary[f"{model}:load{concurrency}"] = {
                "repeats": len(group),
                "journeys_per_repeat": concurrency,
                "completed_journeys": len(journeys),
                "failed_journeys": sum(t["failed_journeys"][model] for t in group),
                "incomplete_journeys": sum(t["incomplete_journeys"][model] for t in group),
                "pooled_trial_wall_seconds": round(group_seconds, 4),
                "completed_journeys_per_wall_second": round(len(journeys) / group_seconds, 6) if group_seconds else 0,
                "transfer_protocol_transactions_completed": transfer_txs,
                "transfer_protocol_transactions_per_wall_second": round(transfer_txs / group_seconds, 6) if group_seconds else 0,
                "business_transactions_completed": business_txs,
                "business_transactions_per_wall_second": round(business_txs / group_seconds, 6) if group_seconds else 0,
                "all_transactions_per_wall_second": round((transfer_txs + business_txs) / group_seconds, 6) if group_seconds else 0,
                "journey_latency_seconds": {
                    "p50": latencies[(len(latencies) - 1) // 2] if latencies else None,
                    "p95_nearest_rank": latencies[max(0, int(0.95 * len(latencies) + 0.999999) - 1)] if latencies else None,
                    "p99_nearest_rank": latencies[max(0, int(0.99 * len(latencies) + 0.999999) - 1)] if latencies else None,
                    "n": len(latencies),
                    "caution": "descriptive only; small n; upper percentiles are not stable tail estimates",
                },
                "mean_gas_per_journey": {
                    "source_export": round(sum(j["begin_gas"] for j in journeys) / len(journeys), 2) if journeys else None,
                    "destination_import": round(sum(j["import_gas"] for j in journeys) / len(journeys), 2) if journeys else None,
                    "business_action": round(sum(j["business_gas"] for j in journeys) / len(journeys), 2) if journeys else None,
                    "total": round(sum(j["begin_gas"] + j["import_gas"] + j["business_gas"] for j in journeys) / len(journeys), 2) if journeys else None,
                },
                "mean_business_think_seconds": round(sum(j["think_seconds"] for j in journeys) / len(journeys), 4) if journeys else None,
            }
    return summary


def main():
    sys.path.insert(0, str(ROOT))
    import config_global

    artifacts_raw = compile_contracts()
    artifacts = {
        "source": artifacts_raw["CrossChainSourceHarness"],
        "destination": artifacts_raw["CrossChainDestinationHarness"],
    }
    if os.environ.get("CROSS_CHAIN_SMOKE_ONLY") == "1":
        trials = [run_trial(2026102701, 1, 1, artifacts, config_global.MNEMONIC, adversarial=True)]
        outpath = ROOT / "experiments" / "revision_20261027" / "cross_chain_smoke.json"
        outpath.parent.mkdir(parents=True, exist_ok=True)
        outpath.write_text(json.dumps({"kind": "two-anvil-functional-smoke", "trial": trials[0]}, indent=2, default=str) + "\n")
        print(json.dumps({"result": "PASS", "output": str(outpath), "security_checks": trials[0]["security_checks"], "final_audits": trials[0]["final_audits"]}, indent=2, default=str))
        return

    trials = []
    for load in LOAD_LEVELS:
        for seed in REPEATS:
            trials.append(run_trial(seed, load, BLOCK_TIME, artifacts, config_global.MNEMONIC))
    summary = summarize(trials)
    result = {
        "metadata": {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "kind": "two-independent-anvil-chains-bounded-load-comparison",
            "base_git_commit": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
            "source_chain_id": SRC_CHAIN,
            "destination_chain_id": DST_CHAIN,
            "block_time_seconds": BLOCK_TIME,
            "repeat_seeds": REPEATS,
            "load_levels_concurrent_journeys_per_model": LOAD_LEVELS,
            "business_think_seconds": THINK_SECONDS,
            "python": sys.version.split()[0],
            "packages": {name: importlib.metadata.version(name) for name in ["web3", "py-solc-x", "urllib3"]},
            "solc": SOLC_VERSION,
            "anvil": subprocess.run([os.environ.get("ANVIL_BIN", shutil.which("anvil") or "anvil"), "--version"], capture_output=True, text=True).stdout.splitlines()[0],
            "source_sha256": {"contracts/CrossChainPairHarness.sol": hashlib.sha256((ROOT / "contracts/CrossChainPairHarness.sol").read_bytes()).hexdigest(), "tests/cross_chain_load_experiment.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
            "model_limits": {
                "DAS": "signed source receipt; the local attestor stands in for source finality verification",
                "TELEPORT_ABSTRACT": "same signed envelope plus preconfigured trusted source; not an XCM executor or relay transport",
                "ATOMIC_UTXO_ABSTRACT": "source export lock plus destination exact-once consume; shared-memory UTXO is represented by a signed proof, not AvalancheGo",
            },
            "limits": ["two Anvil nodes provide separate EVM state and chain IDs, not independent consensus/security domains", "attestor signatures are test abstractions, not production finality proofs", "load 1 versus 3 concurrent journeys per model is a bounded concurrency check, not saturation capacity", "all three models share the same two local Anvil block producers within each trial", "source pending export records remain on the source chain; global reconciliation matches receipt IDs off-chain against destination imports"],
        },
        "summary": summary,
        "trials": trials,
    }
    outdir = ROOT / "experiments" / "revision_20261027"
    outdir.mkdir(parents=True, exist_ok=True)
    raw = outdir / "cross_chain_load_raw.json"
    raw.write_text(json.dumps(result, indent=2, default=str) + "\n")
    csv = outdir / "cross_chain_load_journeys.csv"
    with csv.open("w") as f:
        f.write("seed,concurrency,model,journey_index,completed,journey_seconds,think_seconds,chain_and_rpc_seconds_excluding_think,source_export_gas,destination_import_gas,business_gas,source_pending_records,destination_imported,destination_business_spent,unmatched_source_pending,global_conserved\n")
        for trial in trials:
            for j in trial["journeys"]:
                audit = trial["final_audits"][j["model"]]
                f.write(f"{trial['seed']},{trial['concurrent_journeys_per_model']},{j['model']},{j['journey_index']},{j['completed']},{j['journey_seconds']:.4f},{j['think_seconds']:.4f},{j['chain_and_rpc_seconds_excluding_think']:.4f},{j['begin_gas']},{j['import_gas']},{j['business_gas']},{audit['source_pending']},{audit['destination_imported']},{audit['destination_business_spent']},{audit['unmatched_source_pending']},{audit['conserved']}\n")
    (outdir / "cross_chain_load_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"summary": summary, "raw": str(raw), "journeys_csv": str(csv)}, indent=2))


if __name__ == "__main__":
    main()
