"""Three-run 12-second-block microbenchmark of the abstract state machines.

This measures only same-EVM contract operations with labeled domains. It is not a
production cross-chain throughput benchmark. Each repetition starts a fresh Anvil.
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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import solcx
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
BLOCK_TIME = 12
REPEATS = [2026102701, 2026102702, 2026102703]
SOLC_VERSION = "0.8.24"


def source_hashes():
    result = {}
    for relative in [
        "contracts/MigrationHarness.sol",
        "contracts/ComparatorHarness.sol",
        "contracts/AdmissionAblationHarness.sol",
        "tests/migration_state_machine_smoke.py",
        "tests/formal_state_machine_sample.py",
    ]:
        result[relative] = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
    return result


def start_anvil(mnemonic):
    binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    if not binary:
        raise RuntimeError("anvil was not found; set ANVIL_BIN or add it to PATH")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    proc = subprocess.Popen(
        [binary, "--silent", "--accounts", "10", "--mnemonic", mnemonic, "--chain-id", "31337", "--port", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}", request_kwargs={"timeout": 3}))
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"anvil exited with code {proc.returncode}")
        if w3.is_connected():
            return proc, w3
        time.sleep(0.1)
    proc.terminate()
    proc.wait(timeout=5)
    raise TimeoutError("anvil did not become ready")


def compile_artifacts():
    installed = {str(v) for v in solcx.get_installed_solc_versions()}
    if SOLC_VERSION not in installed:
        solcx.install_solc(SOLC_VERSION)
    solcx.set_solc_version(SOLC_VERSION)
    compiled = solcx.compile_files(
        [str(ROOT / "contracts" / "MigrationHarness.sol"), str(ROOT / "contracts" / "ComparatorHarness.sol")],
        output_values=["abi", "bin"],
        solc_version=SOLC_VERSION,
    )
    return {key.rsplit(":", 1)[-1]: value for key, value in compiled.items()}


def send(w3, fn, sender, label):
    tx_hash = fn.transact({"from": sender, "gas": 1_500_000})
    submitted = time.monotonic()
    return {"label": label, "tx_hash": tx_hash, "submitted_at": submitted, "sender": sender}


def receipts_concurrently(w3, pending):
    def wait(item):
        receipt = w3.eth.wait_for_transaction_receipt(item["tx_hash"], timeout=50)
        return item["label"], item["submitted_at"], receipt, time.monotonic()

    with ThreadPoolExecutor(max_workers=len(pending)) as pool:
        return dict((label, (submitted, receipt, received)) for label, submitted, receipt, received in pool.map(wait, pending))


def deploy(w3, artifact, sender, args=()):
    factory = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["bin"])
    tx_hash = factory.constructor(*args).transact({"from": sender, "gas": 5_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    if receipt.status != 1:
        raise RuntimeError("deployment failed")
    return w3.eth.contract(address=receipt.contractAddress, abi=artifact["abi"]), receipt.gasUsed


def event_id(receipt, event, field):
    entries = event().process_receipt(receipt)
    if len(entries) != 1:
        raise AssertionError("expected exactly one receipt event")
    return entries[0]["args"][field]


def attestation(attestor, receipt_id):
    msg = encode_defunct(primitive=bytes(receipt_id))
    return bytes(Account.sign_message(msg, attestor.key).signature)


def run_repeat(seed, artifacts, mnemonic):
    proc, w3 = start_anvil(mnemonic)
    try:
        accounts = w3.eth.accounts
        Account.enable_unaudited_hdwallet_features()
        attestor = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/1")
        migration, migration_deploy_gas = deploy(w3, artifacts["MigrationHarness"], accounts[0], (attestor.address,))
        comparator, comparator_deploy_gas = deploy(w3, artifacts["ComparatorHarness"], accounts[0])
        asset = Web3.keccak(text=f"asset-{seed}")

        specs = {
            "DAS": {
                "contract": migration,
                "source": Web3.keccak(text=f"das-source-{seed}"),
                "destination": Web3.keccak(text=f"das-dest-{seed}"),
                "sender": accounts[2],
                "recipient": accounts[5],
                "payload": f"seed={seed};variant=DAS;action=transfer".encode(),
            },
            "TELEPORT_ABSTRACT": {
                "contract": comparator,
                "source": Web3.keccak(text=f"teleport-source-{seed}"),
                "destination": Web3.keccak(text=f"teleport-dest-{seed}"),
                "sender": accounts[3],
                "recipient": accounts[6],
                "payload": f"seed={seed};variant=TELEPORT;action=transfer".encode(),
            },
            "ATOMIC_UTXO_ABSTRACT": {
                "contract": comparator,
                "source": Web3.keccak(text=f"atomic-source-{seed}"),
                "destination": Web3.keccak(text=f"atomic-dest-{seed}"),
                "sender": accounts[4],
                "recipient": accounts[7],
                "payload": f"seed={seed};variant=ATOMIC;action=transfer".encode(),
            },
        }
        setup_rows = []
        relay_accounts = {
            "DAS": accounts[0],
            "TELEPORT_ABSTRACT": accounts[8],
            "ATOMIC_UTXO_ABSTRACT": accounts[9],
        }
        for name, spec in specs.items():
            tx_hash = spec["contract"].functions.fund(spec["source"], asset, spec["sender"], 100).transact(
                {"from": accounts[0], "gas": 250_000}
            )
            rc = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)
            if rc.status != 1:
                raise RuntimeError("funding failed for " + name)
            setup_rows.append({"variant": name, "setup_fund_gas": rc.gasUsed})
        tele = specs["TELEPORT_ABSTRACT"]
        tx_hash = comparator.functions.setTrustedTeleportSource(tele["destination"], tele["source"], True).transact(
            {"from": accounts[0], "gas": 150_000}
        )
        trust_receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)
        if trust_receipt.status != 1:
            raise RuntimeError("teleport trust configuration failed")

        # Setup is instant-mined. Only the measured protocol path uses the required 12-second interval.
        setting = w3.provider.make_request("anvil_setIntervalMining", [BLOCK_TIME])
        if "error" in setting:
            raise RuntimeError("could not enable 12-second interval mining: " + str(setting["error"]))

        trial_started = time.monotonic()
        starts = {name: time.monotonic() for name in specs}
        pending = []
        for name, spec in specs.items():
            c = spec["contract"]
            if name == "DAS":
                fn = c.functions.beginMigration(spec["source"], spec["destination"], asset, spec["recipient"], 10, spec["payload"])
            elif name == "TELEPORT_ABSTRACT":
                fn = c.functions.beginTeleport(spec["source"], spec["destination"], asset, spec["recipient"], 10, spec["payload"])
            else:
                fn = c.functions.exportAtomic(spec["source"], spec["destination"], asset, spec["recipient"], 10, spec["payload"])
            pending.append(send(w3, fn, spec["sender"], name + ".begin"))
        begin_receipts = receipts_concurrently(w3, pending)
        ids = {}
        measurements = {name: {"variant": name, "seed": seed, "phases": []} for name in specs}
        for name, spec in specs.items():
            rc = begin_receipts[name + ".begin"][1]
            if rc.status != 1:
                raise RuntimeError(name + " begin failed")
            event = spec["contract"].events.MigrationStarted if name == "DAS" else (
                spec["contract"].events.TeleportStarted if name == "TELEPORT_ABSTRACT" else spec["contract"].events.AtomicExportLocked
            )
            field = "receiptId" if name != "ATOMIC_UTXO_ABSTRACT" else "utxoId"
            ids[name] = event_id(rc, event, field)
            measurements[name]["phases"].append({
                "phase": "begin",
                "gas_used": rc.gasUsed,
                "block_number": rc.blockNumber,
                "seconds_to_receipt": round(begin_receipts[name + ".begin"][2] - starts[name], 4),
            })

        pending = []
        for name, spec in specs.items():
            c = spec["contract"]
            if name == "DAS":
                fn = c.functions.settleMigration(ids[name], attestation(attestor, ids[name]))
            elif name == "TELEPORT_ABSTRACT":
                fn = c.functions.settleTeleport(ids[name], spec["destination"], asset, spec["sender"], spec["recipient"], 10, spec["payload"])
            else:
                fn = c.functions.importAtomic(ids[name], spec["destination"], asset, spec["sender"], spec["recipient"], 10, spec["payload"])
            pending.append(send(w3, fn, relay_accounts[name], name + ".settle"))
        settle_receipts = receipts_concurrently(w3, pending)
        for name in specs:
            rc = settle_receipts[name + ".settle"][1]
            if rc.status != 1:
                raise RuntimeError(name + " settlement failed")
            measurements[name]["phases"].append({
                "phase": "settle",
                "gas_used": rc.gasUsed,
                "block_number": rc.blockNumber,
                "seconds_to_receipt": round(settle_receipts[name + ".settle"][2] - settle_receipts[name + ".settle"][0], 4),
            })

        pending = []
        for name, spec in specs.items():
            fn = spec["contract"].functions.executeBusiness(spec["destination"], asset, 1, f"business-seed-{seed}".encode())
            pending.append(send(w3, fn, spec["recipient"], name + ".business"))
        business_receipts = receipts_concurrently(w3, pending)

        for name, spec in specs.items():
            rc = business_receipts[name + ".business"][1]
            if rc.status != 1:
                raise RuntimeError(name + " business action failed")
            measurements[name]["phases"].append({
                "phase": "business",
                "gas_used": rc.gasUsed,
                "block_number": rc.blockNumber,
                "seconds_to_receipt": round(business_receipts[name + ".business"][2] - business_receipts[name + ".business"][0], 4),
            })
            c = spec["contract"]
            measurements[name]["completed"] = True
            measurements[name]["journey_seconds"] = round(business_receipts[name + ".business"][2] - starts[name], 4)
            measurements[name]["protocol_gas_total"] = sum(x["gas_used"] for x in measurements[name]["phases"])
            if name == "DAS":
                dest_balance = migration.functions.balanceOf(spec["destination"], asset, spec["recipient"]).call()
                invariant = migration.functions.checkInvariant(asset).call()
                pending_amount = migration.functions.pendingByAsset(asset).call()
            else:
                dest_balance = comparator.functions.balanceOf(spec["destination"], asset, spec["recipient"]).call()
                invariant = comparator.functions.checkInvariant(asset).call()
                pending_amount = comparator.functions.pendingByAsset(asset).call()
            measurements[name]["destination_balance_after_business"] = dest_balance
            measurements[name]["pending_after_business"] = pending_amount
            measurements[name]["accounting_invariant"] = invariant
            if dest_balance != 9 or pending_amount != 0 or not invariant:
                raise AssertionError(name + " post-business invariant failed")

        trial_finished = max(business_receipts[name + ".business"][2] for name in specs)
        return {
            "seed": seed,
            "block_time_seconds": BLOCK_TIME,
            "chain_id": w3.eth.chain_id,
            "trial_wall_seconds": round(trial_finished - trial_started, 4),
            "completed_journeys": sum(1 for item in measurements.values() if item["completed"]),
            "failed_journeys": 0,
            "incomplete_journeys": 0,
            # All three models share this one trial wall-clock window. Keep
            # these rates explicitly aggregate; model-level rates are computed
            # below across the independent repeats.
            "aggregate_completed_journeys_per_second": round(3 / (trial_finished - trial_started), 6),
            "aggregate_transfer_protocol_transactions_per_second": round(6 / (trial_finished - trial_started), 6),
            "aggregate_business_transactions_per_second": round(3 / (trial_finished - trial_started), 6),
            "aggregate_all_transactions_per_second": round(9 / (trial_finished - trial_started), 6),
            "setup_gas": {"migration_deploy": migration_deploy_gas, "comparator_deploy": comparator_deploy_gas, "funding": setup_rows, "teleport_trust": trust_receipt.gasUsed},
            "measurements": list(measurements.values()),
            "final_block_number": w3.eth.block_number,
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def main():
    sys.path.insert(0, str(ROOT))
    import config_global

    artifacts = compile_artifacts()
    anvil_version = subprocess.run([os.environ.get("ANVIL_BIN", shutil.which("anvil") or "anvil"), "--version"], capture_output=True, text=True).stdout.splitlines()[0]
    runs = [run_repeat(seed, artifacts, config_global.MNEMONIC) for seed in REPEATS]
    summaries = {}
    for variant in ["DAS", "TELEPORT_ABSTRACT", "ATOMIC_UTXO_ABSTRACT"]:
        measurements = [
            measurement
            for run in runs
            for measurement in run["measurements"]
            if measurement["variant"] == variant and measurement["completed"]
        ]
        values = sorted(measurement["journey_seconds"] for measurement in measurements)
        observed_seconds = sum(values)
        summaries[variant] = {
            "n_completed": len(values),
            "observed_journey_seconds_sum": round(observed_seconds, 4),
            "completed_journeys_per_second": round(len(values) / observed_seconds, 6) if observed_seconds else 0,
            "transfer_protocol_transactions_completed": 2 * len(values),
            "transfer_protocol_transactions_per_second": round(2 * len(values) / observed_seconds, 6) if observed_seconds else 0,
            "business_transactions_completed": len(values),
            "business_transactions_per_second": round(len(values) / observed_seconds, 6) if observed_seconds else 0,
            "all_transactions_per_second": round(3 * len(values) / observed_seconds, 6) if observed_seconds else 0,
            "p50_seconds": values[(len(values) - 1) // 2] if values else None,
            "p95_seconds_nearest_rank": values[max(0, int((0.95 * len(values) + 0.999999)) - 1)] if values else None,
            "p99_seconds_nearest_rank": values[max(0, int((0.99 * len(values) + 0.999999)) - 1)] if values else None,
            "p95_p99_caution": "n=3 only; nearest-rank percentiles are descriptive, not stable tail estimates",
        }
    metadata = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_kind": "12-second-block same-EVM state-machine microbenchmark",
        "base_git_commit": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "source_sha256": source_hashes(),
        "python": sys.version.split()[0],
        "packages": {name: importlib.metadata.version(name) for name in ["web3", "py-solc-x", "urllib3", "pandas", "tqdm"]},
        "solc": SOLC_VERSION,
        "anvil": anvil_version,
        "repeat_seeds": REPEATS,
        "repeat_count": len(REPEATS),
        "latency_summary": summaries,
        "same_evm_domain_labels": True,
        "limitations": [
            "all labeled domains execute inside one Anvil EVM; no network transport or separate-client isolation",
            "XCM and Avalanche are local abstractions with their own modeled safeguards, not production clients",
            "n=3 descriptive microbenchmark only; no claim of statistical equivalence or production throughput",
            "journey completes only after both settlement and a dependent business spend succeed",
        ],
    }
    result = {"metadata": metadata, "runs": runs}
    outdir = ROOT / "experiments" / "revision_20261027"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "12s_state_machine_raw.json").write_text(json.dumps(result, indent=2) + "\n")

    summary = outdir / "12s_state_machine_summary.csv"
    with summary.open("w") as f:
        f.write("seed,variant,completed,journey_seconds,protocol_gas_total,begin_gas,settle_gas,business_gas,pending_after_business,accounting_invariant\n")
        for run in runs:
            for item in run["measurements"]:
                gas = {p["phase"]: p["gas_used"] for p in item["phases"]}
                f.write(
                    f"{run['seed']},{item['variant']},{item['completed']},{item['journey_seconds']},"
                    f"{item['protocol_gas_total']},{gas['begin']},{gas['settle']},{gas['business']},"
                    f"{item['pending_after_business']},{item['accounting_invariant']}\n"
                )
    print(json.dumps({"metadata": metadata, "summary_file": str(summary), "runs": runs}, indent=2))


if __name__ == "__main__":
    main()
