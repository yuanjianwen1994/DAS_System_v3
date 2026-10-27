"""Short-block functional checks for receipt safety and business-credit gating.

This checks local Solidity state-machine invariants. It does not measure production
cross-chain throughput or reproduce Polkadot/Avalanche execution semantics.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import hashlib
import importlib.metadata
from datetime import datetime, timezone
from pathlib import Path

import solcx
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
SOLC_VERSION = "0.8.24"
BLOCK_TIME_SECONDS = 1


def require_revert(call, sender: str, label: str) -> None:
    try:
        call.call({"from": sender, "gas": 1_000_000})
    except Exception:
        return
    raise AssertionError(f"expected rejection: {label}")


def start_anvil(mnemonic: str):
    binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    if not binary:
        raise RuntimeError("anvil was not found; set ANVIL_BIN or add it to PATH")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    cmd = [
        binary,
        "--silent",
        "--accounts",
        "10",
        "--mnemonic",
        mnemonic,
        "--chain-id",
        "31337",
        "--port",
        str(port),
        "--block-time",
        str(BLOCK_TIME_SECONDS),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}", request_kwargs={"timeout": 2}))
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


def compile_contracts():
    installed = {str(version) for version in solcx.get_installed_solc_versions()}
    if SOLC_VERSION not in installed:
        solcx.install_solc(SOLC_VERSION)
    solcx.set_solc_version(SOLC_VERSION)
    compiled = solcx.compile_files(
        [
            str(ROOT / "contracts" / "MigrationHarness.sol"),
            str(ROOT / "contracts" / "ComparatorHarness.sol"),
            str(ROOT / "contracts" / "AdmissionAblationHarness.sol"),
        ],
        output_values=["abi", "bin"],
        solc_version=SOLC_VERSION,
    )
    artifacts = {}
    for key, value in compiled.items():
        artifacts[key.rsplit(":", 1)[-1]] = value
    return artifacts


def experiment_metadata(w3, mnemonic):
    anvil_bin = os.environ.get("ANVIL_BIN") or shutil.which("anvil") or "anvil"
    version = subprocess.run([anvil_bin, "--version"], capture_output=True, text=True).stdout.splitlines()
    paths = [
        "contracts/MigrationHarness.sol",
        "contracts/ComparatorHarness.sol",
        "contracts/AdmissionAblationHarness.sol",
        "tests/migration_state_machine_smoke.py",
    ]
    hashes = {
        path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        for path in paths
    }
    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "base_git_commit": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "source_sha256": hashes,
        "python": sys.version.split()[0],
        "packages": {name: importlib.metadata.version(name) for name in ["web3", "py-solc-x", "urllib3"]},
        "anvil": version[0] if version else "unknown",
        "chain_id": w3.eth.chain_id,
        "block_time_seconds": BLOCK_TIME_SECONDS,
        "solc_version": SOLC_VERSION,
    }


def deploy(w3, artifact, sender, args=()):
    factory = w3.eth.contract(abi=artifact["abi"], bytecode=artifact["bin"])
    tx_hash = factory.constructor(*args).transact({"from": sender, "gas": 5_000_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    assert receipt.status == 1, "contract deployment failed"
    return w3.eth.contract(address=receipt.contractAddress, abi=artifact["abi"]), receipt.gasUsed


def transact(w3, fn, sender: str, label: str, gas_rows: list[dict]):
    started = time.monotonic()
    tx_hash = fn.transact({"from": sender, "gas": 1_500_000})
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
    elapsed = time.monotonic() - started
    assert receipt.status == 1, f"transaction failed: {label}"
    gas_rows.append(
        {
            "label": label,
            "gas_used": receipt.gasUsed,
            "block_number": receipt.blockNumber,
            "receipt_wait_seconds": round(elapsed, 4),
        }
    )
    return receipt


def receipt_id_from(receipt, event, field="receiptId"):
    entries = event().process_receipt(receipt)
    assert len(entries) == 1
    return entries[0]["args"][field]


def sign_receipt(account, receipt_id) -> bytes:
    message = encode_defunct(primitive=bytes(receipt_id))
    return bytes(Account.sign_message(message, account.key).signature)


def run_migration_harness(w3, accounts, mnemonic, artifact, gas_rows):
    Account.enable_unaudited_hdwallet_features()
    attestor = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/1")
    sender = accounts[2]
    recipient = accounts[3]
    attacker = Account.from_mnemonic(mnemonic, account_path="m/44'/60'/0'/0/4")
    assert Web3.to_checksum_address(accounts[1]) == attestor.address

    contract, deployment_gas = deploy(w3, artifact, accounts[0], (attestor.address,))
    gas_rows.append({"label": "migration.deploy", "gas_used": deployment_gas})
    source = Web3.keccak(text="source-A")
    destination = Web3.keccak(text="destination-B")
    asset = Web3.keccak(text="asset-demo")
    payload_a = b"order=1001;action=settle"
    payload_b = b"order=1002;action=settle"

    transact(w3, contract.functions.fund(source, asset, sender, 100), accounts[0], "migration.fund", gas_rows)
    assert contract.functions.checkInvariant(asset).call()
    require_revert(
        contract.functions.executeBusiness(destination, asset, 1, b"premature"),
        recipient,
        "business use before destination credit",
    )
    unknown = Web3.keccak(text="never-burned")
    require_revert(
        contract.functions.settleMigration(unknown, sign_receipt(attestor, unknown)),
        accounts[4],
        "settlement without source debit",
    )

    r_a = transact(
        w3,
        contract.functions.beginMigration(source, destination, asset, recipient, 30, payload_a),
        sender,
        "migration.begin-A",
        gas_rows,
    )
    r_b = transact(
        w3,
        contract.functions.beginMigration(source, destination, asset, recipient, 20, payload_b),
        sender,
        "migration.begin-B",
        gas_rows,
    )
    id_a = receipt_id_from(r_a, contract.events.MigrationStarted)
    id_b = receipt_id_from(r_b, contract.events.MigrationStarted)
    assert contract.functions.balanceOf(source, asset, sender).call() == 50
    assert contract.functions.balanceOf(destination, asset, recipient).call() == 0
    assert contract.functions.pendingByAsset(asset).call() == 50
    assert contract.functions.checkInvariant(asset).call()
    require_revert(
        contract.functions.executeBusiness(destination, asset, 1, b"still-pending"),
        recipient,
        "pending funds cannot be spent",
    )
    require_revert(
        contract.functions.settleMigration(id_a, sign_receipt(attacker, id_a)),
        accounts[4],
        "untrusted attestor",
    )
    changed_payload_id = Web3.keccak(text="tampered-payload-envelope")
    require_revert(
        contract.functions.settleMigration(id_a, sign_receipt(attestor, changed_payload_id)),
        accounts[4],
        "attestation over changed payload envelope",
    )

    # Leave A pending while B settles to exercise delay and out-of-order delivery.
    transact(
        w3,
        contract.functions.settleMigration(id_b, sign_receipt(attestor, id_b)),
        accounts[4],
        "migration.settle-B-before-A",
        gas_rows,
    )
    assert contract.functions.balanceOf(destination, asset, recipient).call() == 20
    assert contract.functions.pendingByAsset(asset).call() == 30
    assert contract.functions.checkInvariant(asset).call()
    transact(
        w3,
        contract.functions.settleMigration(id_a, sign_receipt(attestor, id_a)),
        accounts[4],
        "migration.settle-A",
        gas_rows,
    )
    require_revert(
        contract.functions.settleMigration(id_a, sign_receipt(attestor, id_a)),
        accounts[4],
        "duplicate receipt replay",
    )
    require_revert(
        contract.functions.settleMigration(id_b, sign_receipt(attestor, id_b)),
        accounts[4],
        "duplicate receipt replay B",
    )
    assert contract.functions.balanceOf(destination, asset, recipient).call() == 50
    assert contract.functions.pendingByAsset(asset).call() == 0
    transact(
        w3,
        contract.functions.executeBusiness(destination, asset, 15, b"order-use"),
        recipient,
        "migration.business-spend",
        gas_rows,
    )
    assert contract.functions.balanceOf(destination, asset, recipient).call() == 35
    assert contract.functions.spentByAsset(asset).call() == 15
    assert contract.functions.fundedByAsset(asset).call() == 100
    assert contract.functions.checkInvariant(asset).call()
    return {"funded": 100, "source_balance": 50, "destination_balance": 35, "pending": 0, "spent": 15}


def run_comparator_harness(w3, accounts, artifact, gas_rows):
    contract, deployment_gas = deploy(w3, artifact, accounts[0])
    gas_rows.append({"label": "comparator.deploy", "gas_used": deployment_gas})
    source = Web3.keccak(text="teleport-source")
    destination = Web3.keccak(text="teleport-destination")
    other_destination = Web3.keccak(text="wrong-destination")
    asset = Web3.keccak(text="asset-demo")
    sender = accounts[2]
    recipient = accounts[3]
    payload_a = b"teleport=1;beneficiary=recipient"
    payload_b = b"teleport=2;beneficiary=recipient"

    transact(w3, contract.functions.fund(source, asset, sender, 100), accounts[0], "comparator.fund", gas_rows)
    transact(
        w3,
        contract.functions.setTrustedTeleportSource(destination, source, True),
        accounts[0],
        "teleport.configure-trust",
        gas_rows,
    )
    r_a = transact(
        w3,
        contract.functions.beginTeleport(source, destination, asset, recipient, 30, payload_a),
        sender,
        "teleport.begin-A",
        gas_rows,
    )
    r_b = transact(
        w3,
        contract.functions.beginTeleport(source, destination, asset, recipient, 20, payload_b),
        sender,
        "teleport.begin-B",
        gas_rows,
    )
    id_a = receipt_id_from(r_a, contract.events.TeleportStarted)
    id_b = receipt_id_from(r_b, contract.events.TeleportStarted)
    require_revert(
        contract.functions.executeBusiness(destination, asset, 1, b"pending"),
        recipient,
        "teleport pending funds",
    )
    require_revert(
        contract.functions.settleTeleport(id_a, other_destination, asset, sender, recipient, 30, payload_a),
        accounts[4],
        "teleport wrong destination",
    )
    require_revert(
        contract.functions.settleTeleport(id_a, destination, asset, sender, recipient, 30, b"tampered"),
        accounts[4],
        "teleport altered payload",
    )
    transact(
        w3,
        contract.functions.settleTeleport(id_b, destination, asset, sender, recipient, 20, payload_b),
        accounts[4],
        "teleport.settle-B-before-A",
        gas_rows,
    )
    assert contract.functions.pendingByAsset(asset).call() == 30
    assert contract.functions.checkInvariant(asset).call()
    transact(
        w3,
        contract.functions.settleTeleport(id_a, destination, asset, sender, recipient, 30, payload_a),
        accounts[4],
        "teleport.settle-A",
        gas_rows,
    )
    require_revert(
        contract.functions.settleTeleport(id_a, destination, asset, sender, recipient, 30, payload_a),
        accounts[4],
        "teleport replay",
    )
    require_revert(
        contract.functions.settleTeleport(id_b, other_destination, asset, sender, recipient, 20, payload_b),
        accounts[4],
        "teleport replay B",
    )
    transact(
        w3,
        contract.functions.executeBusiness(destination, asset, 10, b"teleport-business"),
        recipient,
        "teleport.business-spend",
        gas_rows,
    )
    assert contract.functions.checkInvariant(asset).call()

    atomic_source = Web3.keccak(text="atomic-source")
    atomic_destination = Web3.keccak(text="atomic-destination")
    atomic_wrong_destination = Web3.keccak(text="atomic-wrong-destination")
    transact(w3, contract.functions.fund(atomic_source, asset, sender, 100), accounts[0], "atomic.fund", gas_rows)
    r_atomic = transact(
        w3,
        contract.functions.exportAtomic(atomic_source, atomic_destination, asset, recipient, 25, b"utxo-payload"),
        sender,
        "atomic.export",
        gas_rows,
    )
    utxo_id = receipt_id_from(r_atomic, contract.events.AtomicExportLocked, "utxoId")
    require_revert(
        contract.functions.importAtomic(utxo_id, atomic_wrong_destination, asset, sender, recipient, 25, b"utxo-payload"),
        accounts[4],
        "atomic wrong destination",
    )
    require_revert(
        contract.functions.importAtomic(utxo_id, atomic_destination, asset, sender, recipient, 25, b"tampered"),
        accounts[4],
        "atomic altered payload",
    )
    require_revert(
        contract.functions.importAtomic(Web3.keccak(text="unknown-utxo"), atomic_destination, asset, sender, recipient, 25, b"utxo-payload"),
        accounts[4],
        "atomic import without export",
    )
    assert contract.functions.pendingByAsset(asset).call() == 25
    transact(
        w3,
        contract.functions.importAtomic(utxo_id, atomic_destination, asset, sender, recipient, 25, b"utxo-payload"),
        accounts[4],
        "atomic.import",
        gas_rows,
    )
    require_revert(
        contract.functions.importAtomic(utxo_id, atomic_destination, asset, sender, recipient, 25, b"utxo-payload"),
        accounts[4],
        "atomic UTXO replay",
    )
    transact(
        w3,
        contract.functions.executeBusiness(atomic_destination, asset, 5, b"atomic-business"),
        recipient,
        "atomic.business-spend",
        gas_rows,
    )
    assert contract.functions.checkInvariant(asset).call()
    return {
        "funded": contract.functions.fundedByAsset(asset).call(),
        "pending": contract.functions.pendingByAsset(asset).call(),
        "spent": contract.functions.spentByAsset(asset).call(),
        "invariant": contract.functions.checkInvariant(asset).call(),
    }


def run_ablation_harnesses(w3, accounts, artifact, gas_rows):
    source = Web3.keccak(text="ablation-source")
    destination = Web3.keccak(text="ablation-destination")
    wrong_destination = Web3.keccak(text="ablation-wrong-destination")
    asset = Web3.keccak(text="asset-ablation")
    sender = accounts[2]
    recipient = accounts[3]
    attacker = accounts[4]
    payload = b"ablation-payload"
    results = {}

    # RemovedRule.SourceAuthorization = 1: a caller can debit another sender.
    source_auth, _ = deploy(w3, artifact, accounts[0], (1,))
    transact(w3, source_auth.functions.fund(source, asset, sender, 10), accounts[0], "ablation.auth.fund", gas_rows)
    r = transact(
        w3,
        source_auth.functions.beginMigration(source, destination, asset, sender, recipient, 5, payload),
        attacker,
        "ablation.auth.unauthorized-source-debit",
        gas_rows,
    )
    auth_id = receipt_id_from(r, source_auth.events.MigrationStarted)
    assert source_auth.functions.balanceOf(source, asset, sender).call() == 5
    assert source_auth.functions.admissionViolationsByAsset(asset).call() == 1
    assert source_auth.functions.checkAccountingConservation(asset).call()
    assert not source_auth.functions.checkInvariant(asset).call()
    results["source_authorization_removed"] = {
        "victim_balance_after_unauthorized_debit": source_auth.functions.balanceOf(source, asset, sender).call(),
        "receipt_created": auth_id.hex(),
        "accounting_conserved": source_auth.functions.checkAccountingConservation(asset).call(),
        "combined_invariant": source_auth.functions.checkInvariant(asset).call(),
    }

    # RemovedRule.DestinationPayloadBinding = 2: redirect a valid pending receipt.
    binding, _ = deploy(w3, artifact, accounts[0], (2,))
    transact(w3, binding.functions.fund(source, asset, sender, 10), accounts[0], "ablation.binding.fund", gas_rows)
    r = transact(
        w3,
        binding.functions.beginMigration(source, destination, asset, sender, recipient, 5, payload),
        sender,
        "ablation.binding.begin",
        gas_rows,
    )
    binding_id = receipt_id_from(r, binding.events.MigrationStarted)
    transact(
        w3,
        binding.functions.settleMigration(binding_id, source, wrong_destination, attacker, b"tampered-payload"),
        accounts[4],
        "ablation.binding.redirect-and-tamper",
        gas_rows,
    )
    assert binding.functions.balanceOf(wrong_destination, asset, attacker).call() == 5
    assert binding.functions.checkAccountingConservation(asset).call()
    assert not binding.functions.checkInvariant(asset).call()
    transact(
        w3,
        binding.functions.executeBusiness(wrong_destination, asset, 2, b"ablation-business"),
        attacker,
        "ablation.binding.spend-redirected-credit",
        gas_rows,
    )
    assert not binding.functions.checkInvariant(asset).call()
    results["destination_payload_binding_removed"] = {
        "redirected_credit": 5,
        "attacker_spendable_after_business_action": binding.functions.balanceOf(wrong_destination, asset, attacker).call(),
        "accounting_conserved": binding.functions.checkAccountingConservation(asset).call(),
        "combined_invariant": binding.functions.checkInvariant(asset).call(),
    }

    # RemovedRule.ReplayConsumption = 3: one pending receipt can credit twice.
    replay, _ = deploy(w3, artifact, accounts[0], (3,))
    transact(w3, replay.functions.fund(source, asset, sender, 10), accounts[0], "ablation.replay.fund", gas_rows)
    r = transact(
        w3,
        replay.functions.beginMigration(source, destination, asset, sender, recipient, 5, payload),
        sender,
        "ablation.replay.begin",
        gas_rows,
    )
    replay_id = receipt_id_from(r, replay.events.MigrationStarted)
    for label in ["first", "duplicate"]:
        transact(
            w3,
            replay.functions.settleMigration(replay_id, source, destination, recipient, payload),
            accounts[4],
            "ablation.replay." + label,
            gas_rows,
        )
    assert replay.functions.balanceOf(destination, asset, recipient).call() == 10
    assert not replay.functions.checkAccountingConservation(asset).call()
    results["replay_consumption_removed"] = {
        "credited_total": replay.functions.balanceOf(destination, asset, recipient).call(),
        "accounting_conserved": replay.functions.checkAccountingConservation(asset).call(),
        "combined_invariant": replay.functions.checkInvariant(asset).call(),
    }

    # RemovedRule.SourceDebit = 4: unbacked mint is spendable and breaks conservation.
    source_debit, _ = deploy(w3, artifact, accounts[0], (4,))
    transact(
        w3,
        source_debit.functions.mintWithoutSourceDebit(destination, asset, recipient, 5),
        attacker,
        "ablation.source-debit.unbacked-mint",
        gas_rows,
    )
    assert source_debit.functions.balanceOf(destination, asset, recipient).call() == 5
    assert not source_debit.functions.checkAccountingConservation(asset).call()
    transact(
        w3,
        source_debit.functions.executeBusiness(destination, asset, 1, b"unbacked-business"),
        recipient,
        "ablation.source-debit.business-spend-unbacked",
        gas_rows,
    )
    assert source_debit.functions.balanceOf(destination, asset, recipient).call() == 4
    assert not source_debit.functions.checkInvariant(asset).call()
    results["source_debit_removed"] = {
        "unbacked_credit_spendable": 5,
        "remaining_after_business_action": source_debit.functions.balanceOf(destination, asset, recipient).call(),
        "accounting_conserved": source_debit.functions.checkAccountingConservation(asset).call(),
        "combined_invariant": source_debit.functions.checkInvariant(asset).call(),
    }
    return results


def main() -> None:
    sys.path.insert(0, str(ROOT))
    import config_global

    artifacts = compile_contracts()
    proc, w3 = start_anvil(config_global.MNEMONIC)
    gas_rows = []
    try:
        accounts = w3.eth.accounts
        migration = run_migration_harness(w3, accounts, config_global.MNEMONIC, artifacts["MigrationHarness"], gas_rows)
        comparator = run_comparator_harness(w3, accounts, artifacts["ComparatorHarness"], gas_rows)
        ablations = run_ablation_harnesses(w3, accounts, artifacts["AdmissionAblationHarness"], gas_rows)
        result = {
            "kind": "functional_smoke_only",
            "metadata": experiment_metadata(w3, config_global.MNEMONIC),
            "migration_state": migration,
            "comparator_state": comparator,
            "ablation_findings": ablations,
            "gas_samples": gas_rows,
            "limitations": [
                "same-EVM domains are labels, not separate clients",
                "trusted ECDSA attestor models authentication, not production finality",
                "short block time validates behavior only and is not a performance result",
            ],
        }
        output = ROOT / "experiments" / "revision_20261027" / "smoke_results.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


if __name__ == "__main__":
    main()
