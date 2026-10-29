"""Small gas-only sweep for migration count and post-migration business calls.

Domains are labels in one EVM. This is a call-amortization diagnostic, not a
multi-shard throughput or hotspot/settlement-load experiment.
"""
from __future__ import annotations

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
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from migration_state_machine_smoke import compile_contracts, deploy, receipt_id_from, sign_receipt


def start_fast_anvil(mnemonic):
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
    w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}", request_kwargs={"timeout": 2}))
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("anvil exited before startup")
        if w3.is_connected():
            return proc, w3
        time.sleep(0.1)
    proc.terminate()
    proc.wait(timeout=5)
    raise TimeoutError("anvil did not become ready")


def receipt(w3, fn, sender, label):
    tx_hash = fn.transact({"from": sender, "gas": 1_500_000})
    r = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=15)
    if r.status != 1:
        raise AssertionError(label + " failed")
    return r


def main():
    import config_global

    Account.enable_unaudited_hdwallet_features()
    attestor = Account.from_mnemonic(config_global.MNEMONIC, account_path="m/44'/60'/0'/0/1")
    artifacts = compile_contracts()
    proc, w3 = start_fast_anvil(config_global.MNEMONIC)
    rows = []
    try:
        accounts = w3.eth.accounts
        das, _ = deploy(w3, artifacts["MigrationHarness"], accounts[0], (attestor.address,))
        comparator, _ = deploy(w3, artifacts["ComparatorHarness"], accounts[0])
        modes = ["DAS", "TELEPORT_ABSTRACT", "ATOMIC_UTXO_ABSTRACT"]
        profiles = [(1, 1), (1, 5), (5, 1)]  # transfers, business calls per workflow
        sender_by_mode = {"DAS": accounts[2], "TELEPORT_ABSTRACT": accounts[3], "ATOMIC_UTXO_ABSTRACT": accounts[4]}
        recipient_by_mode = {"DAS": accounts[5], "TELEPORT_ABSTRACT": accounts[6], "ATOMIC_UTXO_ABSTRACT": accounts[7]}
        contract_by_mode = {"DAS": das, "TELEPORT_ABSTRACT": comparator, "ATOMIC_UTXO_ABSTRACT": comparator}

        for mode in modes:
            c = contract_by_mode[mode]
            sender = sender_by_mode[mode]
            recipient = recipient_by_mode[mode]
            asset = Web3.keccak(text="applicability-asset-" + mode)
            for transfers, theta in profiles:
                tag = f"{mode}-m{transfers}-theta{theta}"
                source = Web3.keccak(text=tag + "-source")
                destination = Web3.keccak(text=tag + "-destination")
                total_transfer = max(transfers, theta)
                amount_each = total_transfer // transfers
                payloads = [f"{tag}-receipt-{i}".encode() for i in range(transfers)]
                receipt(w3, c.functions.fund(source, asset, sender, 100), accounts[0], tag + ".fund")
                setup_gas = 0
                if mode == "TELEPORT_ABSTRACT":
                    trust = receipt(w3, c.functions.setTrustedTeleportSource(destination, source, True), accounts[0], tag + ".trust")
                    setup_gas = trust.gasUsed

                phase_gas = {"begin": [], "settle": [], "business": []}
                receipt_ids = []
                for i in range(transfers):
                    if mode == "DAS":
                        fn = c.functions.beginMigration(source, destination, asset, recipient, amount_each, payloads[i])
                        r = receipt(w3, fn, sender, tag + ".begin")
                        rid = receipt_id_from(r, c.events.MigrationStarted)
                    elif mode == "TELEPORT_ABSTRACT":
                        fn = c.functions.beginTeleport(source, destination, asset, recipient, amount_each, payloads[i])
                        r = receipt(w3, fn, sender, tag + ".begin")
                        rid = receipt_id_from(r, c.events.TeleportStarted)
                    else:
                        fn = c.functions.exportAtomic(source, destination, asset, recipient, amount_each, payloads[i])
                        r = receipt(w3, fn, sender, tag + ".export")
                        rid = receipt_id_from(r, c.events.AtomicExportLocked, "utxoId")
                    receipt_ids.append(rid)
                    phase_gas["begin"].append(r.gasUsed)

                for i, rid in enumerate(receipt_ids):
                    if mode == "DAS":
                        fn = c.functions.settleMigration(rid, sign_receipt(attestor, rid))
                    elif mode == "TELEPORT_ABSTRACT":
                        fn = c.functions.settleTeleport(rid, destination, asset, sender, recipient, amount_each, payloads[i])
                    else:
                        fn = c.functions.importAtomic(rid, destination, asset, sender, recipient, amount_each, payloads[i])
                    r = receipt(w3, fn, accounts[8 + (i % 2)], tag + ".settle")
                    phase_gas["settle"].append(r.gasUsed)

                for _ in range(theta):
                    r = receipt(
                        w3,
                        c.functions.executeBusiness(destination, asset, 1, f"{tag}-business".encode()),
                        recipient,
                        tag + ".business",
                    )
                    phase_gas["business"].append(r.gasUsed)

                invariant = c.functions.checkInvariant(asset).call()
                pending = c.functions.pendingByAsset(asset).call()
                destination_balance = c.functions.balanceOf(destination, asset, recipient).call()
                if not invariant or pending != 0 or destination_balance != total_transfer - theta:
                    raise AssertionError(tag + " post-workflow invariant failed")
                protocol_gas = sum(sum(values) for values in phase_gas.values())
                rows.append({
                    "variant": mode,
                    "transfers_per_workflow": transfers,
                    "business_calls_per_workflow": theta,
                    "total_transferred": total_transfer,
                    "setup_trust_gas_excluded": setup_gas,
                    "begin_gas_total": sum(phase_gas["begin"]),
                    "settle_gas_total": sum(phase_gas["settle"]),
                    "business_gas_total": sum(phase_gas["business"]),
                    "protocol_gas_total": protocol_gas,
                    "protocol_gas_per_business_call": round(protocol_gas / theta, 2),
                    "pending_after_workflow": pending,
                    "destination_balance_after_business": destination_balance,
                    "accounting_invariant": invariant,
                })

        result = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "kind": "1-second-auto-mining-gas-only-boundary-smoke",
            "profiles": profiles,
            "rows": rows,
            "limitations": [
                "one Anvil EVM with domain labels; no separate execution/settlement nodes",
                "no background settlement saturation or hotspot queue is modeled",
                "gas-per-business-call reflects only these small contract workflows",
            ],
        }
        outdir = ROOT / "experiments" / "revision_20261027"
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "applicability_boundary_gas.json").write_text(json.dumps(result, indent=2) + "\n")
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
