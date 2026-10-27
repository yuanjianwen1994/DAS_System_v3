"""Check every receipt identity field against two independent destination contexts."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from eth_account import Account
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from cross_chain_load_experiment import compile_contracts, deploy, parse_export, sign_digest, start_anvil, stop_anvil


def reject(fn, sender, label):
    try:
        fn.call({"from": sender})
    except Exception:
        return label
    raise AssertionError("accepted tampered envelope field: " + label)


def main():
    import config_global

    Account.enable_unaudited_hdwallet_features()
    attestor = Account.from_mnemonic(config_global.MNEMONIC, account_path="m/44'/60'/0'/0/20")
    compiled = compile_contracts()
    src_artifact = compiled["CrossChainSourceHarness"]
    dst_artifact = compiled["CrossChainDestinationHarness"]
    p_src, w3s = start_anvil(31337, config_global.MNEMONIC)
    p_dst, p_wrong = None, None
    try:
        p_dst, w3d = start_anvil(31338, config_global.MNEMONIC)
        p_wrong, w3wrong = start_anvil(31339, config_global.MNEMONIC)
        admin, sender = w3s.eth.accounts[0], w3s.eth.accounts[1]
        recipient = w3d.eth.accounts[2]
        source_domain = Web3.keccak(text="binding-audit-source")
        destination_domain = Web3.keccak(text="binding-audit-destination")
        asset = Web3.keccak(text="binding-audit-asset")
        payload_hash = Web3.keccak(text="binding-audit-payload")
        source, _ = deploy(w3s, src_artifact, admin, (attestor.address,))
        destination, _ = deploy(w3d, dst_artifact, w3d.eth.accounts[0], (attestor.address, 0, destination_domain))
        decoy, _ = deploy(w3d, dst_artifact, w3d.eth.accounts[0], (attestor.address, 0, destination_domain))
        wrong_chain, _ = deploy(w3wrong, dst_artifact, w3wrong.eth.accounts[0], (attestor.address, 0, destination_domain))
        fund_tx = source.functions.fund(asset, sender, 20).transact({"from": admin, "gas": 200_000})
        if w3s.eth.wait_for_transaction_receipt(fund_tx, timeout=30).status != 1:
            raise RuntimeError("source funding failed")
        export_tx = source.functions.beginExport(
            31338, destination.address, source_domain, destination_domain,
            asset, recipient, 10, payload_hash,
        ).transact({"from": sender, "gas": 1_500_000})
        export_receipt = w3s.eth.wait_for_transaction_receipt(export_tx, timeout=30)
        envelope = parse_export(source, export_receipt)
        signature = sign_digest(attestor, envelope["receiptId"])
        args = [
            envelope["receiptId"], envelope["sourceChainId"], envelope["sourceEndpoint"],
            envelope["sourceDomain"], envelope["destinationDomain"], envelope["asset"],
            envelope["amount"], envelope["sender"], envelope["recipient"],
            envelope["payloadHash"], envelope["nonce"], signature,
        ]
        mutations = {
            "source_chain_id": (1, envelope["sourceChainId"] + 2),
            "source_contract": (2, w3s.eth.accounts[2]),
            "source_domain": (3, Web3.keccak(text="tampered-source-domain")),
            "destination_domain": (4, Web3.keccak(text="tampered-destination-domain")),
            "asset": (5, Web3.keccak(text="tampered-asset")),
            "amount": (6, envelope["amount"] + 1),
            "sender": (7, w3s.eth.accounts[3]),
            "recipient": (8, w3d.eth.accounts[3]),
            "payload_hash": (9, Web3.keccak(text="tampered-payload")),
            "nonce": (10, envelope["nonce"] + 1),
            "signature": (11, bytes([0]) * 65),
        }
        rejected = []
        for label, (index, value) in mutations.items():
            changed = list(args)
            changed[index] = value
            rejected.append(reject(destination.functions.importReceipt(*changed), sender, label))

        # Destination chain ID and endpoint are implicit execution context in
        # the contract; exercise them by replaying the same receipt on decoy
        # contracts at another address and on a node with another chain ID.
        rejected.append(reject(decoy.functions.importReceipt(*args), sender, "destination_endpoint"))
        rejected.append(reject(wrong_chain.functions.importReceipt(*args), sender, "destination_chain_id"))

        good_tx = destination.functions.importReceipt(*args).transact({"from": sender, "gas": 1_500_000})
        good_receipt = w3d.eth.wait_for_transaction_receipt(good_tx, timeout=30)
        if good_receipt.status != 1 or not destination.functions.checkInvariant(asset).call():
            raise AssertionError("positive control receipt import failed")
        reject(destination.functions.importReceipt(*args), sender, "replay")
        output = {
            "kind": "two-chain-full-envelope-binding-audit",
            "source_chain_id": 31337,
            "destination_chain_id": 31338,
            "fields_tested": list(mutations) + ["destination_chain_id", "destination_endpoint", "replay"],
            "tampered_fields_rejected": len(rejected) + 1,
            "positive_control_imported": True,
            "destination_invariant": True,
            "scope": "local EVM contracts and test attestor only; not independent validator finality",
        }
        out = ROOT / "experiments" / "revision_20261027" / "cross_chain_binding_audit.json"
        out.write_text(json.dumps(output, indent=2) + "\n")
        print(json.dumps(output, indent=2))
    finally:
        stop_anvil(p_src)
        if p_dst is not None:
            stop_anvil(p_dst)
        if p_wrong is not None:
            stop_anvil(p_wrong)


if __name__ == "__main__":
    main()
