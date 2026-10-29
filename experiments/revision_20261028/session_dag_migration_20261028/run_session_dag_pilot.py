#!/usr/bin/env python3
"""One-repeat d8 pilot: actual external-CALL Stage DAG under same-EVM migration ledger."""
from __future__ import annotations

import argparse, hashlib, importlib.metadata, json, os, shutil, socket, subprocess, time
from datetime import datetime, timezone
from pathlib import Path

import solcx
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
from web3.logs import DISCARD

ROOT = Path(__file__).resolve().parents[3]
EXP = Path(__file__).resolve().parent
DEV_MNEMONIC = "test test test test test test test test test test test junk"
SOLC_VERSION = "0.8.24"
THETAS = (1, 2, 4, 8)
DEFAULT_DEPTH = 8
OPS_PER_NODE = 16
COST_PER_NODE = 1
STAGE_SOURCE = ROOT / "experiments/revision_20261027/stateful_dag_20261027/dag_workflow_20261027/contracts/DASDistributedCompositionHarness.sol"
LEDGER_SOURCE = ROOT / "contracts/MigrationHarness.sol"
ROUTER_SOURCE = EXP / "contracts/IntegratedDAGSessionRouter.sol"
SRC = Web3.keccak(text="session-dag-source")
DST = Web3.keccak(text="session-dag-destination")


def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def compile_all():
    if SOLC_VERSION not in {str(v) for v in solcx.get_installed_solc_versions()}: solcx.install_solc(SOLC_VERSION)
    solcx.set_solc_version(SOLC_VERSION)
    stage_source = STAGE_SOURCE.read_text().split("contract DASDistributedCompositionHarness", 1)[0]
    sources = {
        "contracts/MigrationHarness.sol": {"content": LEDGER_SOURCE.read_text()},
        "contracts/DAGStage.sol": {"content": stage_source},
        "contracts/IntegratedDAGSessionRouter.sol": {"content": ROUTER_SOURCE.read_text()},
    }
    out = solcx.compile_standard({"language":"Solidity", "sources":sources, "settings":{"optimizer":{"enabled":True,"runs":200},"viaIR":True,"outputSelection":{"*":{"*":["abi","evm.bytecode.object"]}}}})["contracts"]
    def art(file, name): return out[file][name]
    return art("contracts/MigrationHarness.sol","MigrationHarness"), art("contracts/DAGStage.sol","DAGStage"), art("contracts/IntegratedDAGSessionRouter.sol","IntegratedDAGSessionRouter")


def start_anvil(block_time, repeat):
    binary = os.environ.get("ANVIL_BIN") or shutil.which("anvil")
    if not binary: raise RuntimeError("anvil not found on remote host")
    probe=socket.socket(); probe.bind(("127.0.0.1",0)); port=probe.getsockname()[1]; probe.close()
    proc=subprocess.Popen([binary,"--silent","--accounts","10","--mnemonic",DEV_MNEMONIC,"--chain-id",str(41337+repeat),"--port",str(port),"--block-time",str(block_time)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    w3=Web3(Web3.HTTPProvider(f"http://127.0.0.1:{port}",request_kwargs={"timeout":120}))
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        if proc.poll() is not None: raise RuntimeError(f"Anvil exited: {proc.returncode}")
        try:
            if w3.is_connected(): return proc,w3,binary
        except Exception: pass
        time.sleep(.2)
    proc.terminate(); raise TimeoutError("Anvil did not start")


def send(w3, fn, sender, gas=24_000_000):
    tx=fn.transact({"from":sender,"gas":gas})
    receipt=w3.eth.wait_for_transaction_receipt(tx,timeout=180)
    if receipt.status != 1: raise RuntimeError(f"transaction reverted: {tx.hex()}")
    return receipt


def deploy(w3, artifact, sender, args=()):
    c=w3.eth.contract(abi=artifact["abi"],bytecode=artifact["evm"]["bytecode"]["object"])
    r=send(w3,c.constructor(*args),sender)
    return w3.eth.contract(address=r.contractAddress,abi=artifact["abi"]),r


def attest_transfer(w3, ledger, router, attestor, relayer, begin_fn, expected_sender, expected_recipient, src, dst, asset, amount, payload):
    start=time.monotonic(); begin=send(w3,begin_fn,router.functions.owner().call())
    started=ledger.events.MigrationStarted().process_receipt(begin,errors=DISCARD)
    if len(started)!=1: raise AssertionError("migration begin event missing")
    a=started[0]["args"]; rid=a["receiptId"]; p=ledger.functions.pendingReceipt(rid).call()
    bound=(bytes(p[0])==bytes(src) and bytes(p[1])==bytes(dst) and bytes(p[2])==bytes(asset) and int(p[3])==amount and a["sender"].lower()==expected_sender.lower() and a["recipient"].lower()==expected_recipient.lower() and bytes(p[7])==payload and bool(p[8]) and bytes(a["payloadHash"])==Web3.keccak(payload))
    if not bound: raise AssertionError("full migration envelope binding failed")
    sig=bytes(Account.sign_message(encode_defunct(primitive=bytes(rid)),attestor.key).signature)
    settle=send(w3,ledger.functions.settleMigration(rid,sig),relayer)
    settled=ledger.events.MigrationSettled().process_receipt(settle,errors=DISCARD)
    if not ledger.functions.settled(rid).call() or ledger.functions.pendingReceipt(rid).call()[-1] or len(settled)!=1 or settled[0]["args"]["recipient"].lower()!=expected_recipient.lower() or bytes(settled[0]["args"]["payloadHash"])!=Web3.keccak(payload):
        raise AssertionError("migration settlement audit failed")
    return {"seconds":time.monotonic()-start,"receipt_id":rid.hex(),"amount":amount,"nonce":int(a["nonce"]),"sender":a["sender"],"recipient":a["recipient"],"payload_hash":bytes(a["payloadHash"]).hex(),"binding_verified":True,"begin_gas":int(begin.gasUsed),"settle_gas":int(settle.gasUsed)}


def run_session(w3, artifacts, accounts, theta, migrated, repeat, block_time, depth):
    ledger_art, stage_art, router_art=artifacts
    admin,attestor_acct,relayer=accounts[0],Account.from_mnemonic(DEV_MNEMONIC,account_path="m/44'/60'/0'/0/1"),accounts[3]
    asset=Web3.keccak(text=f"session-dag|r{repeat}|theta{theta}|migrated{migrated}")
    session_id=41337000+repeat*100+theta*2+int(migrated)
    parents=[[]]+[[i-1] for i in range(1,depth)]
    router=None; ledger=None; records=[]
    started=time.monotonic(); journey_started=None; row={"repeat":repeat,"theta":theta,"depth":depth,"operations_per_node":OPS_PER_NODE,"per_node_balance_cost":COST_PER_NODE,"arm":"migrated_dag" if migrated else "same_domain_dag","status":"running","incomplete":True,"failed":False,"failure":None,"receipt_bindings":[],"node_audits":[]}
    try:
        ledger,_=deploy(w3,ledger_art,admin,(attestor_acct.address,))
        router,_=deploy(w3,router_art,admin,(ledger.address,SRC,DST,asset,parents))
        for n in range(depth):
            st,_=deploy(w3,stage_art,admin,(router.address,n,parents[n],2**64-1,2**32-1)); records.append(st)
        send(w3,router.functions.setStages([s.address for s in records]),admin)
        total_spend=theta*depth*COST_PER_NODE; seedfund=total_spend+1
        send(w3,ledger.functions.fund(SRC,asset,router.address,seedfund),admin)
        journey_started=time.monotonic()
        entry=None
        if migrated:
            payload=Web3.keccak(text=f"entry|r{repeat}|theta{theta}")
            entry=attest_transfer(w3,ledger,router,attestor_acct,relayer,router.functions.beginEntry(session_id,seedfund,payload),router.address,router.address,SRC,DST,asset,seedfund,payload)
            row["receipt_bindings"].append({"leg":"in",**entry})
        else:
            send(w3,router.functions.enterLocal(session_id,seedfund,Web3.keccak(text="local-entry")),admin)
        entry_commit=Web3.keccak(text=f"session-entry|{session_id}")
        send(w3,router.functions.enterSession(session_id,theta,entry_commit,COST_PER_NODE,1),admin)
        t_internal=time.monotonic()
        workflow_ids=list(range(theta)); seeds=[987654321+workflow*37 for workflow in workflow_ids]
        exec_receipt=send(w3,router.functions.executeWorkflows(session_id,workflow_ids,seeds,OPS_PER_NODE,COST_PER_NODE),admin)
        internal=time.monotonic()-t_internal
        # Audit every actual per-contract result and every stored predecessor value on chain.
        for workflow,seed in zip(workflow_ids,seeds):
            vals=[]
            for node,stage in enumerate(records):
                result=stage.functions.nodeResult(session_id,workflow).call()
                if not result[0] or result[1]!=session_id or result[3]!=(0 if node==0 else 1) or result[4]!=OPS_PER_NODE: raise AssertionError("stage result metadata mismatch")
                if node:
                    present,value=router.functions.nodeInput(session_id,workflow,node,node-1).call() if hasattr(router.functions,"nodeInput") else (stage.functions.hasInput(session_id,workflow,node-1).call(),stage.functions.inputValues(session_id,workflow,node-1).call())
                    if not present or int(value)!=int(vals[node-1]): raise AssertionError("on-chain predecessor binding mismatch")
                vals.append(int(result[2]))
                row["node_audits"].append({"workflow_id":workflow,"node_id":node,"checksum":int(result[2]),"predecessor_value":int(vals[node-1]) if node else None,"bound":True})
        close=send(w3,router.functions.closeSession(session_id,Web3.keccak(text=f"session-exit|{session_id}")),admin)
        exit_leg=None
        if migrated:
            payload=Web3.keccak(text=f"exit|r{repeat}|theta{theta}")
            exit_leg=attest_transfer(w3,ledger,router,attestor_acct,relayer,router.functions.beginExit(session_id,payload),router.address,admin,DST,SRC,asset,1,payload)
            row["receipt_bindings"].append({"leg":"out",**exit_leg})
        src_router=int(ledger.functions.balanceOf(SRC,asset,router.address).call())
        dst_router=int(ledger.functions.balanceOf(DST,asset,router.address).call())
        owner_src=int(ledger.functions.balanceOf(SRC,asset,admin).call())
        pending=int(ledger.functions.pendingByAsset(asset).call()); spent=int(ledger.functions.spentByAsset(asset).call()); funded=int(ledger.functions.fundedByAsset(asset).call()); invariant=bool(ledger.functions.checkInvariant(asset).call())
        if migrated: accounting=(src_router,dst_router,owner_src,pending,spent,funded,invariant)==(0,0,1,0,total_spend,seedfund,True)
        else: accounting=(src_router,dst_router,pending,spent,funded,invariant)==(1,0,0,total_spend,seedfund,True)
        if not accounting: raise AssertionError("terminal ledger accounting mismatch")
        row.update({"status":"completed","incomplete":False,"internal_business_seconds":internal,"journey_seconds":time.monotonic()-journey_started,"entry_seconds":entry["seconds"] if entry else 0,"exit_seconds":exit_leg["seconds"] if exit_leg else 0,"internal_tx_gas":int(exec_receipt.gasUsed),"node_count_executed":theta*depth,"receipt_bindings_passed":len(row["receipt_bindings"]),"receipt_bindings_expected":2 if migrated else 0,"terminal_accounting_valid":True,"source_router_balance":src_router,"destination_router_balance":dst_router,"source_owner_balance":owner_src,"pending_balance":pending,"funded":funded,"spent":spent,"conservation_invariant":invariant,"execution_tx":exec_receipt.transactionHash.hex(),"close_tx":close.transactionHash.hex(),"anvil_block_time_seconds":block_time})
    except Exception as e:
        row.update({"status":"failed","failed":True,"incomplete":True,"failure":f"{type(e).__name__}: {e}","journey_seconds":time.monotonic()-started})
    return row


def main():
    Account.enable_unaudited_hdwallet_features()
    ap=argparse.ArgumentParser(); ap.add_argument("--repeat",type=int,default=1); ap.add_argument("--block-time",type=int,default=1); ap.add_argument("--depth",type=int,default=DEFAULT_DEPTH); ap.add_argument("--output",type=Path,default=EXP/"pilot_d8_raw.json"); args=ap.parse_args()
    if args.depth not in (8,20): raise SystemExit("pilot depth must be 8 or 20")
    artifacts=compile_all(); proc,w3,binary=start_anvil(args.block_time,args.repeat); accounts=w3.eth.accounts
    w3.provider._test_private_key=Account.from_mnemonic(DEV_MNEMONIC,account_path="m/44'/60'/0'/0/1").key
    runs=[]
    try:
        for theta in THETAS:
            for migrated in (False,True): runs.append(run_session(w3,artifacts,accounts,theta,migrated,args.repeat,args.block_time,args.depth))
    finally:
        proc.terminate()
        try: proc.wait(timeout=10)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=10)
    data={"experiment":f"same-EVM actual external-CALL d{args.depth} DAG plus controlled MigrationHarness session","model_boundary":"same_evm_two_logical_domains_trusted_attestor_no_native_bridge","repeat":args.repeat,"depth":args.depth,"block_time_seconds":args.block_time,"theta_values":list(THETAS),"operations_per_node":OPS_PER_NODE,"per_node_balance_cost":COST_PER_NODE,"parents":[[]]+[[i-1] for i in range(1,args.depth)],"workflow_pre_submission":"theta independent workflow IDs and seeds are submitted in one transaction; no inter-workflow result dependency is claimed","source_hashes":{str(p.relative_to(ROOT)):digest(p) for p in (LEDGER_SOURCE,STAGE_SOURCE,ROUTER_SOURCE)},"runtime":{"anvil":subprocess.check_output([binary,"--version"],text=True).strip(),"solc":SOLC_VERSION,"web3":importlib.metadata.version("web3"),"py_solc_x":importlib.metadata.version("py-solc-x"),"python":__import__("platform").python_version()},"runs":runs,"completed":sum(r["status"]=="completed" for r in runs),"failed":sum(r["failed"] for r in runs),"incomplete":sum(r["incomplete"] for r in runs),"created_utc":datetime.now(timezone.utc).isoformat()}
    args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(data,indent=2)+"\n")
    print(json.dumps({"output":str(args.output),"completed":data["completed"],"failed":data["failed"],"incomplete":data["incomplete"],"runs":[{"arm":r["arm"],"theta":r["theta"],"status":r["status"],"seconds":r.get("journey_seconds"),"failure":r.get("failure")} for r in runs]},indent=2))

if __name__=="__main__": main()
