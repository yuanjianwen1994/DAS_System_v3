"""Measured controlled-EVM path screen; no production-platform speed claims."""
import argparse, csv, hashlib, json, os, socket, subprocess, time
from pathlib import Path
import solcx
from web3 import Web3

ROOT = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
ap.add_argument('--block-seconds', type=int, default=1)
ap.add_argument('--depths', default='1,4,8')
ap.add_argument('--rounds', type=int, default=2)
ap.add_argument('--repeats', type=int, default=3)
args = ap.parse_args()
src = (ROOT/'WorkflowScreen.sol').read_text()
compiled = solcx.compile_standard({'language':'Solidity','sources':{'W.sol':{'content':src}},'settings':{'optimizer':{'enabled':True,'runs':200},'outputSelection':{'*':{'*':['abi','evm.bytecode.object']}}}},solc_version='0.8.24')['contracts']['W.sol']

def deploy(w3, name, params=()):
    c=compiled[name]
    f=w3.eth.contract(abi=c['abi'],bytecode=c['evm']['bytecode']['object'])
    r=w3.eth.wait_for_transaction_receipt(f.constructor(*params).transact({'from':w3.eth.accounts[0],'gas':20_000_000}))
    assert r.status==1
    return w3.eth.contract(address=r.contractAddress,abi=c['abi'])

rows=[]
raw=[]
for depth in map(int,args.depths.split(',')):
 for repeat in range(args.repeats):
    s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
    proc=subprocess.Popen([os.environ.get('ANVIL_BIN','/private/tmp/foundry-1.8.6/anvil'),'--silent','--port',str(port),'--gas-limit','30000000'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
      w3=Web3(Web3.HTTPProvider('http://127.0.0.1:'+str(port)))
      for _ in range(100):
        if w3.is_connected():break
        time.sleep(.1)
      # Four local controls share the same execution locality. Differences among
      # those names are intentionally absent; platform migration isn't modeled.
      names=['DAS_LOCAL','AVALANCHE_LOCAL_CONTROL','TELEPORT_LOCAL_CONTROL','ASYNC_RECEIPT_MODEL']
      adapters={n:deploy(w3,'BoundaryAdapter') for n in names}
      flows={n:deploy(w3,'Workflow',(adapters[n].address,depth)) for n in names}
      session=Web3.keccak(text=f'depth={depth};repeat={repeat}')
      for n in names:w3.eth.wait_for_transaction_receipt(adapters[n].functions.fund(session).transact({'from':w3.eth.accounts[0]}))
      result=w3.provider.make_request('anvil_setIntervalMining',[args.block_seconds]);assert 'error' not in result,result
      gas={n:0 for n in names}; txs={n:0 for n in names};start={};end={};blocks={}
      def phase(functions,label):
        submitted=[]
        for n,fn in functions:
          now=time.monotonic();start.setdefault(n,now)
          h=fn.transact({'from':w3.eth.accounts[0],'gas':5_000_000})
          submitted.append((n,h,now))
        for n,h,t in submitted:
          r=w3.eth.wait_for_transaction_receipt(h,timeout=max(90,args.block_seconds*4),poll_latency=.05)
          assert r.status==1,(n,label)
          gas[n]+=r.gasUsed;txs[n]+=1;end[n]=time.monotonic()
          blocks.setdefault(n,[]).append(r.blockNumber)
          raw.append({'depth':depth,'repeat':repeat,'model':n,'phase':label,'hash':h.hex(),'block':r.blockNumber,'gas':r.gasUsed,'observed_seconds':end[n]-t})
      phase([(n,adapters[n].functions.burnIn(session)) for n in names],'burn_in')
      phase([(n,adapters[n].functions.mintIn(session)) for n in names],'mint_in')
      # Each group advances independently: local controls withdraw after their
      # own work completes, without waiting for the longer asynchronous model.
      for step in range(depth*args.rounds+2):
        fs=[]
        for n in names:
          work_steps=depth*args.rounds if n=='ASYNC_RECEIPT_MODEL' else args.rounds
          if step<work_steps:
            fn=flows[n].functions.asyncStage(session) if n=='ASYNC_RECEIPT_MODEL' else flows[n].functions.local(session)
          elif step==work_steps:fn=adapters[n].functions.burnOut(session)
          elif step==work_steps+1:fn=adapters[n].functions.mintOut(session)
          else:continue
          fs.append((n,fn))
        phase(fs,'work_or_exit_'+str(step))
      outputs={n:flows[n].functions.state(session).call().hex() for n in names}
      assert len(set(outputs.values()))==1,outputs
      for n in names:
        a=adapters[n]
        assert a.functions.settlement(session).call()==100-args.rounds
        assert a.functions.execution(session).call()==0
        assert a.functions.pending(session).call()==0
        assert a.functions.spent(session).call()==args.rounds
        assert flows[n].functions.completed(session).call()==args.rounds
        row={'depth':depth,'repeat':repeat,'model':n,'rounds':args.rounds,'block_seconds':args.block_seconds,'transactions':txs[n],'first_last_block_span':blocks[n][-1]-blocks[n][0],'journey_seconds':end[n]-start[n],'gas':gas[n],'final_state':outputs[n],'invariants_passed':True}
        rows.append(row)
      print(json.dumps({'depth':depth,'repeat':repeat,'rows':rows[-4:]}),flush=True)
      (ROOT/f'raw_{args.block_seconds}s.json').write_text(json.dumps({'kind':'controlled_EVM_execution_path_screen_NOT_native_NEAR','source_sha256':hashlib.sha256(src.encode()).hexdigest(),'config':vars(args),'rows':rows,'transactions':raw},indent=2))
    finally:
      proc.terminate()
      try:proc.wait(timeout=5)
      except subprocess.TimeoutExpired:proc.kill();proc.wait()
with (ROOT/f'summary_{args.block_seconds}s.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
