#!/usr/bin/env python3
"""Generate identical seeded exponential-arrival offsets for paired EVM and NEAR runs."""
import argparse, hashlib, json, random
from pathlib import Path

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--seed',type=int,required=True)
    ap.add_argument('--warmup-sec',type=float,default=15)
    ap.add_argument('--measurement-sec',type=float,default=60)
    ap.add_argument('--experiment',default='paired-exponential-open-loop-d8-pilot')
    ap.add_argument('--output',type=Path,required=True)
    args=ap.parse_args()
    rng=random.Random(args.seed)
    phase_rng=random.Random(args.seed ^ 0x5EEDC0DE)
    phase_offset=phase_rng.random() # Uniform phase within the 1 s block period.
    rates=[0.5,1.0]
    total_duration=phase_offset+args.warmup_sec+args.measurement_sec
    # One common unit-rate exponential sequence is scaled by lambda for paired rate traces.
    unit_gaps=[]; cumulative=0.0
    while cumulative < max(rates)*total_duration:
        gap=rng.expovariate(1.0); unit_gaps.append(gap); cumulative+=gap
    offsets={}
    for rate in rates:
        ts=phase_offset; values=[]
        for gap in unit_gaps:
            if ts >= total_duration: break
            values.append(round(ts,9)); ts += gap/rate
        offsets[str(rate)]=values
    data={
      'schema_version':1,
      'experiment':args.experiment,
      'profile_id':'dag-07-chain-d8-w1-theta1-nu1',
      'node_count':8,
      'operations_per_node':16,
      'seed':args.seed,
      'block_period_sec':1.0,
      'arrival_rule':'first offer after a uniform random phase in [0,1) s; subsequent interarrival gaps are exponential with mean 1/lambda; exact offsets shared across both platforms',
      'phase_random_offset_sec':phase_offset,
      'rates_per_sec':rates,
      'warmup_sec':args.warmup_sec,
      'measurement_sec':args.measurement_sec,
      'scenarios':{'poisson_d8':{
        'scenario_index':0,
        'rates_per_sec':rates,
        'warmup_sec':args.warmup_sec,
        'measurement_sec':args.measurement_sec,
        'phase_random_offset_sec':phase_offset,
        'arrival_offsets_sec_by_rate':offsets,
        'arrival_seed':args.seed,
        'arrival_rule':'paired_exponential',
      }},
      'resource_policy':{'sender_accounts_per_platform':64,'per_sender_submission_serialization':True,'stop_when':'continuous fixed-workload generation ends; drain all offered workflows; preserve failures, pending rows, long tails and block-level spans'},
    }
    canonical=json.dumps(data,sort_keys=True,separators=(',',':')).encode()
    data['schedule_content_sha256']=hashlib.sha256(canonical).hexdigest()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(data,indent=2)+'\n')
    print(json.dumps({'output':str(args.output),'seed':args.seed,'phase_offset_sec':phase_offset,
      'counts':{rate:len(values) for rate,values in offsets.items()},'first_offsets':{rate:values[:5] for rate,values in offsets.items()}},indent=2))

if __name__=='__main__': main()
