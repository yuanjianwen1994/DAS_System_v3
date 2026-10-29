#!/usr/bin/env python3
"""Validate and summarize paired randomized d8 runs by independent seed."""
import csv, hashlib, html, json, math
from pathlib import Path

ROOT=Path(__file__).resolve().parent
PILOT=ROOT/'pilot_d8_seed20261028'
RUNS=[(20261028,1,PILOT,PILOT/'poisson_d8_pilot_seed20261028.json','pilot'),
      (20261029,2,ROOT/'formal_seed20261029',ROOT/'formal_seed20261029'/'poisson_d8_seed20261029.json','extended'),
      (20261030,3,ROOT/'formal_seed20261030',ROOT/'formal_seed20261030'/'poisson_d8_seed20261030.json','concurrent_exploratory'),
      (20261031,4,ROOT/'formal_seed20261031',ROOT/'formal_seed20261031'/'poisson_d8_seed20261031.json','exclusive_2_13_4'),
      (20261032,5,ROOT/'formal_seed20261032'/'results',ROOT/'formal_seed20261032'/'input'/'poisson_d8_seed20261032.json','exclusive_2_13_4_audit_limited')]
samples=[]; per_run=[]; trace_rows=[]; ratio_rows=[]; run_objects=[]

def pct(values,q):
    values=sorted(values)
    return values[max(0,math.ceil(q*len(values))-1)] if values else None

def latency(row,platform,metric):
    key=('offered_to_sink_ms' if metric=='offer' else 'submit_to_sink_ms') if platform=='EVM' else ('offered_to_sink_queryable_ms' if metric=='offer' else 'call_to_sink_queryable_ms')
    return float(row[key]) if row.get(key) is not None else None

for seed,repeat,folder,schedule_path,kind in RUNS:
    schedule_text=schedule_path.read_text(); schedule=json.loads(schedule_text); scenario=schedule['scenarios']['poisson_d8']
    ep=json.loads(next((folder/'evm').glob('*_raw.json')).read_text()); np=json.loads(next((folder/'near_corrected').glob('*_raw.json')).read_text())
    es=json.loads(next((folder/'evm').glob('*_results.json')).read_text()); ns=json.loads(next((folder/'near_corrected').glob('*_results.json')).read_text())
    assert int((folder/'evm'/'exit_code.txt').read_text())==0 and int((folder/'near_corrected'/'exit_code.txt').read_text())==0
    sha=hashlib.sha256(schedule_text.encode()).hexdigest(); assert ep['schedule_sha256']==np['schedule_sha256']==sha
    run_objects.append((seed,repeat,kind,schedule,ep,np,es,ns))
    for ephase,nphase,esum,nsum in zip(ep['phases'],np['phases'],es['phases'],ns['phases']):
        rate=float(ephase['offered_rate_per_sec']); offsets=[float(x) for x in scenario['arrival_offsets_sec_by_rate'][str(rate)]]
        assert [float(x) for x in ephase['planned_arrival_offsets_sec']]==offsets
        assert [float(x) for x in nphase['planned_arrival_offsets_sec']]==offsets
        trace=f"{rate:g}|{seed}|{float(scenario['phase_random_offset_sec']):.12f}|"+','.join(f"{x:.9f}" for x in offsets)
        digest=hashlib.sha256(trace.encode()).hexdigest(); assert nphase['arrival_trace_sha256']==digest
        erows={str(r['workflow_id']):r for r in ephase['rows']}; nrows={str(r['workflow_id']):r for r in nphase['rows']}
        assert set(erows)==set(nrows)
        trace_rows.append({'seed':seed,'repeat':repeat,'rate_workflows_per_sec':rate,'schedule_sha256':sha,'arrival_trace_sha256':digest,'planned_offsets_match':True,'workflow_id_sets_match':True,'offers_per_platform':len(erows)})
        for platform,phase,summary,rowsmap in [('EVM',ephase,esum,erows),('NEAR',nphase,nsum,nrows)]:
            ordered=sorted(rowsmap,key=lambda x:int(x)); rows=[rowsmap[x] for x in ordered]
            heights=[int(x['height'] if x.get('height') is not None else x['block_number']) for x in (summary.get('block_usage_samples') or []) if x.get('height') is not None or x.get('block_number') is not None]
            start=phase['observation_start_at']; end=phase['measurement_window_end_at']
            measured=[r for r in rows if not r.get('warmup')]
            within=[r for r in measured if r.get('succeeded') and start<=r.get('sink_first_queryable_at','')<=end]
            for i,(wid,row) in enumerate(zip(ordered,rows),1):
                samples.append({'seed':seed,'repeat':repeat,'run_kind':kind,'rate_workflows_per_sec':rate,'platform':platform,'arrival_order':i,
                    'workflow_id':wid,'warmup':row.get('warmup'),'planned_at':row.get('planned_at'),'offered_at':row.get('offered_at'),
                    'sink_first_queryable_at':row.get('sink_first_queryable_at'),'offer_to_sink_ms':latency(row,platform,'offer'),
                    'submit_to_sink_ms':latency(row,platform,'submit'),'schedule_lag_ms':row.get('generator_schedule_lag_ms'),
                    'sink_poll_bracket_ms':row.get('sink_poll_detection_bracket_ms'),'succeeded':row.get('succeeded'),'failure':row.get('failure'),
                    'audit_pending':row.get('audit_pending')})
            for metric in ('offer','submit'):
                allvals=[latency(r,platform,metric) for r in measured]; allvals=[x for x in allvals if x is not None]
                winvals=[latency(r,platform,metric) for r in within]; winvals=[x for x in winvals if x is not None]
                per_run.append({'seed':seed,'repeat':repeat,'run_kind':kind,'rate_workflows_per_sec':rate,'platform':platform,
                    'metric':'offer_to_sink_ms' if metric=='offer' else 'submit_to_sink_ms','offered_total':len(rows),
                    'warmup_offers':sum(bool(r.get('warmup')) for r in rows),'measured_offers':len(measured),
                    'completed_in_window_n':len(winvals),'window_p50_ms':pct(winvals,.5),'window_p95_ms':pct(winvals,.95),
                    'window_max_ms':max(winvals) if winvals else None,'measured_p50_ms':pct(allvals,.5),'measured_p95_ms':pct(allvals,.95),
                    'successes':sum(bool(r.get('succeeded')) for r in rows),'failures':sum(bool(r.get('failure')) for r in rows),
                    'audit_pending':sum(bool(r.get('audit_pending')) for r in rows),'planned_interarrival_cv':phase.get('planned_interarrival_cv'),
                    'block_audit_status':summary.get('audit_status'),'block_audit_pending_count':summary.get('audit_pending_count',0),
                    'schedule_lag_p95_ms':pct([float(r['generator_schedule_lag_ms']) for r in rows if r.get('generator_schedule_lag_ms') is not None],.95),
                    'sampling_bound_ms':phase.get('receipt_sampling_error_bound_ms') if platform=='EVM' else max((r.get('sink_poll_detection_bracket_ms') or 0 for r in rows),default=None),
                    'block_start':min(heights) if heights else summary.get('phase_start_block'),
                    'block_end':max(heights) if heights else summary.get('phase_end_block'),
                    'block_samples':len(summary.get('block_usage_samples') or [])})

for seed,repeat,kind,_,_,_,_,_ in run_objects:
    for rate in (.5,1.0):
        e=next(x for x in per_run if x['seed']==seed and x['rate_workflows_per_sec']==rate and x['platform']=='EVM' and x['metric']=='offer_to_sink_ms')
        n=next(x for x in per_run if x['seed']==seed and x['rate_workflows_per_sec']==rate and x['platform']=='NEAR' and x['metric']=='offer_to_sink_ms')
        ratio_rows.append({'seed':seed,'repeat':repeat,'run_kind':kind,'rate_workflows_per_sec':rate,'evm_window_p50_ms':e['window_p50_ms'],'near_window_p50_ms':n['window_p50_ms'],
            'p50_ratio_near_over_evm':n['window_p50_ms']/e['window_p50_ms'],'evm_window_p95_ms':e['window_p95_ms'],'near_window_p95_ms':n['window_p95_ms'],
            'p95_ratio_near_over_evm':n['window_p95_ms']/e['window_p95_ms'],'evm_n':e['completed_in_window_n'],'near_n':n['completed_in_window_n']})

def write_csv(path,rows):
    with path.open('w',newline='') as f: w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
write_csv(ROOT/'randomized_d8_workflow_samples.csv',samples)
write_csv(ROOT/'randomized_d8_per_seed_summary.csv',per_run)
write_csv(ROOT/'randomized_d8_trace_validation.csv',trace_rows)
write_csv(ROOT/'randomized_d8_per_seed_ratios.csv',ratio_rows)

colors={'EVM':'#2764ae','NEAR':'#df7622'}
def txt(x,y,t,size=12,anchor='middle',fill='#222',weight='normal'):
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}">{html.escape(str(t))}</text>'

def scatter(zoom=False):
    W,H=1220,560; svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">','<rect width="100%" height="100%" fill="white"/>']
    svg += [txt(W/2,26,'d8 randomized arrivals: workflow latency by arrival order',19,weight='bold'),txt(W/2,48,'color: platform · marker: seed · one point per workflow · no added jitter · '+('zoom 0–2 s' if zoom else 'full scale 0–12 s'),11,fill='#555')]
    maxorders={}
    for row in samples:
        if not row['warmup']:
            key=(row['seed'],row['rate_workflows_per_sec'],row['platform'])
            maxorders[key]=max(maxorders.get(key,0),row['arrival_order'])
    for j,rate in enumerate((.5,1.0)):
        x0=70+j*575; xL=x0+48; xR=x0+550; yT=92; yB=462; ymax=2 if zoom else 12
        svg.append(f'<rect x="{x0}" y="70" width="550" height="410" fill="white" stroke="#aaa"/>'); svg.append(txt(x0+275,88,f'λ = {rate:g} workflow/s',13,weight='bold'))
        for k in range(5):
            val=ymax*k/4; yy=yB-val/ymax*(yB-yT); svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>'); svg.append(txt(xL-7,yy+4,f'{val:.1f}',10,'end','#555'))
        for row in samples:
            if row['rate_workflows_per_sec']!=rate or row['warmup'] or row['offer_to_sink_ms'] is None: continue
            sec=row['offer_to_sink_ms']/1000
            if sec>ymax: continue
            maxorder=maxorders.get((row['seed'],rate,row['platform']),1); xx=xL+(row['arrival_order']-1)/max(1,maxorder-1)*(xR-xL); yy=yB-sec/ymax*(yB-yT)
            if row['seed']==20261028:
                svg.append(f'<circle cx="{xx:.2f}" cy="{yy:.2f}" r="2.5" fill="{colors[row["platform"]]}" fill-opacity=".7"/>')
            elif row['seed']==20261029:
                svg.append(f'<path d="M{xx:.2f},{yy-2.8:.2f} L{xx+2.7:.2f},{yy+2:.2f} L{xx-2.7:.2f},{yy+2:.2f} Z" fill="{colors[row["platform"]]}" fill-opacity=".7"/>')
            else:
                svg.append(f'<path d="M{xx:.2f},{yy-2.8:.2f} L{xx+2.8:.2f},{yy:.2f} L{xx:.2f},{yy+2.8:.2f} L{xx-2.8:.2f},{yy:.2f} Z" fill="{colors[row["platform"]]}" fill-opacity=".7"/>')
        svg.append(txt((xL+xR)/2,yB+22,'workflow arrival order',10,fill='#555'))
    for i,p in enumerate(('EVM','NEAR')):
        x=450+i*150;svg.append(f'<circle cx="{x}" cy="524" r="4" fill="{colors[p]}"/>');svg.append(txt(x+12,528,p,11,'start'))
    for i,seed in enumerate((20261028,20261029,20261030,20261031,20261032)):
        x=760+i*110; svg.append(f'<circle cx="{x}" cy="524" r="3" fill="#555"/>'); svg.append(txt(x+8,528,str(seed)[-2:],10,'start'))
    svg.append('</svg>');(ROOT/('randomized_d8_scatter_zoom.svg' if zoom else 'randomized_d8_scatter_full.svg')).write_text('\n'.join(svg))

def ecdf(zoom=False):
    W,H=1220,560; svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">','<rect width="100%" height="100%" fill="white"/>']
    svg += [txt(W/2,26,'d8 randomized arrivals: workflow latency ECDF',19,weight='bold'),txt(W/2,48,'non-warmup arrivals · colors: platform · line styles: independent seed · '+('zoom 0–2 s' if zoom else 'full scale 0–12 s'),11,fill='#555')]
    for j,rate in enumerate((.5,1.0)):
        x0=70+j*575; xL=x0+48; xR=x0+550; yT=92; yB=462; xmax=2 if zoom else 12
        svg.append(f'<rect x="{x0}" y="70" width="550" height="410" fill="white" stroke="#aaa"/>');svg.append(txt(x0+275,88,f'λ = {rate:g} workflow/s',13,weight='bold'))
        for k in range(5):
            p=k/4;yy=yB-p*(yB-yT);xv=xmax*k/4;xx=xL+xv/xmax*(xR-xL);svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>');svg.append(txt(xL-7,yy+4,f'{p:.2f}',10,'end','#555'));svg.append(txt(xx,yB+17,f'{xv:.1f}',9,fill='#555'))
        for seed,_,_,_,_ in RUNS:
            for platform in ('EVM','NEAR'):
                group=sorted(r['offer_to_sink_ms']/1000 for r in samples if r['seed']==seed and r['rate_workflows_per_sec']==rate and r['platform']==platform and not r['warmup'] and r['offer_to_sink_ms'] is not None)
                denom=len(group); clipped=[v for v in group if v<=xmax]
                if not clipped: continue
                pts=[(xL+v/xmax*(xR-xL),yB-(i/denom)*(yB-yT)) for i,v in enumerate(clipped,1)]
                d=f'M{pts[0][0]:.2f},{pts[0][1]:.2f}'
                for a,b in zip(pts,pts[1:]):d+=f' H{b[0]:.2f} V{b[1]:.2f}'
                d+=f' H{xR}';dash=' stroke-dasharray="5,3"' if seed==20261028 else (' stroke-dasharray="2,2"' if seed==20261030 else '')
                svg.append(f'<path d="{d}" fill="none" stroke="{colors[platform]}" stroke-width="1.5"{dash}/>')
    for i,p in enumerate(('EVM','NEAR')):
        x=430+i*150;svg.append(f'<path d="M{x-8},524 H{x+8}" stroke="{colors[p]}" stroke-width="2"/>');svg.append(txt(x+14,528,p,11,'start'))
    svg.append('</svg>');(ROOT/('randomized_d8_ecdf_zoom.svg' if zoom else 'randomized_d8_ecdf_full.svg')).write_text('\n'.join(svg))

scatter(False);scatter(True);ecdf(False);ecdf(True)

def repeat_ranges():
    W,H=1040,500; svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">','<rect width="100%" height="100%" fill="white"/>']
    svg += [txt(W/2,26,'d8 only: observed NEAR/EVM end-to-end quotients',19,weight='bold'),txt(W/2,48,'descriptive ratios of audited-success percentiles for this workload; do not scale by contract count',11,fill='#555')]
    colors_m={'p50':'#2764ae','p95':'#df7622'}
    for panel,(metric,label) in enumerate((('p50_ratio_near_over_evm','median ratio'),('p95_ratio_near_over_evm','p95 ratio'))):
        x0=70+panel*500; xL=x0+58; xR=x0+460; yT=90; yB=420; ymax=25
        svg.append(f'<rect x="{x0}" y="70" width="470" height="380" fill="white" stroke="#aaa"/>');svg.append(txt(x0+235,88,label,13,weight='bold'))
        for k in range(6):
            v=ymax*k/5; yy=yB-v/ymax*(yB-yT);svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>');svg.append(txt(xL-8,yy+4,f'{v:.0f}×',10,'end','#555'))
        for j,rate in enumerate((.5,1.0)):
            xx=xL+105+j*190; svg.append(txt(xx,yB+25,f'λ={rate:g}',11))
            formal=[r[metric] for r in ratio_rows if r['rate_workflows_per_sec']==rate and r['run_kind']=='extended']
            if formal:
                lo,hi=min(formal),max(formal); y1=yB-hi/ymax*(yB-yT); y2=yB-lo/ymax*(yB-yT)
                svg.append(f'<path d="M{xx},{y1:.2f} V{y2:.2f}" stroke="{colors_m["p50" if metric.startswith("p50") else "p95"]}" stroke-width="5" stroke-linecap="round" opacity=".45"/>')
                for r in ratio_rows:
                    if r['rate_workflows_per_sec']==rate and r['run_kind']=='extended':
                        yy=yB-r[metric]/ymax*(yB-yT); svg.append(f'<circle cx="{xx}" cy="{yy:.2f}" r="5" fill="{colors_m["p50" if metric.startswith("p50") else "p95"]}" stroke="white"/>');svg.append(txt(xx+14,yy+3,str(r['seed'])[-2:],9,'start','#555'))
            for pilot in (r for r in ratio_rows if r['rate_workflows_per_sec']==rate and r['run_kind']=='pilot'):
                yy=yB-pilot[metric]/ymax*(yB-yT);svg.append(f'<circle cx="{xx}" cy="{yy:.2f}" r="6" fill="white" stroke="{colors_m["p50" if metric.startswith("p50") else "p95"]}" stroke-width="2"/>')
                svg.append(txt(xx+15,yy+3,'pilot',9,'start','#555'))
    svg.append('</svg>');(ROOT/'randomized_d8_repeat_ratio_ranges.svg').write_text('\n'.join(svg))

repeat_ranges()
print(f'validated {len(trace_rows)} paired per-seed traces; {len(samples)} workflow rows; {len(ratio_rows)} seed-level ratios')
