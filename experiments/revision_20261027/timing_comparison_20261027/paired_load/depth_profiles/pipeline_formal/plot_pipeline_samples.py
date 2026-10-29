#!/usr/bin/env python3
"""Plot all primary per-workflow d8/d20 latency samples without synthetic jitter."""
import csv, html, json, math
from datetime import datetime
from pathlib import Path

ROOT=Path(__file__).resolve().parent
RUNS=[]
COLORS={1:'#2764ae',2:'#e07a24',3:'#2c8c62'}
MARKERS={1:'circle',2:'triangle',3:'square'}
ROUTES=[('fused_single_contract','EVM fused'),('independent_stage','EVM independent stages'),('same_receiver','NEAR same receiver'),('cross_contract','NEAR cross-contract')]

def ts(v): return datetime.fromisoformat(v.replace('Z','+00:00')).timestamp() if v else None
for depth in (8,20):
    for repeat in (1,2,3):
        candidates=[
            (ROOT/'control_matrix'/f'd{depth}_r{repeat}_evm_domain_control', 'evm'),
            (ROOT/'control_matrix'/f'd{depth}_r{repeat}_near_domain_control', 'near'),
            (ROOT/f'd{depth}_r{repeat}_evm_independent', 'evm'),
            (ROOT/f'd{depth}_r{repeat}_near_independent', 'near'),
        ]
        for folder,platform in candidates:
            files=list(folder.glob('*_raw.json'))
            if not files: continue
            data=json.loads(files[0].read_text())
            if platform=='evm': route='fused_single_contract' if 'domain_control' in folder.name else 'independent_stage'
            else: route='same_receiver' if 'domain_control' in folder.name else 'cross_contract'
            phase=next((x for x in data['phases'] if float(x.get('offered_rate_per_sec',0))==1.0),None)
            if phase is None: continue
            allrows=sorted(phase.get('rows',[]),key=lambda x:ts(x.get('offered_at')) or 0)
            start,end=ts(phase.get('observation_start_at')),ts(phase.get('measurement_window_end_at'))
            window=[r for r in allrows if r.get('succeeded') and r.get('sink_first_queryable_at') and start<=ts(r['sink_first_queryable_at'])<=end]
            rp=json.loads(files[0].read_text())
            polling=(phase.get('receipt_sampling_error_bound_ms') or phase.get('receipt_sampling_bound_ms') or phase.get('sink_poll_interval_ms'))
            if polling is None and platform=='near': polling='sink_read_after_receipt_tree'
            RUNS.append({'depth':depth,'repeat':repeat,'platform':data.get('platform'),'route':route,'data':data,'phase':phase,'rows':allrows,'window':window,'polling':polling,'source':files[0]})

def val(row, field):
    x=row.get(field)
    if x is not None: return float(x)/1000
    if field=='offered_to_sink_ms' and row.get('offered_to_sink_queryable_ms') is not None: return float(row['offered_to_sink_queryable_ms'])/1000
    if field=='submit_to_sink_ms' and row.get('call_to_sink_queryable_ms') is not None: return float(row['call_to_sink_queryable_ms'])/1000
    return None

samples=[]
summary=[]
for run in RUNS:
    phase=run['phase']; rows=run['rows']; successful=sum(bool(r.get('succeeded')) for r in rows)
    for i,row in enumerate(rows,1):
        samples.append({'depth':run['depth'],'repeat':run['repeat'],'platform':run['platform'],'route':run['route'],
            'rate_workflows_per_sec':1.0,'arrival_order':i,'workflow_id':row.get('workflow_id'),
            'warmup':row.get('warmup'),'offered_at':row.get('offered_at'),'tx_submit_started_at':row.get('tx_submit_started_at',row.get('call_issued_at')),
            'sink_first_queryable_at':row.get('sink_first_queryable_at'),'offered_to_sink_sec':val(row,'offered_to_sink_ms'),
            'submit_to_sink_sec':val(row,'submit_to_sink_ms'),'succeeded':row.get('succeeded'),'failure':row.get('failure'),
            'receipt_status':row.get('receipt_status',row.get('transaction_outcome_status'))})
    for metric in ('offered_to_sink_ms','submit_to_sink_ms'):
        vals=sorted(x for x in (val(r,metric) for r in run['window']) if x is not None)
        p95=vals[max(0,math.ceil(.95*len(vals))-1)] if vals else None
        summary.append({'depth':run['depth'],'repeat':run['repeat'],'platform':run['platform'],'route':run['route'],
            'offered_rate_workflows_per_sec':1.0,'observed_rows':len(rows),'successful_rows':successful,'failed_rows':len(rows)-successful,
            'p95_completion_window_n':len(vals),'latency_metric':metric,'p95_sec':p95,
            'sampling_bound_or_method':run['polling'],'source_file':str(run['source'].relative_to(ROOT))})

with (ROOT/'pipeline_latency_samples.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(samples[0]));w.writeheader();w.writerows(samples)
with (ROOT/'pipeline_sample_p95_and_resolution.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)

def txt(x,y,s,size=12,anchor='middle',fill='#222',weight='normal'):
    return f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" font-family="sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}">{html.escape(str(s))}</text>'
def marker(x,y,kind,color):
    if kind=='circle': return f'<circle cx="{x:.2f}" cy="{y:.2f}" r="2.5" fill="{color}" fill-opacity=".72"/>'
    if kind=='triangle': return f'<path d="M{x:.2f},{y-3:.2f} L{x+3:.2f},{y+2.5:.2f} L{x-3:.2f},{y+2.5:.2f} Z" fill="{color}" fill-opacity=".76"/>'
    return f'<rect x="{x-2.4:.2f}" y="{y-2.4:.2f}" width="4.8" height="4.8" fill="{color}" fill-opacity=".78"/>'

def base_svg(title,subtitle):
    W,H=1760,620
    return W,H,[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">','<rect width="100%" height="100%" fill="white"/>',txt(W/2,28,title,20,weight='bold'),txt(W/2,50,subtitle,12,fill='#555')]

def make_scatter(zoom=False):
    title='Per-workflow offer-to-sink samples: arrival order (1 workflow/s)'
    subtitle=('Primary runs only; all 90 workflow offers per repeat are retained; colors encode repeat; no x/y jitter.' if not zoom else
      'Local y-axis zoom per panel to expose observed variation; panel scales differ. All 90 samples/repeat; no synthetic jitter.')
    W,H,svg=base_svg(title,subtitle)
    left,top,pw,ph,gx,gy=72,88,390,210,28,48
    for drow,depth in enumerate((8,20)):
      for col,(route,label) in enumerate(ROUTES):
        x0=left+col*(pw+gx); y0=top+drow*(ph+gy)
        subset=[r for r in RUNS if r['depth']==depth and r['route']==route]
        points=[(r, i, val(row,'offered_to_sink_ms')) for r in subset for i,row in enumerate(r['rows'],1) if val(row,'offered_to_sink_ms') is not None]
        vals=[v for _,_,v in points]
        if not vals: continue
        if zoom:
            span=max(vals)-min(vals); pad=max(.015,span*.15); ymin=max(0,min(vals)-pad); ymax=max(vals)+pad
        else: ymin,ymax=0,25
        xL,xR=x0+42,x0+pw-10; yT,yB=y0+12,y0+ph-28
        svg.append(f'<rect x="{x0}" y="{y0}" width="{pw}" height="{ph}" fill="white" stroke="#aaa"/>')
        svg.append(txt(x0+pw/2,y0+13,f'd{depth} · {label}',12,weight='bold'))
        for j in range(5):
            v=ymin+(ymax-ymin)*j/4; yy=yB-(v-ymin)/(ymax-ymin)*(yB-yT)
            svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"/>')
            svg.append(txt(xL-6,yy+4,f'{v:.2f}',9,'end','#555'))
        for r in subset:
            for i,row in enumerate(r['rows'],1):
                v=val(row,'offered_to_sink_ms')
                if v is None: continue
                xx=xL+(i-1)/89*(xR-xL); yy=yB-(v-ymin)/(ymax-ymin)*(yB-yT)
                svg.append(marker(xx,yy,MARKERS[r['repeat']],COLORS[r['repeat']]))
        svg.append(txt((xL+xR)/2,yB+17,'workflow arrival order',9,fill='#555'))
        svg.append(txt(x0+11,(yT+yB)/2,'s',9,fill='#555'))
    for i,rep in enumerate((1,2,3)):
        x=665+i*120; svg.append(marker(x,582,MARKERS[rep],COLORS[rep]));svg.append(txt(x+25,586,f'r{rep}',11,'start'))
    svg.append('</svg>')
    (ROOT/('pipeline_latency_scatter_zoom.svg' if zoom else 'pipeline_latency_scatter_full.svg')).write_text('\n'.join(svg))

def make_ecdf(zoom=False):
    title='Per-workflow offer-to-sink ECDF (1 workflow/s)'
    subtitle=('Full latency scale (seconds); primary three repeats, 90 offers per repeat.' if not zoom else
      'Local x-axis zoom per panel; axis ranges differ. Primary three repeats, all observed samples, no added jitter.')
    W,H,svg=base_svg(title,subtitle)
    left,top,pw,ph,gx,gy=72,88,390,210,28,48
    for drow,depth in enumerate((8,20)):
      for col,(route,label) in enumerate(ROUTES):
        x0=left+col*(pw+gx); y0=top+drow*(ph+gy)
        subset=[r for r in RUNS if r['depth']==depth and r['route']==route]
        vals_by_rep={r['repeat']:sorted(v for v in (val(row,'offered_to_sink_ms') for row in r['rows']) if v is not None) for r in subset}
        vals=[v for xs in vals_by_rep.values() for v in xs]
        if not vals: continue
        if zoom:
            span=max(vals)-min(vals); pad=max(.015,span*.15); xmin=max(0,min(vals)-pad); xmax=max(vals)+pad
        else: xmin,xmax=0,25
        xL,xR=x0+42,x0+pw-10; yT,yB=y0+12,y0+ph-28
        svg.append(f'<rect x="{x0}" y="{y0}" width="{pw}" height="{ph}" fill="white" stroke="#aaa"/>')
        svg.append(txt(x0+pw/2,y0+13,f'd{depth} · {label}',12,weight='bold'))
        for j in range(5):
            p=j/4; yy=yB-p*(yB-yT); svg.append(f'<path d="M{xL},{yy:.1f} H{xR}" stroke="#e5e5e5"');svg[-1]+='/>'
            svg.append(txt(xL-6,yy+4,f'{p:.2f}',9,'end','#555'))
        for rep,vs in vals_by_rep.items():
            if not vs: continue
            pts=[]
            for i,v in enumerate(vs,1):
                xx=xL+(v-xmin)/(xmax-xmin)*(xR-xL); yy=yB-(i/len(vs))*(yB-yT); pts.append((xx,yy))
            path='M'+f'{pts[0][0]:.2f},{pts[0][1]:.2f}'
            for a,b in zip(pts,pts[1:]): path+=f' H{b[0]:.2f} V{b[1]:.2f}'
            path+=f' H{xR}'
            svg.append(f'<path d="{path}" fill="none" stroke="{COLORS[rep]}" stroke-width="1.7"/>')
        svg.append(txt((xL+xR)/2,yB+17,'offer-to-sink latency (s)',9,fill='#555'))
    for i,rep in enumerate((1,2,3)):
        x=665+i*120; svg.append(f'<path d="M{x-8},582 H{x+8}" stroke="{COLORS[rep]}" stroke-width="2"/>');svg.append(txt(x+25,586,f'r{rep}',11,'start'))
    svg.append('</svg>')
    (ROOT/('pipeline_latency_ecdf_zoom.svg' if zoom else 'pipeline_latency_ecdf_full.svg')).write_text('\n'.join(svg))

for zoom in (False,True): make_scatter(zoom); make_ecdf(zoom)
print(f'wrote {len(samples)} per-workflow samples, {len(summary)} run/metric rows from {len(RUNS)} primary runs')
