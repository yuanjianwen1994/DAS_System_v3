"""Reanalyse retained results and plot a protocol-level state-commit-unit count.

This script starts no nodes and runs no experiment. Its input is immutable
NEAR/EVM evidence from the completed matrix. No wall-clock ratio is inferred.
"""
import csv
import hashlib
import json
import os
import statistics
import sys
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/private/tmp/das-direct-chart-mpl-cache')
if Path('/private/tmp/das-root-plot-libs').exists():
    sys.path.insert(0, '/private/tmp/das-root-plot-libs')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

ROOT=Path(__file__).resolve().parent
DATA=ROOT.parent/'stateful_dag_20261027'
NEAR=DATA/'near_runtime_20261027/near_dag_full_results.json'
EVM=DATA/'dag_workflow_20261027/das_distributed_composition_results.json'
n=json.loads(NEAR.read_text());e=json.loads(EVM.read_text())
selected={f'dag-{i:02d}-chain-d{d}-w1-theta1-nu1':d for i,d in [(1,2),(4,4),(7,8)]}
observations=[]
for repeat in n['completed_repeats']:
    for p in repeat['profiles']:
        pid=p['profile']['profile_id']
        if pid not in selected: continue
        depth=selected[pid]
        for s in p['sessions']:
            assert all(x['all_nodes_present'] and x['nodes_match_same_receiver'] for x in s['same_nodes_and_checksum'])
            runs=s['multi_contract']['runs'];assert len(runs)==1
            run=runs[0];assert run['succeeded'] and not run['failed']
            business=[r for r in run['records'] if any(x.startswith('DAG_NODE|') for x in r.get('logs',[]))]
            commits={r['outcome_id'] for r in business}
            assert len(commits)==depth
            assert len({r['executor_id'] for r in business})==depth
            control=s['same_receiver_best_control']
            assert control['run']['receipt_count']==1
            observations.append({'model':'NEAR independent contracts','depth':depth,'repeat':repeat['repeat'],
              'session_id':s['session_id'],'protocol_state_commit_units':len(commits),
              'node_execution_records':len(business),'evidence_ids':sorted(commits)})
for repeat in e['completed_repeats']:
    for p in repeat['profiles']:
        pid=p['profile']['profile_id']
        if pid not in selected: continue
        depth=selected[pid]
        addresses={a['address'].lower() for a in p['per_node_stage_contracts']}
        assert len(addresses)==depth
        for s in p['independent_stage_sessions']:
            assert len(s['theta_workflows'])==1
            assert s['session_state_valid'] and s['checksums_match_near_and_stage_inputs_valid']
            r=s['router_execution'];assert r['status']==1 and r['transaction_count']==1
            frames=r['external_stage_call_frames']
            assert r['node_event_count']==depth and len(frames)==depth
            assert {f['to'].lower() for f in frames}==addresses
            assert all(f.get('error') is None for f in frames)
            observations.append({'model':'DAS local independent contracts','depth':depth,'repeat':repeat['repeat'],
              'session_id':s['session_id'],'protocol_state_commit_units':r['transaction_count'],
              'node_execution_records':len(frames),'evidence_ids':[r['tx_hash']]})
summary=[]
for depth in [2,4,8]:
    for model in ['DAS local independent contracts','NEAR independent contracts']:
        a=[r['protocol_state_commit_units'] for r in observations if r['depth']==depth and r['model']==model]
        assert len(a)==6
        summary.append({'depth':depth,'theta':1,'model':model,'n_sessions':len(a),'fresh_runtime_repeats':3,
                        'mean_protocol_state_commit_units':statistics.mean(a),'sample_sd':statistics.stdev(a)})
with (ROOT/'direct_comparison.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
audit={'metric':'protocol-level state-commit units per workflow',
 'definition':{'NEAR':'distinct successful execution receipt outcomes emitting DAG_NODE (callbacks/refunds excluded)',
               'DAS':'outer successful Router transaction committing all independent Stage CALLs'},
 'source_files':{str(p.relative_to(DATA)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [NEAR,EVM]},
 'scope':'Internal workflow execution only. Excludes deployment and migration. Native gas and elapsed times are not divided.',
 'observations':observations,'summary':summary}
(ROOT/'metric_audit.json').write_text(json.dumps(audit,indent=2))

fontpath=Path('/System/Library/Fonts/Supplemental/Songti.ttc')
if fontpath.exists():
    font=FontProperties(fname=str(fontpath));plt.rcParams['font.family']=font.get_name()
plt.rcParams['axes.unicode_minus']=False
plt.rcParams['svg.fonttype']='path'
fig,ax=plt.subplots(figsize=(10.5,4.8))
fig.suptitle('工作流执行的协议级提交单元计数',fontsize=19,fontweight='bold',y=.96)
fig.text(.5,.87,'同一链式 DAG；每个节点为独立合约，θ = 1',ha='center',fontsize=12)
depths=[2,4,8]
das=[next(r['mean_protocol_state_commit_units'] for r in summary if r['model']=='DAS local independent contracts' and r['depth']==d) for d in depths]
near=[next(r['mean_protocol_state_commit_units'] for r in summary if r['model']=='NEAR independent contracts' and r['depth']==d) for d in depths]
table=ax.table(cellText=[[str(d),f'{int(n)} 次成功节点收据',f'{int(x)} 笔成功 Router 外层交易'] for d,n,x in zip(depths,near,das)],
               colLabels=['串行节点数','NEAR 独立合约路径','DAS 本地组合路径'],
               cellLoc='center',colLoc='center',loc='center',colWidths=[.18,.38,.44])
table.auto_set_font_size(False);table.set_fontsize(13);table.scale(1,2.0)
for (row,col),cell in table.get_celld().items():
    cell.set_edgecolor('#D8DEE5')
    if row==0:
        cell.set_facecolor('#EEF2F6');cell.set_text_props(weight='bold',color='#243447')
    elif row%2==1:
        cell.set_facecolor('#FAFBFC')
ax.axis('off')
ax.text(.5,.13,'计数随独立合约节点数变化，反映执行/持久化边界如何划分；不表示工作量倍数或性能倍数。',
        transform=ax.transAxes,ha='center',fontsize=11,fontweight='bold',color='#333333')
ax.text(.5,.04,'每配置 3 个全新运行时 × 2 个会话；节点输入和成功终态一致。EVM 外层交易整体回滚；NEAR 各节点收据独立提交。',
        transform=ax.transAxes,ha='center',fontsize=9.5,color='#555555')
fig.subplots_adjust(left=.04,right=.96,bottom=.05,top=.82)
fig.savefig(ROOT/'das_vs_near_direct.png',dpi=180,facecolor='white')
fig.savefig(ROOT/'das_vs_near_direct.svg',facecolor='white')
print(json.dumps(summary,ensure_ascii=False,indent=2))
