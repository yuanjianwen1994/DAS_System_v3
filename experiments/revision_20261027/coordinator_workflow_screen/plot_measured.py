"""Static scientific figure from recorded observations only."""
import os,sys,json,statistics
from pathlib import Path
os.environ['MPLCONFIGDIR']='/private/tmp/das-root-mpl-cache'
sys.path.insert(0,'/private/tmp/das-root-plot-libs')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=Path(__file__).resolve().parent
one=json.loads((root/'raw_1s.json').read_text())['rows']
formal=json.loads((root/'raw_12s.json').read_text())['rows']
fig,axes=plt.subplots(1,2,figsize=(11.5,4.3))
colors=['#0072B2','#D55E00']
labels=['Local composition','Sequential-confirmation model']
models=['DAS_LOCAL','ASYNC_RECEIPT_MODEL']
for m,c,l in zip(models,colors,labels):
 vals=[[r['journey_seconds'] for r in one if r['model']==m and r['depth']==d] for d in [1,4,8]]
 axes[0].errorbar([1,4,8],[statistics.mean(a) for a in vals],yerr=[statistics.stdev(a) for a in vals],marker='o',capsize=4,label=l,color=c)
axes[0].set(xlabel='Dependent contract stages per round',ylabel='Measured journey duration (s)',title='1-second blocks; two rounds; 3 runs',xticks=[1,4,8],ylim=(0,23))
for i,(m,c,l) in enumerate(zip(models,colors,labels)):
 a=[r['journey_seconds'] for r in formal if r['model']==m]
 mu=statistics.mean(a);sd=statistics.stdev(a) if len(a)>1 else 0
 axes[1].bar(i,mu,yerr=sd,capsize=5,color=c,width=.55)
 axes[1].text(i,mu+4,f'{mu:.2f} s\n(n={len(a)})',ha='center',fontsize=10)
axes[1].set(xticks=[0,1],xticklabels=['Local\ncomposition','Sequential-confirmation\nmodel'],ylabel='Measured journey duration (s)',title='12-second blocks; depth 4; two rounds',ylim=(0,170))
for ax in axes:
 ax.spines[['top','right']].set_visible(False);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
axes[0].legend(loc='upper left',frameon=False,fontsize=9)
fig.suptitle('Dependency-chain screen: same final state, different confirmation paths',fontsize=14)
fig.text(.5,.025,'Controlled EVM models, not native NEAR measurements. Same-domain Avalanche/Teleport controls match local composition.\nFour common boundary operations are included. Error bars: sample SD across fresh runs. No saturation-throughput claim.',ha='center',fontsize=8)
fig.tight_layout(rect=[0,.13,1,.92])
fig.savefig(root/'measured_workflow_comparison.png',dpi=180,facecolor='white')
fig.savefig(root/'measured_workflow_comparison.svg',facecolor='white')
print(root/'measured_workflow_comparison.png')
