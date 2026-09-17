import pandas as pd, numpy as np, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
BLUE,ORANGE,AQUA='#2a78d6','#eb6834','#1baf7a'
INK,INK2,MUT='#0b0b0b','#52514e','#8a8a86'
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.facecolor':'#ffffff',
 'figure.facecolor':'#ffffff','axes.edgecolor':'#d9d8d4','axes.linewidth':.8,
 'xtick.color':INK2,'ytick.color':INK2,'text.color':INK,'axes.labelcolor':INK2})
df=pd.read_csv('data.csv',parse_dates=['week'])
w=df.groupby('week')[['vendor_ui','rule_based','mmm']].sum().reset_index()
fig,ax=plt.subplots(figsize=(13.2,3.95),dpi=220)
series=[('vendor_ui','Vendor UI',BLUE,0),('mmm','MMM (incremental)',AQUA,0),('rule_based','Rule-based (first touch)',ORANGE,0)]
for c,lab,col,_ in series:
    ax.plot(w.week,w[c],color=col,lw=2.2,zorder=3,solid_capstyle='round')
for s in ['top','right']: ax.spines[s].set_visible(False)
ax.grid(axis='y',color='#ecebe7',lw=.8); ax.set_axisbelow(True)
ax.set_ylabel('Registrations per week',fontsize=10.5)
ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_:f'{v:,.0f}'))
ax.set_ylim(0,3350)
ax.axvspan(pd.Timestamp('2022-04-29'),pd.Timestamp('2022-05-27'),color=ORANGE,alpha=.11,zorder=1)
ax.axvline(pd.Timestamp('2022-11-01'),color=MUT,lw=1.2,ls=(0,(4,3)),zorder=2)
ax.axvspan(pd.Timestamp('2022-11-01'),pd.Timestamp('2022-12-31'),color=BLUE,alpha=.06,zorder=1)
ax.annotate('1. TRACKING OUTAGE  (May, 4 wks)\nTradedesk rule-based drops to zero.\n£34.6k spend left unattributed,\nreported CPA £206 vs £19 true.',
  xy=(pd.Timestamp('2022-05-13'),415),xytext=(pd.Timestamp('2022-01-08'),1520),fontsize=9.3,color=ORANGE,
  weight='bold',ha='left',linespacing=1.45,
  arrowprops=dict(arrowstyle='-|>',color=ORANGE,lw=1.5,connectionstyle='arc3,rad=-0.22'),zorder=6)
ax.annotate('2. Q4 REGIME BREAK  (Nov–Dec)\nVendor UI accelerates away from MMM:\ngap widens from +22% to +54%.',
  xy=(pd.Timestamp('2022-11-25'),2450),xytext=(pd.Timestamp('2022-04-20'),2680),fontsize=9.3,color=BLUE,
  weight='bold',ha='left',linespacing=1.45,
  arrowprops=dict(arrowstyle='-|>',color=BLUE,lw=1.5,connectionstyle='arc3,rad=0.12'),zorder=6)
for c,lab,col,_ in series:
    y=w[c].iloc[-1]
    ax.annotate(f'{lab}\n{y:,.0f}/wk',xy=(w.week.iloc[-1],y),xytext=(10,0),textcoords='offset points',
                va='center',fontsize=9.8,color=col,weight='bold',linespacing=1.35)
ax.set_xlim(w.week.min()-pd.Timedelta(days=5),pd.Timestamp('2023-02-25'))
ax.set_title('Weekly registrations: a stable gap for 10 months — then it breaks',
             fontsize=13.5,weight='bold',color=INK,pad=12,loc='left')
fig.text(0.008,0.002,'52 weeks · paid display · Tradedesk + Google UAC · £888k spend · 3 duplicate rows removed before analysis.',
         fontsize=8.5,color=MUT)
fig.tight_layout(rect=[0,0.035,1,1]); fig.savefig('fig1_divergence.png',dpi=220,bbox_inches='tight')
print('ok')
