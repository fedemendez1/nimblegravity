import pandas as pd, numpy as np, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import FancyBboxPatch
BLUE,ORANGE,AQUA='#2a78d6','#eb6834','#1baf7a'
VIOLET,YELLOW='#4a3aa7','#eda100'
INK,INK2,MUT='#0b0b0b','#52514e','#8a8a86'
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.facecolor':'#fff',
 'figure.facecolor':'#fff','axes.edgecolor':'#d9d8d4','axes.linewidth':.8,
 'xtick.color':INK2,'ytick.color':INK2,'text.color':INK,'axes.labelcolor':INK2})
df=pd.read_csv('data.csv',parse_dates=['week'])

def rbar(ax,x,h,w,col,r=0.9):
    p=FancyBboxPatch((x-w/2,0),w,h,boxstyle=f"round,pad=0,rounding_size={r}",
        linewidth=0,facecolor=col,mutation_aspect=0.06,zorder=3)
    ax.add_patch(p); return p
def style(ax):
    for s in ['top','right']: ax.spines[s].set_visible(False)
    ax.grid(axis='y',color='#ecebe7',lw=.8); ax.set_axisbelow(True)

# ---- FIG 2 ----
t=df.groupby('vendor')[['spend','vendor_ui','rule_based','mmm']].sum()
keys=['google_uac','tradedesk']
fig,ax=plt.subplots(figsize=(8.2,4.4),dpi=220)
x=np.arange(2); bw=.20
for i,(m,lab,col) in enumerate([('vendor_ui','Vendor UI',BLUE),('rule_based','Rule-based',ORANGE),('mmm','MMM',AQUA)]):
    for j,k in enumerate(keys):
        v=t.loc[k,'spend']/t.loc[k,m]
        rbar(ax,x[j]+(i-1)*bw,v,bw*0.88,col)
        ax.annotate(f'£{v:.2f}',(x[j]+(i-1)*bw,v),xytext=(0,5),textcoords='offset points',
                    ha='center',fontsize=9.8,weight='bold',color=INK,zorder=5)
    ax.bar([np.nan],[np.nan],color=col,label=lab)
style(ax); ax.set_xticks(x); ax.set_xticklabels(['Google UAC\n£350k spend · 39%','Tradedesk\n£538k spend · 61%'],
    fontsize=11,weight='bold',color=INK); ax.tick_params(axis='x',length=0)
ax.set_xlim(-.5,1.5); ax.set_ylim(0,54)
ax.set_ylabel('Cost per registration, full year',fontsize=10.5)
ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_:f'£{v:.0f}'))
ax.legend(frameon=False,ncol=3,fontsize=10,loc='upper left',bbox_to_anchor=(-0.01,1.15),handlelength=1.1,handleheight=1.1)
ax.annotate('2.4× the MMM read.\nFirst-touch click tracking\nis blind to view-through\nprogrammatic display.',
   xy=(1-bw-0.045,44.3),xytext=(0.28,41),fontsize=9.3,color=ORANGE,weight='bold',ha='left',va='top',linespacing=1.45,
   arrowprops=dict(arrowstyle='-|>',color=ORANGE,lw=1.4,shrinkA=6,shrinkB=3,
   connectionstyle='arc3,rad=-0.18'),zorder=6)
ax.set_title('Same vendor, same year — the ruler changes the verdict by up to 2.9×',
             fontsize=12.5,weight='bold',color=INK,pad=32,loc='left')
fig.tight_layout(); fig.savefig('fig2_cpa.png',dpi=220,bbox_inches='tight'); print('fig2 ok')

# ---- FIG 3 ----
q=df[df.week>='2022-11-01'].groupby('vendor')[['spend','vendor_ui','mmm']].sum()
pairs=[('vendor_ui','Vendor UI says:\n“Google is the better buy”'),('mmm','MMM says:\n“Tradedesk is the better buy”')]
fig,ax=plt.subplots(figsize=(8.0,4.3),dpi=220)
x=np.arange(2); bw=.26
for i,(k,lab,col) in enumerate([('google_uac','Google UAC',VIOLET),('tradedesk','Tradedesk',YELLOW)]):
    for j,(m,_) in enumerate(pairs):
        v=q.loc[k,'spend']/q.loc[k,m]
        rbar(ax,x[j]+(i-.5)*bw,v,bw*0.88,col,r=0.55)
        ax.annotate(f'£{v:.2f}',(x[j]+(i-.5)*bw,v),xytext=(0,5),textcoords='offset points',
                    ha='center',fontsize=10.5,weight='bold',color=INK,zorder=5)
    ax.bar([np.nan],[np.nan],color=col,label=lab)
style(ax); ax.set_xticks(x); ax.set_xticklabels([p[1] for p in pairs],fontsize=10.3,weight='bold',color=INK)
ax.tick_params(axis='x',length=0); ax.set_xlim(-.55,1.55); ax.set_ylim(0,24)
ax.set_ylabel('Q4 cost per registration',fontsize=10.5)
ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_:f'£{v:.0f}'))
ax.legend(frameon=False,ncol=2,fontsize=10,loc='upper left',bbox_to_anchor=(-0.01,1.13),handlelength=1.1,handleheight=1.1)
ax.set_title('Q4: the two rulers rank the same two vendors in opposite order',
             fontsize=12.5,weight='bold',color=INK,pad=30,loc='left')
fig.text(0.008,0.002,'Nov–Dec 2022, £230k spend. Steering 20% of Tradedesk budget to Google on the UI signal costs ~105 registrations.',
         fontsize=8.4,color=MUT)
fig.tight_layout(rect=[0,0.04,1,1]); fig.savefig('fig3_reversal.png',dpi=220,bbox_inches='tight'); print('fig3 ok')
