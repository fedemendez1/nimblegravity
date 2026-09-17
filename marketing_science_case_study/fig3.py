import pandas as pd, numpy as np, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import FancyBboxPatch
INK,INK2,MUT='#0b0b0b','#52514e','#8a8d84'
VIOLET,YELLOW='#4a3aa7','#eda100'
plt.rcParams.update({'font.family':'DejaVu Sans','axes.facecolor':'#fff','figure.facecolor':'#fff',
 'axes.edgecolor':'#d9d8d4','axes.linewidth':.8,'xtick.color':INK2,'ytick.color':INK2,
 'text.color':INK,'axes.labelcolor':INK2})
df=pd.read_csv('data.csv',parse_dates=['week'])
q=df[df.week>='2022-11-01'].groupby('vendor')[['spend','vendor_ui','mmm']].sum()
fig,ax=plt.subplots(figsize=(10.2,3.05),dpi=220)
x=np.arange(2); bw=.24
for i,(k,lab,col) in enumerate([('google_uac','Google UAC',VIOLET),('tradedesk','Tradedesk',YELLOW)]):
    for j,m in enumerate(['vendor_ui','mmm']):
        v=q.loc[k,'spend']/q.loc[k,m]
        ax.add_patch(FancyBboxPatch((x[j]+(i-.5)*bw-bw*.44,0),bw*.88,v,
            boxstyle="round,pad=0,rounding_size=0.45",lw=0,facecolor=col,mutation_aspect=.09,zorder=3))
        ax.annotate(f'£{v:.2f}',(x[j]+(i-.5)*bw,v),xytext=(0,4),textcoords='offset points',
            ha='center',fontsize=10.5,weight='bold',color=INK,zorder=5)
    ax.bar([np.nan],[np.nan],color=col,label=lab)
for s in ['top','right']: ax.spines[s].set_visible(False)
ax.grid(axis='y',color='#ecebe7',lw=.8); ax.set_axisbelow(True)
ax.set_xticks(x); ax.set_xticklabels(['Vendor UI says:  “Google is the better buy”',
    'MMM says:  “Tradedesk is the better buy”'],fontsize=10.5,weight='bold',color=INK)
ax.tick_params(axis='x',length=0); ax.set_xlim(-.55,1.55); ax.set_ylim(0,24)
ax.set_ylabel('Q4 CPA',fontsize=10)
ax.yaxis.set_major_formatter(FuncFormatter(lambda v,_:f'£{v:.0f}'))
ax.legend(frameon=False,ncol=2,fontsize=9.5,loc='upper left',bbox_to_anchor=(-0.005,1.17),
          handlelength=1.0,handleheight=1.0,columnspacing=1.2)
ax.set_title('Q4: the two rulers rank the same two vendors in opposite order',
             fontsize=12,weight='bold',color=INK,pad=26,loc='left')
fig.tight_layout(); fig.savefig('fig3_reversal.png',dpi=220,bbox_inches='tight')
from PIL import Image; im=Image.open('fig3_reversal.png'); print(im.size,'aspect %.3f'%(im.size[0]/im.size[1]))
