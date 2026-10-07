import sys, pandas as pd, matplotlib.pyplot as plt
d = pd.read_csv(sys.argv[1], parse_dates=['time'])
d = d[d.time.dt.year <= 2024]
d['promo'] = d.volume_kg * d.promo_share_vol
d['full'] = d.volume_kg - d.promo
y = d.groupby(d.time.dt.year)[['full', 'promo', 'volume_kg']].mean() / 1e6
print(y.round(2), (y.promo / y.volume_kg).round(3))

ink, muted, blue, orange, bg = '#14213D', '#6B7785', '#2A6FDB', '#D9662B', '#F6F7F4'
fig, ax = plt.subplots(figsize=(9, 5.6), facecolor=bg)
ax.set_facecolor(bg)
x = range(len(y))
ax.bar(x, y.full, width=0.55, color=blue, edgecolor=bg, linewidth=2, label='Sold at full price')
ax.bar(x, y.promo, bottom=y.full, width=0.55, color=orange, edgecolor=bg, linewidth=2, label='Sold on promotion')
for i, (f, p, t) in enumerate(zip(y.full, y.promo, y.volume_kg)):
    ax.text(i, t + 0.06, f'{t:.2f}m kg', ha='center', va='bottom', fontsize=12, fontweight='bold', color=ink)
    ax.text(i, f / 2, f'{f:.2f}', ha='center', va='center', fontsize=11, color='white')
    ax.text(i, f + p / 2, f'{p:.2f}\n({p / t:.0%})', ha='center', va='center', fontsize=11, color='white')
ax.set_xticks(list(x), [str(i) for i in y.index], fontsize=12, color=ink)
ax.set_ylabel('Average weekly volume (m kg)', color=muted)
ax.set_ylim(0, y.volume_kg.max() * 1.15)
for s in ['top', 'right', 'left']:
    ax.spines[s].set_visible(False)
ax.spines['bottom'].set_color('#DCE3E8')
ax.tick_params(axis='y', colors=muted, length=0)
ax.grid(axis='y', color='#DCE3E8', linewidth=0.8)
ax.set_axisbelow(True)
ax.legend(frameon=False, loc='upper center', bbox_to_anchor=(0.5, -0.08), ncol=2, fontsize=11)
fig.suptitle('Promotions grew, total volume did not', x=0.06, ha='left', fontsize=16, fontweight='bold', color=ink)
ax.set_title('Promo kg replaced full-price kg instead of adding to them', loc='left', fontsize=12, color=muted)
fig.tight_layout()
fig.savefig(sys.argv[2], dpi=150, facecolor=bg)
