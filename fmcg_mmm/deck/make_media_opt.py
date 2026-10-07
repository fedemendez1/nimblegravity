import sys, pandas as pd
tab, out = sys.argv[1], sys.argv[2]
mix = pd.read_csv(f"{tab}/opt_mix.csv")
bud = pd.read_csv(f"{tab}/opt_budget.csv")
base = mix[mix.model == "base LN(0,0.7)"].set_index("channel")
now_spend, now_rev = base.spend_now.sum() / 1e6, (base.spend_now * base.roi_now).sum() / 1e6
cut = bud[bud.budget_x == 0.85].iloc[0]
cut_spend, cut_rev = cut.spend / 1e6, cut.inc_revenue / 1e6
roi_now, roi_cut = now_rev / now_spend, cut_rev / cut_spend
keep, lift = cut_rev / now_rev, roi_cut / roi_now - 1
same = bud[bud.budget_x == 1].iloc[0].inc_revenue / 1e6 / now_spend / roi_now - 1
m = lambda v: f"{v:+.0f}%".replace("-", "−")
print(f"now {now_spend:.2f}/{now_rev:.2f} roi {roi_now:.2f}; -15% {cut_spend:.2f}/{cut_rev:.2f} roi {roi_cut:.2f}; "
      f"keep {keep:.0%} lift {lift:.0%}; same-budget lift {same:.1%}")

# left: change in spend by channel (stable across all four model variants)
rows = [("Outdoor", "ooh"), ("Social", "social"), ("Digital video", "digital_video"), ("TV", "tv")]
cx, k = 420, 5.5
left = [f'<line x1="{cx}" y1="0" x2="{cx}" y2="300" stroke="#B8C2CC" stroke-width="2"/>']
for i, (name, ch) in enumerate(rows):
    v = base.loc[ch, "spend_change_pct"]; y = 20 + i * 72; w = abs(v) * k
    x = cx if v > 0 else cx - w
    col = "#2A6FDB" if v > 0 else "#D9662B"
    left.append(f'<text x="0" y="{y + 30}" font-size="28" fill="#14213D">{name}</text>')
    left.append(f'<rect x="{x:.0f}" y="{y}" width="{w:.0f}" height="40" rx="4" fill="{col}"/>')
    tx, anc = (cx + w + 12, "start") if v > 0 else (cx - w - 12, "end")
    left.append(f'<text x="{tx:.0f}" y="{y + 30}" text-anchor="{anc}" font-size="28" font-weight="700" fill="#14213D">{m(v)}</text>')
left.append('<text x="0" y="350" font-size="24" fill="#6B7785">Partnership and search: about the same</text>')

# right: media-driven sales vs spend, optimal mix at each budget
X = lambda s: 80 + s / 1.7 * 660
Y = lambda r: 330 - r / 1.6 * 310
pts = [(0, 0)] + list(zip(bud.spend / 1e6, bud.inc_revenue / 1e6))
pts = [p for p in pts if p[0] <= 1.7]
right = []
for g in (0.5, 1.0, 1.5):
    right.append(f'<line x1="80" y1="{Y(g):.0f}" x2="740" y2="{Y(g):.0f}" stroke="#DCE3E8" stroke-width="2"/>')
    right.append(f'<text x="66" y="{Y(g) + 8:.0f}" text-anchor="end" font-size="24" fill="#6B7785">£{g:g}m</text>')
right.append(f'<line x1="80" y1="330" x2="740" y2="330" stroke="#B8C2CC" stroke-width="2"/>')
for s in (0.5, 1.0, 1.5):
    right.append(f'<text x="{X(s):.0f}" y="362" text-anchor="middle" font-size="24" fill="#6B7785">£{s:g}m</text>')
right.append(f'<line x1="{X(0):.0f}" y1="{Y(0):.0f}" x2="{X(1.6):.0f}" y2="{Y(1.6):.0f}" stroke="#6B7785" stroke-width="2" stroke-dasharray="8 8"/>')
right.append('<text x="300" y="268" font-size="22" fill="#6B7785">£1 back per £1</text>')
right.append('<text x="80" y="362" font-size="22" fill="#6B7785">Spend</text>')
right.append('<text x="84" y="16" font-size="22" fill="#6B7785">Sales from media</text>')
right.append('<polyline points="' + " ".join(f"{X(s):.0f},{Y(r):.0f}" for s, r in pts)
             + '" fill="none" stroke="#2A6FDB" stroke-width="6" stroke-linejoin="round"/>')
right.append(f'<circle cx="{X(now_spend):.0f}" cy="{Y(now_rev):.0f}" r="11" fill="#14213D"/>')
right.append(f'<text x="{X(now_spend) + 20:.0f}" y="{Y(now_rev) + 38:.0f}" text-anchor="start" font-size="24" font-weight="700" fill="#14213D">Today</text>')
right.append(f'<circle cx="{X(cut_spend):.0f}" cy="{Y(cut_rev):.0f}" r="11" fill="#2A6FDB"/>')
right.append(f'<text x="{X(cut_spend) - 20:.0f}" y="{Y(cut_rev) - 22:.0f}" text-anchor="end" font-size="24" font-weight="700" fill="#2A6FDB">−15%</text>')

card = "position:absolute;top:236px;height:600px;display:flex;flex-direction:column;gap:16px;background:#FFFFFF;border:1px solid #DCE3E8;border-radius:16px;padding:40px"
html = f'''<section id="media" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">3. Rebalance media: almost the same sales for less money</h2>
<div style="{card};left:128px;width:780px">
<h3 style="font-size:32px;font-weight:700">Move money between channels</h3>
<p style="font-size:24px;color:#4A5568">Change in spend, same total budget</p>
<svg aria-label="Optimal change in spend by channel at the same budget: outdoor {m(base.loc['ooh', 'spend_change_pct'])}, social {m(base.loc['social', 'spend_change_pct'])}, digital video {m(base.loc['digital_video', 'spend_change_pct'])}, TV {m(base.loc['tv', 'spend_change_pct'])}; partnership and search about the same" viewBox="0 0 700 370" style="width:700px;height:370px">
{chr(10).join(left)}
</svg>
<p style="font-size:24px;color:#4A5568">Same direction in all four model versions we tested</p>
</div>
<div style="{card};left:948px;width:844px">
<h3 style="font-size:32px;font-weight:700">Spend a little less</h3>
<p style="font-size:24px;color:#4A5568">Sales from media vs media spend, a year</p>
<svg aria-label="Sales from media against media spend: the curve flattens, so the last pounds bring back about 50p each. Today £{now_spend:.2f}m of spend brings £{now_rev:.2f}m of sales; 15% less spend, better mixed, keeps £{cut_rev:.2f}m." viewBox="0 0 764 370" style="width:764px;height:370px">
{chr(10).join(right)}
</svg>
<p style="font-size:24px;color:#4A5568">The last £1 of media brings back about 50p</p>
</div>
<p style="position:absolute;left:128px;top:868px;width:1664px;font-size:32px;font-weight:700;color:#2A6FDB">Spend 15% less and move it to outdoor and social: {lift:.0%} more back per £1, {keep:.0%} of the sales.</p>
<aside>What the optimiser says, and how sure we are. Today media costs about £{now_spend:.2f}m a year and brings back about £{now_rev:.2f}m of sales (£{roi_now:.2f} per £1, short term, before margin). 1) Mix: Meridian budget optimiser, same total budget, each channel allowed to move up to 30%: more outdoor and social, less digital video and TV; partnership and search about the same. The gain is small, about +{same:.0%} return (about £20k a year). Re-run on the three other model versions (different starting assumptions on returns): outdoor up, social up and digital video down in all four, TV down in three of four; gains £8k to £42k a year. 2) Level: the curve flattens; the last £1 brings back about 50p of sales. 15% less spend, re-mixed, keeps about {keep:.0%} of media sales (£{cut_rev:.2f}m) and lifts return per £1 by about {lift:.0%} (£{roi_cut:.2f}); 30% less keeps about 83% and lifts return about 19%. More budget does not pay: +30% spend brings about 52p per extra £1. With any realistic margin media does not pay back in the short term; its case is long-term brand effects, which this data cannot measure, so trim and test rather than switch off. 3) Limits: each campaign ran at one or two weights and channels launched together, so curve shapes lean on the model standard assumptions while the averages come from the data; that is why we show directions that hold in every version, not an exact mix. 4) Next: vary weight by region and stagger channel launches so the next model can draw each curve; geo holdouts to calibrate. Media is a small lever for this brand (under 1% of volume).</aside>
</section>
'''
open(out, "w").write(html)
