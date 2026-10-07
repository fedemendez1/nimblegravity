import sys, numpy as np, pandas as pd
tab, out = sys.argv[1], sys.argv[2]
rc = pd.read_csv(f"{tab}/results_response_curves.csv")
mix = pd.read_csv(f"{tab}/opt_mix.csv")
bud = pd.read_csv(f"{tab}/opt_budget.csv")
base = mix[mix.model == "base LN(0,0.7)"].set_index("channel")
now_spend, now_rev = base.spend_now.sum() / 1e6, (base.spend_now * base.roi_now).sum() / 1e6
cut = bud[bud.budget_x == 0.85].iloc[0]
cut_spend, cut_rev = cut.spend / 1e6, cut.inc_revenue / 1e6
roi_now, roi_cut = now_rev / now_spend, cut_rev / cut_spend
keep, lift, saved = cut_rev / now_rev, roi_cut / roi_now - 1, now_spend - cut_spend
m = lambda v: f"{v:+.0f}%".replace("-", "−")

W, H, L, T, R, B = 1020, 400, 90, 20, 1000, 340
XM, YM = 550, 550
X = lambda s: L + s / 1e3 / XM * (R - L)
Y = lambda v: B - v / 1e3 / YM * (B - T)
CH = [("ooh", "Outdoor", "#2A6FDB"), ("social", "Social", "#1B3F8B"),
      ("digital_video", "Digital video", "#D9662B"), ("tv", "TV", "#8F3A12")]
el = []
for g in (200, 400):
    el.append(f'<line x1="{L}" y1="{Y(g * 1e3):.0f}" x2="{R}" y2="{Y(g * 1e3):.0f}" stroke="#DCE3E8" stroke-width="2"/>')
    el.append(f'<text x="{L - 14}" y="{Y(g * 1e3) + 8:.0f}" text-anchor="end" font-size="24" fill="#6B7785">£{g}k</text>')
    el.append(f'<text x="{X(g * 1e3):.0f}" y="{B + 34}" text-anchor="middle" font-size="24" fill="#6B7785">£{g}k</text>')
el.append(f'<line x1="{L}" y1="{B}" x2="{R}" y2="{B}" stroke="#B8C2CC" stroke-width="2"/>')
el.append(f'<text x="{R}" y="{B + 34}" text-anchor="end" font-size="22" fill="#6B7785">Spend a year</text>')
el.append(f'<text x="{L + 8}" y="{T + 6}" font-size="22" fill="#6B7785">Sales from media a year</text>')
for ch, name, col in CH:
    d = rc[(rc.channel == ch) & (rc.spend <= XM * 1e3)].sort_values("spend")
    el.append('<polyline points="' + " ".join(f"{X(s):.0f},{Y(v):.0f}" for s, v in zip(d.spend, d["mean"]))
              + f'" fill="none" stroke="{col}" stroke-width="5" stroke-linejoin="round"/>')
    s0, s1 = base.loc[ch, "spend_now"], base.loc[ch, "spend_opt"]
    v0, v1 = (np.interp(s, d.spend, d["mean"]) for s in (s0, s1))
    el.append(f'<circle cx="{X(s0):.0f}" cy="{Y(v0):.0f}" r="10" fill="#FFFFFF" stroke="{col}" stroke-width="4"/>')
    el.append(f'<circle cx="{X(s1):.0f}" cy="{Y(v1):.0f}" r="11" fill="{col}"/>')
x0 = L
for ch, name, col in CH:
    el.append(f'<line x1="{x0}" y1="{B + 76}" x2="{x0 + 32}" y2="{B + 76}" stroke="{col}" stroke-width="6"/>')
    el.append(f'<text x="{x0 + 42}" y="{B + 84}" font-size="22" font-weight="700" fill="{col}">{name} {m(base.loc[ch, "spend_change_pct"])}</text>')
    x0 += 42 + 13 * len(f"{name} +00%") + 36
el.append(f'<circle cx="{L + 10}" cy="{B + 112}" r="10" fill="#FFFFFF" stroke="#4A5568" stroke-width="4"/>')
el.append(f'<text x="{L + 32}" y="{B + 120}" font-size="22" fill="#4A5568">Today</text>')
el.append(f'<circle cx="{L + 140}" cy="{B + 112}" r="11" fill="#4A5568"/>')
el.append(f'<text x="{L + 162}" y="{B + 120}" font-size="22" fill="#4A5568">Proposed, same budget (partnership and search about the same)</text>')

label = "; ".join(f"{n} {m(base.loc[c, 'spend_change_pct'])}" for c, n, _ in CH)
big = lambda v, t: (f'<div style="display:flex;flex-direction:column;gap:4px"><p style="font-size:72px;font-weight:700;line-height:1;color:#8FB4F0">{v}</p>'
                    f'<p style="font-size:26px;line-height:1.3;color:#F6F7F4">{t}</p></div>')
html = f'''<section id="media" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">3. Rebalance media: almost the same sales for less money</h2>
<div style="position:absolute;left:128px;top:236px;width:1120px;height:600px;display:flex;flex-direction:column;gap:12px;background:#FFFFFF;border:1px solid #DCE3E8;border-radius:16px;padding:40px">
<h3 style="font-size:32px;font-weight:700">Move money to the channels that return more</h3>
<svg aria-label="Sales from media against spend for four channels. Outdoor and social sit on higher curves, digital video and TV on lower ones; all flatten as spend grows. Proposed change at the same budget: {label}; partnership and search about the same." viewBox="0 0 {W} 470" style="width:{W}px;height:470px">
{chr(10).join(el)}
</svg>
</div>
<div style="position:absolute;left:1288px;top:236px;width:504px;height:600px;display:flex;flex-direction:column;justify-content:center;gap:36px;background:#14213D;border-radius:16px;padding:48px">
<p style="font-size:30px;font-weight:700;color:#F6F7F4">And spend 15% less</p>
{big(f"+{lift:.0%}", "more back per £1")}
{big(f"{keep:.0%}", "of the sales media brings today")}
{big(f"£{saved * 1e3:.0f}k", "saved a year")}
</div>
<p style="position:absolute;left:128px;top:868px;width:1664px;font-size:32px;font-weight:700;color:#2A6FDB">The last £1 of media brings back about 50p: spend less, and move it to outdoor and social.</p>
<aside>What the optimiser says, and how sure we are. Today media costs about £{now_spend:.2f}m a year and brings back about £{now_rev:.2f}m of sales (£{roi_now:.2f} per £1, short term, before margin). The chart: each line is one channel's sales against its spend. Outdoor and social sit on higher curves (more back per £), digital video and TV on lower ones; every curve flattens as spend grows. Open dots are today, filled dots the optimiser's mix at the same total budget (each channel allowed to move up to 30%): more outdoor and social, less digital video and TV; partnership and search about the same. Re-run on three other model versions (different starting assumptions on returns): outdoor up, social up and digital video down in all four, TV down in three of four. The gain from the mix alone is small (about £20k a year). The bigger lever is the level: the last £1 brings back about 50p, so 15% less spend, re-mixed, keeps about {keep:.0%} of media sales (£{cut_rev:.2f}m) and lifts return per £1 by about {lift:.0%} (£{roi_cut:.2f}); 30% less keeps about 83% and lifts return about 19%; more budget does not pay (+30% spend brings about 52p per extra £1). Limits: the model separates channels by how high their curve sits, not by how saturated each one is; each campaign ran at one or two weights and channels launched together, so curve shapes lean on the model's standard assumptions. That is why we show directions that hold in every version, not an exact mix. With any realistic margin media does not pay back in the short term; its case is long-term brand effects, which this data cannot measure, so trim and test rather than switch off. Next: vary weight by region and stagger channel launches so the next model can draw each curve; geo holdouts to calibrate.</aside>
</section>
'''
open(out, "w").write(html)
print(f"keep {keep:.0%} lift {lift:.0%} saved {saved:.3f}m")
