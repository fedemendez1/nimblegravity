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

W = 1020
NAMES = {"ooh": "Outdoor", "social": "Social", "search_rdm": "Search", "partnership": "Partnership",
         "tv": "TV", "digital_video": "Digital video"}
LO, HI, X0, X1 = 0.3, 1.1, 250, 860
X = lambda v: X0 + (v - LO) / (HI - LO) * (X1 - X0)
eq = base.drop("ooh").mroi_opt.median()
el = [f'<text x="{(X0 + X1) / 2:.0f}" y="24" text-anchor="middle" font-size="22" fill="#6B7785">Sales back from the last £1</text>',
      f'<text x="{W}" y="24" text-anchor="end" font-size="22" fill="#6B7785">Spend</text>']
for v in (0.5, 1.0):
    el.append(f'<line x1="{X(v):.0f}" y1="44" x2="{X(v):.0f}" y2="396" stroke="#DCE3E8" stroke-width="2"/>')
    el.append(f'<text x="{X(v):.0f}" y="424" text-anchor="middle" font-size="22" fill="#6B7785">£{v:.2f}</text>')
el.append(f'<line x1="{X(eq):.0f}" y1="44" x2="{X(eq):.0f}" y2="396" stroke="#2A6FDB" stroke-width="2" stroke-dasharray="8 6"/>')
for i, (ch, r) in enumerate(base.sort_values("mroi_now", ascending=False).iterrows()):
    y = 72 + i * 58
    a, b, c = r.mroi_now, r.mroi_opt, r.spend_change_pct
    el.append(f'<text x="0" y="{y + 9}" font-size="26" fill="#14213D">{NAMES[ch]}</text>')
    el.append(f'<line x1="{X(min(a, b)):.0f}" y1="{y}" x2="{X(max(a, b)):.0f}" y2="{y}" stroke="#B8C2CC" stroke-width="6"/>')
    el.append(f'<circle cx="{X(a):.0f}" cy="{y}" r="11" fill="#FFFFFF" stroke="#4A5568" stroke-width="4"/>')
    el.append(f'<circle cx="{X(b):.0f}" cy="{y}" r="12" fill="#2A6FDB"/>')
    col = "#2A6FDB" if c > 3 else "#B4501C" if c < -3 else "#6B7785"
    txt = m(c) if abs(c) > 3 else "same"
    el.append(f'<text x="{W}" y="{y + 9}" text-anchor="end" font-size="26" font-weight="700" fill="{col}">{txt}</text>')
el.append(f'<circle cx="{X0 + 10}" cy="455" r="11" fill="#FFFFFF" stroke="#4A5568" stroke-width="4"/>')
el.append(f'<text x="{X0 + 32}" y="463" font-size="22" fill="#4A5568">Today</text>')
el.append(f'<circle cx="{X0 + 140}" cy="455" r="12" fill="#2A6FDB"/>')
el.append(f'<text x="{X0 + 162}" y="463" font-size="22" fill="#4A5568">After moving money, same budget</text>')

label = "; ".join(f"{NAMES[c]} {r.mroi_now:.2f} to {r.mroi_opt:.2f}" for c, r in base.iterrows())
big = lambda v, t: (f'<div style="display:flex;flex-direction:column;gap:4px"><p style="font-size:72px;font-weight:700;line-height:1;color:#8FB4F0">{v}</p>'
                    f'<p style="font-size:26px;line-height:1.3;color:#F6F7F4">{t}</p></div>')
html = f'''<section id="media" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">3. Rebalance media: almost the same sales for less money</h2>
<div style="position:absolute;left:128px;top:236px;width:1120px;height:600px;display:flex;flex-direction:column;gap:12px;background:#FFFFFF;border:1px solid #DCE3E8;border-radius:16px;padding:40px">
<h3 style="font-size:32px;font-weight:700">Move money from where the last £1 returns least</h3>
<svg aria-label="Sales back from the last £1 of each channel, today and after moving money at the same budget: {label}. Money moves from digital video and TV to outdoor and social until the returns meet at about £{eq:.2f}." viewBox="0 0 {W} 470" style="width:{W}px;height:470px">
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
<p style="position:absolute;left:128px;bottom:64px;width:1664px;font-size:24px;color:#6B7785">Sales a year, short term, before margin. Curve shapes partly use the model's standard assumptions. Source: marketing mix model, budget optimiser.</p>
<aside>What the optimiser says, and how sure we are. Today media costs about £{now_spend:.2f}m a year and brings back about £{now_rev:.2f}m of sales (£{roi_now:.2f} per £1, short term, before margin). The chart: for each channel, how much the last £1 brings back today (open dot) and after the optimiser moves money at the same total budget (filled dot; each channel allowed to move up to 30%). The rule: take money from channels where the last £1 returns least (digital video, TV) and give it to those where it returns most (outdoor, social) until they meet at about {eq:.2f}; outdoor stays higher because it hits the +30% cap. Re-run on three other model versions (different starting assumptions on returns): outdoor up, social up and digital video down in all four, TV down in three of four. The gain from the mix alone is small (about £20k a year). The bigger lever is the level: the last £1 brings back about 50p, so 15% less spend, re-mixed, keeps about {keep:.0%} of media sales (£{cut_rev:.2f}m) and lifts return per £1 by about {lift:.0%} (£{roi_cut:.2f}); 30% less keeps about 83% and lifts return about 19%; more budget does not pay (+30% spend brings about 52p per extra £1). Limits: the model separates channels by how high their curve sits, not by how saturated each one is; each campaign ran at one or two weights and channels launched together, so curve shapes lean on the model's standard assumptions. That is why we show directions that hold in every version, not an exact mix. With any realistic margin media does not pay back in the short term; its case is long-term brand effects, which this data cannot measure, so trim and test rather than switch off. Next: vary weight by region and stagger channel launches so the next model can draw each curve; geo holdouts to calibrate.</aside>
</section>
'''
open(out, "w").write(html)
print(f"keep {keep:.0%} lift {lift:.0%} saved {saved:.3f}m")
