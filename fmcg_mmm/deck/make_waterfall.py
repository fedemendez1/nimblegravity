import sys, pandas as pd
t = pd.read_csv(sys.argv[1]); out = sys.argv[2]
LBL = {"distribution": "Distribution (stores)", "log_rel_price": "Relative price", "heat_excess": "Hot weeks (above 20°C)",
       "temp_avg": "Average temperature", "rainfall": "Rainfall", "promo_intensity": "Our promotions",
       "comp_c_promo_share": "Brand C promotions", "comp_media_spend": "Competitor media", "tv": "TV",
       "digital_video": "Digital video", "social": "Social", "partnership": "Partnerships", "ooh": "Outdoor",
       "search_rdm": "Search & retail digital", "calendar": "Seasonality & New Year", "resid_lag1": "Week-to-week carry-over",
       "Other / unexplained": "Unexplained (incl. 53rd week)"}
P = ["2022-2023", "2023-2024"]
v = {}
for p in P:
    d = t[t.period == p].set_index("driver").pct
    d["calendar"] = d[["season_sin", "season_cos", "new_year_week"]].sum()
    v[p] = d.drop(["season_sin", "season_cos", "new_year_week"])
order = v[P[1]].drop(["Other / unexplained", "Total change"]).sort_values(ascending=False).index.tolist() + ["Other / unexplained"]
rows = order + ["Total change"]
TOP, BOT = 300, 900
rh = (BOT - TOP) / len(rows); bh = rh * 0.62
# cumulative ranges
rng = {}
for p in P:
    c, lo, hi = 0, 0, 0
    for k in order:
        c += v[p][k]; lo, hi = min(lo, c), max(hi, c)
    rng[p] = (lo, hi)
W = 600; PAD = 70
s = (W - 2 * PAD) / max(h - l for l, h in rng.values())
X0 = {P[0]: 520, P[1]: 1192}
BLUE, RED, GREY, NAVY = "#2A6FDB", "#C8372D", "#9AA6B2", "#14213D"
el = []
fmt = lambda x: f"{x:+.1f}".replace("-", "−") if abs(x) >= 0.05 else "0.0"
for i, k in enumerate(rows):
    y = TOP + i * rh
    lab = "Total change" if k == "Total change" else LBL[k]
    w = "700" if k == "Total change" else "400"
    el.append(f'<p style="position:absolute;left:128px;top:{y + (rh - 30) / 2:.0f}px;width:360px;text-align:right;font-size:24px;font-weight:{w};color:#14213D">{lab}</p>')
for p in P:
    lo, hi = rng[p]
    x0 = X0[p] + PAD + (-lo) * s + (W - 2 * PAD - (hi - lo) * s) / 2  # x of zero
    el.append(f'<div style="position:absolute;left:{x0:.0f}px;top:{TOP - 8}px;width:2px;height:{BOT - TOP + 8}px;background:#B8C2CC"></div>')
    c = 0
    for i, k in enumerate(rows):
        y = TOP + i * rh + (rh - bh) / 2
        val = v[p][k]
        if k == "Total change":
            a, b, col = 0, val, NAVY
        else:
            a, b = c, c + val; c = b
            col = GREY if k == "Other / unexplained" else (BLUE if val >= 0 else RED)
        l, r = min(a, b), max(a, b)
        bw = max((r - l) * s, 2)
        el.append(f'<div style="position:absolute;left:{x0 + l * s:.0f}px;top:{y:.0f}px;width:{bw:.0f}px;height:{bh:.0f}px;background:{col}"></div>')
        tc = NAVY if k == "Total change" else ("#6B7785" if col == GREY else col)
        tw = "700" if k == "Total change" or abs(val) >= 1 else "400"
        ty = TOP + i * rh + (rh - 30) / 2
        if val >= 0:
            el.append(f'<p style="position:absolute;left:{x0 + r * s + 8:.0f}px;top:{ty:.0f}px;width:80px;font-size:24px;font-weight:{tw};color:{tc}">{fmt(val)}</p>')
        else:
            el.append(f'<p style="position:absolute;left:{x0 + l * s - 88:.0f}px;top:{ty:.0f}px;width:80px;text-align:right;font-size:24px;font-weight:{tw};color:{tc}">{fmt(val)}</p>')
    a0, a1 = t[(t.period == p) & (t.driver == "Total change")][["start_m_kg", "end_m_kg"]].iloc[0]
    yy = p.split("-")
    el.append(f'<p style="position:absolute;left:{X0[p]}px;top:236px;width:{W}px;text-align:center;font-size:28px;font-weight:700;color:#14213D">{yy[0]} → {yy[1]}<span style="font-weight:400;color:#4A5568"> · {a0:.0f}m → {a1:.0f}m kg</span></p>')
html = f'''<section id="duetofull" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">What moved our volume each year, driver by driver</h2>
{chr(10).join(el)}
<p style="position:absolute;left:128px;bottom:64px;width:1664px;font-size:24px;color:#6B7785">Change in volume, % of the previous year. Blue adds, red takes away; sorted by 2023 → 2024. Source: marketing mix model.</p>
<aside>Every driver in the model, year on year. Read each column top to bottom: bars start where the previous one ended and add up to the total change. 2022 to 2023 (−8.2%): relative price took −7.7% and Brand C promotions −1.8%; everything else was small. 2023 to 2024 (+8.2%): distribution added +13.3%, Brand C promotions eased (+1.7%), media added about +0.7% in total, while relative price (−4.3%) and fewer hot weeks (−1.7%) took volume away. Week-to-week carry-over = persistent shocks the model tracks from one week to the next (stock-outs, displays, retailer activity); it averages out. Unexplained includes the 53rd week of 2022.</aside>
</section>
'''
open(out, "w").write(html)
print(len(el), "elements; rows", rows)
