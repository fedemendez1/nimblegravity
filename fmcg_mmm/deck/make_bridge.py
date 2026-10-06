import sys, pandas as pd
t = pd.read_csv(sys.argv[1]); out = sys.argv[2]
MEDIA = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
NAMED = {"distribution": "Distribution", "log_rel_price": "Relative price", "comp_c_promo_share": "Brand C promos",
         "heat_excess": "Hot weeks", "promo_intensity": "Our promos", "media": "Media"}
cols = []  # (label, value, kind) ; kind total/pos/neg/other
for p in ["2022-2023", "2023-2024"]:
    d = t[t.period == p].set_index("driver")
    tot = d.loc["Total change"]
    if not cols: cols.append(("2022", tot.start_m_kg, "total"))
    v = {k: d.m_kg[k] / 1e6 for k in NAMED if k != "media"}
    v["media"] = d.m_kg[MEDIA].sum() / 1e6
    other = (tot.end_m_kg - tot.start_m_kg) - sum(v.values())
    for k in sorted(v, key=lambda k: -v[k]):
        cols.append((NAMED[k], v[k], "pos" if v[k] >= 0 else "neg"))
    cols.append(("All other", other, "other"))
    cols.append((p[-4:], tot.end_m_kg, "total"))
W, H = 1664, 640
BASE, TOP = 140, 182
y0, y1 = 500, 40
Y = lambda v: y0 - (v - BASE) * (y0 - y1) / (TOP - BASE)
n = len(cols); pitch = W / n; bw = pitch * 0.62
C = {"pos": "#2A6FDB", "neg": "#C8372D", "other": "#9AA6B2", "total": "#14213D"}
el = [f'<line x1="0" y1="{y0}" x2="{W}" y2="{y0}" stroke="#14213D" stroke-width="2"/>']
run = None
for i, (lab, val, kind) in enumerate(cols):
    x = i * pitch + (pitch - bw) / 2; cx = i * pitch + pitch / 2
    if kind == "total":
        a, b = BASE, val; run = val; txt = f"{val:.0f}"
    else:
        a, b = run, run + val; run = b; txt = (f"{val:+.1f}").replace("-", "−")
    top, bot = Y(max(a, b)), Y(min(a, b))
    el.append(f'<rect x="{x:.0f}" y="{top:.0f}" width="{bw:.0f}" height="{max(bot - top, 3):.0f}" fill="{C[kind]}"/>')
    tc = {"pos": "#2A6FDB", "neg": "#B4501C", "other": "#6B7785", "total": "#14213D"}[kind]
    fw = "700" if kind == "total" or abs(val) >= 1 else "400"
    el.append(f'<text x="{cx:.0f}" y="{top - 10:.0f}" text-anchor="middle" font-size="24" font-weight="{fw}" fill="{tc}">{txt}</text>')
    if kind == "total":
        el.append(f'<text x="{cx:.0f}" y="{y0 + 34}" text-anchor="middle" font-size="26" font-weight="700" fill="#14213D">{lab}</text>')
    else:
        el.append(f'<text x="{cx + 8:.0f}" y="{y0 + 26}" text-anchor="end" font-size="22" fill="#4A5568" transform="rotate(-40 {cx + 8:.0f} {y0 + 26})">{lab}</text>')
svg = f'<svg aria-label="Volume bridge 2022 to 2024 by driver: 2022 167m kg; relative price −12.8 and Brand C promotions −3.1 bring 2023 to 154m; distribution +20.4 and Brand C promotions +2.6, less relative price −6.6 and hot weeks −2.5, bring 2024 to 166m" viewBox="0 0 {W} {H}" style="position:absolute;left:128px;top:250px;width:{W}px;height:{H}px">\n' + "\n".join(el) + "\n</svg>"
html = f'''<section id="dueto" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">Price cost us volume in 2023; distribution won it back in 2024</h2>
{svg}
<p style="position:absolute;left:128px;bottom:64px;width:1664px;font-size:24px;color:#6B7785">Annual volume, million kg; axis starts at 140m. All other: temperature, rain, calendar, competitor media, unexplained. Source: MMM.</p>
<aside>Read left to right: each bar moves volume from one year to the next. 2022 to 2023 (167m to 154m kg, −8%): the rise in our price relative to competitors cost ~13m kg; Brand C promotions another 3m. 2023 to 2024 (back to 166m, +8%): distribution gains added ~20m kg, more than offsetting a further price effect (−6.6m) and cooler summers (−2.5m). Our promotions and media each add under 1m kg a year. Every bar comes from the marketing mix model; media channels and the remaining drivers are detailed in the appendix.</aside>
</section>
'''
open(out, "w").write(html)
print([(c[0], round(c[1], 1)) for c in cols])
