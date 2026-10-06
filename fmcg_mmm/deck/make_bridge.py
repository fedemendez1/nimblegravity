import sys, pandas as pd
t = pd.read_csv(sys.argv[1]); out = sys.argv[2]
MEDIA = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
BASE = {"distribution": "Distribution", "log_rel_price": "Relative price", "comp_c_promo_share": "Brand C promos",
        "heat_excess": "Hot weeks"}
INCR = {"promo_intensity": "Our promos", "media": "Media"}
W, H = 1664, 640
PW, GAP = 792, 80
LO, HI = -12, 18
y0t, y1t = 470, 50
Y = lambda v: y0t - (v - LO) * (y0t - y1t) / (HI - LO)
el = []
for j, p in enumerate(["2022-2023", "2023-2024"]):
    d = t[t.period == p].set_index("driver").pct
    tot = d["Total change"]
    b = {k: d[k] for k in BASE}
    inc = {"promo_intensity": d["promo_intensity"], "media": d[MEDIA].sum()}
    other = tot - sum(b.values()) - sum(inc.values())
    cols = [(BASE[k], b[k], "base") for k in sorted(b, key=lambda k: -b[k])]
    cols += [("All other", other, "other")]
    cols += [(INCR[k], inc[k], "incr") for k in sorted(inc, key=lambda k: -inc[k])]
    cols += [("Total", tot, "total")]
    x0 = j * (PW + GAP); pitch = PW / len(cols); bw = pitch * 0.62
    y_a, y_b = p[:4], p[-4:]
    el.append(f'<text x="{x0}" y="22" font-size="28" font-weight="700" fill="#14213D">{y_b} vs {y_a}: volume {f"{tot:+.1f}".replace("-", "−")}%</text>')
    el.append(f'<line x1="{x0}" y1="{Y(0):.0f}" x2="{x0 + PW}" y2="{Y(0):.0f}" stroke="#14213D" stroke-width="2"/>')
    xi = x0 + pitch * 5
    el.append(f'<line x1="{xi:.0f}" y1="44" x2="{xi:.0f}" y2="{y0t + 10}" stroke="#B8C2CC" stroke-width="2" stroke-dasharray="6 6"/>')
    el.append(f'<text x="{x0 + pitch * 2.5:.0f}" y="62" text-anchor="middle" font-size="22" fill="#6B7785">Base</text>')
    el.append(f'<text x="{xi + pitch * 1.5:.0f}" y="62" text-anchor="middle" font-size="22" fill="#6B7785">Incremental</text>')
    run = 0
    for i, (lab, val, kind) in enumerate(cols):
        x = x0 + i * pitch + (pitch - bw) / 2; cx = x0 + i * pitch + pitch / 2
        if kind == "total":
            a, bb = 0, val; col = "#14213D"; tc = "#14213D"
        else:
            a, bb = run, run + val; run = bb
            if kind == "other": col, tc = "#9AA6B2", "#6B7785"
            elif kind == "incr": col, tc = ("#8FB4F0", "#2A6FDB") if val >= 0 else ("#F0A47A", "#B4501C")
            else: col, tc = ("#2A6FDB", "#2A6FDB") if val >= 0 else ("#C8372D", "#B4501C")
        top, bot = Y(max(a, bb)), Y(min(a, bb))
        el.append(f'<rect x="{x:.0f}" y="{top:.0f}" width="{bw:.0f}" height="{max(bot - top, 3):.0f}" fill="{col}"/>')
        txt = f"{val:+.1f}".replace("-", "−")
        fw = "700" if kind == "total" or abs(val) >= 1 else "400"
        ty = top - 10 if val >= 0 or kind == "total" and val >= 0 else bot + 28
        el.append(f'<text x="{cx:.0f}" y="{ty:.0f}" text-anchor="middle" font-size="24" font-weight="{fw}" fill="{tc}">{txt}</text>')
        el.append(f'<text x="{cx + 8:.0f}" y="{y0t + 40}" text-anchor="end" font-size="22" font-weight="{700 if kind == "total" else 400}" fill="#4A5568" transform="rotate(-40 {cx + 8:.0f} {y0t + 40})">{lab}</text>')
svg = f'<svg aria-label="Volume due-to, weekly average. 2023 vs 2022, volume −6.4%: relative price −8.2 points, Brand C promotions −1.9, hot weeks −0.6, distribution +2.4; our promotions +0.6, media +0.5. 2024 vs 2023, volume +8.2%: distribution +13.3 points, Brand C promotions +1.7, hot weeks −1.6, relative price −4.3; media +0.6, our promotions +0.3. Two years net: volume +1.3%." viewBox="0 0 {W} {H}" style="position:absolute;left:128px;top:240px;width:{W}px;height:{H}px">\n' + "\n".join(el) + "\n</svg>"
html = f'''<section id="dueto" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">Volume due-to: price took it, distribution gave it back</h2>
{svg}
<p style="position:absolute;left:128px;bottom:64px;width:1664px;font-size:24px;color:#6B7785">Points of change in average weekly volume vs the previous year (2022 had 53 weeks). All other: temperature, rain, calendar, unexplained.</p>
<aside>How to read a due-to: average weekly volume changed by X% versus last year; each bar is how many of those percentage points are due to each driver, and they add up to the total. Weekly averages are used because 2022 had 53 weeks. 2023 vs 2022, volume −6.4%: the rise in our price relative to competitors took 8.2 points and Brand C promotions 1.9; distribution gave back 2.4. 2024 vs 2023, volume +8.2%: distribution added 13.3 points and Brand C easing its promotions 1.7; relative price took 4.3 and cooler summers 1.6. Over the two years volume ended roughly where it started (+1.3% per week): price took about 12 points, distribution gave back about 16. Base drivers move volume by whole points; promotions and media by fractions of a point. Every bar comes from the marketing mix model; media channels are detailed in the appendix.</aside>
</section>
'''
open(out, "w").write(html)
