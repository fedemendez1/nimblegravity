import datetime as dt, sys
out = sys.argv[1]
D = lambda s: dt.date.fromisoformat(s)
def doy(d): return (d - dt.date(d.year, 1, 1)).days / 365
W = 1060; LX = 220; RX = 1050; MW = (RX - LX)
X = lambda f: LX + f * MW
rows = ["TV", "Digital video", "Social", "Partnership", "Outdoor", "Search"]
asrun = {"TV": [("2023-04-15", "2023-05-05")],
         "Digital video": [("2024-04-13", "2024-05-31"), ("2024-07-06", "2024-10-11"), ("2024-11-09", "2024-12-27")],
         "Social": [("2022-10-01", "2022-11-18"), ("2023-09-23", "2023-11-03"), ("2024-07-13", "2024-11-01")],
         "Partnership": [("2023-03-11", "2023-06-09")],
         "Outdoor": [("2024-04-13", "2024-04-26"), ("2024-05-11", "2024-05-17"), ("2024-07-27", "2024-08-09")],
         "Search": [("2024-01-06", "2024-11-08")]}
prop = {"TV": [("2025-05-19", "2025-06-08")],
        "Digital video": [("2025-06-09", "2025-06-29"), ("2025-07-21", "2025-08-10")],
        "Social": [("2025-06-30", "2025-07-20"), ("2025-08-11", "2025-08-31")],
        "Partnership": [("2025-05-05", "2025-05-25")],
        "Search": [("2025-01-06", "2025-12-20")]}
light = {"Digital video": [("2025-01-06", "2025-05-04"), ("2025-09-08", "2025-12-20")],
         "Outdoor": [("2025-01-06", "2025-05-04"), ("2025-09-15", "2025-12-20")]}
heat = ("2025-06-01", "2025-08-31")
idx = [86, 84, 91, 98, 108, 124, 123, 129, 102, 89, 86, 80]
el = []
el.append(f'<rect x="{X(4/12):.0f}" y="40" width="{MW*4/12:.0f}" height="540" fill="#E3ECFB"/>')
el.append(f'<text x="{X(6/12):.0f}" y="28" text-anchor="middle" font-size="24" font-weight="700" fill="#2A6FDB">Peak: May–Aug</text>')
for m, l in enumerate("JFMAMJJASOND"):
    el.append(f'<text x="{X((m+.5)/12):.0f}" y="604" text-anchor="middle" font-size="22" fill="#6B7785">{l}</text>')
el.append('<text x="0" y="104" font-size="24" fill="#4A5568">Demand</text>')
for m, v in enumerate(idx):
    h = (v - 70) * 1.2
    el.append(f'<rect x="{X(m/12)+10:.0f}" y="{124-h:.0f}" width="{MW/12-20:.0f}" height="{h:.0f}" fill="#9AB8E8"/>')
def panel(y, title, data, color):
    el.append(f'<text x="0" y="{y}" font-size="26" font-weight="700" fill="#14213D">{title}</text>')
    for i, r in enumerate(rows):
        yy = y + 14 + i * 30
        el.append(f'<text x="0" y="{yy+19}" font-size="22" fill="#4A5568">{r}</text>')
        el.append(f'<line x1="{LX}" y1="{yy+12}" x2="{RX}" y2="{yy+12}" stroke="#DCE3E8" stroke-width="1"/>')
        for a, b in data.get(r, []):
            a, b = D(a), D(b)
            el.append(f'<rect x="{X(doy(a)):.0f}" y="{yy+2}" width="{max(X(doy(b))-X(doy(a)),6):.0f}" height="20" rx="4" fill="{color}"/>')
        if data is prop:
            for a, b in light.get(r, []):
                a, b = D(a), D(b)
                el.append(f'<rect x="{X(doy(a)):.0f}" y="{yy+8}" width="{X(doy(b))-X(doy(a)):.0f}" height="8" rx="4" fill="#9AB8E8"/>')
    return y + 14
panel(166, "As run, 2022–24 (one calendar)", asrun, "#4A5568")
y2 = panel(398, "Proposed", prop, "#2A6FDB")
yo = y2 + 4 * 30
el.append(f'<rect x="{X(doy(D(heat[0]))):.0f}" y="{yo+2}" width="{X(doy(D(heat[1])))-X(doy(D(heat[0]))):.0f}" height="20" rx="4" fill="none" stroke="#2A6FDB" stroke-width="3" stroke-dasharray="10 6"/>')
el.append(f'<text x="{X(doy(D("2025-07-16"))):.0f}" y="{yo+17}" text-anchor="middle" font-size="20" font-weight="700" fill="#2A6FDB">if 25°C+ forecast</text>')
svg = f'<svg aria-label="Media timeline. As run: spend spread across the year, a TV burst in April, digital video and social into November and December, channels launched together. Proposed: about 80% of the budget in staggered 3-week pulses from mid-May to August, outdoor triggered by heatwave forecasts, light always-on video and outdoor the rest of the year, and search on all year." viewBox="0 0 {W} 612" style="position:absolute;left:128px;top:236px;width:{W}px;height:612px">\n' + "\n".join(el) + "\n</svg>"
html = f'''<section id="media" data-transition="fade" style="background:#F6F7F4;color:#14213D;font-family:'DM Sans', Arial, sans-serif;padding:128px 128px 160px;display:flex;flex-direction:column;gap:24px">
<h2 style="font-family:'Domine', Georgia, serif;font-size:56px;font-weight:700;line-height:1.15">3. Re-time media: same budget, run when shoppers buy</h2>
{svg}
<div style="position:absolute;left:1236px;top:236px;width:556px;height:612px;display:flex;flex-direction:column;justify-content:center;gap:28px;background:#FFFFFF;border:1px solid #DCE3E8;border-radius:16px;padding:40px">
<h3 style="font-size:32px;font-weight:700">What changes</h3>
<p style="font-size:28px;line-height:1.35"><b>80% in May–August</b>, in 3-week pulses, one channel after another</p>
<p style="font-size:28px;line-height:1.35"><b>20% always on</b>, light video and outdoor the rest of the year to keep the brand present</p>
<p style="font-size:28px;line-height:1.35"><b>Digital outdoor on standby</b>: screens pre-booked for summer, live only in weeks forecast above 25°C</p>
</div>
<p style="position:absolute;left:128px;top:868px;width:1664px;font-size:32px;font-weight:700;color:#2A6FDB">About +£0.5–1m a year at the same budget, and every channel measurable with regional holdouts.</p>
<p style="position:absolute;left:128px;bottom:64px;width:1664px;font-size:24px;color:#6B7785">Demand = average weekly volume by month. Media returns £1.06 per £1 today (short term, before margin). Source: MMM.</p>
<aside>What changes, and why. 1) Timing: today 60% of media money runs outside May to August, including a TV burst in cold April and almost a third of digital video in November and December. The model's early read is that media works about twice as hard in summer and heatwave weeks; moving the off-season money into the peak is worth roughly +£0.5m to +£1m a year at the same budget (early read, wide range). 2) Pulses: 3-week bursts instead of thin, long flights. Today's digital weight is very low (about 0.02 to 0.24 impressions per adult per week), well below an effective frequency; concentrating it gives each burst a chance to register. Carry-over is short (one to two weeks), so be on air in the peak itself, starting mid-May. 3) Stagger channels: TV, then video, then social, so the next model can read each one; today video and outdoor launched the same week and TV ran inside the partnership flight. 4) Digital outdoor on standby: screens and budget agreed in advance for June to August, but the copy only goes live in weeks when the 3-to-5-day forecast is above about 25°C (dynamic, weather-triggered buying, which digital outdoor and online video platforms support). Hot weeks sell far more, so that is when each impression has the most shoppers to reach. 5) Brand presence: keep about 20% of the budget as a light always-on layer in brand-building channels (online video and outdoor, the most cost-efficient reach) outside the peak, so the brand does not go dark for eight months; search stays on all year as demand capture, not as brand building (it is tiny: about £10k a year); long-term brand effects are not measurable with this data, so we do not recommend switching off entirely. 6) Measure: hold out one or two ITV regions in each pulse (geo lift) to calibrate the model. Media is a small lever for this brand (under 1% of volume): the point is to make it pay and to measure it, not to expect it to move the total.</aside>
</section>
'''
open(out, "w").write(html)
print(len(svg))
