"""Weekly view of own promotions: what the model sees once every other driver is removed."""
import sys
import numpy as np, pandas as pd, matplotlib.pyplot as plt

d = pd.read_csv("../data/clean/model_data.csv", parse_dates=["time"])
fit = pd.read_csv("../outputs/tables/story_fit.csv", parse_dates=["time"])
eff = pd.read_csv("../outputs/tables/story_effects.csv").set_index("effect").loc["volume_pct_per_10pp_promo_intensity"]
d = d.merge(fit, on="time")
mu = d.volume_kg.mean()
slope = eff[["median", "lo", "hi"]].astype(float) * 10  # % of mean volume per 1.0 of intensity
# partial residual: actual minus everything the model explains except own promotions
d["promo_only"] = (d.actual_m_kg - d.fitted_m_kg) * 1e6 / mu * 100 + slope["median"] * d.promo_intensity
d["x"] = d.promo_intensity * 100
print(slope.round(1).to_dict(), "corr", round(d[["x", "promo_only"]].corr().iloc[0, 1], 2))

ink, muted, grid, blue, orange, bg = "#14213D", "#6B7785", "#DCE3E8", "#2A6FDB", "#D9662B", "#F6F7F4"
fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 8.4), facecolor=bg, gridspec_kw={"height_ratios": [1, 1.5], "hspace": 0.55})
for a in (a1, a2):
    a.set_facecolor(bg)
    for s in ("top", "right", "left"):
        a.spines[s].set_visible(False)
    a.spines["bottom"].set_color(grid)
    a.tick_params(colors=muted, length=0)
    a.grid(axis="y", color=grid, linewidth=0.8)
    a.set_axisbelow(True)

a1.plot(d.time, d.x, color=orange, linewidth=2)
a1.set_title("1. Promotion intensity each week (depth × breadth, %)", loc="left", fontsize=12, color=ink, fontweight="bold", pad=26)
a1.text(0, 1.02, "Promotions switch on and off week to week: that variation is what the model uses", transform=a1.transAxes, fontsize=10, va="bottom", color=muted)

a2.axhline(0, color=muted, linewidth=1)
a2.scatter(d.x, d.promo_only, s=60, color=orange, alpha=0.75, edgecolor=bg, linewidth=1.5)
xs = np.linspace(0, d.x.max(), 50)
a2.fill_between(xs, slope["lo"] * xs / 100, slope["hi"] * xs / 100, color=blue, alpha=0.15, linewidth=0)
a2.plot(xs, slope["median"] * xs / 100, color=blue, linewidth=2.5)
a2.set_xlabel("Promotion intensity that week (%)", color=muted)
a2.set_ylabel("Volume vs normal week (%)", color=muted)
a2.set_title("2. Weekly volume after removing price, distribution, weather, season and media", loc="left", fontsize=12, color=ink, fontweight="bold", pad=26)
a2.text(0, 1.02, "Each dot is a week. Blue line = model's promotion effect (band = 90% range): nearly flat", transform=a2.transAxes, fontsize=10, va="bottom", color=muted)
fig.savefig(sys.argv[1], dpi=150, facecolor=bg, bbox_inches="tight")
