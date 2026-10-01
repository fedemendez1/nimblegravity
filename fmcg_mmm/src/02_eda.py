"""A handful of descriptive charts that frame the modelling choices."""
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from style import CHANNEL_COLORS, INK_2, ROOT, SERIES, TAB, save

df = pd.read_csv(ROOT / "data/clean/model_data.csv", parse_dates=["time"])
t = df["time"]

# 1. Sales vs temperature
fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 5.5), sharex=True, height_ratios=[2, 1])
a1.plot(t, df.volume_kg / 1e6, color=SERIES[0])
a1.set(title="Weekly volume sales (m kg)", ylabel="m kg")
a2.plot(t, df.temp_avg, color=SERIES[1])
a2.set(title="Average temperature (°C)", ylabel="°C")
a2.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
save(fig, "eda_01_sales_temperature")

# 2. Pricing and promotions
fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 5.5), sharex=True)
a1.plot(t, df.base_price_per_kg, color=SERIES[0], label="Brand base price")
a1.plot(t, df.comp_price_per_kg, color=SERIES[1], label="Competitor avg price")
a1.set(title="Price per kg (£)", ylabel="£ / kg")
a1.legend(loc="upper left")
a2.plot(t, df.promo_share_vol * 100, color=SERIES[2])
a2.set(title="Share of volume sold on promotion (%)", ylabel="%")
a2.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
save(fig, "eda_02_price_promo")

# 3. Media flighting by channel group
groups = list(CHANNEL_COLORS)
fig, ax = plt.subplots(figsize=(10, 4))
bottom = np.zeros(len(df))
for g in groups:
    v = df[f"spend_{g}"] / 1e3
    ax.bar(t, v, bottom=bottom, width=6, color=CHANNEL_COLORS[g], label=g, edgecolor="white", linewidth=0.5)
    bottom += v
ax.set(title="Weekly media spend by channel group (£k)", ylabel="£k")
ax.legend(ncol=5, loc="upper left")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
save(fig, "eda_03_media_flighting")

# 4. Volume vs temperature
fig, ax = plt.subplots(figsize=(6, 4.5))
ax.scatter(df.temp_avg, df.volume_kg / 1e6, s=24, color=SERIES[0], edgecolor="white", linewidth=1)
r = df[["temp_avg", "volume_kg"]].corr().iloc[0, 1]
ax.set(title=f"Volume vs temperature (r = {r:.2f})", xlabel="Average temperature (°C)", ylabel="Volume (m kg)")
save(fig, "eda_04_volume_vs_temperature")

# Summary tables
yearly = (df.assign(year=t.dt.year, media=df.filter(regex="^spend_").sum(1))
          .groupby("year")
          .agg(weeks=("time", "size"), volume_m_kg=("volume_kg", lambda x: x.sum() / 1e6),
               value_m_gbp=("value_gbp", lambda x: x.sum() / 1e6), base_price=("base_price_per_kg", "mean"),
               comp_price=("comp_price_per_kg", "mean"), promo_share=("promo_share_vol", "mean"),
               distribution=("distribution", "mean"), temp=("temp_avg", "mean"),
               media_k_gbp=("media", lambda x: x.sum() / 1e3))
          .round(3))
yearly.to_csv(TAB / "eda_yearly_summary.csv")
print(yearly.to_string())

drivers = ["temp_avg", "rainfall", "distribution", "base_price_per_kg", "rel_base_price", "promo_depth",
           "promo_distribution", "comp_media_spend"] + [f"spend_{g}" for g in groups]
corr = df[drivers + ["volume_kg"]].corr()["volume_kg"].drop("volume_kg").sort_values()
corr.round(3).to_csv(TAB / "eda_correlations.csv")
print(corr.round(2).to_string())
