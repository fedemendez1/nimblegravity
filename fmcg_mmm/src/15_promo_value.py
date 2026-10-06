"""What promotions cost and return: discount given away (volume x (base price - price paid)) vs the volume the model
credits to promo_intensity, and a 'back to 2022 promo levels' scenario on 2024 volumes.

Output: tables/story_promo_value.csv
"""
import pandas as pd

from style import ROOT, TAB

df = pd.read_csv(ROOT / "data/clean/model_data.csv", parse_dates=["time"])
eff = pd.read_csv(TAB / "story_effects.csv").set_index("effect").loc["volume_pct_per_10pp_promo_intensity", ["median", "lo", "hi"]]
df["full"] = df.volume_kg * df.base_price_per_kg
y = df[df.time.dt.year < 2025].groupby(df.time.dt.year).agg(volume=("volume_kg", "sum"), revenue=("value_gbp", "sum"),
                                                              full=("full", "sum"), intensity=("promo_intensity", "mean"))
y["discount"] = y.full - y.revenue
y["discount_pct"] = y.discount / y.full
y24, y22 = y.loc[2024], y.loc[2022]
ppkg = y24.revenue / y24.volume
rows = []
for k, e in eff.items():
    gained = y24.intensity / 0.10 * e / 100 * y24.volume * ppkg            # revenue from promo-driven volume, 2024
    lost = (y24.intensity - y22.intensity) / 0.10 * e / 100 * y24.volume * ppkg
    saved = y24.discount - y22.discount_pct * y24.full
    rows.append({"bound": k, "discount_2024": y24.discount, "promo_revenue_2024": gained, "return_per_gbp": gained / y24.discount,
                 "back_to_2022_discount_saved": saved, "back_to_2022_revenue_lost": lost, "net": saved - lost})
out = pd.DataFrame(rows)
out.round(3).to_csv(TAB / "story_promo_value.csv", index=False)
print(y.round(3).to_string()); print((out.set_index("bound") / [1e6, 1e6, 1, 1e6, 1e6, 1e6]).round(2).to_string())
