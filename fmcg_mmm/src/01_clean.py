"""Load raw weekly data, run sanity checks and build the modelling dataset."""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/model_variables.xlsx"
OUT = ROOT / "data/clean/model_data.csv"

CHANNEL_GROUPS = {
    "video": ["TV", "VOD", "OLV"],
    "social": ["Social"],
    "partnership": ["Online_Partnership"],
    "ooh": ["OOH"],
    "search_rdm": ["Search", "RDM"],
}


def check(df):
    assert df["Date"].is_unique and df["Date"].diff().dropna().dt.days.eq(7).all(), "weeks not contiguous"
    assert not df.isna().any().any(), "missing values"
    num = df.select_dtypes("number")
    assert (num.drop(columns=[c for c in num if c.startswith("weather")]) >= 0).all().all(), "negative values"
    # identity checks: value = volume * price, promo <= total
    gap = (df["Value_Sales"] / (df["Volume_Sales"] * df["Avg_PPKG"]) - 1).abs().max()
    print(f"max |value / (volume * ppkg) - 1|: {gap:.4f}")
    assert (df["Promotion_Volume_Sales"] <= df["Volume_Sales"]).all()


def build(df):
    comps = ["A", "B", "C"]
    comp_vol = df[[f"Comp_Brand_{c}_Volume_Sales" for c in comps]].to_numpy()
    comp_ppkg = df[[f"Comp_Brand_{c}_Avg_PPKG" for c in comps]].to_numpy()
    comp_price = (comp_vol * comp_ppkg).sum(1) / comp_vol.sum(1)

    # promo price above base price in a few weeks -> no real discount
    depth = (1 - df["Promotion_Avg_PPKG"] / df["Base_Avg_PPKG"]).clip(lower=0)

    out = pd.DataFrame({
        "time": df["Date"].dt.strftime("%Y-%m-%d"),
        "volume_kg": df["Volume_Sales"],
        "value_gbp": df["Value_Sales"],
        "price_per_kg": df["Avg_PPKG"],
        "base_price_per_kg": df["Base_Avg_PPKG"],
        "comp_price_per_kg": comp_price,
        "rel_base_price": df["Base_Avg_PPKG"] / comp_price,
        "promo_share_vol": df["Promotion_Volume_Sales"] / df["Volume_Sales"],
        "promo_distribution": df["Promotion_ACV_Weighted_Distribution_wtd"],
        "promo_depth": depth,
        "distribution": df["ACV_Weighted_Distribution_wtd"],
        "temp_avg": df["weather_average_temp"],
        # degrees above 20C weekly max: captures heatwave peaks a linear temp term misses
        "heat_excess": (df["weather_max_temp"] - 20).clip(lower=0),
        "rainfall": df["weather_rainfall"],
        "comp_media_spend": df["Comp_Brand_A_Media_Spends"] + df["Comp_Brand_B_Media_Spends"],
        "comp_volume_kg": comp_vol.sum(1),
        # week starting 27 Dec - 2 Jan: ~20% volume drop every year
        "new_year_week": (((df["Date"].dt.month == 12) & (df["Date"].dt.day >= 27))
                          | ((df["Date"].dt.month == 1) & (df["Date"].dt.day <= 2))).astype(int),
    })
    for ch in [c for g in CHANNEL_GROUPS.values() for c in g]:
        out[f"raw_spend_{ch}"] = df[f"Media_{ch}_Spends"]
    for grp, chs in CHANNEL_GROUPS.items():
        out[f"spend_{grp}"] = out[[f"raw_spend_{c}" for c in chs]].sum(1)
    return out


if __name__ == "__main__":
    raw = pd.read_excel(RAW, sheet_name="Model variables")
    check(raw)
    clean = build(raw)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(OUT, index=False)

    spend = clean.filter(regex="^(raw_)?spend_")
    print(f"{len(clean)} weeks: {clean.time.iloc[0]} -> {clean.time.iloc[-1]}")
    print(pd.DataFrame({"total_gbp": spend.sum().round(), "active_weeks": (spend > 0).sum()}))
    print(f"media / value sales: {clean.filter(regex='^spend_').sum().sum() / clean.value_gbp.sum():.2%}")
    print(f"weeks with promo depth clipped to 0: {(clean.promo_depth == 0).sum()}")
