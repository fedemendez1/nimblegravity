"""Load raw weekly data, run sanity checks and build the modelling dataset."""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/model_variables.xlsx"
OUT = ROOT / "data/clean/model_data.csv"

# group -> (raw channels, execution columns). None = no execution metric, spend is used.
# TV kept apart from digital video: GRPs and impressions don't add up.
CHANNEL_GROUPS = {
    "tv": (["TV"], ["Media_TV_GRPs"]),
    "digital_video": (["VOD", "OLV"], ["Media_VOD_Impressions", "Media_OLV_Impressions"]),
    "social": (["Social"], ["Media_Social_Impressions"]),
    "partnership": (["Online_Partnership"], None),
    "ooh": (["OOH"], None),
    "search_rdm": (["Search", "RDM"], ["Media_Search_Impressions", "Media_RDM_Impressions"]),
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
        # logs so coefficients read as elasticities
        "log_base_price": np.log(df["Base_Avg_PPKG"]),
        "log_comp_price": np.log(comp_price),
        # own and competitor log prices share the inflation trend (r=0.86): model the premium instead
        "log_rel_price": np.log(df["Base_Avg_PPKG"] / comp_price),
        "promo_share_vol": df["Promotion_Volume_Sales"] / df["Volume_Sales"],
        "promo_distribution": df["Promotion_ACV_Weighted_Distribution_wtd"],
        "promo_depth": depth,
        # discount x share of stores on promo: one promo pressure measure
        "promo_intensity": depth * df["Promotion_ACV_Weighted_Distribution_wtd"] / 100,
        "distribution": df["ACV_Weighted_Distribution_wtd"],
        "temp_avg": df["weather_average_temp"],
        # degrees above 20C weekly max: captures heatwave peaks a linear temp term misses
        "heat_excess": (df["weather_max_temp"] - 20).clip(lower=0),
        "rainfall": df["weather_rainfall"],
        "comp_media_spend": df["Comp_Brand_A_Media_Spends"] + df["Comp_Brand_B_Media_Spends"],
        "comp_volume_kg": comp_vol.sum(1),
        # annual seasonality beyond temperature (spring sells more than autumn at equal temp)
        "season_sin": np.sin(2 * np.pi * df["Date"].dt.dayofyear / 365.25),
        "season_cos": np.cos(2 * np.pi * df["Date"].dt.dayofyear / 365.25),
        # week starting 27 Dec - 2 Jan: ~20% volume drop every year
        "new_year_week": (((df["Date"].dt.month == 12) & (df["Date"].dt.day >= 27))
                          | ((df["Date"].dt.month == 1) & (df["Date"].dt.day <= 2))).astype(int),
    })
    for grp, (chs, exe) in CHANNEL_GROUPS.items():
        for ch in chs:
            out[f"raw_spend_{ch}"] = df[f"Media_{ch}_Spends"]
        out[f"spend_{grp}"] = out[[f"raw_spend_{c}" for c in chs]].sum(1)
        out[f"exec_{grp}"] = df[exe].sum(1) if exe else out[f"spend_{grp}"]
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
