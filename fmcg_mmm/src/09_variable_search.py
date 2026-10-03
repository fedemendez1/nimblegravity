"""Search every available variable for residual autocorrelation fixes, using a fast linear surrogate.

The surrogate mirrors the Meridian spec (same drivers/controls, adstocked + saturated media constrained >= 0)
and reproduces its residuals (corr 0.99, same lag-1 ACF), so ~250 candidates can be tested in seconds.
Each candidate (raw column or engineered transform) is added on top of the base + brand C promo model.

Output: tables/search_candidates.csv (acf, Durbin-Watson, BIC, 13-week holdout MAPE, price elasticity, t-stat)
"""
import re

import numpy as np
import pandas as pd
from scipy.optimize import lsq_linear

from style import ROOT, TAB

CHANNELS = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
BASE = ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "comp_media_spend",
        "rainfall", "new_year_week", "season_sin", "season_cos", "comp_c_promo_share"]
# own/competitor sales (outcome or endogenous) and stores selling (moves with demand: r=0.74 with volume)
EXCLUDE = re.compile(r"^(Volume|Unit|Value|Promotion_(Volume|Unit|Value)|Avg_|Number_of_Stores|Promotion_Number"
                     r"|Comp_Brand_._(Volume|Unit|Value|Promotion_(Volume|Unit|Value)|Number))")


def acf1(x):
    x = x - x.mean()
    return float(np.sum(x[1:] * x[:-1]) / np.sum(x * x))


def adstock(x, a=0.5, lag=8):
    w = a ** np.arange(lag + 1)
    return np.convolve(x, w / w.sum())[:len(x)]


def candidates(md, raw):
    c = {k: raw[k] for k in raw.select_dtypes("number") if not EXCLUDE.match(k)}
    log, t, mo = np.log, md.temp_avg, pd.to_datetime(md.time).dt.month
    for b in "ABC":
        p = f"Comp_Brand_{b}_"
        c[f"rel_price_vs_{b}"] = log(raw.Base_Avg_PPKG / raw[p + "Base_Avg_PPKG"])
        c[f"rel_dist_vs_{b}"] = raw.ACV_Weighted_Distribution_wtd / raw[p + "ACV_Weighted_Distribution_wtd"]
        c[f"promo_depth_{b}"] = (1 - raw[p + "Promotion_Avg_PPKG"] / raw[p + "Base_Avg_PPKG"]).clip(lower=0)
    for b in "AB":
        for a in (0.5, 0.8):
            c[f"comp_media_{b}_adstock{a}"] = adstock(raw[f"Comp_Brand_{b}_Media_Spends"].to_numpy(), a, 12)
    for th in (15, 18, 22, 25):
        c[f"heat_above_{th}"] = (raw.weather_max_temp - th).clip(lower=0)
    for a, b in ((5, 10), (10, 15), (15, 20)):
        c[f"temp_segment_{a}_{b}"] = t.clip(a, b) - a
    c["temp_sq"] = t ** 2
    c["diurnal_range"] = raw.weather_max_temp - raw.weather_min_temp
    c["log_rain"] = np.log1p(md.rainfall)
    c["heat_x_season_sin"] = md.heat_excess * md.season_sin
    c["temp_x_season_sin"] = t * md.season_sin
    for k in (2, 4, 8):
        c[f"temp_roll{k}"] = t.rolling(k, min_periods=1).mean()
        c[f"rain_roll{k}"] = md.rainfall.rolling(k, min_periods=1).mean()
    for v in ("temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity"):
        for k in (1, 2, 4):
            c[f"{v}_lag{k}"] = md[v].shift(k).bfill()
    for w in (8, 13):  # short-run price change vs recent reference price
        c[f"rel_price_dev{w}"] = md.log_rel_price - md.log_rel_price.rolling(w, min_periods=1).mean().shift(1).bfill()
    c["post_promo"] = md.promo_intensity.shift(1).bfill() - md.promo_intensity
    for k in (2, 3):
        doy = pd.to_datetime(md.time).dt.dayofyear
        c[f"fourier_sin{k}"], c[f"fourier_cos{k}"] = np.sin(2 * np.pi * k * doy / 365.25), np.cos(2 * np.pi * k * doy / 365.25)
    c["summer_holidays"] = mo.isin([7, 8]).astype(int)
    c["pre_christmas"] = ((mo == 12) & pd.to_datetime(md.time).dt.day.between(13, 26)).astype(int)
    c["easter_week"] = md.time.isin(["2022-04-16", "2023-04-08", "2024-03-30"]).astype(int)
    out = pd.DataFrame(c).replace([np.inf, -np.inf], np.nan).fillna(0)
    return out.loc[:, out.std() > 0]


def fit(X, y, pos, n):
    """Least squares with media >= 0 on the first n rows; returns full residuals, coefs, BIC."""
    s = X.std(0)
    s[s == 0] = 1
    b = lsq_linear(X[:n] / s, y[:n], bounds=(np.where(pos, 0, -np.inf), np.inf)).x / s
    r = y - X @ b
    return r, b, n * np.log((r[:n] ** 2).sum() / n) + X.shape[1] * np.log(n)


if __name__ == "__main__":
    md = pd.read_csv(ROOT / "data/clean/model_data.csv")
    raw = pd.read_excel(ROOT / "data/raw/model_variables.xlsx", sheet_name="Model variables")
    media = np.column_stack([adstock(md[f"exec_{c}"].to_numpy()) for c in CHANNELS])
    media = media / (media + np.nanmedian(np.where(media > 0, media, np.nan), 0))
    y, n_ho = md.volume_kg.to_numpy(), 13
    base = np.column_stack([np.ones(len(md)), md[BASE], media])
    pos = np.r_[np.zeros(1 + len(BASE), bool), np.ones(len(CHANNELS), bool)]
    i_price = 1 + BASE.index("log_rel_price")

    def evaluate(name, X, p):
        r, b, bic = fit(X, y, p, len(y))
        r_ho = fit(X, y, p, len(y) - n_ho)[0][-n_ho:]
        rss = (r ** 2).sum() / (len(y) - X.shape[1])
        se = np.sqrt(rss * np.linalg.pinv(X.T @ X)[-1, -1])
        return {"variable": name, "acf_lag1": acf1(r), "durbin_watson": np.sum(np.diff(r) ** 2) / np.sum((r - r.mean()) ** 2),
                "bic": bic, "holdout_mape": np.mean(np.abs(r_ho) / y[-n_ho:]),
                "price_elasticity": b[i_price] / y.mean(), "t_stat": b[-1] / se}

    rows = [evaluate("(base + brand C promo)", base, pos)]
    for k, x in candidates(md, raw).items():
        rows.append(evaluate(k, np.column_stack([base, x]), np.r_[pos, False]))
    out = pd.DataFrame(rows).round(3).sort_values("acf_lag1")
    out.to_csv(TAB / "search_candidates.csv", index=False)
    print(f"{len(out) - 1} candidates tested")
    print(out.head(15).to_string(index=False))
