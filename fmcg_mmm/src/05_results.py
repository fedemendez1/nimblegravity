"""Numbers behind the presentation, all from the final model.

- media ROI by campaign: channels that ran together are read jointly
- year-on-year due-to by driver, on weekly averages (2022 has 53 weeks)
- per-unit effects and yearly facts
- what promotions cost and return, and a "back to 2022 promotion levels" scenario
- weekly actual vs fitted
"""
import importlib
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from meridian.analysis import analyzer

from style import MODELS, ROOT, TAB

diag = importlib.import_module("04_diagnostics")

CAMPAIGNS = {"2023 burst: TV + partnership": ["tv", "partnership"],
             "2024 launch: digital video + OOH": ["digital_video", "ooh"],
             "Always-on social": ["social"], "Search + retail digital": ["search_rdm"],
             "All media": ["tv", "partnership", "digital_video", "ooh", "social", "search_rdm"]}
q = lambda x: dict(zip(["median", "lo", "hi"], (np.median(x), np.quantile(x, 0.05), np.quantile(x, 0.95))))


def campaign_roi(mmm, a, df):
    ch = list(mmm.input_data.media_channel.values)
    inc = np.asarray(a.incremental_outcome(use_kpi=False))  # media channels first, then drivers
    inc = inc.reshape(-1, inc.shape[-1])[:, :len(ch)]
    spend = df[[f"spend_{c}" for c in ch]].sum().to_numpy()
    rows = []
    for name, chs in CAMPAIGNS.items():
        i = [ch.index(c) for c in chs]
        r = inc[:, i].sum(1) / spend[i].sum()
        rows.append({"campaign": name, "spend_k": spend[i].sum() / 1e3, "incr_revenue_k": np.median(inc[:, i].sum(1)) / 1e3,
                     **q(r), "p_roi_above_1": (r > 1).mean()})
    return pd.DataFrame(rows)


def weekly_terms(mmm, a, df):
    """Draws x weeks volume (kg) from each driver, control and media channel."""
    post, sd_y = mmm.inference_data.posterior, df.volume_kg.std(ddof=0)
    terms = {}
    for var, dim in (("gamma_n", "non_media_channel"), ("gamma_c", "control_variable")):
        for n in map(str, post[var][dim].values):
            g = post[var].sel({dim: n}).values.ravel()
            terms[n] = np.outer(g * sd_y / df[n].std(ddof=0), df[n].to_numpy())
    inc = np.asarray(a.incremental_outcome(use_kpi=True, aggregate_times=False))
    inc = inc.reshape(-1, *inc.shape[-2:])
    for i, c in enumerate(mmm.input_data.media_channel.values):
        terms[str(c)] = inc[..., i]
    return terms


def due_to(df, terms):
    yr = pd.to_datetime(df.time).dt.year.to_numpy()
    rows = []
    for y0, y1 in ((2022, 2023), (2023, 2024)):
        v0, v1 = df.volume_kg[yr == y0].mean(), df.volume_kg[yr == y1].mean()
        explained = 0
        for k, c in terms.items():
            x = (c[:, yr == y1].mean(1) - c[:, yr == y0].mean(1)) / v0 * 100
            rows.append({"period": f"{y0}-{y1}", "driver": k, **q(x)})
            explained += np.median(x)
        total = (v1 / v0 - 1) * 100
        rows.append({"period": f"{y0}-{y1}", "driver": "unexplained", "median": total - explained})
        rows.append({"period": f"{y0}-{y1}", "driver": "total", "median": total})
    return pd.DataFrame(rows).rename(columns={"median": "pct_points"})


def effects(mmm, df):
    post, sd_y, mu = mmm.inference_data.posterior, df.volume_kg.std(ddof=0), df.volume_kg.mean()
    g = lambda n: post.gamma_n.sel(non_media_channel=n).values.ravel() * sd_y / df[n].std(ddof=0) / mu * 100
    out = {
        "volume_pct_per_distribution_point": g("distribution"),
        "volume_pct_per_degree_above_20C": g("heat_excess"),
        "price_elasticity_vs_competitors": g("log_rel_price") / 100,
        "volume_pct_per_10pp_promo_intensity": g("promo_intensity") * 0.10,
    }
    return pd.DataFrame([{"effect": k, **q(v)} for k, v in out.items()])


def yearly_facts(df):
    d = df.assign(year=pd.to_datetime(df.time).dt.year)
    d = d[d.year < 2025]
    return d.groupby("year").apply(lambda x: pd.Series({
        "weeks": len(x), "volume_m_kg": x.volume_kg.sum() / 1e6, "value_m_gbp": x.value_gbp.sum() / 1e6,
        "shelf_price_per_kg": x.base_price_per_kg.mean(), "price_paid_per_kg": x.value_gbp.sum() / x.volume_kg.sum(),
        "premium_vs_competitors": x.rel_base_price.mean(), "promo_share": x.promo_share_vol.mean(),
        "distribution": x.distribution.mean(), "media_k_gbp": x.filter(regex="^spend_").sum().sum() / 1e3}))


def promo_value(df, eff):
    """Discount given = volume x (shelf price - price paid); promo revenue from the promo_intensity effect."""
    e = eff.set_index("effect").loc["volume_pct_per_10pp_promo_intensity", ["median", "lo", "hi"]]
    d = df.assign(year=pd.to_datetime(df.time).dt.year, full=df.volume_kg * df.base_price_per_kg)
    y = d[d.year < 2025].groupby("year").agg(volume=("volume_kg", "sum"), revenue=("value_gbp", "sum"),
                                            full=("full", "sum"), intensity=("promo_intensity", "mean"))
    y["discount"] = y.full - y.revenue
    y24, y22 = y.loc[2024], y.loc[2022]
    ppkg = y24.revenue / y24.volume
    saved = y24.discount - y22.discount / y22.full * y24.full
    rows = []
    for bound, b in e.items():
        gained = y24.intensity / 0.10 * b / 100 * y24.volume * ppkg
        lost = (y24.intensity - y22.intensity) / 0.10 * b / 100 * y24.volume * ppkg
        rows.append({"bound": bound, "discount_2024": y24.discount, "promo_revenue_2024": gained,
                     "return_per_gbp": gained / y24.discount, "back_to_2022_discount_saved": saved,
                     "back_to_2022_revenue_lost": lost, "back_to_2022_net": saved - lost})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    mmm = diag.load()
    a = analyzer.Analyzer(mmm)
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    df["resid_lag1"] = pd.read_csv(MODELS / "stage1_residuals.csv").resid_kg.shift(1).fillna(0).to_numpy()

    outputs = {"results_campaign_roi": campaign_roi(mmm, a, df),
               "results_due_to": due_to(df, weekly_terms(mmm, a, df)),
               "results_effects": effects(mmm, df),
               "results_yearly_facts": yearly_facts(df)}
    outputs["results_promo_value"] = promo_value(df, outputs["results_effects"])
    exp, act, e = diag.fitted(a)
    outputs["results_fit_weekly"] = pd.DataFrame({
        "time": df.time, "actual_m_kg": act / 1e6, "fitted_m_kg": exp / 1e6,
        "lo_m_kg": e.expected.sel(metric="ci_lo").values / 1e6, "hi_m_kg": e.expected.sel(metric="ci_hi").values / 1e6})

    for name, t in outputs.items():
        t.round(4).to_csv(TAB / f"{name}.csv", index=name == "results_yearly_facts")
        print(f"== {name}\n{t.round(3).to_string()}\n")
