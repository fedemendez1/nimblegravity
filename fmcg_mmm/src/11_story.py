"""Numbers for the client story, all from the final model posterior (mmm_final).

- campaign ROI: channels that ran together are read jointly (their split is not identified)
- year-on-year due-to: change in annual volume explained by each driver (no arbitrary reference level)
- per-unit effects quoted in the deck (distribution point, heatwave degree, price premium, promo)
- weekly actual vs fitted for the model-performance slide

Outputs: tables/story_campaign_roi.csv, story_due_to.csv, story_effects.csv, story_fit.csv
"""
import importlib
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from meridian.analysis import analyzer

from style import ROOT, TAB

diag = importlib.import_module("04_diagnostics")

CAMPAIGNS = {"2023 burst: TV + partnership": ["tv", "partnership"], "2024 launch: digital video + OOH": ["digital_video", "ooh"],
             "Always-on social": ["social"], "Search + retail digital": ["search_rdm"],
             "All media": ["tv", "partnership", "digital_video", "ooh", "social", "search_rdm"]}
GROUPS = {"Distribution": ["distribution"], "Price premium": ["log_rel_price"],
          "Weather": ["temp_avg", "heat_excess", "rainfall"], "Own promotions": ["promo_intensity"],
          "Competitors": ["comp_media_spend", "comp_c_promo_share"], "Media": ["media"],
          "Calendar & other": ["season_sin", "season_cos", "new_year_week", "resid_lag1"]}
q = lambda x: (float(np.median(x)), float(np.quantile(x, 0.05)), float(np.quantile(x, 0.95)))


def campaign_roi(mmm, a, df):
    ch = list(mmm.input_data.media_channel.values)
    inc = np.asarray(a.incremental_outcome(use_kpi=False))  # revenue; media channels first, then non-media
    inc = inc.reshape(-1, inc.shape[-1])[:, :len(ch)]
    spend = df[[f"spend_{c}" for c in ch]].sum().to_numpy()
    rows = []
    for name, chs in CAMPAIGNS.items():
        i = [ch.index(c) for c in chs]
        r = inc[:, i].sum(1) / spend[i].sum()
        rows.append({"campaign": name, "spend_k": spend[i].sum() / 1e3, "incr_revenue_k": np.median(inc[:, i].sum(1)) / 1e3,
                     **dict(zip(["roi", "roi_lo", "roi_hi"], q(r))), "p_roi_above_1": float((r > 1).mean())})
    return pd.DataFrame(rows)


def weekly_contributions(mmm, a, df):
    """Draws x weeks contribution (kg) per driver group; drivers are linear on standardised scales."""
    post, sd_y = mmm.inference_data.posterior, df.volume_kg.std(ddof=0)
    terms = {}
    for var, dim in (("gamma_n", "non_media_channel"), ("gamma_c", "control_variable")):
        for n in map(str, post[var][dim].values):
            g = post[var].sel({dim: n}).values.ravel()
            terms[n] = np.outer(g * sd_y / df[n].std(ddof=0), df[n].to_numpy())
    inc = np.asarray(a.incremental_outcome(use_kpi=True, aggregate_times=False))
    terms["media"] = inc.reshape(-1, *inc.shape[-2:])[..., :len(mmm.input_data.media_channel)].sum(-1)
    return {k: sum(terms[v] for v in vs) for k, vs in GROUPS.items()}, terms


def due_to(df, groups):
    yr = pd.to_datetime(df.time).dt.year.to_numpy()
    rows = []
    for y0, y1 in ((2022, 2023), (2023, 2024)):
        v0, v1 = df.volume_kg[yr == y0].sum(), df.volume_kg[yr == y1].sum()
        expl = 0
        for k, c in groups.items():
            x = (c[:, yr == y1].sum(1) - c[:, yr == y0].sum(1)) / v0 * 100
            rows.append({"period": f"{y0}-{y1}", "driver": k, **dict(zip(["pct", "lo", "hi"], q(x)))})
            expl += np.median(x)
        # 2022 has 53 weeks: the remainder includes that extra week of baseline sales
        rows.append({"period": f"{y0}-{y1}", "driver": "Other / unexplained", "pct": (v1 / v0 - 1) * 100 - expl})
        rows.append({"period": f"{y0}-{y1}", "driver": "Total change", "pct": (v1 / v0 - 1) * 100,
                     "start_m_kg": v0 / 1e6, "end_m_kg": v1 / 1e6})
    return pd.DataFrame(rows)


def effects(mmm, df):
    post, sd_y, mu = mmm.inference_data.posterior, df.volume_kg.std(ddof=0), df.volume_kg.mean()
    g = lambda n: post.gamma_n.sel(non_media_channel=n).values.ravel() * sd_y / df[n].std(ddof=0) / mu * 100
    yr = pd.to_datetime(df.time).dt.year
    out = {
        "volume_pct_per_distribution_point": g("distribution"),
        "volume_pct_per_degree_above_20C": g("heat_excess"),
        "volume_pct_per_10pct_higher_premium": g("log_rel_price") * np.log(1.10),
        "volume_pct_per_10pp_promo_intensity": g("promo_intensity") * 0.10,
    }
    rows = [{"effect": k, **dict(zip(["median", "lo", "hi"], q(v)))} for k, v in out.items()]
    by_year = df.assign(year=yr).groupby("year")
    for y, d in by_year:
        rows.append({"effect": f"facts_{y}", "volume_m_kg": d.volume_kg.sum() / 1e6, "value_m_gbp": d.value_gbp.sum() / 1e6,
                     "price_per_kg": d.value_gbp.sum() / d.volume_kg.sum(), "premium_vs_comp": d.rel_base_price.mean(),
                     "promo_share": d.promo_share_vol.mean(), "distribution": d.distribution.mean(),
                     "media_k_gbp": d.filter(regex="^spend_").sum().sum() / 1e3, "weeks": len(d)})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    mmm = diag.load("mmm_final")
    a = analyzer.Analyzer(mmm)
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    r = pd.read_csv(TAB / "resid_mmm_stage1.csv", index_col=0).iloc[:, 0]
    df["resid_lag1"] = r.shift(1).fillna(0).to_numpy() * 1e6  # same AR term the final model was fit with

    camp = campaign_roi(mmm, a, df)
    camp.round(3).to_csv(TAB / "story_campaign_roi.csv", index=False)
    print(camp.round(2).to_string(index=False))

    groups, _ = weekly_contributions(mmm, a, df)
    dt = due_to(df, groups)
    dt.round(2).to_csv(TAB / "story_due_to.csv", index=False)
    print(dt.round(1).to_string(index=False))

    eff = effects(mmm, df)
    eff.round(3).to_csv(TAB / "story_effects.csv", index=False)
    print(eff.round(3).to_string(index=False))

    exp, act, e = diag.fitted(a)
    pd.DataFrame({"time": df.time, "actual_m_kg": act / 1e6, "fitted_m_kg": exp / 1e6,
                  "lo_m_kg": e.expected.sel(metric="ci_lo").values / 1e6,
                  "hi_m_kg": e.expected.sel(metric="ci_hi").values / 1e6}).round(4).to_csv(TAB / "story_fit.csv", index=False)
