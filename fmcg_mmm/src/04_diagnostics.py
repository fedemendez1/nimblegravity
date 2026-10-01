"""Model checks (convergence, fit, residuals, signs, prior vs posterior) and headline results.

Usage:
    python 04_diagnostics.py                 # uses outputs/mmm_full.pkl (+ mmm_holdout13.pkl if present)
    python 04_diagnostics.py --model mmm_quick
"""
import argparse
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import arviz as az
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from meridian.analysis import analyzer
from meridian.model import model

from style import CHANNEL_COLORS, GRID, INK_2, ROOT, SERIES, TAB, save

EXPECTED_SIGN = {"temp_avg": 1, "heat_excess": 1, "distribution": 1, "rel_base_price": -1,
                 "promo_distribution": 1, "comp_media_spend": -1,
                 "rainfall": -1, "new_year_week": -1}


def convergence(mmm):
    post = mmm.inference_data.posterior
    params = ["roi_m", "alpha_m", "ec_m", "beta_m", "gamma_c", "gamma_n", "sigma", "knot_values"]
    params = [p for p in params if p in post]
    rhat, ess = az.rhat(post[params]), az.ess(post[params])
    out = pd.DataFrame({"max_rhat": [float(rhat[p].max()) for p in params],
                        "min_ess_bulk": [float(ess[p].min()) for p in params]}, index=params)
    n_div = int(mmm.inference_data.sample_stats.diverging.sum())
    print(out.round(3).to_string(), f"\ndivergent transitions: {n_div}")
    out.round(3).to_csv(TAB / "diag_convergence.csv")
    return out, n_div


def fit_quality(a, label):
    acc = a.predictive_accuracy(use_kpi=True).to_dataframe().reset_index()
    acc.insert(0, "model", label)
    return acc


def fitted_vs_actual(a, times):
    e = a.expected_vs_actual_data(use_kpi=True).sel(geo="national_geo")
    exp = e.expected.sel(metric="mean").values
    lo, hi = e.expected.sel(metric="ci_lo").values, e.expected.sel(metric="ci_hi").values
    act = e.actual.values
    resid = act - exp

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 5.5), sharex=True, height_ratios=[2, 1])
    a1.fill_between(times, lo / 1e6, hi / 1e6, color=SERIES[0], alpha=0.2, lw=0, label="Model 90% CI")
    a1.plot(times, exp / 1e6, color=SERIES[0], label="Model")
    a1.plot(times, act / 1e6, color=INK_2, lw=1.2, label="Actual")
    a1.set(title="Actual vs modelled volume (m kg)", ylabel="m kg")
    a1.legend(ncol=3, loc="upper left")
    a2.bar(times, resid / 1e6, width=6, color=SERIES[1])
    a2.axhline(0, color=INK_2, lw=0.8)
    a2.set(title="Residuals (m kg)", ylabel="m kg")
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    save(fig, "diag_01_fit_residuals")

    r = resid - resid.mean()
    acf = {f"lag{k}": float(np.sum(r[k:] * r[:-k]) / np.sum(r * r)) for k in (1, 2, 4, 52)}
    print("residual ACF:", {k: round(v, 2) for k, v in acf.items()})
    return acf


def coefficient_signs(mmm):
    post = mmm.inference_data.posterior
    rows = []
    for var, dim in (("gamma_n", "non_media_channel"), ("gamma_c", "control_variable")):
        for name in post[var][dim].values:
            x = post[var].sel({dim: name}).values.ravel()
            exp = EXPECTED_SIGN.get(str(name), 0)
            rows.append({"variable": str(name), "mean": x.mean(), "q05": np.quantile(x, 0.05),
                         "q95": np.quantile(x, 0.95), "p_positive": (x > 0).mean(), "expected_sign": exp,
                         "sign_ok": (np.sign(x.mean()) == exp) if exp else None})
    out = pd.DataFrame(rows)
    out.round(3).to_csv(TAB / "diag_coefficient_signs.csv", index=False)
    print(out.round(3).to_string(index=False))
    return out


def media_results(a):
    s = a.summary_metrics(use_kpi=False)
    roi = s.roi.to_dataframe().roi.unstack(["distribution", "metric"])
    out = pd.DataFrame({
        "spend_k": s.spend.to_series() / 1e3,
        "roi_prior_median": roi[("prior", "median")],
        "roi_post_median": roi[("posterior", "median")],
        "roi_post_lo": roi[("posterior", "ci_lo")],
        "roi_post_hi": roi[("posterior", "ci_hi")],
        "mroi_post_median": s.mroi.sel(distribution="posterior", metric="median").to_series(),
    })
    # how much the data narrowed the ROI interval vs the prior (1 = learned nothing)
    out["ci_width_post_vs_prior"] = ((roi[("posterior", "ci_hi")] - roi[("posterior", "ci_lo")])
                                     / (roi[("prior", "ci_hi")] - roi[("prior", "ci_lo")]))
    out.round(3).to_csv(TAB / "results_media_roi.csv")
    print(out.round(2).to_string())

    ch = [c for c in out.index if c != "All Channels"]
    fig, ax = plt.subplots(figsize=(8, 4))
    y = np.arange(len(ch))
    ax.hlines(y + 0.15, roi.loc[ch, ("prior", "ci_lo")], roi.loc[ch, ("prior", "ci_hi")], color=GRID, lw=6,
              label="Prior 90% CI")
    ax.hlines(y - 0.15, out.loc[ch, "roi_post_lo"], out.loc[ch, "roi_post_hi"],
              color=[CHANNEL_COLORS[c] for c in ch], lw=6, label="Posterior 90% CI")
    ax.scatter(out.loc[ch, "roi_post_median"], y - 0.15, color="white", edgecolor=INK_2, zorder=3, s=40)
    ax.axvline(1, color=INK_2, lw=0.8, ls="--")
    ax.set_yticks(y, ch)
    ax.invert_yaxis()
    ax.set(title="Media ROI: prior vs posterior (£ revenue per £ spent)", xlabel="ROI")
    ax.legend(loc="lower right")
    save(fig, "results_01_roi_prior_posterior")
    return out


def contributions(a):
    s = a.summary_metrics(use_kpi=True, include_non_paid_channels=True)
    base = a.baseline_summary_metrics(use_kpi=True)
    pct =s.pct_of_contribution.sel(distribution="posterior").to_pandas()
    pct.loc["baseline"] = base.pct_of_contribution.sel(distribution="posterior").to_pandas()
    pct = pct.drop(index="All Channels")
    pct.round(2).to_csv(TAB / "results_contributions_pct.csv")
    print(pct.round(2).to_string())

    pct = pct.sort_values("mean")
    fig, ax = plt.subplots(figsize=(8, 4.5))
    colors = [SERIES[0] if v >= 0 else SERIES[7] for v in pct["mean"]]
    ax.barh(pct.index, pct["mean"], color=colors, height=0.6)
    ax.errorbar(pct["mean"], pct.index, xerr=[pct["mean"] - pct["ci_lo"], pct["ci_hi"] - pct["mean"]],
                fmt="none", ecolor=INK_2, lw=1)
    ax.axvline(0, color=INK_2, lw=0.8)
    ax.set(title="Volume contribution by driver (% of total, 90% CI)\nnon-media drivers measured vs their lowest observed level",
           xlabel="% of total volume")
    save(fig, "results_02_contributions")
    return pct


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="mmm_full")
    ap.add_argument("--holdout-model", default="mmm_holdout13")
    args = ap.parse_args()

    mmm = model.load_mmm(str(ROOT / f"outputs/{args.model}.pkl"))
    a = analyzer.Analyzer(mmm)
    times = pd.to_datetime(mmm.input_data.time.values)

    print("== convergence"); convergence(mmm)
    print("== fit")
    acc = [fit_quality(a, args.model)]
    ho_path = ROOT / f"outputs/{args.holdout_model}.pkl"
    if ho_path.exists():
        acc.append(fit_quality(analyzer.Analyzer(model.load_mmm(str(ho_path))), args.holdout_model))
    acc = pd.concat(acc)
    acc.round(3).to_csv(TAB / "diag_fit.csv", index=False)
    print(acc.round(3).to_string(index=False))
    fitted_vs_actual(a, times)
    print("== coefficient signs"); coefficient_signs(mmm)
    print("== media ROI"); media_results(a)
    print("== contributions"); contributions(a)
