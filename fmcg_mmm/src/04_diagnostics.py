"""Model checks (convergence, fit, residuals, signs, prior vs posterior) and headline results.

Usage:
    python 04_diagnostics.py                  # outputs/mmm_base.pkl (+ mmm_base_ho13.pkl if present)
    python 04_diagnostics.py --model mmm_final   # + mmm_final_ho26 / _ho13 if present
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
from scipy import stats

from style import CHANNEL_COLORS, GRID, INK_2, ROOT, SERIES, TAB, save

EXPECTED_SIGN = {"temp_avg": 1, "heat_excess": 1, "distribution": 1, "log_rel_price": -1,
                 "promo_intensity": 1, "comp_media_spend": -1,
                 "comp_c_promo_share": -1, "comp_a_promo_depth": -1, "rainfall": -1, "new_year_week": -1}


def load(name):
    return model.load_mmm(str(ROOT / f"outputs/{name}.pkl"))


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


def residual_acf(resid):
    r = resid - resid.mean()
    return {f"lag{k}": float(np.sum(r[k:] * r[:-k]) / np.sum(r * r)) for k in (1, 2, 4, 52)}


def fitted(a):
    e = a.expected_vs_actual_data(use_kpi=True).sel(geo="national_geo")
    return e.expected.sel(metric="mean").values, e.actual.values, e


def fitted_vs_actual(a, times):
    exp, act, e = fitted(a)
    residual_tests(act - exp, exp)
    lo, hi = e.expected.sel(metric="ci_lo").values, e.expected.sel(metric="ci_hi").values
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

    acf = residual_acf(resid)
    print("residual ACF:", {k: round(v, 2) for k, v in acf.items()})
    return acf


def vif(mmm):
    """VIF of every regressor (drivers, controls, media exposure): diagonal of the inverse correlation matrix."""
    d = mmm.input_data
    x = pd.concat([d.non_media_treatments.sel(geo="national_geo").to_pandas(),
                   d.controls.sel(geo="national_geo").to_pandas(),
                   d.media.sel(geo="national_geo").isel(media_time=slice(-len(d.time), None)).to_pandas()
                   .set_axis(d.time.values)], axis=1)
    out = pd.Series(np.diag(np.linalg.inv(np.corrcoef(x.to_numpy(float), rowvar=False))), index=x.columns, name="vif")
    out.sort_values(ascending=False).round(2).to_csv(TAB / "diag_vif.csv")
    print(out.sort_values(ascending=False).round(2).to_string())
    return out


def residual_tests(resid, fit):
    """Ljung-Box (no autocorrelation), Jarque-Bera (normality), Breusch-Pagan vs fitted (constant variance)."""
    n, r = len(resid), resid - resid.mean()
    acf = np.array([np.sum(r[k:] * r[:-k]) / np.sum(r * r) for k in range(1, 27)])
    rows = [{"test": f"ljung_box_lag{h}", "stat": n * (n + 2) * np.sum(acf[:h] ** 2 / (n - np.arange(1, h + 1))),
             "df": h} for h in (1, 4, 13, 26)]
    for row in rows:
        row["p_value"] = stats.chi2.sf(row["stat"], row["df"])
    jb = stats.jarque_bera(resid)
    rows.append({"test": "jarque_bera", "stat": jb.statistic, "p_value": jb.pvalue,
                 "excess_kurtosis": stats.kurtosis(resid), "skew": stats.skew(resid)})
    for label, e in (("kg", resid), ("pct", resid / fit)):
        lm = n * stats.linregress(fit, e ** 2).rvalue ** 2
        rows.append({"test": f"breusch_pagan_{label}", "stat": lm, "df": 1, "p_value": stats.chi2.sf(lm, 1)})
    out = pd.DataFrame(rows)
    out.round(4).to_csv(TAB / "diag_residual_tests.csv", index=False)
    print(out.round(3).to_string(index=False))
    return out


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


def roi_table(a):
    s = a.summary_metrics(use_kpi=False)
    roi = s.roi.to_dataframe().roi.unstack(["distribution", "metric"])
    out = pd.DataFrame({
        "spend_k": s.spend.to_series() / 1e3,
        "roi_prior_median": roi[("prior", "median")],
        "roi_prior_lo": roi[("prior", "ci_lo")],
        "roi_prior_hi": roi[("prior", "ci_hi")],
        "roi_post_median": roi[("posterior", "median")],
        "roi_post_lo": roi[("posterior", "ci_lo")],
        "roi_post_hi": roi[("posterior", "ci_hi")],
        "mroi_post_median": s.mroi.sel(distribution="posterior", metric="median").to_series(),
    })
    draws = np.asarray(a.roi(use_kpi=False)).reshape(-1, len(s.channel) - 1)
    out["p_roi_above_1"] = pd.Series((draws > 1).mean(0), index=s.channel.values[:-1])
    # how much the data narrowed the ROI interval vs the prior (1 = learned nothing)
    out["ci_width_post_vs_prior"] = ((roi[("posterior", "ci_hi")] - roi[("posterior", "ci_lo")])
                                     / (roi[("prior", "ci_hi")] - roi[("prior", "ci_lo")]))
    return out, roi


def media_results(a):
    out, roi = roi_table(a)
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


def price_promo_effects(mmm):
    """Coefficients are on standardised scales: dVolume/dx = gamma * sd(volume) / sd(x)."""
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    g = mmm.inference_data.posterior.gamma_n
    vol_sd, vol_mean = df.volume_kg.std(ddof=0), df.volume_kg.mean()
    effects = {
        # % volume change per 1% increase in price premium vs competitors
        "price_elasticity": g.sel(non_media_channel="log_rel_price").values.ravel()
        * vol_sd / df.log_rel_price.std(ddof=0) / vol_mean,
        # % volume change per +10pp of promo intensity (discount x share of stores on promo)
        "promo_uplift_per_10pp": g.sel(non_media_channel="promo_intensity").values.ravel()
        * vol_sd / df.promo_intensity.std(ddof=0) * 0.10 / vol_mean * 100,
    }
    out = pd.DataFrame({k: {"median": np.median(v), "q05": np.quantile(v, 0.05), "q95": np.quantile(v, 0.95)}
                        for k, v in effects.items()}).T
    out.round(3).to_csv(TAB / "results_price_promo.csv")
    print(out.round(3).to_string())
    return out


def response_curves(a, n_weeks):
    """Incremental revenue vs spend, scaling each channel's historical flighting. Annualised."""
    mult = np.round(np.arange(0, 3.01, 0.1), 2)
    rc = a.response_curves(spend_multipliers=list(mult))
    years = n_weeks / 52
    out = (rc.incremental_outcome.to_dataframe().incremental_outcome.unstack("metric")
           .join(rc.spend.to_dataframe()).reset_index())
    out[["spend", "mean", "ci_lo", "ci_hi"]] /= years
    out.round(1).to_csv(TAB / "results_response_curves.csv", index=False)

    chans = list(rc.channel.values)
    fig, axes = plt.subplots(2, 3, figsize=(11, 6))
    for ax, ch in zip(axes.ravel(), chans):
        d = out[out.channel == ch]
        cur = d[d.spend_multiplier == 1].iloc[0]
        ax.fill_between(d.spend / 1e3, d.ci_lo / 1e3, d.ci_hi / 1e3, color=CHANNEL_COLORS[ch], alpha=0.2, lw=0)
        ax.plot(d.spend / 1e3, d["mean"] / 1e3, color=CHANNEL_COLORS[ch])
        ax.plot(d.spend / 1e3, d.spend / 1e3, color=INK_2, lw=0.8, ls="--")
        ax.scatter(cur.spend / 1e3, cur["mean"] / 1e3, color=CHANNEL_COLORS[ch], edgecolor="white", s=50, zorder=3)
        ax.set(title=ch, xlabel="annual spend £k", ylabel="incr. revenue £k")
    fig.suptitle("Response curves (annualised): dot = current spend, dashed = break-even", x=0.01, ha="left")
    save(fig, "results_03_response_curves")
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
    ap.add_argument("--model", default="mmm_base")
    args = ap.parse_args()

    mmm = load(args.model)
    a = analyzer.Analyzer(mmm)
    times = pd.to_datetime(mmm.input_data.time.values)

    print("== convergence"); convergence(mmm)
    print("== fit")
    acc = [fit_quality(a, args.model)]
    for ho in (f"{args.model}_ho26", f"{args.model}_ho13"):
        if (ROOT / f"outputs/{ho}.pkl").exists():
            acc.append(fit_quality(analyzer.Analyzer(load(ho)), ho))
    acc = pd.concat(acc)
    acc.round(3).to_csv(TAB / "diag_fit.csv", index=False)
    print(acc.round(3).to_string(index=False))
    fitted_vs_actual(a, times)
    print("== VIF"); vif(mmm)
    print("== coefficient signs"); coefficient_signs(mmm)
    print("== price & promo"); price_promo_effects(mmm)
    print("== media ROI"); media_results(a)
    print("== contributions"); contributions(a)
    print("== response curves"); response_curves(a, len(times))
