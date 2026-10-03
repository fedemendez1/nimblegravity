"""Residual autocorrelation deep-dive: where it happens and which unused variables track it.

Usage:
    python 07_residuals.py --model mmm_cC
Outputs: tables/resid_<model>.csv (weekly residuals), resid_acf_<model>.csv (ACF/DW by year),
         resid_screen_<model>.csv (raw columns and driver lags vs residuals), figure diag_02_residual_autocorr_<model>
"""
import argparse
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from meridian.analysis import analyzer
from meridian.model import model

from style import GRID, INK_2, ROOT, SERIES, TAB, save

SPRING22 = (pd.Timestamp("2022-02-26"), pd.Timestamp("2022-05-28"))


def acf1(x):
    x = x - x.mean()
    return float(np.sum(x[1:] * x[:-1]) / np.sum(x * x))


def dw(x):
    x = x - x.mean()
    return float(np.sum(np.diff(x) ** 2) / np.sum(x * x))


def residuals(name):
    mmm = model.load_mmm(str(ROOT / f"outputs/{name}.pkl"))
    e = analyzer.Analyzer(mmm).expected_vs_actual_data(use_kpi=True).sel(geo="national_geo")
    r = (e.actual.values - e.expected.sel(metric="mean").values) / 1e6
    return pd.Series(r, index=pd.to_datetime(e.time.values), name="resid_m_kg")


def screen(r):
    """Correlate residuals with every raw column and with lags of the model drivers (weekly and 13w-smoothed)."""
    raw = pd.read_excel(ROOT / "data/raw/model_variables.xlsx", sheet_name="Model variables")
    md = pd.read_csv(ROOT / "data/clean/model_data.csv")
    cand = {c: raw[c] for c in raw.select_dtypes("number").columns}
    for v in ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "rainfall"]:
        for lag in (1, 2, 4, 8):
            cand[f"{v}_lag{lag}"] = md[v].shift(lag).bfill()
    sm = lambda x: pd.Series(np.asarray(x, float)).rolling(13, center=True, min_periods=5).mean()
    rv = r.values
    rows = [(k, np.corrcoef(x, rv)[0, 1], np.corrcoef(sm(x), sm(rv))[0, 1]) for k, x in cand.items() if np.std(x) > 0]
    out = pd.DataFrame(rows, columns=["variable", "corr_weekly", "corr_13w_smoothed"]).dropna()
    return out.reindex(out.corr_13w_smoothed.abs().sort_values(ascending=False).index)


def plot(r, name, win=26):
    t, v = r.index, r.values
    roll = r.rolling(win, center=True).apply(acf1, raw=True)
    fig, ax = plt.subplots(3, 1, figsize=(10, 8), height_ratios=[1.3, 1, 1.2])
    ax[1].sharex(ax[0])
    ax[0].bar(t, v, width=6, color=np.where(v > 0, SERIES[1], SERIES[0]))
    ax[0].axhline(0, color=INK_2, lw=0.8)
    ax[0].set(title="Residuals by week (m kg): orange = model under-predicts, blue = over-predicts", ylabel="m kg")
    ax[1].plot(roll.index, roll.values, color=SERIES[0])
    ax[1].axhline(acf1(v), color=INK_2, lw=0.8, ls="--")
    ax[1].axhline(0, color=INK_2, lw=0.8)
    ax[1].text(t[3], acf1(v) + 0.04, f"whole period {acf1(v):.2f}", color=INK_2, fontsize=9)
    ax[1].set(title=f"Lag-1 autocorrelation of residuals, rolling {win}-week window (centred)", ylabel="autocorr.")
    for a in ax[:2]:
        a.axvspan(*SPRING22, color=SERIES[3], alpha=0.15, lw=0)
        a.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    spring = (t[1:] >= SPRING22[0]) & (t[1:] <= SPRING22[1])
    ax[2].scatter(v[:-1], v[1:], s=18, c=np.where(spring, SERIES[3], SERIES[0]), alpha=0.8)
    ax[2].axhline(0, color=GRID)
    ax[2].axvline(0, color=GRID)
    ax[2].set(title="Residual this week vs last week (yellow = spring 2022)",
              xlabel="last week (m kg)", ylabel="this week (m kg)")
    save(fig, f"diag_02_residual_autocorr_{name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="mmm_base")
    name = ap.parse_args().model

    r = residuals(name)
    r.to_csv(TAB / f"resid_{name}.csv")
    acf = [("all", acf1(r.values), dw(r.values))]
    acf += [(str(y), acf1(x.values), dw(x.values)) for y, x in r.groupby(r.index.year) if len(x) > 10]
    acf = pd.DataFrame(acf, columns=["period", "acf_lag1", "durbin_watson"]).round(3)
    acf.to_csv(TAB / f"resid_acf_{name}.csv", index=False)
    print(acf.to_string(index=False))
    sc = screen(r)
    sc.round(3).to_csv(TAB / f"resid_screen_{name}.csv", index=False)
    print(sc.head(10).round(2).to_string(index=False))
    plot(r, name)
