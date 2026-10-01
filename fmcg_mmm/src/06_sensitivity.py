"""Compare model variants (knots, ROI priors) side by side: fit, holdout, residual ACF, ROI, elasticity.

Usage:
    python 06_sensitivity.py --tags k1 k6 k12 --out sens_knots
    python 06_sensitivity.py --tags k6 wide tight high --out sens_priors
"""
import argparse
import importlib
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from meridian.analysis import analyzer

from style import CHANNEL_COLORS, INK_2, ROOT, TAB, save

diag = importlib.import_module("04_diagnostics")


def summarise(tag):
    mmm = diag.load(f"mmm_{tag}")
    a = analyzer.Analyzer(mmm)
    exp, act, _ = diag.fitted(a)
    acc = a.predictive_accuracy(use_kpi=True).to_dataframe().value
    row = {"tag": tag, "r2": float(acc.xs("R_Squared", level="metric").iloc[0]),
           "mape": float(acc.xs("MAPE", level="metric").iloc[0]),
           "acf_lag1": diag.residual_acf(act - exp)["lag1"]}
    ho = ROOT / f"outputs/mmm_{tag}_ho13.pkl"
    if ho.exists():
        acc_ho = analyzer.Analyzer(diag.load(f"mmm_{tag}_ho13")).predictive_accuracy(use_kpi=True).to_dataframe().value
        row["holdout_mape"] = float(acc_ho.xs(("MAPE", "Test"), level=("metric", "evaluation_set")).iloc[0])
    roi, _ = diag.roi_table(a)
    for ch, r in roi.iterrows():
        row[f"roi_{ch}"] = r.roi_post_median
        row[f"roi_lo_{ch}"] = r.roi_post_lo
        row[f"roi_hi_{ch}"] = r.roi_post_hi
    pe = diag.price_promo_effects(mmm)
    row["price_elasticity"] = pe.loc["price_elasticity", "median"]
    return row


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tags", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    res = pd.DataFrame([summarise(t) for t in args.tags]).set_index("tag")
    res.round(3).to_csv(TAB / f"{args.out}.csv")
    cols = ["r2", "mape", "holdout_mape", "acf_lag1", "price_elasticity", "roi_All Channels"]
    print(res[[c for c in cols if c in res]].round(3).to_string())
    print(res.filter(regex="^roi_(?!lo|hi)").round(2).to_string())

    # ROI by channel across variants
    chans = [c for c in CHANNEL_COLORS if f"roi_{c}" in res]
    fig, ax = plt.subplots(figsize=(9, 0.6 * len(chans) * len(res) / 2 + 1.5))
    step = 0.8 / len(res)
    for j, tag in enumerate(res.index):
        y = np.arange(len(chans)) + j * step
        r = res.loc[tag]
        ax.hlines(y, [r[f"roi_lo_{c}"] for c in chans], [r[f"roi_hi_{c}"] for c in chans],
                  color=[CHANNEL_COLORS[c] for c in chans], lw=4, alpha=0.35 + 0.65 * (j + 1) / len(res))
        ax.scatter([r[f"roi_{c}"] for c in chans], y, color="white", edgecolor=INK_2, s=22, zorder=3)
        for yi in y:
            ax.text(-0.05, yi, tag, ha="right", va="center", fontsize=7, color=INK_2,
                    transform=ax.get_yaxis_transform())
    ax.axvline(1, color=INK_2, lw=0.8, ls="--")
    ax.set_yticks(np.arange(len(chans)) + 0.4 - step / 2, chans)
    ax.tick_params(axis="y", pad=40)
    ax.invert_yaxis()
    ax.set(title=f"Media ROI across variants ({args.out}), median and 90% CI", xlabel="ROI")
    save(fig, args.out)
