"""Refit the final model under other ROI priors and heat thresholds (shorter chains) and compare the results
that matter: fit, price elasticity, media ROI and the direction of the optimiser's reallocation."""
import importlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from meridian.analysis import analyzer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from style import MODELS, ROB  # noqa: E402

m03 = importlib.import_module("03_model")
diag = importlib.import_module("04_diagnostics")
opt = importlib.import_module("06_optimize")

VARIANTS = {
    "base": {},
    "prior wide LogNormal(0, 1.5)": dict(roi_prior=(0.0, 1.5)),
    "prior tight LogNormal(0, 0.35)": dict(roi_prior=(0.0, 0.35)),
    "prior median 2 LogNormal(0.69, 0.7)": dict(roi_prior=(0.693, 0.7)),
    **{f"heat above {t}C": dict(drivers=[f"heat_excess_{t}" if d == "heat_excess" else d for d in m03.DRIVERS])
       for t in (18, 22, 24)},
}


def summarise(mmm, label):
    a = analyzer.Analyzer(mmm)
    acc = a.predictive_accuracy(use_kpi=True).to_dataframe().value
    df = m03.load_df()
    heat = next(d for d in mmm.input_data.non_media_channel.values if str(d).startswith("heat"))
    g = mmm.inference_data.posterior.gamma_n.sel(non_media_channel="log_rel_price").values.ravel()
    row = {"variant": label, "heat_driver": str(heat),
           "r2": float(acc.xs("R_Squared", level="metric").iloc[0]), "mape": float(acc.xs("MAPE", level="metric").iloc[0]),
           "price_elasticity": np.median(g * df.volume_kg.std(ddof=0) / df.log_rel_price.std(ddof=0) / df.volume_kg.mean())}
    roi, _ = diag.roi_table(a)
    row.update({f"roi_{c}": r.roi_post_median for c, r in roi.iterrows()})
    mix, _ = opt.reallocate(mmm)
    row.update({f"spend_change_{c}": v for c, v in mix.spend_change_pct.items()})
    return row


if __name__ == "__main__":
    resid = pd.read_csv(MODELS / "stage1_residuals.csv", index_col=0).resid_kg
    rows = []
    for label, kw in VARIANTS.items():
        mmm = m03.fit(m03.load_df(resid), draws="screen", **kw)
        rows.append(summarise(mmm, label))
        print(pd.Series(rows[-1]).to_string(), "\n")
    pd.DataFrame(rows).round(3).to_csv(ROB / "sensitivity.csv", index=False)
