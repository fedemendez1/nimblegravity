"""Fit the MMM in two stages: a first fit, then the final model with last week's first-stage residual as a
control (AR(1) errors via feasible GLS, since Meridian has no AR term).

    python 03_model.py                 # full sample  -> outputs/models/mmm_stage1.pkl, mmm_final.pkl
    python 03_model.py --holdout 26    # last 26 weeks held out -> *_ho26.pkl
"""
import argparse
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from meridian import backend
from meridian.analysis import analyzer
from meridian.data import data_frame_input_data_builder as dfb
from meridian.model import model, prior_distribution, spec

from style import MODELS, ROOT

CHANNELS = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
DRIVERS = ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "comp_media_spend",
           "comp_c_promo_share"]
CONTROLS = ["rainfall", "new_year_week", "season_sin", "season_cos"]

ROI_PRIOR = (0.0, 0.7)  # LogNormal: median 1, 90% ~ [0.3, 3.2]
MAX_LAG = 8
SEED = 42
DRAWS = {
    "screen": dict(n_chains=2, n_adapt=500, n_burnin=300, n_keep=500),
    "full": dict(n_chains=4, n_adapt=1000, n_burnin=500, n_keep=1000),
}


def load_df(resid=None):
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    if resid is not None:
        df["resid_lag1"] = resid.shift(1).fillna(0).to_numpy()
    return df


def input_data(df, drivers=DRIVERS):
    controls = CONTROLS + (["resid_lag1"] if "resid_lag1" in df else [])
    return (dfb.DataFrameInputDataBuilder(kpi_type="non_revenue")
            .with_kpi(df, kpi_col="volume_kg")
            .with_revenue_per_kpi(df, revenue_per_kpi_col="price_per_kg")
            .with_controls(df, control_cols=controls)
            .with_non_media_treatments(df, non_media_treatment_cols=drivers)
            .with_media(df, media_cols=[f"exec_{c}" for c in CHANNELS],
                        media_spend_cols=[f"spend_{c}" for c in CHANNELS], media_channels=CHANNELS)
            .build())


def fit(df, holdout=0, roi_prior=ROI_PRIOR, drivers=DRIVERS, draws="full"):
    data = input_data(df, drivers)
    times = [str(t) for t in data.time.values]
    model_spec = spec.ModelSpec(
        prior=prior_distribution.PriorDistribution(
            roi_m=backend.tfd.LogNormal(*map(np.float64, roi_prior), name="roi_m")),
        media_prior_type="roi", max_lag=MAX_LAG, knots=1,
        non_media_treatments_prior_type="coefficient",  # drivers: N(0, 5) on standardised scale
        holdout=spec.HoldoutSpec(spec=[spec.DateRange(times[-holdout], None)]) if holdout else None)
    mmm = model.Meridian(input_data=data, model_spec=model_spec)
    mmm.sample_prior(500, seed=SEED)
    mmm.sample_posterior(**DRAWS[draws], seed=SEED)
    return mmm


def residuals(mmm):
    e = analyzer.Analyzer(mmm).expected_vs_actual_data(use_kpi=True).sel(geo="national_geo")
    return pd.Series(e.actual.values - e.expected.sel(metric="mean").values,
                     index=pd.to_datetime(e.time.values), name="resid_kg")


def fit_two_stage(holdout=0, **kw):
    stage1 = fit(load_df(), holdout, **kw)
    resid = residuals(stage1)
    return stage1, fit(load_df(resid), holdout, **kw), resid


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdout", type=int, default=0)
    args = ap.parse_args()

    sfx = f"_ho{args.holdout}" if args.holdout else ""
    stage1, final, resid = fit_two_stage(args.holdout)
    resid.to_csv(MODELS / f"stage1_residuals{sfx}.csv")
    model.save_mmm(stage1, str(MODELS / f"mmm_stage1{sfx}.pkl"))
    model.save_mmm(final, str(MODELS / f"mmm_final{sfx}.pkl"))
