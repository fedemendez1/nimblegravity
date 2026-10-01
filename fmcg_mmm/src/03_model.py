"""Fit the Meridian MMM (national, weekly). Volume KPI, revenue via price per kg.

Usage:
    python 03_model.py              # full-sample fit
    python 03_model.py --holdout 13 # hold out last 13 weeks for out-of-sample check
"""
import argparse
import os

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
import warnings

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from meridian import backend
from meridian.data import data_frame_input_data_builder as dfb
from meridian.model import model, prior_distribution, spec

from style import ROOT

CHANNELS = ["video", "social", "partnership", "ooh", "search_rdm"]
# business drivers: modelled as non-media treatments so Meridian reports their contribution.
# promo_depth dropped: r=0.77 with promo_distribution and mechanically tied to base price.
DRIVERS = ["temp_avg", "heat_excess", "distribution", "rel_base_price", "promo_distribution", "comp_media_spend"]
CONTROLS = ["rainfall", "new_year_week"]

# ROI prior: median 1.0 (£ revenue per £ spent), 90% interval ~[0.3, 3.2]
ROI_PRIOR = (0.0, 0.7)
MAX_LAG = 8
KNOTS = 1
SEED = 42


def load_data(path=ROOT / "data/clean/model_data.csv"):
    df = pd.read_csv(path)
    spend = [f"spend_{c}" for c in CHANNELS]
    return (dfb.DataFrameInputDataBuilder(kpi_type="non_revenue")
            .with_kpi(df, kpi_col="volume_kg")
            .with_revenue_per_kpi(df, revenue_per_kpi_col="price_per_kg")
            .with_controls(df, control_cols=CONTROLS)
            .with_non_media_treatments(df, non_media_treatment_cols=DRIVERS)
            .with_media(df, media_cols=spend, media_spend_cols=spend, media_channels=CHANNELS)
            .build())


def model_spec(holdout_weeks=0, times=None):
    prior = prior_distribution.PriorDistribution(
        roi_m=backend.tfd.LogNormal(*map(np.float64, ROI_PRIOR), name="roi_m"))
    holdout = None
    if holdout_weeks:
        holdout = spec.HoldoutSpec(spec=[spec.DateRange(times[-holdout_weeks], None)])
    # drivers: weakly-informative coefficient prior (gamma_n ~ N(0, 5)), contribution measured vs min
    return spec.ModelSpec(prior=prior, media_prior_type="roi", max_lag=MAX_LAG, knots=KNOTS,
                          non_media_treatments_prior_type="coefficient", holdout=holdout)


def fit(holdout_weeks=0, n_chains=4, n_adapt=1000, n_burnin=500, n_keep=1000):
    data = load_data()
    times = [str(t) for t in data.time.values]
    mmm = model.Meridian(input_data=data, model_spec=model_spec(holdout_weeks, times))
    mmm.sample_prior(500, seed=SEED)
    mmm.sample_posterior(n_chains=n_chains, n_adapt=n_adapt, n_burnin=n_burnin,
                         n_keep=n_keep, seed=SEED)
    return mmm


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdout", type=int, default=0)
    ap.add_argument("--quick", action="store_true", help="small run to smoke-test the pipeline")
    args = ap.parse_args()

    draws = dict(n_chains=2, n_adapt=200, n_burnin=100, n_keep=200) if args.quick else {}
    mmm = fit(args.holdout, **draws)
    name = "mmm_quick" if args.quick else f"mmm_holdout{args.holdout}" if args.holdout else "mmm_full"
    model.save_mmm(mmm, str(ROOT / f"outputs/{name}.pkl"))
    print(f"saved outputs/{name}.pkl")
