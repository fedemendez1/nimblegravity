"""Fit the Meridian MMM (national, weekly). Volume KPI, revenue via price per kg.

Usage:
    python 03_model.py                              # base spec, full sample -> outputs/mmm_base.pkl
    python 03_model.py --holdout 13                 # + last 13 weeks held out -> mmm_base_ho13.pkl
    python 03_model.py --tag k6 --knots 6           # sensitivity variants
    python 03_model.py --tag wide --roi-prior 0,1.5
    python 03_model.py --draws quick                # smoke test
"""
import argparse
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from meridian import backend
from meridian.data import data_frame_input_data_builder as dfb
from meridian.model import model, prior_distribution, spec

from style import ROOT

CHANNELS = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
# business drivers: modelled as non-media treatments so Meridian reports their contribution
DRIVERS = ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "comp_media_spend"]
CONTROLS = ["rainfall", "new_year_week", "season_sin", "season_cos"]

ROI_PRIOR = (0.0, 0.7)  # LogNormal: median ROI 1.0, 90% interval ~[0.3, 3.2]
MAX_LAG = 8
SEED = 42
DRAWS = {
    "quick": dict(n_chains=2, n_adapt=200, n_burnin=100, n_keep=200),
    "screen": dict(n_chains=2, n_adapt=500, n_burnin=300, n_keep=500),
    "full": dict(n_chains=4, n_adapt=1000, n_burnin=500, n_keep=1000),
}


def load_data(path=ROOT / "data/clean/model_data.csv"):
    df = pd.read_csv(path)
    return (dfb.DataFrameInputDataBuilder(kpi_type="non_revenue")
            .with_kpi(df, kpi_col="volume_kg")
            .with_revenue_per_kpi(df, revenue_per_kpi_col="price_per_kg")
            .with_controls(df, control_cols=CONTROLS)
            .with_non_media_treatments(df, non_media_treatment_cols=DRIVERS)
            .with_media(df, media_cols=[f"exec_{c}" for c in CHANNELS],
                        media_spend_cols=[f"spend_{c}" for c in CHANNELS], media_channels=CHANNELS)
            .build())


def model_spec(times, holdout_weeks=0, knots=1, roi_prior=ROI_PRIOR):
    prior = prior_distribution.PriorDistribution(
        roi_m=backend.tfd.LogNormal(*map(np.float64, roi_prior), name="roi_m"))
    holdout = spec.HoldoutSpec(spec=[spec.DateRange(times[-holdout_weeks], None)]) if holdout_weeks else None
    # drivers: weakly-informative coefficient prior (gamma_n ~ N(0, 5)), contribution measured vs min
    return spec.ModelSpec(prior=prior, media_prior_type="roi", max_lag=MAX_LAG, knots=knots,
                          non_media_treatments_prior_type="coefficient", holdout=holdout)


def fit(holdout_weeks=0, knots=1, roi_prior=ROI_PRIOR, draws="full"):
    data = load_data()
    times = [str(t) for t in data.time.values]
    mmm = model.Meridian(input_data=data, model_spec=model_spec(times, holdout_weeks, knots, roi_prior))
    mmm.sample_prior(500, seed=SEED)
    mmm.sample_posterior(**DRAWS[draws], seed=SEED)
    return mmm


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="base")
    ap.add_argument("--holdout", type=int, default=0)
    ap.add_argument("--knots", type=int, default=1)
    ap.add_argument("--roi-prior", default=",".join(map(str, ROI_PRIOR)), help="lognormal mu,sigma")
    ap.add_argument("--draws", choices=DRAWS, default="full")
    ap.add_argument("--no-season", action="store_true", help="drop Fourier seasonality controls")
    args = ap.parse_args()

    roi_prior = tuple(float(v) for v in args.roi_prior.split(","))
    if args.no_season:
        CONTROLS[:] = [c for c in CONTROLS if not c.startswith("season_")]
    mmm = fit(args.holdout, args.knots, roi_prior, args.draws)
    name = f"mmm_{args.tag}" + (f"_ho{args.holdout}" if args.holdout else "")
    model.save_mmm(mmm, str(ROOT / f"outputs/{name}.pkl"))
    print(f"saved outputs/{name}.pkl")
