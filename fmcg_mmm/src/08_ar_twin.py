"""Bayesian twin of the Meridian spec with AR(1) errors, estimated jointly (PyMC).

Same structure as 03_model.py: geometric adstock (max lag 8) + Hill on media exposure, ROI ~ LogNormal(0, 0.7)
per channel, drivers and controls standardised with N(0, 5) coefficients, flat baseline. Adds
e_t = rho * e_{t-1} + eta_t. Used to check that the Meridian results hold once the persistence is modelled.

Usage:
    python 08_ar_twin.py                 # AR(1) twin
    python 08_ar_twin.py --no-ar         # same model without AR (should reproduce Meridian)
"""
import argparse
import importlib

import arviz as az
import numpy as np
import pandas as pd
import pymc as pm
import pytensor.tensor as pt

from style import ROOT, TAB

m03 = importlib.import_module("03_model")
CHANNELS, MAX_LAG = m03.CHANNELS, m03.MAX_LAG
DRIVERS = m03.DRIVERS + ["comp_c_promo_share"]
CONTROLS = m03.CONTROLS


def acf1(x):
    x = x - x.mean()
    return float(np.sum(x[1:] * x[:-1]) / np.sum(x * x))


def build(df, ar=True):
    y = df.volume_kg.to_numpy()
    y_mu, y_sd = y.mean(), y.std()
    ys = (y - y_mu) / y_sd
    exe = df[[f"exec_{c}" for c in CHANNELS]].to_numpy()
    x = exe / np.array([np.median(c[c > 0]) for c in exe.T])
    lags = np.stack([np.vstack([np.zeros((l, x.shape[1])), x[:len(x) - l]]) for l in range(MAX_LAG + 1)], 1)
    spend = df[[f"spend_{c}" for c in CHANNELS]].sum().to_numpy()
    rpk = df.price_per_kg.to_numpy()
    z = df[DRIVERS + CONTROLS]
    zs = ((z - z.mean()) / z.std(ddof=0)).to_numpy()

    with pm.Model(coords={"channel": CHANNELS, "var": DRIVERS + CONTROLS}) as mdl:
        alpha = pm.Uniform("alpha", 0, 1, dims="channel")
        ec = pm.TruncatedNormal("ec", 0.8, 0.8, lower=0.1, upper=10, dims="channel")
        roi = pm.LogNormal("roi", 0, 0.7, dims="channel")
        w = alpha[None, :] ** np.arange(MAX_LAG + 1)[:, None]
        ad = (lags * w[None]).sum(1) / w.sum(0)
        hill = ad / (ad + ec)
        # ROI parametrisation: total incremental revenue = roi * spend
        beta = pm.Deterministic("beta", roi * spend / (y_sd * (hill * rpk[:, None]).sum(0)), dims="channel")
        gamma = pm.Normal("gamma", 0, 5, dims="var")
        tau = pm.Normal("tau", 0, 5)
        sigma = pm.HalfNormal("sigma", 5)
        mu = pm.Deterministic("mu", tau + (hill * beta).sum(1) + pt.dot(zs, gamma))
        if ar:
            rho = pm.Uniform("rho", -0.99, 0.99)
            e_prev = ys[:-1] - mu[:-1]
            pm.Normal("y0", mu[0], sigma / pt.sqrt(1 - rho ** 2), observed=ys[0])
            pm.Normal("y", mu[1:] + rho * e_prev, sigma, observed=ys[1:])
        else:
            pm.Normal("y", mu, sigma, observed=ys)
    return mdl, dict(y_mu=y_mu, y_sd=y_sd, z=z, spend=spend, ys=ys)


def summarise(idata, info, df, label):
    post = idata.posterior
    g = post.gamma.sel(var="log_rel_price").values.ravel()
    el = g * info["y_sd"] / info["z"].log_rel_price.std(ddof=0) / info["y_mu"]
    roi = post.roi.values.reshape(-1, len(CHANNELS))
    roi_tot = (roi * info["spend"]).sum(1) / info["spend"].sum()
    mu = post.mu.mean(("chain", "draw")).values
    resid = info["ys"] - mu
    innov = resid[1:] - float(post.rho.mean()) * resid[:-1] if "rho" in post else resid
    q = lambda v: f"{np.median(v):.2f} ({np.quantile(v, .05):.2f} / {np.quantile(v, .95):.2f})"
    row = {"model": label, "price_elasticity": q(el), "roi_total": q(roi_tot),
           "rho": q(post.rho.values.ravel()) if "rho" in post else "",
           "resid_acf1": round(acf1(resid), 3), "innovation_acf1": round(acf1(innov), 3),
           "max_rhat": round(float(az.rhat(post[["roi", "gamma", "sigma"]]).max().to_array().max()), 3),
           "divergences": int(idata.sample_stats.diverging.sum())}
    row.update({f"roi_{c}": round(float(np.median(roi[:, i])), 2) for i, c in enumerate(CHANNELS)})
    row.update({f"gamma_{v}": round(float(post.gamma.sel(var=v).median()), 3) for v in DRIVERS})
    return row


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-ar", action="store_true")
    ap.add_argument("--draws", type=int, default=1000)
    args = ap.parse_args()

    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    label = "twin_noar" if args.no_ar else "twin_ar1"
    mdl, info = build(df, ar=not args.no_ar)
    with mdl:
        idata = pm.sample(args.draws, tune=1000, chains=4, cores=4, target_accept=0.9, random_seed=42)
    idata.to_netcdf(ROOT / f"outputs/{label}.nc")
    row = summarise(idata, info, df, label)
    out = TAB / "ar_twin.csv"
    prev = pd.read_csv(out) if out.exists() else pd.DataFrame()
    pd.concat([prev[prev.get("model", pd.Series(dtype=str)) != label], pd.DataFrame([row])]).to_csv(out, index=False)
    print(pd.Series(row).to_string())
