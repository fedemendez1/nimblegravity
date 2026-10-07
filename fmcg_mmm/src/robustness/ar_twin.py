"""PyMC version of the final model with the AR(1) error estimated jointly instead of in two stages.

Same media transforms, priors and drivers as 03_model.py. Variants: without AR (should match Meridian's first
stage), and with noise growing with the sales level.
"""
import importlib
import sys
from pathlib import Path

import arviz as az
import numpy as np
import pandas as pd
import pymc as pm
import pytensor.tensor as pt
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from style import ROB, ROOT  # noqa: E402

m03 = importlib.import_module("03_model")
CHANNELS, MAX_LAG, DRIVERS, CONTROLS = m03.CHANNELS, m03.MAX_LAG, m03.DRIVERS, m03.CONTROLS


def acf1(x):
    x = x - x.mean()
    return float(np.sum(x[1:] * x[:-1]) / np.sum(x * x))


def build(df, ar=True, student_t=False, het=False):
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
        # total incremental revenue = roi * spend
        beta = pm.Deterministic("beta", roi * spend / (y_sd * (hill * rpk[:, None]).sum(0)), dims="channel")
        gamma = pm.Normal("gamma", 0, 5, dims="var")
        tau = pm.Normal("tau", 0, 5)
        sigma = pm.HalfNormal("sigma", 5)
        mu = pm.Deterministic("mu", tau + (hill * beta).sum(1) + pt.dot(zs, gamma))
        sd = sigma * pt.exp(pm.Normal("delta", 0, 1) * mu) if het else sigma * pt.ones(len(ys))
        nu = pm.Gamma("nu", 2, 0.1) if student_t else None
        lik = (lambda name, m, s, obs: pm.StudentT(name, nu=nu, mu=m, sigma=s, observed=obs)) if student_t else \
            (lambda name, m, s, obs: pm.Normal(name, m, s, observed=obs))
        if ar:
            rho = pm.Uniform("rho", -0.99, 0.99)
            e_prev = ys[:-1] - mu[:-1]
            lik("y0", mu[0], sd[0] / pt.sqrt(1 - rho ** 2), ys[0])
            lik("y", mu[1:] + rho * e_prev, sd[1:], ys[1:])
        else:
            lik("y", mu, sd, ys)
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
    scale = np.exp(float(post.delta.mean()) * mu) if "delta" in post else np.ones_like(mu)
    z = innov / scale[-len(innov):]
    bp = stats.linregress(mu[-len(z):], np.abs(z - z.mean()))
    q = lambda v: f"{np.median(v):.2f} ({np.quantile(v, .05):.2f} / {np.quantile(v, .95):.2f})"
    row = {"model": label, "price_elasticity": q(el), "roi_total": q(roi_tot),
           "rho": q(post.rho.values.ravel()) if "rho" in post else "",
           "resid_acf1": round(acf1(resid), 3), "innovation_acf1": round(acf1(innov), 3),
           "nu": q(post.nu.values.ravel()) if "nu" in post else "",
           "delta": q(post.delta.values.ravel()) if "delta" in post else "",
           "excess_kurtosis": round(float(stats.kurtosis(z)), 2), "jarque_bera_p": round(float(stats.jarque_bera(z).pvalue), 4),
           "abs_resid_vs_level_p": round(float(bp.pvalue), 3),
           "max_rhat": round(float(az.rhat(post[["roi", "gamma", "sigma"]]).max().to_array().max()), 3),
           "divergences": int(idata.sample_stats.diverging.sum())}
    row.update({f"roi_{c}": round(float(np.median(roi[:, i])), 2) for i, c in enumerate(CHANNELS)})
    row.update({f"gamma_{v}": round(float(post.gamma.sel(var=v).median()), 3) for v in DRIVERS})
    return row


VARIANTS = {"no AR": dict(ar=False), "AR(1)": dict(ar=True), "AR(1), noise grows with level": dict(ar=True, het=True),
            "AR(1), Student-t + level noise": dict(ar=True, het=True, student_t=True)}

if __name__ == "__main__":
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    rows = []
    for label, kw in VARIANTS.items():
        mdl, info = build(df, **kw)
        with mdl:
            idata = pm.sample(1000, tune=1000, chains=4, cores=4, target_accept=0.9, random_seed=42)
        rows.append(summarise(idata, info, df, label))
        print(pd.Series(rows[-1]).to_string(), "\n")
    pd.DataFrame(rows).to_csv(ROB / "ar_twin.csv", index=False)
