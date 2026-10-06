"""Does price elasticity grow with the premium? Linear mirror of the final model (log volume, same drivers and
controls, media as adstock, AR(1) errors by GLS) with the price term split at a knot (piecewise-linear in
log_rel_price) and, as a second check, a quadratic term.

Output: tables/elasticity_by_premium.csv
"""
import importlib

import numpy as np
import pandas as pd
import statsmodels.api as sm

from style import ROOT, TAB

unc = importlib.import_module("10_unconstrained")
CONTROLS = [c for c in unc.X_BASE if c != "log_rel_price"]
Z90 = 1.645


def fit(df, price_cols):
    m = np.column_stack([unc.transform(df[f"exec_{c}"].to_numpy(float), 0.4, np.inf) for c in unc.CHANNELS])
    X = pd.DataFrame(np.column_stack([df[CONTROLS + price_cols].to_numpy(float), m]),
                     columns=CONTROLS + price_cols + unc.CHANNELS)
    return sm.GLSAR(np.log(df.volume_kg.to_numpy()), sm.add_constant(X), rho=1).iterative_fit(maxiter=20)


def row(model, label, est, se, n):
    return {"model": model, "segment": label, "elasticity": est, "lo": est - Z90 * se, "hi": est + Z90 * se, "weeks": n}


if __name__ == "__main__":
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    x = df.log_rel_price
    out = []

    res = fit(df, ["log_rel_price"])
    out.append(row("single", "all weeks", res.params["log_rel_price"], res.bse["log_rel_price"], len(df)))

    for knot in [1.7, 1.8, 1.9]:
        k = np.log(knot)
        df["p_low"], df["p_high"] = np.minimum(x, k), np.maximum(x - k, 0)
        res = fit(df, ["p_low", "p_high"])
        b, V = res.params, res.cov_params()
        hi, se_hi = b.p_low + b.p_high, np.sqrt(V.loc["p_low", "p_low"] + V.loc["p_high", "p_high"] + 2 * V.loc["p_low", "p_high"])
        n_hi = int((x > k).sum())
        out += [row(f"piecewise {knot}x", f"premium < {knot}x", b.p_low, res.bse.p_low, len(df) - n_hi),
                row(f"piecewise {knot}x", f"premium > {knot}x", hi, se_hi, n_hi),
                row(f"piecewise {knot}x", "difference (high - low)", b.p_high, res.bse.p_high, len(df))]

    c = x.mean()
    df["p_lin"], df["p_sq"] = x - c, (x - c) ** 2
    res = fit(df, ["p_lin", "p_sq"])
    b, V = res.params, res.cov_params()
    for prem in [1.4, 1.6, 1.8, 1.95, 2.1]:
        g = np.array([1, 2 * (np.log(prem) - c)])
        idx = ["p_lin", "p_sq"]
        out.append(row("quadratic", f"at {prem}x", g @ b[idx], np.sqrt(g @ V.loc[idx, idx] @ g), int(len(df))))

    t = pd.DataFrame(out).round(3)
    t.to_csv(TAB / "elasticity_by_premium.csv", index=False)
    print(t.to_string(index=False))
