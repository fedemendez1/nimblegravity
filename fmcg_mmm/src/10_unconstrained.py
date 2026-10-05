"""Unconstrained classical MMM (Nielsen-style check): what does each channel get with no priors and no sign limits?

Media enter as adstock (geometric, max lag 8) + Hill (slope 1) on exposure, same drivers and controls as the final
Meridian model, AR(1) errors by iterated feasible GLS (Cochrane-Orcutt). Adstock decay and half-saturation are picked
per channel by grid search on BIC (coordinate descent); every other coefficient is free, negatives allowed.

Outputs:
    tables/unconstrained_roi.csv     ROI per channel and total, 90% CI (from GLS standard errors), t-stat
    tables/unconstrained_shapes.csv  ROI and BIC for every adstock/saturation pair per channel (shape identification)
"""
import numpy as np
import pandas as pd
import statsmodels.api as sm

from style import ROOT, TAB

CHANNELS = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
X_BASE = ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "comp_media_spend",
          "comp_c_promo_share", "rainfall", "new_year_week", "season_sin", "season_cos"]
# channels that share flighting are also reported jointly: DV and OOH launched the same week (Apr 2024),
# TV ran inside the partnership flight (Apr 2023)
GROUPS = {"All ex search_rdm": ["tv", "digital_video", "social", "partnership", "ooh"],
          "digital_video + ooh": ["digital_video", "ooh"], "tv + partnership": ["tv", "partnership"]}
ALPHAS = [0.0, 0.2, 0.4, 0.6, 0.8]
ECS = [0.5, 1.0, 2.0, 4.0, np.inf]  # half-saturation as multiple of median active exposure; inf = linear
MAX_LAG, Z90 = 8, 1.645


def adstock(x, a):
    w = a ** np.arange(MAX_LAG + 1)
    return np.convolve(x, w / w.sum())[:len(x)]


def transform(x, a, ec):
    x = adstock(x / np.median(x[x > 0]), a)
    return x if np.isinf(ec) else x / (x + ec)


def fit(df, shapes):
    m = np.column_stack([transform(df[f"exec_{c}"].to_numpy(float), *shapes[c]) for c in CHANNELS])
    X = sm.add_constant(np.column_stack([df[X_BASE].to_numpy(float), m]))
    res = sm.GLSAR(df.volume_kg.to_numpy(), X, rho=1).iterative_fit(maxiter=20)
    return res, m


def roi(df, res, m):
    """Incremental revenue / spend; linear in the media coefficients, so the CI comes straight from their covariance."""
    k = len(X_BASE) + 1
    rev = (m * df.price_per_kg.to_numpy()[:, None]).sum(0)  # revenue per unit coefficient
    spend = df[[f"spend_{c}" for c in CHANNELS]].sum().to_numpy()
    b, V = res.params[k:], res.cov_params()[k:, k:]
    out = pd.DataFrame({"roi": b * rev / spend, "se": np.sqrt(np.diag(V)) * rev / spend, "t": b / np.sqrt(np.diag(V))},
                       index=CHANNELS)
    sp = pd.Series(spend, index=CHANNELS)
    for name, chs in {"All Channels": CHANNELS, **GROUPS}.items():
        g = np.where(np.isin(CHANNELS, chs), rev, 0) / sp[chs].sum()
        out.loc[name] = [g @ b, np.sqrt(g @ V @ g), (g @ b) / np.sqrt(g @ V @ g)]
    out["lo"], out["hi"] = out.roi - Z90 * out.se, out.roi + Z90 * out.se
    out["spend_k"] = list(spend / 1e3) + [sp[chs].sum() / 1e3 for chs in [CHANNELS, *GROUPS.values()]]
    return out


if __name__ == "__main__":
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    shapes = {c: (0.4, np.inf) for c in CHANNELS}
    grid = []
    for sweep in range(3):
        for c in CHANNELS:
            best = None
            for a in ALPHAS:
                for ec in ECS:
                    res, m = fit(df, {**shapes, c: (a, ec)})
                    r = roi(df, res, m).loc[c]
                    if sweep == 2:
                        grid.append({"channel": c, "alpha": a, "ec": ec, "bic": res.bic, "roi": r.roi, "t": r.t})
                    if best is None or res.bic < best[0]:
                        best = (res.bic, (a, ec))
            shapes[c] = best[1]

    res, m = fit(df, shapes)
    out = roi(df, res, m)
    out.insert(0, "alpha", [shapes[c][0] for c in CHANNELS] + [np.nan] * (1 + len(GROUPS)))
    out.insert(1, "ec", [shapes[c][1] for c in CHANNELS] + [np.nan] * (1 + len(GROUPS)))
    out.round(3).to_csv(TAB / "unconstrained_roi.csv")

    g = pd.DataFrame(grid)
    g["dbic"] = g.bic - g.groupby("channel").bic.transform("min")
    g.round(3).to_csv(TAB / "unconstrained_shapes.csv", index=False)

    k = len(X_BASE) + 1
    el = res.params[X_BASE.index("log_rel_price") + 1] / df.volume_kg.mean()
    print(f"rho {res.model.rho[0]:.2f}  R2 {res.rsquared:.3f}  price elasticity {el:.2f}  "
          f"resid ACF1 {pd.Series(res.wresid).autocorr():.2f}")
    print(out.round(2).to_string())
    print("\nROI range across shapes within 2 BIC points of the best (shape not identified if wide):")
    print(g[g.dbic < 2].groupby("channel").agg(n=("roi", "size"), roi_min=("roi", "min"), roi_max=("roi", "max"))
          .round(2).to_string())
