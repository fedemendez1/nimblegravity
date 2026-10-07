"""Classical regression version of the model: no priors, any sign allowed, AR(1) errors by iterated GLS.

Adstock decay and half-saturation are picked per channel by BIC grid search. Shows what the data alone says
about media ROI, and whether the curve shapes are identified (the BIC grid is nearly flat if not).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from style import ROB, ROOT  # noqa: E402

CHANNELS = ["tv", "digital_video", "social", "partnership", "ooh", "search_rdm"]
X_BASE = ["temp_avg", "heat_excess", "distribution", "log_rel_price", "promo_intensity", "comp_media_spend",
          "comp_c_promo_share", "rainfall", "new_year_week", "season_sin", "season_cos"]
# channels that ran together are also read jointly
GROUPS = {"All ex search_rdm": ["tv", "digital_video", "social", "partnership", "ooh"],
          "digital_video + ooh": ["digital_video", "ooh"], "tv + partnership": ["tv", "partnership"]}
ALPHAS = [0.0, 0.2, 0.4, 0.6, 0.8]
ECS = [0.5, 1.0, 2.0, 4.0, np.inf]  # x median active exposure; inf = linear
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
    """Linear in the media coefficients, so the CI comes from their covariance."""
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
    out.round(3).to_csv(ROB / "unconstrained_roi.csv")

    g = pd.DataFrame(grid)
    g["dbic"] = g.bic - g.groupby("channel").bic.transform("min")
    g.round(3).to_csv(ROB / "unconstrained_shapes.csv", index=False)

    el = res.params[X_BASE.index("log_rel_price") + 1] / df.volume_kg.mean()
    print(f"rho {res.model.rho[0]:.2f}  R2 {res.rsquared:.3f}  price elasticity {el:.2f}  "
          f"resid ACF1 {pd.Series(res.wresid).autocorr():.2f}")
    print(out.round(2).to_string())
    print("\nROI range across shapes within 2 BIC points of the best:")
    print(g[g.dbic < 2].groupby("channel").agg(n=("roi", "size"), roi_min=("roi", "min"), roi_max=("roi", "max"))
          .round(2).to_string())
