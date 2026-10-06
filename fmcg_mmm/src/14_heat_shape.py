import importlib, sys, numpy as np, pandas as pd, statsmodels.api as sm
sys.path.insert(0, '.')
from style import ROOT
unc = importlib.import_module("10_unconstrained")
df = pd.read_csv(ROOT / "data/clean/model_data.csv")
tmax = df.heat_excess.where(df.heat_excess > 0)  # only know tmax above 20
print("weeks with peak >20/>25/>28:", (df.heat_excess>0).sum(), (df.heat_excess>5).sum(), (df.heat_excess>8).sum())
base = [c for c in unc.X_BASE if c != "heat_excess"]
m = np.column_stack([unc.transform(df[f"exec_{c}"].to_numpy(float), 0.4, np.inf) for c in unc.CHANNELS])
def fit(cols):
    X = sm.add_constant(pd.DataFrame(np.column_stack([df[base].to_numpy(float), cols, m])))
    return sm.GLSAR(np.log(df.volume_kg.to_numpy()), X, rho=1).iterative_fit(maxiter=20)
h = df.heat_excess.to_numpy()
for name, cols in {"linear": [h], "hinge20+25": [h, np.maximum(h-5,0)], "hinge20+28": [h, np.maximum(h-8,0)], "quadratic": [h, h**2]}.items():
    r = fit(np.column_stack(cols)); k = len(base)+1
    b, se = r.params[k:k+len(cols)], r.bse[k:k+len(cols)]
    print(f"{name:12s} bic {r.bic:8.1f}", " ".join(f"{x*100:+.2f}%(se {s*100:.2f})" for x, s in zip(b, se)))
