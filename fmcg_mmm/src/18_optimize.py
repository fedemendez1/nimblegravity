"""Meridian budget optimiser: same-budget reallocation (±30% per channel) on the final model and its prior
variants, plus total-budget scenarios on the final model. Annualised.  -> opt_mix.csv, opt_budget.csv"""
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import pandas as pd
from meridian.analysis import optimizer
from meridian.model import model

from style import ROOT, TAB

MODELS = {"final": "base LN(0,0.7)", "final_wide": "wide LN(0,1.5)", "final_tight": "tight LN(0,0.35)",
          "final_high": "median 2 LN(0.69,0.7)"}
BOUND = 0.3


def table(ds, years):
    io = ds.incremental_outcome.sel(metric="mean").values
    return pd.DataFrame({"spend": ds.spend.values / years, "inc_revenue": io / years,
                         "roi": ds.roi.sel(metric="mean").values, "mroi": ds.mroi.sel(metric="mean").values},
                        index=ds.channel.values)


mix, budget = [], []
for tag, label in MODELS.items():
    mmm = model.load_mmm(str(ROOT / f"outputs/mmm_{tag}.pkl"))
    years = mmm.n_times / 52
    opt = optimizer.BudgetOptimizer(mmm)
    r = opt.optimize(spend_constraint_lower=BOUND, spend_constraint_upper=BOUND)
    cur, new = table(r.nonoptimized_data, years), table(r.optimized_data, years)
    m = cur.join(new, lsuffix="_now", rsuffix="_opt")
    m["spend_change_pct"] = (m.spend_opt / m.spend_now - 1) * 100
    mix.append(m.assign(model=label).rename_axis("channel").reset_index())
    print(f"\n== {label}: revenue gain £{(new.inc_revenue.sum() - cur.inc_revenue.sum()) / 1e3:.0f}k a year")
    print(m[["spend_now", "spend_change_pct", "roi_now", "mroi_now", "mroi_opt"]].round(2).to_string())

    if tag != "final":
        continue
    total = float(r.nonoptimized_data.attrs["budget"])
    for k in sorted({*(round(0.1 * i, 1) for i in range(1, 16)), 0.85, 1.15}):
        s = opt.optimize(budget=total * k, spend_constraint_lower=BOUND, spend_constraint_upper=BOUND).optimized_data
        io = s.incremental_outcome.sum("channel")
        budget.append({"budget_x": k, "spend": total * k / years, "inc_revenue": float(io.sel(metric="mean")) / years,
                       "lo": float(io.sel(metric="ci_lo")) / years, "hi": float(io.sel(metric="ci_hi")) / years})

pd.concat(mix).round(3).to_csv(TAB / "opt_mix.csv", index=False)
b = pd.DataFrame(budget)
base = b[b.budget_x == 1].iloc[0]
b["d_spend"], b["d_revenue"] = b.spend - base.spend, b.inc_revenue - base.inc_revenue
b["return_on_change"] = b.d_revenue / b.d_spend
b.round(3).to_csv(TAB / "opt_budget.csv", index=False)
print("\n== budget scenarios (optimal mix at each level)\n", b.round(2).to_string())
