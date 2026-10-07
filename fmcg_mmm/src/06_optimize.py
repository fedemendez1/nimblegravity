"""Meridian budget optimiser on the final model, annualised.

- same budget, each channel allowed to move up to 30%
- optimal mix at total budgets from 10% to 150% of today's
"""
import importlib
import os
import warnings

os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
warnings.filterwarnings("ignore")

import pandas as pd
from meridian.analysis import optimizer

from style import TAB

diag = importlib.import_module("04_diagnostics")
BOUND = 0.3


def table(ds, years):
    return pd.DataFrame({"spend": ds.spend.values / years,
                         "inc_revenue": ds.incremental_outcome.sel(metric="mean").values / years,
                         "roi": ds.roi.sel(metric="mean").values, "mroi": ds.mroi.sel(metric="mean").values},
                        index=pd.Index(ds.channel.values, name="channel"))


def reallocate(mmm):
    years = mmm.n_times / 52
    r = optimizer.BudgetOptimizer(mmm).optimize(spend_constraint_lower=BOUND, spend_constraint_upper=BOUND)
    mix = table(r.nonoptimized_data, years).join(table(r.optimized_data, years), lsuffix="_now", rsuffix="_opt")
    mix["spend_change_pct"] = (mix.spend_opt / mix.spend_now - 1) * 100
    return mix, float(r.nonoptimized_data.attrs["budget"])


def budget_scenarios(mmm, total):
    years, opt = mmm.n_times / 52, optimizer.BudgetOptimizer(mmm)
    rows = []
    for k in sorted({*(round(0.1 * i, 1) for i in range(1, 16)), 0.85, 1.15}):
        s = opt.optimize(budget=total * k, spend_constraint_lower=BOUND, spend_constraint_upper=BOUND).optimized_data
        io = s.incremental_outcome.sum("channel")
        rows.append({"budget_x": k, "spend": total * k / years, "inc_revenue": float(io.sel(metric="mean")) / years,
                     "lo": float(io.sel(metric="ci_lo")) / years, "hi": float(io.sel(metric="ci_hi")) / years})
    b = pd.DataFrame(rows)
    now = b[b.budget_x == 1].iloc[0]
    b["return_on_change"] = (b.inc_revenue - now.inc_revenue) / (b.spend - now.spend)
    return b


if __name__ == "__main__":
    mmm = diag.load()
    mix, total = reallocate(mmm)
    mix.round(3).to_csv(TAB / "optimizer_mix.csv")
    print(mix.round(2).to_string())
    b = budget_scenarios(mmm, total)
    b.round(3).to_csv(TAB / "optimizer_budget.csv", index=False)
    print(b.round(2).to_string(index=False))
