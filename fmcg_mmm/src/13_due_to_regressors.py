"""Year-on-year due-to at regressor level (each driver, control and media channel on its own) from mmm_final.

Weekly-average basis (52-week equivalent), so the extra week in 2022 is not read as a change.

Output: tables/story_due_to_regressors.csv
"""
import importlib

import numpy as np
import pandas as pd
from meridian.analysis import analyzer

from style import ROOT, TAB

diag = importlib.import_module("04_diagnostics")
story = importlib.import_module("11_story")

if __name__ == "__main__":
    mmm = diag.load("mmm_final")
    a = analyzer.Analyzer(mmm)
    df = pd.read_csv(ROOT / "data/clean/model_data.csv")
    r = pd.read_csv(TAB / "resid_mmm_stage1.csv", index_col=0).iloc[:, 0]
    df["resid_lag1"] = r.shift(1).fillna(0).to_numpy() * 1e6

    _, terms = story.weekly_contributions(mmm, a, df)
    terms.pop("media")
    ch = list(mmm.input_data.media_channel.values)
    inc = np.asarray(a.incremental_outcome(use_kpi=True, aggregate_times=False))
    inc = inc.reshape(-1, *inc.shape[-2:])
    for i, c in enumerate(ch):
        terms[c] = inc[..., i]

    # weekly averages, so 2022's 53rd week does not count as a change
    yr = pd.to_datetime(df.time).dt.year.to_numpy()
    rows = []
    for y0, y1 in ((2022, 2023), (2023, 2024)):
        v0, v1 = df.volume_kg[yr == y0].mean(), df.volume_kg[yr == y1].mean()
        expl = 0
        for k, c in terms.items():
            x = (c[:, yr == y1].mean(1) - c[:, yr == y0].mean(1)) / v0 * 100
            rows.append({"period": f"{y0}-{y1}", "driver": k, **dict(zip(["pct", "lo", "hi"], story.q(x)))})
            expl += np.median(x)
        rows.append({"period": f"{y0}-{y1}", "driver": "Other / unexplained", "pct": (v1 / v0 - 1) * 100 - expl})
        rows.append({"period": f"{y0}-{y1}", "driver": "Total change", "pct": (v1 / v0 - 1) * 100,
                     "start_m_kg": v0 * 52 / 1e6, "end_m_kg": v1 * 52 / 1e6})
    dt = pd.DataFrame(rows)
    dt["m_kg"] = dt.pct / 100 * dt.period.str[:4].astype(int).map({y: df.volume_kg[yr == y].mean() * 52 for y in (2022, 2023)})
    dt.round(3).to_csv(TAB / "story_due_to_regressors.csv", index=False)
    print(dt.round(2).to_string(index=False))
