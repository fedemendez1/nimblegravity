"""Year-on-year due-to at regressor level (each driver, control and media channel on its own) from mmm_final.

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

    dt = story.due_to(df, terms)
    yr = pd.to_datetime(df.time).dt.year.to_numpy()
    v = {y: df.volume_kg[yr == y].sum() for y in (2022, 2023)}
    dt["m_kg"] = dt.pct / 100 * dt.period.str[:4].astype(int).map(v)
    dt.round(3).to_csv(TAB / "story_due_to_regressors.csv", index=False)
    print(dt.round(2).to_string(index=False))
