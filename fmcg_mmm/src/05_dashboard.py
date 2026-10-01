"""Render outputs/tables/*.csv into a single self-contained HTML dashboard."""
import json

import pandas as pd

from style import ROOT, TAB

TEMPLATE = ROOT / "src/dashboard_template.html"
OUT = ROOT / "outputs/dashboard.html"


def records(name, index_col=None):
    df = pd.read_csv(TAB / f"{name}.csv")
    if index_col is not None:
        df = df.rename(columns={df.columns[0]: index_col})
    return json.loads(df.to_json(orient="records"))


data = {
    "roi": records("results_media_roi"),
    "contrib": records("results_contributions_pct"),
    "signs": records("diag_coefficient_signs"),
    "conv": records("diag_convergence", "param"),
    "fit": records("diag_fit"),
    "yearly": records("eda_yearly_summary"),
    "corr": records("eda_correlations", "driver"),
}
html = TEMPLATE.read_text().replace("/*__DATA__*/null", json.dumps(data))
OUT.write_text(html)
print(f"wrote {OUT.relative_to(ROOT)}")
