# FMCG bottled water – Marketing Mix Model

Weekly national MMM (Jan 2022 – Jan 2025, 161 weeks) built with Google Meridian.

## Run

```bash
python -m venv .venv && .venv/bin/pip install -r fmcg_mmm/requirements.txt
cd fmcg_mmm/src
python 01_clean.py          # data/clean/model_data.csv
python 02_eda.py            # outputs/figures/eda_*
python 03_model.py          # outputs/mmm_full.pkl (~10 min on 4 CPUs)
python 03_model.py --holdout 13
python 04_diagnostics.py    # outputs/tables/*, outputs/figures/diag_*, results_*
```

## Model definition

| Piece | Choice | Why |
|---|---|---|
| KPI | `Volume_Sales` (kg), `revenue_per_kpi = Avg_PPKG` | Keeps price as a driver, not baked into the KPI; ROI still reported in £ |
| Media | 5 groups, spend as execution: video (TV+VOD+OLV), social, partnership, OOH, search+RDM | TV has 3 active weeks, OOH 5, search 8 – not identifiable individually |
| Media transforms | Geometric adstock (max lag 8) + Hill saturation | Meridian defaults |
| Media prior | ROI ~ LogNormal(0, 0.7): median 1.0, 90% ≈ [0.3, 3.2] | Weakly informative FMCG prior; same for all channels so differences come from data |
| Business drivers (non-media treatments) | avg temperature, heat above 20°C max, ACV distribution, base price relative to competitors, promo distribution, competitor media | Contribution reported natively; weak N(0,5) coefficient prior so signs come from data |
| Controls | rainfall, New Year week dummy | ~20% volume dip the week of 1 Jan every year |
| Time effect | 1 knot (flat intercept) | Temperature carries seasonality; more knots risk absorbing media |

Dropped on purpose: promo depth (r=0.77 with promo distribution, mechanically tied to base price), promo share of volume (outcome-derived), competitor volume (proxy of category demand, endogenous).
