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

## Diagnostics (full model, 4 chains × 1,000 draws)

| Check | Result |
|---|---|
| Convergence | max R-hat 1.003, min bulk ESS > 3,500, 0 divergences |
| In-sample fit | R² 0.90, MAPE 4.9% |
| Out-of-sample (last 13 weeks held out) | MAPE 5.8% vs 5.0% train. Test R² is low (0.14) because winter holdout weeks have little variance – MAPE is the meaningful metric here |
| Residuals | lag-1 autocorrelation 0.33 (remaining – see next steps), no yearly pattern (lag-52 ≈ 0) |
| Signs | All drivers as expected except competitor media (positive, P=0.97) – read as category expansion |
| Collinearity | VIF < 5 for all regressors after dropping promo depth |

## Headline results

- **Media drives ~1% of volume.** Total spend £3.5m over 3 years (0.8% of value sales). Blended revenue ROI **£1.31 per £1** (90% CI 0.76–2.06), before margin.
- **Video and social:** the data narrows the ROI interval (posterior ~0.65–0.70 of the prior width). ROI ≈ 1.0 for video and 0.85 for social.
- **Partnership, OOH and search/RDM:** the data cannot identify them; the posterior is as wide as or wider than the prior. Their point estimates should not drive budget decisions.
- **Base business drivers** (contribution measured against each driver's lowest observed level):
  - distribution **+28%**
  - temperature **+20%**, plus **+6%** from heatwave weeks
  - relative price premium **−15%**
  - promo breadth ~+1% (weak)

## With more time

1. Use impressions/GRPs as the execution metric where available, and a reach & frequency setup for video.
2. Calibrate ROI priors with lift tests or geo experiments. This is the only real fix for channels with 3–5 active weeks.
3. Run a geo-level model (Meridian's real strength) if regional sales and media data exist.
4. Run prior sensitivity (tighter/looser ROI priors), knots sensitivity, and binomial adstock for video.
5. Fix residual autocorrelation: test a time-varying baseline (AKS knots) against the risk of absorbing media.
6. Model price and promo properly: price elasticity curve, promo depth × breadth interaction, and the price endogeneity of promo.
7. Run budget optimisation and response curves (`meridian.analysis.optimizer`) once channel ROIs are better identified.
8. Measure profit ROI (margin data) and long-term/brand effects.
