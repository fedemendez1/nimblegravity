# FMCG bottled water – Marketing Mix Model

Weekly national MMM (Jan 2022 – Jan 2025, 161 weeks) built with Google Meridian 2.1.
Results dashboard: `outputs/dashboard.html` (rebuilt by `05_dashboard.py`).

## Run

```bash
python -m venv .venv && .venv/bin/pip install -r fmcg_mmm/requirements.txt
cd fmcg_mmm/src
python 01_clean.py                       # data/clean/model_data.csv
python 02_eda.py                         # outputs/figures/eda_*
python 03_model.py                       # outputs/mmm_base.pkl (~12 min on 4 CPUs)
python 03_model.py --holdout 13          # outputs/mmm_base_ho13.pkl
python 04_diagnostics.py                 # outputs/tables/*, figures diag_* and results_*
python 05_dashboard.py                   # outputs/dashboard.html
# sensitivity (shorter chains)
python 03_model.py --tag k6 --knots 6 --no-season --draws screen        # etc.
python 03_model.py --tag wide --roi-prior 0,1.5 --draws screen
python 06_sensitivity.py --tags f1 wide tight high --out sens_priors
```

## Model definition

| Piece | Choice | Why |
|---|---|---|
| KPI | `Volume_Sales` (kg), `revenue_per_kpi = Avg_PPKG` | Price stays a driver, not part of the KPI; ROI still in £ |
| Media execution | TV GRPs; digital video (VOD+OLV), social, search+RDM impressions; partnership and OOH spend (no execution metric) | Transformations run on exposure, spend only enters the ROI denominator. Social CPM varies 3× (CV 29%), RDM CV 93% |
| Channels | 6: TV, digital video, social, partnership, OOH, search+RDM | TV kept apart from digital video: GRPs and impressions don't add up, and TV CPM (~£2) vs VOD (~£29) would let TV dominate a merged group |
| Media transforms | Geometric adstock (max lag 8) + Hill | Meridian defaults |
| Media prior | ROI ~ LogNormal(0, 0.7): median 1.0, 90% ≈ [0.3, 3.2] | Same for all channels so differences come from data; tested against alternatives below |
| Business drivers (non-media treatments) | avg temperature, heat above 20°C max, ACV distribution, log price premium vs competitors, promo intensity (depth × breadth), competitor media | Contribution reported natively; weak N(0,5) coefficient prior so signs come from data |
| Controls | rainfall, New Year week dummy, annual Fourier pair | Fourier: at equal temperature spring sells more than autumn |
| Baseline | 1 knot (flat intercept) | 6 knots absorb the price effect (elasticity → 0); 12 knots overfit (holdout MAPE 34%) |

Price: own and competitor log prices correlate 0.86 (shared inflation trend), so own and cross elasticities are not separable – the model uses the premium. Promo: depth and breadth are combined into one intensity measure (breadth alone r=0.95 with it).

## Diagnostics (base model, 4 chains × 1,000 draws)

| Check | Result |
|---|---|
| Convergence | max R-hat 1.003, min bulk ESS > 2,900, 0 divergences |
| In-sample fit | R² 0.91, MAPE 4.4% |
| Out-of-sample (last 13 weeks) | MAPE 4.0% (train 4.5%), R² 0.65 |
| Residuals | lag-1 autocorrelation 0.35, no yearly pattern. Short-run persistence that neither knots nor seasonality remove; effective sample ≈ half, so true intervals are ~1.4× wider than reported |
| Signs | All as expected; competitor media now ≈ 0 (its earlier positive effect was unmodelled seasonality) |

## Headline results

- **Media drives ~0.8% of volume.** £3.5m over 3 years (0.8% of value sales). Blended revenue ROI **1.13** (90%: 0.69–1.80), before margin.
- **Channel ROIs are not identified by this data.** Fit is identical across ROI priors (R² 0.914–0.915) and channel ROIs move with the prior. Read them as "around break-even, ranking uncertain".
  - Blended ROI is fairly robust: 1.06–1.16 across median-1 priors; 1.75 when the prior is centred on 2 (the data pulls it down from 2).
  - TV is the only channel the data pulls below its prior: 0.72, P(ROI>1) = 26%, and lowest under the wide prior too.
- **Price is the biggest lever after distribution.** Elasticity to the premium vs competitors is **−0.48** (90%: −0.58 to −0.39). The premium (~1.8× competitors) costs ~16% of volume against the lowest observed premium.
- **Other drivers** (vs lowest observed level):
  - distribution **+27%**
  - temperature **+10%**, plus **+6%** in heat weeks
  - promo intensity ≈ 0 (+1% per +10pp, 90% interval −3% to +5%)

## With more time

1. Calibrate channel ROIs with geo/lift tests (Meridian `roi_calibration`). The sensitivity shows the data alone cannot rank channels.
2. Run a geo-level model if regional data exists.
3. Measure profit ROI (margin), and long-term/brand effects.
4. Separate own vs cross price elasticity with more price variation or category-level data; model the post-promo dip explicitly.
5. Address residual autocorrelation outside Meridian (e.g. AR(1) errors in a custom PyMC/Stan model) to check interval width.
6. Run budget optimisation (`meridian.analysis.optimizer`) once channel ROIs are calibrated.
