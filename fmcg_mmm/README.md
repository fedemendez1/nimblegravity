# FMCG bottled water – Marketing Mix Model

Weekly national MMM (Jan 2022 – Jan 2025, 161 weeks) built with Google Meridian 2.1.
Results dashboard: `outputs/dashboard.html` (rebuilt by `05_dashboard.py`).

## Run

```bash
python -m venv .venv && .venv/bin/pip install -r fmcg_mmm/requirements.txt
cd fmcg_mmm/src
python 01_clean.py                       # data/clean/model_data.csv
python 02_eda.py                         # outputs/figures/eda_*
# final model: first stage (+ brand C promo), then AR(1) correction from its lagged residuals (~12 min each)
python 03_model.py --tag stage1 --extra comp_c_promo_share
python 03_model.py --tag final --extra comp_c_promo_share --ar-from mmm_stage1
python 03_model.py --tag stage1 --extra comp_c_promo_share --holdout 26
python 03_model.py --tag final --extra comp_c_promo_share --ar-from mmm_stage1_ho26 --holdout 26
python 04_diagnostics.py --model mmm_final   # outputs/tables/*, figures diag_* and results_*
python 05_dashboard.py                       # outputs/dashboard.html
# sensitivity (shorter chains)
python 03_model.py --tag k6 --knots 6 --no-season --draws screen        # etc.
python 03_model.py --tag wide --roi-prior 0,1.5 --draws screen
python 06_sensitivity.py --tags f1 wide tight high --out sens_priors
python 03_model.py --tag final_h22 --heat 22 --extra comp_c_promo_share --ar-from mmm_stage1 --draws screen
python 07_residuals.py --model mmm_cC        # residual autocorrelation deep-dive
python 08_ar_twin.py [--student-t] [--het]   # PyMC twin: joint AR(1), fat tails, level-dependent noise
```

## Model definition

| Piece | Choice | Why |
|---|---|---|
| KPI | `Volume_Sales` (kg), `revenue_per_kpi = Avg_PPKG` | Price stays a driver, not part of the KPI; ROI still in £ |
| Media execution | TV GRPs; digital video (VOD+OLV), social, search+RDM impressions; partnership and OOH spend (no execution metric) | Transformations run on exposure, spend only enters the ROI denominator. Social CPM varies 3× (CV 29%), RDM CV 93% |
| Channels | 6: TV, digital video, social, partnership, OOH, search+RDM | TV kept apart from digital video: GRPs and impressions don't add up, and TV CPM (~£2) vs VOD (~£29) would let TV dominate a merged group |
| Media transforms | Geometric adstock (max lag 8) + Hill | Meridian defaults |
| Media prior | ROI ~ LogNormal(0, 0.7): median 1.0, 90% ≈ [0.3, 3.2] | Same for all channels so differences come from data; tested against alternatives below |
| Business drivers (non-media treatments) | avg temperature, heat above 20°C max, ACV distribution, log price premium vs competitors, promo intensity (depth × breadth), competitor media, brand C promo share | Contribution reported natively; weak N(0,5) coefficient prior so signs come from data. Brand C promo was the only omitted variable with a sensible link to the residuals |
| Controls | rainfall, New Year week dummy, annual Fourier pair | Fourier: at equal temperature spring sells more than autumn |
| Baseline | 1 knot (flat intercept) | 6 knots absorb the price effect (elasticity → 0); 12 knots overfit (holdout MAPE 34%) |
| Residual persistence | AR(1) errors via feasible GLS: last week's first-stage residual enters as a control | Meridian has no AR term. Demand shocks last weeks (displays, stock-outs, retailer actions). The term adds no volume on average and leaves elasticity and ROI unchanged; it stops the model treating 161 weeks as independent. Checked against a PyMC twin that estimates ρ jointly (0.42) |

Price: own and competitor log prices correlate 0.86 (shared inflation trend), so own and cross elasticities are not separable – the model uses the premium. Promo: depth and breadth are combined into one intensity measure (breadth alone r=0.95 with it).

## Diagnostics (final model, 4 chains × 1,000 draws)

| Check | Result |
|---|---|
| Convergence | max R-hat 1.004, min bulk ESS > 2,900, 0 divergences |
| In-sample fit | R² 0.93, MAPE 4.0% (part of the R² gain over 0.91 comes from the AR term, not from better drivers) |
| Out-of-sample (last 26 weeks, Aug 24 – Jan 25) | R² 0.91, MAPE 4.6% (train 0.93 / 4.1%). The 13-week holdout gives R² 0.67 with MAPE 3.8%: Nov–Jan is flat, so R² is low against little variance |
| Autocorrelation | lag-1 ACF −0.02, Durbin-Watson 2.01; Ljung-Box p = 0.76 / 0.67 / 0.80 / 0.32 at lags 1 / 4 / 13 / 26 (0.35 and DW 1.26 before the AR correction) |
| Collinearity | max VIF 8.7 (avg temperature, with seasonality and heat); price 3.4, distribution 3.2, media channels 1.5–3.1 |
| Normality / variance | Residuals have fat tails (excess kurtosis 2.0, Jarque-Bera p < 0.001) and are larger in summer (Breusch-Pagan p < 0.001 in kg, 0.02 in %). Both come from noise scaling with volume: a twin with level-dependent variance gives normal, homoskedastic residuals (JB p 0.15, BP p 0.20) and the same conclusions (elasticity −0.41, ROI 1.06) |
| Heat threshold | 18/20/22/24°C: elasticity −0.46 to −0.50, blended ROI 1.03–1.15, TV lowest and OOH highest in all; 20–22°C fit best |
| Signs | All as expected; competitor media ≈ 0 (its earlier positive effect was unmodelled seasonality) |

## Headline results

- **Media drives ~0.8% of volume.** £3.5m over 3 years (0.8% of value sales). Blended revenue ROI **1.06** (90%: 0.65–1.66), before margin.
- **Channel ROIs are not identified by this data.** Fit is identical across ROI priors (R² 0.929–0.930) and channel ROIs move with the prior. Read them as "around break-even, ranking uncertain". This is weak signal, not collinearity (media VIFs ≤ 3.1).
  - Blended ROI is fairly robust: 0.99–1.06 across median-1 priors; 1.61 when the prior is centred on 2 (the data pulls it down from 2).
  - TV is the channel the data pulls furthest below its prior: 0.69, P(ROI>1) = 24%, and lowest under every prior and heat threshold tested.
- **Price is the biggest lever after distribution.** Elasticity to the premium vs competitors is **−0.47** (90%: −0.56 to −0.38; −0.61 to −0.32 once AR(1) uncertainty is propagated). The premium (~1.8× competitors) costs ~15% of volume against the lowest observed premium.
- **Other drivers** (vs lowest observed level):
  - distribution **+27%**
  - temperature **+9%**, plus **+6%** in heat weeks
  - brand C promotions **−2.4%**
  - promo intensity ≈ 0 (+2% per +10pp, 90% interval −2% to +5%)

## With more time

1. Calibrate channel ROIs with geo/lift tests (Meridian `roi_calibration`). The sensitivity shows the data alone cannot rank channels.
2. Run a geo-level model if regional data exists.
3. Measure profit ROI (margin), and long-term/brand effects.
4. Separate own vs cross price elasticity with more price variation or category-level data; model the post-promo dip explicitly.
5. Run budget optimisation (`meridian.analysis.optimizer`) once channel ROIs are calibrated.
