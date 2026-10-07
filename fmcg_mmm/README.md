# Bottled water brand – marketing mix model

Weekly national model, Jan 2022 – Jan 2025 (161 weeks), built with Google Meridian 2.1.
The presentation is in `presentation/`.

## Structure

```
data/raw/            client data and brief
data/clean/          modelling dataset (01_clean.py)
src/
  01_clean.py        checks and feature build
  02_eda.py          descriptive charts and tables
  03_model.py        two-stage fit of the final model (+ 26-week holdout)
  04_diagnostics.py  convergence, fit, holdout, residual tests, VIF, signs, ROI, response curves
  05_results.py      numbers used in the presentation (due-to, campaign ROI, effects, promotions)
  06_optimize.py     budget optimiser: same-budget mix and total-budget scenarios
  robustness/        sensitivity to priors and heat threshold, PyMC AR(1) check, unconstrained regression
outputs/
  models/            final model, first stage, holdout versions
  figures/, tables/  outputs of 02, 04, 05, 06
  robustness/        outputs of src/robustness
```

## Run

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cd src
python 01_clean.py
python 02_eda.py
python 03_model.py                 # ~25 min
python 03_model.py --holdout 26
python 04_diagnostics.py
python 05_results.py
python 06_optimize.py
python robustness/sensitivity.py   # optional, ~40 min
python robustness/ar_twin.py
python robustness/unconstrained.py
```

The fitted models are included, so steps 04–06 run without refitting.

## Model

| | |
|---|---|
| KPI | volume (kg); revenue = volume × price per kg, so ROI is in £ |
| Media | 6 channels: TV (GRPs), digital video (VOD + OLV impressions), social, search + retail digital (impressions), partnership and outdoor (spend). Geometric adstock (max 8 weeks) + Hill |
| Media prior | ROI ~ LogNormal(0, 0.7) for every channel (median 1) |
| Drivers | average temperature, degrees above 20°C (weekly max), weighted distribution, log price vs competitors, promotion intensity (depth × breadth), competitor media, brand C promotions. N(0, 5) on standardised scale |
| Controls | rainfall, New Year week, annual Fourier pair |
| Baseline | flat (1 knot); more knots absorb the price effect |
| Residual persistence | AR(1) via two-stage feasible GLS: last week's first-stage residual enters as a control |

Own and competitor prices move together (r = 0.86), so the model uses the price premium rather than both.

## Diagnostics (final model, 4 chains × 1,000 draws)

| | |
|---|---|
| Convergence | R-hat ≤ 1.004, no divergences |
| Fit | R² 0.93, MAPE 4.0% |
| Holdout (last 26 weeks) | R² 0.91, MAPE 4.6% |
| Autocorrelation | Durbin-Watson 2.01, lag-1 ACF −0.02, Ljung-Box p > 0.3 up to lag 26 (DW 1.26 before the AR term) |
| Collinearity | max VIF 8.7 (temperature); price 3.4, distribution 3.2, media 1.5–3.1 |
| Residuals | fat tails and more noise in summer: the noise grows with sales; conclusions hold when that is modelled (`robustness/ar_twin.py`) |

## Main results

- Volume was flat (167m kg in 2022, 166m in 2024). The price premium vs competitors went from 1.5× to 1.95× and cost volume (elasticity −0.47); distribution (39% → 44%, 46% today) brought it back, about +3.6% volume per point.
- Each degree above 20°C in a week adds about 4% volume.
- Promotions grew to a third of volume with little incremental volume: about £0.13 back per £1 of discount.
- Media is under 1% of volume and returns about £1.06 per £1 in short-term revenue (90%: 0.65–1.66). Channels that ran together are read by campaign; the curve shapes are not identified by this data, so the optimiser is read for direction (less TV and digital video, more outdoor and social) and budget level (the last £1 returns about 50p).

## Robustness

- ROI prior and heat threshold: elasticity, total media ROI and the optimiser's direction hold across variants (`outputs/robustness/sensitivity.csv`).
- Joint AR(1) in PyMC: same elasticity and ROI, wider intervals (`ar_twin.csv`).
- Regression without priors: total media ROI about 1.2; individual channels not separable (`unconstrained_roi.csv`).

## Next steps

1. Geo tests and staggered channel launches to measure each channel and draw its response curve.
2. Margins and trade terms, to move from revenue to profit and to size the promotion saving.
3. Retailer-level data for distribution and price.
