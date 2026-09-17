# Marketing Science Manager — Case Study

**Deliverable:** `Marketing_Science_Case_Study.pptx` (4 slides, stands on its own).

## The question

One year of weekly paid display data (52 weeks, 2 vendors, £888k spend) measured
three different ways — vendor UI, internal first-touch rule-based attribution, and
MMM. What should the paid display team actually do with three numbers that disagree?

## Data preparation

| Step | Finding |
|---|---|
| Parsed source | 107 rows |
| Removed exact duplicates | 3 rows (`2022-09-19` + `2022-10-17` Tradedesk, `2022-10-24` Google UAC) |
| Final dataset | 104 rows = 52 weeks × 2 vendors |
| Flagged, not dropped | 4 weeks in May with `rule_based = 0` — a tag outage, not zero performance |

`data.csv` is the cleaned dataset.

## Headline findings

1. **The gap is stable, so it is calibratable.** For 10 months the ratios barely
   move — vendor UI runs +22% above MMM, rule-based 29% below. Per vendor,
   UI/MMM is 1.17 (Tradedesk) and 1.28 (Google UAC).
2. **Rule-based structurally breaks programmatic display.** It sees 47% of
   Tradedesk's incremental registrations → reported CPA £44.31 vs £18.77 on MMM.
3. **Q4 inverts the signals.** Vendor UI CPA improves while MMM says Google UAC
   got 23% worse. Controlling for spend, the Q4 effect is +20% CPA at identical
   budget (seasonality, not saturation): `log(CPA) = 2.31 + 0.047·log(spend) + 0.180·Q4`.
   The two rulers rank the vendors in opposite order.

## Reproducing the analysis

```bash
pip install pandas numpy matplotlib
python3 parse.py     # dataset -> data.csv (dedup + row count checks)
python3 a3.py        # outage, structural bias, Q4 regime break, ranking reversal
python3 a4.py        # elasticity / saturation tests, reallocation scenario
python3 a5.py        # Q4 decomposition, calibration factors, target translation
python3 verify.py    # every number quoted on the slides
```

Charts (`charts/`) are regenerated with `fig1.py`, `fig23.py` and `fig3.py`;
`deck.js` builds the .pptx with pptxgenjs (`npm install pptxgenjs && node deck.js`).

## Assumptions

- MMM output is treated as the best available estimate of incremental registrations.
  It is an estimate with confidence intervals, not ground truth — slide 4 says so
  explicitly and proposes experiments as the anchor.
- The `internal_registrations` column in the source is the
  `rule_based_tracked_registrations` field described in the brief.
- Elasticity is measured within the observed spend range only; no claim is made
  about behaviour beyond it.
