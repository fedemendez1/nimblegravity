# Session handoff — Marketing Science Manager case study

Written so a fresh session can pick this up without re-reading the whole transcript.
Read this file first, then `README.md` for the analysis itself.

## 1. The task

A take-home for a **Marketing Science Manager** role at **Wise**. Two PDFs were
supplied by the user (the brief and the dataset). They are **uploads, not in the
repo** — if you need them again, ask the user to re-attach.

The brief in one line: one year of weekly paid display data measured three
different ways (vendor UI, internal first-touch rule-based attribution, MMM).
Produce insights and recommendations for the paid display team.

Five sub-questions, all answered in the deck:
- **a** top 3 insights
- **b** what explains the differences; advantages and limitations of each method
- **c** how the team should use these approaches for investment decisions
- **d** risks and opportunities of the proposed approach
- **e** what additional approaches to implement

Hard constraints from the brief: **max 4 slides** for the task, output as
PowerPoint/Google Slides, and it **must stand on its own** (no live presenter).

## 2. Current state — DONE and delivered

Branch `claude/marketing-science-takehome-u4jms5`, pushed. Two commits:
`608e713` (analysis + deck) and `0218617` (slide images + wording fix).

The deck is finished, schema-validated, visually QA'd at true font metrics, and
the user has seen all four slides as images. **Nothing is outstanding.**

```
marketing_science_case_study/
├── Marketing_Science_Case_Study.pptx   ← THE DELIVERABLE (4 slides, 13.33×7.5)
├── CONTEXT.md                          ← this file
├── README.md                           ← analysis write-up + how to reproduce
├── data.csv                            ← cleaned dataset (104 rows)
├── parse.py                            ← PDF text → data.csv, dedup checks
├── a3.py a4.py a5.py                   ← the three analysis passes
├── verify.py                           ← re-checks every number quoted on slides
├── fig1.py fig23.py fig3.py            ← chart generators
├── deck.js                             ← builds the .pptx (pptxgenjs)
├── charts/   fig1_divergence.png, fig2_cpa.png, fig3_reversal.png
└── slides/   slide-1.jpg … slide-4.jpg  (150 dpi renders)
```

Note `fig23.py` generates fig2 **and** an older fig3; `fig3.py` then overwrites
fig3 with the compact wide version the deck actually uses. Run `fig23.py` before
`fig3.py`, never after.

## 3. Data preparation (do not skip if re-deriving)

Source PDF parsed to 107 rows. Two deliberate traps:

| Issue | Handling |
|---|---|
| 3 exact duplicate rows (`2022-09-19` + `2022-10-17` Tradedesk, `2022-10-24` Google UAC) | removed → 104 rows = 52 weeks × 2 vendors |
| 4 May weeks with `rule_based = 0` (2022-05-02/09/16/23, Tradedesk) | a tag outage, **flagged not dropped** — £34.6k spend, apparent CPA £206 |

The PDF's header row is garbled on extraction. Correct column order is:
`week, channel, vendor, vendor_ui_registrations, rule_based(internal), mmm, spend_gbp`.

## 4. The three findings (all verified by `verify.py`)

**1 — The gap is stable, so it is calibratable.** Across Jan–Oct the ratios barely
move. Vendor UI runs +22% above MMM, rule-based 29% below. Per vendor, UI/MMM is
1.17 (Tradedesk) and 1.28 (Google UAC); rule-based/MMM is 0.47 and 1.06.
Full year: spend £887,990 · UI 65,670 · rule-based 36,119 · MMM 50,534 regs.

**2 — Rule-based structurally breaks programmatic display.** It sees 47% of
Tradedesk's incremental registrations → CPA £44.31 vs £18.77 on MMM (2.4×).
Tradedesk is 61% of spend but 57% of incremental registrations.

**3 — Q4 inverts the signals.** Vendor UI CPA *improves* (Google −13%, Tradedesk
−22%) while MMM says Google got 23% *worse*. Regression on Google UAC:
`log(CPA_mmm) = 2.31 + 0.047·log(spend) + 0.180·Q4` → the Q4 effect is **+20% at
identical spend**, i.e. seasonality/incrementality, **not saturation**.
The rankings flip: vendor UI says Google is the better Q4 buy (£10.31 vs £12.81);
MMM says Tradedesk (£17.48 vs £18.75).

**Calibration factors (k = MMM ÷ platform-reported), the deck's actionable core:**

| Vendor | k Jan–Oct | k Nov–Dec | UI CPA target for an £18 true CPA |
|---|---|---|---|
| Google UAC | 0.78 | 0.55 | £14.09 → £9.90 |
| Tradedesk | 0.86 | 0.73 | £15.39 → £13.19 |

Applying the Jan–Oct k to December overstates incremental output by **27% overall,
42% on Google UAC**.

**Deliberately NOT claimed:** no saturation curve. Elasticity is 0.96–0.99 with
R² > 0.99 in the observed range — there is no evidence of a ceiling, and the deck
says so and proposes testing for it. This honesty is also listed as a risk on
slide 4 (co-movement ≠ causation).

## 5. Deck structure

| Slide | Answers | Content |
|---|---|---|
| 1 | a | 3 insight cards + hero line chart (weekly registrations, both events annotated) |
| 2 | b | CPA-by-vendor×method bar chart, three drivers of the gap, 3-column advantages/limitations |
| 3 | c | Three roles (MMM / calibrated UI / experiments), calibration table, 4 rules of engagement, Q4 reversal chart |
| 4 | d + e | Dark slide: risks & mitigations, opportunities, prioritised roadmap |

Design: forest `163300` / lime `9FE870`, Cambria titles + Calibri body, cards with
numbered badges. Chart palette is the validated dataviz categorical set
(blue `2A78D6` / orange `EB6834` / aqua `1BAF7A`), vendors use violet `4A3AA7` /
yellow `EDA100`. Deck is in **English** (recipient is `marketing_analysts@wise.com`,
data in GBP); the user is addressed in **Spanish**.

## 6. Environment gotchas — THIS WILL SAVE YOU AN HOUR

A fresh container reverts all of these. Re-apply before touching the deck.

```bash
# PDF text extraction — system cryptography is broken (pyo3 panic on import)
pip install --ignore-installed cffi cryptography && pip install pdfplumber

# Deck build — pptxgenjs is NOT preinstalled here despite the skill saying so
npm install pptxgenjs

# Rendering. LibreOffice ships with ONLY libreoffice-core/-common: no document
# filters at all, so it fails to load even a .txt with the misleading error
# "source file could not be loaded". This is NOT a corrupt-file problem.
apt-get update -qq
apt-get install -y --no-install-recommends libreoffice-impress poppler-utils
# Metric-compatible fonts, or Cambria/Calibri get substituted by wider faces and
# the render shows overflow that will not occur in real PowerPoint:
apt-get install -y --no-install-recommends fonts-crosextra-caladea fonts-crosextra-carlito

# Then:
soffice --headless -env:UserInstallation=file:///tmp/loprof \
        --convert-to pdf --outdir <dir> <dir>/deck.pptx
pdftoppm -jpeg -r 150 deck.pdf slide
```

Installing the fonts is not optional: the first true render showed the slide-1
title wrapping to two lines and colliding with the subtitle. That was pure font
substitution — with Caladea/Carlito installed it fits on one line.

## 7. QA before shipping any deck change

```bash
node deck.js                                                   # rebuild
python3 <pptx-skill>/scripts/office/validate.py Marketing_Science_Case_Study.pptx
python3 verify.py                                              # numbers still true?
# then render per §6 and LOOK at all four images
```

The transcript also built a throwaway PIL-based QA renderer for overflow and
overlap detection. It was only needed because LibreOffice was unavailable and it
**underestimated Cambria's width**, so it is not worth recreating — install the
fonts and render for real instead.

## 8. If the user asks for changes

Everything is generated, so edit the source and rebuild — never hand-edit the
`.pptx` XML. Text and layout live in `deck.js`; chart content in `fig*.py`.
Slide geometry is tight by design: content sits between y≈1.58 and y≈7.08 on a
7.5" slide, with 0.5" side margins. If you add a line, something else must give.
