# Speech plan: Wise case study presentation

**Audience:** Bart de Groot (Analytics Lead) and Ekaterina Bogdanova (Lead Analyst).
**Slot:** 15–20 min for the presentation and Q&A. **Target:** talk for 10 minutes
and leave at least 7 for questions. The questions are where they score
"reasoning when challenged".
**Deck:** `../Marketing_Science_Case_Study.pptx` (4 slides).
**Brief:** `INTERVIEW_BRIEF.md`.

---

## 0. The one sentence

If they remember one thing, it should be this:

> **"The three numbers don't disagree by accident. They answer three different
> questions. So I'd give each one the decision it can actually answer, and
> convert between them with a calibration factor we re-check every quarter."**

Everything in the talk supports this sentence. If you lose your place, come
back to it.

## 1. Delivery rules for a management-level talk

- **Headline first, then evidence.** Each slide opens with its title as a
  sentence. Give the number after that, and the method only if they ask.
- **One number per point.** Say the number that drives the decision, not every
  number on the slide.
- **Keep technical detail out of the main talk.** Regression, elasticity and
  R² go in the backup section (section 4). Mention them only when asked.
- **Speak in decisions and money, not models.** Say "we'd be cutting the vendor
  that drives 57% of growth", not "first-touch has low recall on view-through".
- **Keep the customer in view.** At Wise, a wasted marketing pound is a pound
  not spent on keeping prices low for customers. Say this once, in the close.
- **Signpost.** At each slide change, say one line on where you are going.
- **Use "I".** They want your individual contribution. Say "I found" and "I'd
  recommend", not "we".

## 2. The talk (≈10 min)

The talk is built as STAR across the whole presentation: **Situation and Task**
in the opening, **Action** in slides 1–3, **Result** in slide 4 and the close.

### Opening: Situation and Task (≈45 s, no slide or title slide)

> "Thanks for having me. Let me start with the problem as I understood it.
>
> The paid display team spent about **£888k over a year** on two vendors,
> Tradedesk and Google UAC. They had **three systems** telling them how many
> registrations that spend drove: the vendor dashboards, our internal
> first-touch attribution, and the MMM. The three answers differed by up to
> **30%**.
>
> So the real question wasn't 'which number is right'. It was: **which number
> should the team use for which decision, so budget goes where it creates
> real new customers?**
>
> I'll take you through three findings, what explains them, how I'd have the
> team use each system, and the risks and next steps. About ten minutes, then
> I'd love your questions."

### Slide 1: the three findings (≈2.5 min)

**Headline:** *Three rulers, three answers, and only one should set the budget.*

> "The chart shows weekly registrations as each system sees them. Blue is the
> vendor dashboards, green is the MMM, orange is first-touch.
>
> Before any analysis I cleaned the data. There were **three duplicate rows**,
> and **four weeks in May where first-touch dropped to zero** for Tradedesk.
> That was a tracking outage, not a performance drop. I flagged those weeks
> rather than deleting them, because they turned out to be a finding.
>
> Then three insights."

1. **"The gap is stable, so it's a conversion factor, not a crisis."**
   For ten months, vendor dashboards ran about **22% above** the MMM and
   first-touch about **29% below**, almost constant week to week. A predictable
   bias can be corrected.

2. **"First-touch structurally breaks programmatic display."**
   It sees only **47% of Tradedesk's** incremental registrations, so Tradedesk
   looks **2.4× more expensive** than it is (£44 vs £19 per registration). If
   the team managed on it, they'd cut the vendor delivering **57% of
   incremental growth**. It also failed silently for four weeks in May.

3. **"In Q4 the signals invert."**
   In Nov–Dec the vendor dashboards say performance **improved**. The MMM says
   Google got **23% worse**. The dashboards pointed the team the wrong way at
   the most expensive time of year.

**Transition:** "So why do they disagree this much?"

### Slide 2: why they differ (≈2 min)

**Headline:** *They disagree because they answer three different questions.*

> "None of them is wrong. Each answers a different question:
>
> - The **vendor dashboard** asks: *did my platform see a conversion after my
>   ad?* Each vendor marks its own homework, and they double-count people both
>   vendors touched.
> - **First-touch** asks: *which tracked touchpoint came first?* It only sees
>   clicks it can cookie, which is fine for in-app UAC and very poor for
>   display.
> - The **MMM** asks: *what would not have happened without the spend?* It's
>   the only one that takes out demand that would have come anyway.
>
> That third point explains Q4. When organic demand rises, the two tracking
> systems take credit for it. The MMM doesn't.
>
> Each has a job it's good at. The dashboard is granular and real-time, and
> it's the only signal the bidding algorithms use. First-touch is consistent
> and independent, so it's an excellent tracking-health monitor. The MMM is
> the only measure of incrementality, but it's slow and aggregate.
>
> The mistake is comparing them like-for-like."

**Transition:** "So here's how I'd put them to work."

### Slide 3: what the team should do (≈2.5 min, the most important slide)

**Headline:** *MMM sets the budget, calibrated platform data steers it.*

> "Three roles, each matched to the decision it can answer:
>
> - **How much?** The MMM sets the budget and owns **one true cost per
>   registration target**, refreshed quarterly.
> - **Where and what?** The vendor dashboards run daily optimisation of
>   creative, audiences and bidding, but only *within* a vendor, never to
>   compare vendors.
> - **Is it true?** Experiments twice a year settle disagreements and keep
>   the other two honest.
>
> The link between them is one number, **k**: the share of what the platform
> reports that is actually incremental. For Google it's **0.78** most of the
> year and **0.55** in Q4.
>
> In practice: the business says it can afford **£18 per real registration**.
> In Google's dashboard that means a target of **£14 for most of the year, and
> £10 in Q4**. Same business goal, translated into the language the platform
> and the media buyer actually use."

Point to the Q4 bar chart:

> "This is why it matters. In Q4 the dashboards say Google is the better buy.
> The MMM says Tradedesk is. Without calibration, the team would have moved
> money the wrong way."

Four rules, one line each:

> "Set one target and translate it. Never compare vendors on raw platform
> numbers. Demote first-touch to a monitor, with an alert that would have
> caught May on day one. When the systems disagree, run a test instead of
> arguing about it."

**Transition:** "No framework is risk-free, so let me be clear about where
this one could break."

### Slide 4: risks, opportunities, next steps (≈2 min)

**Headline:** *The framework is only as good as the calibration behind it.*

> "Two risks matter most:
>
> - **The calibration factor drifts.** Q4 proved it. If you keep using the
>   Jan–Oct factor in December, you overstate results by **27%**, and by
>   **42%** on Google. Mitigation: re-fit quarterly and alert on drift.
> - **The MMM becomes a single point of failure.** It's an estimate with wide
>   intervals, so it never drives in-flight decisions, and we always show the
>   range next to the number.
>
> The biggest opportunity: **spend tripled during the year with no visible
> sign of diminishing returns.** I'm deliberately not claiming there's
> headroom, because the data can't prove that. But it's the cheapest big
> question to test.
>
> What I'd build next, in order:
> 1. **Geo-holdout experiments per vendor.** Start now. Everything else is
>    calibrated against them.
> 2. **One calibrated KPI** instead of three competing numbers.
> 3. **Better tracking at source** (server-side conversions), so the gap
>    shrinks instead of being corrected afterwards.
> 4. **Vendor-level MMM with response curves**, so budgets move on marginal
>    cost, not average cost."

### Close: Result (≈30 s)

> "So, back to the question. Instead of three numbers that create arguments,
> the team gets **one target, one currency and a clear owner for each
> decision**.
>
> The impact: if the team planned on the vendor dashboards, they'd be counting
> about **15,000 registrations a year that the spend didn't actually create**,
> roughly **30% overstated**. They'd also have shifted Q4 money in the wrong
> direction. For Wise, every pound that doesn't create a new customer is a
> pound that could keep prices low for the customers we have.
>
> Happy to go deeper on any part."

**Stop talking there.** Don't fill the silence.

---

## 3. Your own STAR card for this case (if asked "walk me through your approach")

| | |
|---|---|
| **Situation** | £888k display spend, two vendors, three measurement systems disagreeing by up to 30%. |
| **Task** | Turn three conflicting numbers into a decision framework the paid display team can use. 4 slides, must stand alone. |
| **Action** | Cleaned and checked the data (duplicates, outage). Compared the systems as ratios over time rather than as levels. Separated stable bias from regime change (Q4). Tested saturation before claiming it, and found none. Designed roles, the calibration factor and the rules. Every number on the slides re-checked by script. |
| **Result** | A framework that stops a ~30% overstatement, stops a wrong-way Q4 reallocation, and catches tracking outages on day one. |
| **Learning / what I'd do differently** | I'd want the MMM's confidence intervals. I treated it as the best estimate, but without the intervals I can't say how wide k's uncertainty is. With more time I'd have mocked up the geo-test design: sample size and duration. |

## 4. Technical backup (for Q&A only, not for the talk)

### Data and method
- **Data cleaning:** 107 rows parsed → 3 exact duplicates removed → 104 =
  52 weeks × 2 vendors. The 4 May weeks with first-touch = 0 for Tradedesk
  (£34.6k spend, apparent CPA £206 vs ~£19 true) were flagged, not dropped.
- **Totals:** spend £887,990 · vendor UI 65,670 · first-touch 36,119 · MMM
  50,534 registrations. Blended MMM CPA **£17.57**. Vendor-UI CPA would read
  £13.52.
- **Ratios:** UI/MMM = 1.17 Tradedesk, 1.28 Google. First-touch/MMM = 0.47
  Tradedesk, 1.06 Google.
- **k definition:** k = MMM registrations ÷ platform-reported registrations,
  per vendor, per period. Google 0.78 → 0.55, Tradedesk 0.86 → 0.73.
  Target translation: platform CPA target = true CPA target × k
  (£18 × 0.78 = £14.04, shown as £14.09 because the fitted k is unrounded).
- **Q4 regression (Google UAC):**
  `log(CPA_mmm) = 2.31 + 0.047·log(spend) + 0.180·Q4`. The spend coefficient
  is ~0, so cost isn't rising with spend. The Q4 dummy is +0.18 log points,
  about **+20% at identical spend**. That is seasonality and lower
  incrementality, **not saturation**.
- **Saturation check:** regression of log(MMM registrations) on log(spend),
  per vendor, Jan–Oct: elasticity 0.96 (Google) and 0.99 (Tradedesk), R²
  0.995–0.999. Elasticity ≈ 1 means double the spend, double the
  registrations: constant cost per registration, so no sign of diminishing
  returns *within the observed range* (monthly spend £35k → £122k). Q4 alone
  for Google is too noisy to read (8 weeks, R² 0.29). The near-perfect fit is
  very likely circular: the MMM attributes display results largely in
  proportion to display spend, so regressing its output on spend partly
  recovers its own formula. Both series also share the same upward trend.
- **Q4 reallocation sizing:** Q4 Google spend £93,520 at MMM CPA £18.75;
  Tradedesk £17.48. Moving 20% (£18,704) buys 18,704 / 17.48 = 1,070 instead
  of 18,704 / 18.75 = 998, so +72 registrations (+0.6% of Q4). Assumes average
  CPA = marginal CPA, which the elasticity ≈ 1 supports for Tradedesk.
- **Q4 rule-based step:** in the week of 7 Nov, rule-based registrations
  *fall* for both vendors (Google −5%, Tradedesk −4%) while spend keeps its
  trend and vendor UI *jumps* ~+19%. Rule-based CPA steps up ~8% overnight on
  both vendors at once. A simultaneous one-week step on two independent
  vendors points to a change in what is being measured, not in performance.

### Likely challenges and how to answer them

**"Why do you trust the MMM as the truth?"**
> "I don't treat it as truth. I treat it as the only one of the three that even
> tries to measure incrementality. That's why experiments sit above it in the
> framework: the experiments calibrate the MMM, and the MMM calibrates the
> platforms. Slide 4 lists 'MMM as single point of failure' as a top risk for
> that reason."

**"Your R² of 0.99 looks too good."**
> "Agreed, and I say so on slide 4. Spend and MMM output share a trend, and
> the MMM may partly be built on spend. That's co-movement, not proof. It's
> exactly why I don't claim headroom and propose a test instead."

**"Why not just reallocate from Google to Tradedesk in Q4?"**
> "I modelled it. Moving 20% of Google's Q4 spend gains only about 70
> registrations, around 0.6%. The value isn't in that reallocation. It's in
> not making the opposite move the dashboards suggest, and in setting the
> right targets. I'd rather tell you the gain is small than oversell it."

**"Why first-touch and not multi-touch or data-driven attribution?"**
> "First-touch was what the team had. Any click-based model, multi-touch
> included, shares the same blind spot: it can't see view-through or
> cookieless paths, which is where display works. A better attribution model
> would spread credit differently, but it still wouldn't measure
> incrementality."

**"How would you design the geo-holdout?"**
> "Match regions on historical registrations, turn one vendor off or down in
> the test regions for 4–6 weeks, and read the difference with synthetic
> control or difference-in-differences. Run a power analysis first to size
> regions and duration. Avoid Q4 for the first test, or run it there on
> purpose to measure the seasonal k."

**"How often would you re-fit k, and what if it moves?"**
> "Quarterly on the latest 13 weeks, with a holdout of the last 8 weeks to
> check it. Alert if it moves more than 20%. A big move triggers a test, not
> an automatic budget change."

**"How would you get the paid display team to adopt this?"**
> "I wouldn't take their dashboard away. They keep it for daily optimisation,
> which is what they're good at. What changes is the target they optimise to.
> I'd pilot it with one vendor for one quarter, show the outcome, then scale.
> I'd agree up front which decision each number owns, so the conversation is
> about the decision, not about whose number wins."

**"What about privacy changes (ITP, SKAN, consent)?"**
> "They widen the gap between platform-reported and real, and that pushes
> towards the MMM-plus-experiments stack. That's part of why I'd invest in
> server-side conversions: fix the signal at source."

**"What's the customer impact?"**
> "Better measurement means fewer marketing pounds wasted on registrations
> that would have happened anyway. That keeps acquisition cost down, which is
> part of how Wise keeps fees low. Fixing tracking also improves what the
> bidding algorithms learn, so ads reach people who actually need Wise."

**"So which number do you report to executives, and why?"**
> "For *how many* registrations we got: the database. It's the real count,
> and the MMM total should never replace it. For *how much of that marketing
> drove*: the MMM, scaled so baseline plus channel contributions add up to the
> database total, with the range shown. For efficiency: incremental cost per
> registration from the MMM. Platform numbers never go in an exec report.
> One total, one decomposition, one efficiency metric."

**"But a multiplicative MMM can inflate totals, can't it?"**
> "The fitted total is anchored to actuals, so it shouldn't drift much. The
> risk is in the decomposition. In a multiplicative model the channel effects
> interact, so if you compute each channel as 'switch it off and see what's
> lost' and then add those up, the sum can exceed the real incremental total.
> I'd handle it three ways: reconcile contributions to the database total
> (proportional or Shapley allocation), check that baseline plus contributions
> equals actuals each period, and validate the display contribution with a geo
> test. If the MMM's contributions add up to more than what the database
> recorded, that's a model problem to fix, not a number to report."

**"Why does rule-based CPA jump in November?"**
> "Rule-based registrations drop the same week on both vendors while spend
> follows its trend. Two independent vendors changing on the same day is a
> measurement change, not performance. Most likely another channel started
> touching users first in Q4, Black Friday or seasonal campaigns, so
> first-touch handed the credit elsewhere. A tracking or consent change is the
> other candidate. I'd check the channel mix of first touches over time, the
> marketing calendar, tag and consent-rate logs, and whether total database
> registrations rose while display's first-touch share fell."

**"Why is k built against vendor UI and not rule-based?"**
> "Because k exists to translate a business target into the number the team
> actually steers with. Bids, target CPAs and daily optimisation all live in
> the platform, and the algorithm only sees the platform's conversions.
> Rule-based lives in our warehouse, isn't fed to the bidders, sees only 47% of
> Tradedesk and broke silently in May. You could compute a rule-based k for
> internal reporting, but nobody would make a decision with it."

**"What would you do differently?"**
> "Ask for the MMM's confidence intervals and model spec on day one. Every
> conclusion inherits its uncertainty. I'd also have sized the first geo test."

## 5. The rest of the interview (from the brief)

### Scenario questions: use the same structure out loud
1. **Clarify the decision.** What will we do differently depending on the
   answer?
2. **Define success** and the metric, and the guardrail metric.
3. **Hypotheses**, ranked by likelihood and impact.
4. **Data and method**: the simplest thing that answers it.
5. **Recommendation, risks, and how we'd know we were wrong.**

Think aloud. They score the process, not only the answer.

### STAR stories to prepare (fill in from your own experience)
Have each one at 90 seconds, with **one number** in the Result.
- [ ] A successful project you delivered.
- [ ] A project that didn't achieve the desired outcome, and what you learned.
- [ ] Competing priorities: how you chose, and what data you used.
- [ ] An ambiguous problem where requirements changed.
- [ ] Influencing a marketing stakeholder who disagreed with the data.

### Marketing analytics refresh (one line each)
- **Channels:** paid search (capture intent), paid social and display
  (create demand), affiliates, CRM (retention), referral (word of mouth).
  Referral is especially relevant at Wise.
- **Acquisition:** CAC, CPA, conversion rate, incremental CPA. **Engagement:**
  activation (first transfer), frequency. **Retention:** cohort retention,
  LTV, LTV:CAC, payback period.
- **Measurement stack:** attribution (fast, biased), MMM (strategic, slow),
  experiments (gold standard, costly). Triangulate.

### Questions to ask them (5+)
1. How is marketing measurement owned today: does the MMM sit in this team or
   with an external partner, and how is it validated?
2. How do marketing teams use the numbers this team produces? Where does
   analysis most often get lost before it becomes a decision?
3. What's the team's current experimentation capacity: geo tests, holdouts,
   conversion lift?
4. How does referral and word of mouth show up in measurement, given how much
   of Wise's growth it drives?
5. What would success look like for this role in the first 6 and 12 months?
6. How is the team structured, and how would this manager role split time
   between people leadership and hands-on analysis?
7. What's the hardest measurement disagreement the team is dealing with right
   now?

## 6. Final checklist before the call
- [ ] Rehearse the talk with a timer twice. It must land under 11 minutes.
- [ ] Deck open in presenter mode. Speak the headline of each slide first.
- [ ] Memorise the key numbers: **£888k · 22% / 29% · 47% → 2.4× · 57% ·
      Q4 −23% · k 0.78 → 0.55 · £18 → £14 → £10 · 27% / 42% · 30%**.
- [ ] Have this file open on a second screen for Q&A backups.
- [ ] Water, quiet room, camera at eye level.
