// Rebuilds the 3 "three rulers" slides as an editable .pptx.
// Coordinates are written in pixels of the 2000x1125 reference images (150 px = 1 inch).
const pptxgen = require("pptxgenjs");
const out = process.argv[2] || "deck.pptx";

const P = (v) => v / 150;
const C = {
  ink: "15201A", body: "3F4441", muted: "7A7D7A",
  blue: "2F7BD6", orange: "E8683A", green: "1AA878",
  card: "F2F3EE", cardLine: "DADDD3", grid: "E6E6E6",
  darkGreen: "1D3B1F", lightGreen: "9EDF86",
  purple: "4B3AA8", gold: "E8A200", bandOrange: "F9DED3", bandBlue: "EAF1FA",
};
const SERIF = "Cambria", SANS = "Calibri", CHART = "Arial";

const pres = new pptxgen();
pres.layout = "LAYOUT_WIDE"; // 13.333 x 7.5
pres.title = "Three rulers, three answers";

function text(slide, t, x, y, w, h, o = {}) {
  slide.addText(t, {
    x: P(x), y: P(y), w: P(w), h: P(h), margin: 0, isTextBox: true,
    fontFace: SANS, fontSize: 11, color: C.body, valign: "top", ...o,
  });
}
function eyebrow(slide, t) {
  text(slide, t, 75, 44, 1800, 28, { fontSize: 10, bold: true, color: C.muted, charSpacing: 2 });
}
function header(slide, eyebrowText, title, subtitleRuns) {
  eyebrow(slide, eyebrowText);
  text(slide, title, 75, 88, 1850, 80, { fontFace: SERIF, fontSize: 28, bold: true, color: C.ink, valign: "middle" });
  text(slide, subtitleRuns, 75, 182, 1850, 36, { fontSize: 13, color: C.body });
}
function pageNum(slide, n) {
  text(slide, `${n} / 4`, 1840, 1086, 85, 24, { fontSize: 9, color: C.muted, align: "right" });
}
function card(slide, x, y, w, h) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, {
    x: P(x), y: P(y), w: P(w), h: P(h), rectRadius: 0.1,
    fill: { color: C.card }, line: { color: C.cardLine, width: 1 },
  });
}
function circle(slide, x, y, d, fill, label, color, fontSize = 12) {
  slide.addShape(pres.shapes.OVAL, { x: P(x), y: P(y), w: P(d), h: P(d), fill: { color: fill }, line: { color: fill } });
  if (label) text(slide, label, x, y, d, d, { fontSize, bold: true, color, align: "center", valign: "middle" });
}
const B = (t, o = {}) => ({ text: t, options: { bold: true, ...o } });
const N = (t, o = {}) => ({ text: t, options: o });

// ---------------------------------------------------------------- slide 1
{
  const s = pres.addSlide();
  s.background = { color: "FFFFFF" };
  eyebrow(s, "PAID DISPLAY  ·  52 WEEKS  ·  £888K  ·  TRADEDESK + GOOGLE UAC");
  text(s, "Three rulers, three answers — and only one should set the budget", 75, 88, 1850, 80,
    { fontFace: SERIF, fontSize: 28, bold: true, color: C.ink, valign: "middle" });
  text(s, [N("The three systems disagree by "), B("−29% to +30%"),
    N(" on the same registrations. That gap is structural and stable — and therefore "), B("usable"),
    N(" — until Q4, when it breaks.")], 75, 182, 1850, 36, { fontSize: 13 });

  text(s, "Weekly registrations: a stable gap for 10 months — then it breaks", 205, 234, 1400, 36,
    { fontFace: CHART, fontSize: 14, bold: true, color: "111111" });

  // Weekly series, reconstructed from the reference chart (approximate).
  // Knots are [week index, value]; week 0 = w/c 3 Jan 2022, week 43 = w/c 31 Oct.
  const interp = (knots, w) => {
    for (let i = 0; i < knots.length - 1; i++) {
      const [a, va] = knots[i], [b, vb] = knots[i + 1];
      if (w >= a && w <= b) return va + (vb - va) * (w - a) / (b - a);
    }
    return knots[knots.length - 1][1];
  };
  const vendorKnots = [[0, 440], [3, 550], [4, 520], [5, 640], [6, 720], [8, 805], [10, 700], [11, 810], [12, 845],
    [14, 735], [15, 820], [17, 920], [20, 1050], [22, 940], [23, 1050], [25, 1135], [26, 1020], [28, 1180],
    [29, 1230], [30, 1320], [31, 1375], [32, 1355], [34, 1450], [36, 1320], [37, 1450], [42, 1715],
    [43, 2115], [51, 2812]];
  const mmmQ4 = [[42, 1420], [43, 1540], [44, 1445], [51, 1777]];
  const ruleQ4 = [[42, 1045], [43, 995], [51, 1316]];
  const outage = { 17: 355, 18: 375, 19: 395, 20: 415 };
  const WEEKS = 52, SLOTS = 60; // extra empty slots leave room for the end labels
  const vendor = [], mmm = [], rule = [], labels = [];
  const monthLabels = { 0: "2022-01", 8: "2022-03", 17: "2022-05", 26: "2022-07", 35: "2022-09", 43: "2022-11", 52: "2023-01" };
  for (let w = 0; w < SLOTS; w++) {
    labels.push(monthLabels[w] || "");
    if (w >= WEEKS) { vendor.push(null); mmm.push(null); rule.push(null); continue; }
    const v = interp(vendorKnots, w);
    const m = w >= 42 ? interp(mmmQ4, w) : v / 1.22;
    const r = w >= 42 ? interp(ruleQ4, w) : (outage[w] ?? m * 0.725);
    vendor.push(Math.round(v)); mmm.push(Math.round(m)); rule.push(Math.round(r));
  }

  // Plot area in reference px, used to place the shaded bands and annotations
  const plot = { x: 205, y: 285, w: 1665, h: 420 };
  const slotW = plot.w / SLOTS;
  const wx = (w) => plot.x + (w + 0.5) * slotW;
  const band = (w0, w1, color) => s.addShape(pres.shapes.RECTANGLE, {
    x: P(wx(w0) - slotW / 2), y: P(plot.y), w: P((w1 - w0 + 1) * slotW), h: P(plot.h),
    fill: { color }, line: { type: "none" },
  });
  band(17, 20, C.bandOrange);
  band(43, 51, C.bandBlue);

  const frame = { x: 90, y: 280, w: 1830, h: 470 };
  s.addChart(pres.charts.LINE, [
    { name: "Vendor UI", labels, values: vendor },
    { name: "MMM (incremental)", labels, values: mmm },
    { name: "Rule-based (first touch)", labels, values: rule },
  ], {
    x: P(frame.x), y: P(frame.y), w: P(frame.w), h: P(frame.h),
    layout: { x: (plot.x - frame.x) / frame.w, y: (plot.y - frame.y) / frame.h, w: plot.w / frame.w, h: plot.h / frame.h },
    chartColors: [C.blue, C.green, C.orange], lineSize: 2.25, lineDataSymbol: "none",
    showLegend: false, valAxisMinVal: 0, valAxisMaxVal: 3300, valAxisMajorUnit: 500,
    valAxisLabelFormatCode: "#,##0", valAxisLabelFontSize: 10, catAxisLabelFontSize: 10,
    valAxisLabelFontFace: CHART, catAxisLabelFontFace: CHART,
    valAxisLabelColor: "444444", catAxisLabelColor: "444444",
    valGridLine: { color: C.grid, size: 0.75 }, catGridLine: { style: "none" },
    catAxisLineShow: true, catAxisLineColor: "BBBBBB", valAxisLineShow: false,
    catAxisLabelFrequency: 1, catAxisMajorTickMark: "none",
    showValAxisTitle: true, valAxisTitle: "Registrations per week", valAxisTitleFontSize: 10, valAxisTitleColor: "444444",
    plotArea: { fill: { color: "FFFFFF", transparency: 100 } },
  });

  // Q4 divider
  s.addShape(pres.shapes.LINE, { x: P(wx(43) - slotW / 2), y: P(plot.y), w: 0, h: P(plot.h), line: { color: "777777", width: 1.25, dashType: "dash" } });

  // End-of-line labels
  const endLab = (t1, t2, y, color) => text(s, [B(t1, { breakLine: true }), B(t2)], 1650, y, 270, 60,
    { fontFace: CHART, fontSize: 10.5, color });
  endLab("Vendor UI", "2,812/wk", 328, C.blue);
  endLab("MMM (incremental)", "1,777/wk", 456, C.green);
  endLab("Rule-based (first touch)", "1,316/wk", 514, C.orange);

  // Annotations
  text(s, [B("1. TRACKING OUTAGE  (May, 4 wks)", { breakLine: true }),
    B("Tradedesk rule-based drops to zero.", { breakLine: true }),
    B("£34.6k spend left unattributed,", { breakLine: true }),
    B("reported CPA £206 vs £19 true.")], 245, 414, 420, 110,
    { fontFace: CHART, fontSize: 9.5, color: C.orange, lineSpacingMultiple: 1.2 });
  s.addShape(pres.shapes.LINE, { x: P(605), y: P(528), w: P(130), h: P(120), line: { color: C.orange, width: 1.25, endArrowType: "triangle" } });

  text(s, [B("2. Q4 REGIME BREAK  (Nov-Dec)", { breakLine: true }),
    B("Vendor UI accelerates away from MMM:", { breakLine: true }),
    B("gap widens from +22% to +54%.")], 645, 297, 460, 80,
    { fontFace: CHART, fontSize: 9.5, color: C.blue, lineSpacingMultiple: 1.2 });
  s.addShape(pres.shapes.LINE, { x: P(1012), y: P(382), w: P(490), h: P(18), line: { color: C.blue, width: 1.25, endArrowType: "triangle" } });

  text(s, "52 weeks · paid display · Tradedesk + Google UAC · £888k spend · 3 duplicate rows removed before analysis.",
    90, 766, 1400, 24, { fontFace: CHART, fontSize: 8.5, color: "888888" });

  // Takeaway cards
  const cards = [
    { x: 75, color: C.green, n: "1", head: "The gap is stable — so it is a conversion factor, not a crisis",
      body: [N("For 10 months the ratios barely move: Vendor UI runs "), B("+22%"), N(" above MMM, rule-based "), B("−29%"),
        N(" below. Per vendor they are near-constant (UI/MMM: Tradedesk 1.17, Google 1.28). A predictable bias can be calibrated away.")] },
    { x: 705, color: C.orange, n: "2", head: "Rule-based structurally breaks programmatic display",
      body: [N("First-touch click tracking sees only "), B("47%"), N(" of Tradedesk’s incremental registrations → reported CPA "),
        B("£44.31 vs £18.77"), N(". Governing on it would cut the vendor delivering 57% of incremental growth — and it failed silently for 4 weeks in May.")] },
    { x: 1333, color: C.blue, n: "3", head: "In Q4 the signals invert — and the vendor UI points the wrong way",
      body: [N("Vendor UI CPA "), B("improves"), N(" (Google −13%, Tradedesk −22%) while MMM says Google got "), B("23% worse"),
        N(". At identical spend Q4 costs +20% per incremental registration: seasonality, not saturation.")] },
  ];
  for (const c of cards) {
    card(s, c.x, 815, 590, 243);
    circle(s, c.x + 27, 843, 44, c.color, c.n, "FFFFFF", 12);
    text(s, c.head, c.x + 83, 836, 480, 60, { fontSize: 12.5, bold: true, color: C.ink, lineSpacingMultiple: 1.05 });
    text(s, c.body, c.x + 26, 905, 540, 130, { fontSize: 10.5, color: C.body, lineSpacingMultiple: 1.1 });
  }
  pageNum(s, 1);
  s.addNotes("Line-chart weekly values are reconstructed from the original chart image (approximate). Endpoints (2,812 / 1,777 / 1,316) are exact. Right-click the chart > Edit Data to paste the real series.");
}

// ---------------------------------------------------------------- slide 2
{
  const s = pres.addSlide();
  s.background = { color: "FFFFFF" };
  header(s, "B  ·  WHAT EXPLAINS THE DIFFERENCES, AND WHAT EACH METHOD IS GOOD FOR",
    "They disagree because they answer three different questions",
    "None is “the right one”: each has a different counting rule, field of view and definition of a conversion. The error is comparing them like-for-like.");

  text(s, "Same vendor, same year — the ruler changes the verdict by up to 2.9×", 145, 240, 780, 30,
    { fontFace: CHART, fontSize: 11, bold: true, color: "111111" });
  s.addChart(pres.charts.BAR, [
    { name: "Vendor UI", labels: ["Google UAC\n£350k spend · 39%", "Tradedesk\n£538k spend · 61%"], values: [11.43, 15.35] },
    { name: "Rule-based", labels: ["Google UAC\n£350k spend · 39%", "Tradedesk\n£538k spend · 61%"], values: [14.59, 44.31] },
    { name: "MMM", labels: ["Google UAC\n£350k spend · 39%", "Tradedesk\n£538k spend · 61%"], values: [16.01, 18.77] },
  ], {
    x: P(75), y: P(265), w: P(820), h: P(400), barDir: "col", barGapWidthPct: 60,
    chartColors: [C.blue, C.orange, C.green],
    showLegend: true, legendPos: "t", legendFontSize: 9, legendFontFace: CHART,
    showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "£0.00", dataLabelFontSize: 9, dataLabelFontBold: true, dataLabelFontFace: CHART,
    valAxisMinVal: 0, valAxisMaxVal: 50, valAxisMajorUnit: 10, valAxisLabelFormatCode: "£0",
    valAxisLabelFontSize: 9, catAxisLabelFontSize: 10, catAxisLabelFontFace: CHART, valAxisLabelFontFace: CHART,
    catAxisLabelColor: "111111", valAxisLabelColor: "555555",
    valGridLine: { color: C.grid, size: 0.75 }, catGridLine: { style: "none" }, valAxisLineShow: false, catAxisLineColor: "BBBBBB",
    showValAxisTitle: true, valAxisTitle: "Cost per registration, full year", valAxisTitleFontSize: 9, valAxisTitleColor: "555555",
  });
  text(s, [B("2.4× the MMM read.", { breakLine: true }), B("First-touch click tracking", { breakLine: true }),
    B("is blind to view-through", { breakLine: true }), B("programmatic display.")], 432, 380, 230, 80,
    { fontFace: CHART, fontSize: 8.5, color: C.orange, lineSpacingMultiple: 1.15 });
  s.addShape(pres.shapes.LINE, { x: P(575), y: P(372), w: P(30), h: P(12), flipV: true, line: { color: C.orange, width: 1, endArrowType: "triangle" } });

  text(s, "THREE DRIVERS OF THE GAP", 937, 246, 900, 26, { fontSize: 10.5, bold: true, color: C.muted, charSpacing: 2.5 });
  const drivers = [
    { y: 288, h: "Counting rules", b: "Each vendor marks its own homework: self-attributed view-through and cross-device conversions, different lookback windows, and no de-duplication between vendors. Summing two vendor UIs double-counts people both of them touched." },
    { y: 428, h: "Observability", b: "Rule-based tracking only sees clicks it can cookie. That is survivable for in-app UAC (rule-based ≈ 1.06× MMM) and fatal for programmatic display (0.47×). First touch then over-credits whichever tracked channel appeared first." },
    { y: 568, h: "Incrementality", b: "MMM is the only one that subtracts baseline demand. Neither tracking system asks “would this have happened anyway?”, so both inflate exactly when organic demand rises — which is what Q4 shows." },
  ];
  drivers.forEach((d, i) => {
    circle(s, 937, d.y + 2, 38, C.darkGreen, String(i + 1), "B8E6A5", 11);
    text(s, d.h, 993, d.y, 900, 34, { fontSize: 14, bold: true, color: C.ink, valign: "middle" });
    text(s, d.b, 993, d.y + 45, 930, 80, { fontSize: 10.5, color: C.body, lineSpacingMultiple: 1.1 });
  });

  const methods = [
    { x: 75, color: C.blue, name: "VENDOR UI", q: "“Did my platform see a conversion after my ad?”",
      adv: "Daily and granular (campaign, creative, audience, placement). The only signal the bidding algorithm can actually consume. No lag.",
      lim: "Self-reported and self-interested; inconsistent windows; no cross-vendor de-duplication; inflates when demand rises — Q4 premium jumped from +22% to +54%." },
    { x: 702, color: C.orange, name: "RULE-BASED (1ST TOUCH)", q: "“Which tracked touchpoint came first?”",
      adv: "One consistent rule across vendors, de-duplicated, user-level, independent of vendor incentives. Excellent as a tracking-health monitor.",
      lim: "Blind to view-through and to consent/ITP-limited paths; first touch over-credits upper funnel; breaks silently when tags break (May: £34.6k of spend read as zero)." },
    { x: 1329, color: C.green, name: "MMM", q: "“What would not have happened without the spend?”",
      adv: "Covers every channel including TV, OOH and podcast; privacy-durable; captures view-through and halo; separates baseline from incremental.",
      lim: "Weekly and aggregate, 2–4 week latency, wide confidence intervals, cannot optimise creative or audience, and needs real spend variation to identify effects." },
  ];
  for (const m of methods) {
    card(s, m.x, 705, 595, 355);
    circle(s, m.x + 30, 741, 20, m.color);
    text(s, m.name, m.x + 60, 736, 500, 30, { fontSize: 12, bold: true, color: C.ink, charSpacing: 1.5, valign: "middle" });
    text(s, m.q, m.x + 27, 796, 545, 34, { fontSize: 11.5, italic: true, color: m.color });
    text(s, [B("Advantages  ", { color: C.ink }), N(m.adv)], m.x + 27, 855, 545, 80, { fontSize: 10.5, lineSpacingMultiple: 1.1 });
    text(s, [B("Limitations  ", { color: C.ink }), N(m.lim)], m.x + 27, 960, 545, 80, { fontSize: 10.5, lineSpacingMultiple: 1.1 });
  }
  pageNum(s, 2);
}

// ---------------------------------------------------------------- slide 3
{
  const s = pres.addSlide();
  s.background = { color: "FFFFFF" };
  header(s, "C  ·  HOW THE PAID DISPLAY TEAM SHOULD USE THESE APPROACHES",
    "MMM sets the budget, calibrated platform data steers it",
    "Stop asking which number is right. Give each system the decision it can actually answer, then convert between them with an explicit, refreshed calibration factor.");

  const roles = [
    { x: 75, color: C.green, k: "HOW MUCH?", t: "MMM",
      b: "Sets the display budget envelope and the cross-channel split, and owns the one true CPA target. Refreshed quarterly. Never used for in-flight decisions." },
    { x: 703, color: C.blue, k: "WHERE & WHAT?", t: "Calibrated vendor UI",
      b: "Daily optimisation inside a vendor: creative, audience, bidding, pacing. Used at face value only within a vendor — never to compare vendors." },
    { x: 1331, color: C.orange, k: "IS IT TRUE?", t: "Experiments",
      b: "Geo-holdout or PSA tests per vendor, twice a year. Calibrates MMM, validates the k factors and settles disagreements between the other two." },
  ];
  for (const r of roles) {
    card(s, r.x, 237, 592, 212);
    text(s, r.k, r.x + 30, 264, 520, 26, { fontSize: 10.5, bold: true, color: r.color, charSpacing: 2 });
    text(s, r.t, r.x + 30, 294, 540, 44, { fontFace: SERIF, fontSize: 18, bold: true, color: C.ink, valign: "middle" });
    text(s, r.b, r.x + 30, 350, 535, 85, { fontSize: 10.5, color: C.body, lineSpacingMultiple: 1.1 });
  }

  text(s, "THE CALIBRATION MECHANIC  ·  MULTIPLY THE PLATFORM NUMBER BY k", 75, 498, 940, 26,
    { fontSize: 10, bold: true, color: C.muted, charSpacing: 1.5 });
  const hd = (t, align = "left") => ({ text: t, options: { bold: true, align } });
  const cellOpts = { fontFace: SANS, fontSize: 11.5, color: C.ink, valign: "middle", border: { type: "solid", pt: 1, color: "D5D5D5" }, margin: [0, 0.08, 0, 0.08] };
  s.addTable([
    [hd("Vendor"), hd("k  Jan–Oct", "center"), hd("k  Nov–Dec", "center"), hd("In-platform CPA target\nto hit an £18 true CPA", "center")],
    ["Google UAC", { text: "0.78", options: { align: "center" } }, { text: "0.55", options: { align: "center", color: C.orange } }, { text: "£14.09  →  £9.90", options: { align: "center" } }],
    ["Tradedesk", { text: "0.86", options: { align: "center" } }, { text: "0.73", options: { align: "center", color: C.orange } }, { text: "£15.39  →  £13.19", options: { align: "center" } }],
  ], { x: P(75), y: P(533), w: P(907), colW: [P(232), P(158), P(158), P(359)], rowH: [P(69), P(48), P(48)], ...cellOpts });

  text(s, [B("Same nominal in-platform target buys 25–35% less real value in Q4. ", { color: C.ink }),
    N("Applying the Jan–Oct factor to December would overstate incremental registrations by 27% overall — and by 42% on Google UAC.")],
    75, 724, 907, 56, { fontSize: 10.5, lineSpacingMultiple: 1.1 });

  text(s, "Q4: the two rulers rank the same two vendors in opposite order", 135, 812, 800, 24,
    { fontFace: CHART, fontSize: 10.5, bold: true, color: "111111" });
  s.addChart(pres.charts.BAR, [
    { name: "Google UAC", labels: ["Vendor UI says:  “Google is the better buy”", "MMM says:  “Tradedesk is the better buy”"], values: [10.31, 18.75] },
    { name: "Tradedesk", labels: ["Vendor UI says:  “Google is the better buy”", "MMM says:  “Tradedesk is the better buy”"], values: [12.81, 17.48] },
  ], {
    x: P(75), y: P(832), w: P(905), h: P(240), barDir: "col", barGapWidthPct: 110,
    chartColors: [C.purple, C.gold],
    showLegend: true, legendPos: "t", legendFontSize: 8, legendFontFace: CHART,
    showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "£0.00", dataLabelFontSize: 8.5, dataLabelFontBold: true, dataLabelFontFace: CHART,
    valAxisMinVal: 0, valAxisMaxVal: 20, valAxisMajorUnit: 5, valAxisLabelFormatCode: "£0",
    valAxisLabelFontSize: 8, catAxisLabelFontSize: 9, catAxisLabelFontFace: CHART, valAxisLabelFontFace: CHART,
    catAxisLabelColor: "111111", valAxisLabelColor: "555555",
    valGridLine: { color: C.grid, size: 0.75 }, catGridLine: { style: "none" }, valAxisLineShow: false, catAxisLineColor: "BBBBBB",
    showValAxisTitle: true, valAxisTitle: "Q4 CPA", valAxisTitleFontSize: 8, valAxisTitleColor: "555555",
  });

  text(s, "RULES OF ENGAGEMENT", 1034, 498, 880, 26, { fontSize: 10.5, bold: true, color: C.muted, charSpacing: 2.5 });
  const rules = [
    { y: 538, h: "Set one target, translate it.",
      b: "The business affords one true CPA. Convert it into a per-vendor, per-season in-platform target using k. Re-fit k every quarter on the latest 13 weeks." },
    { y: 677, h: "Never compare vendors on raw platform CPA.",
      b: "In Q4 Google looks 20% cheaper than Tradedesk (£10.31 vs £12.81) and is actually 7% more expensive (£18.75 vs £17.48)." },
    { y: 816, h: "Demote rule-based to diagnostics.",
      b: "Keep it as a tag-health monitor, not a budget input. Alert when a vendor’s rule-based/MMM ratio shifts more than 20% week-on-week — that would have caught May’s outage on day one." },
    { y: 955, h: "When the rulers disagree, test — don’t argue.",
      b: "If MMM and calibrated UI point in opposite directions for two consecutive weeks, that vendor goes into a geo holdout." },
  ];
  rules.forEach((r, i) => {
    circle(s, 1034, r.y + 1, 38, C.lightGreen, String(i + 1), C.darkGreen, 11);
    text(s, r.h, 1091, r.y, 830, 36, { fontSize: 13, bold: true, color: C.ink, valign: "middle" });
    text(s, r.b, 1091, r.y + 50, 830, 70, { fontSize: 10.5, color: C.body, lineSpacingMultiple: 1.1 });
  });
  pageNum(s, 3);
}

pres.writeFile({ fileName: out }).then((f) => console.log("wrote", f));
