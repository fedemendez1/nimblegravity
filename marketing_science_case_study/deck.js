const pptxgen = require('pptxgenjs');
const p = new pptxgen();
p.layout = 'LAYOUT_WIDE';            // 13.333 x 7.5
p.author = 'Marketing Science Manager - Case Study';
p.title  = 'Paid display measurement framework';

const FOREST='163300', LIME='9FE870', INK='16200E', BODY='474A43', MUTE='8A8D84',
      CARD='F3F6EE', LINE='DCE2D2', WHITE='FFFFFF', DEEPCARD='234A0D',
      BLUE='2A78D6', ORANGE='EB6834', AQUA='1BAF7A';
const H='Cambria', B='Calibri';
const W=13.333, M=0.5, CW=W-2*M;    // content width 12.333

// ---------- helpers ----------
function eyebrow(s, txt, color){
  s.addText(txt,{x:M,y:0.26,w:CW,h:0.22,isTextBox:true,margin:0,fontFace:B,fontSize:9.5,
    bold:true,charSpacing:1.6,color:color||MUTE});
}
function title(s, txt, y, size, color){
  s.addText(txt,{x:M,y:y,w:CW,h:0.62,isTextBox:true,margin:0,fontFace:H,fontSize:size||27,
    bold:true,color:color||INK,lineSpacing:30});
}
function kicker(s, runs, y, h){
  s.addText(runs,{x:M,y:y,w:CW,h:h||0.34,isTextBox:true,margin:0,fontFace:B,fontSize:11.5,
    color:BODY,lineSpacing:16});
}
function card(s,x,y,w,h,fill,lineCol){
  s.addShape(p.ShapeType.roundRect,{x,y,w,h,fill:{color:fill||CARD},rectRadius:0.08,
    line:{color:lineCol||LINE,width:0.75}});
}
function badge(s,x,y,d,txt,fill,col){
  s.addShape(p.ShapeType.ellipse,{x,y,w:d,h:d,fill:{color:fill}});
  s.addText(txt,{x,y,w:d,h:d,isTextBox:true,margin:0,align:'center',valign:'middle',
    fontFace:B,fontSize:11,bold:true,color:col});
}
function pageNo(s,n,dark){
  s.addText(`${n} / 4`,{x:W-1.15,y:7.20,w:0.65,h:0.24,isTextBox:true,margin:0,align:'right',
    fontFace:B,fontSize:9,color:dark?'6E8A5A':MUTE});
}

/* =======================================================================
   SLIDE 1 - Executive summary + top 3 insights   (question a)
   ======================================================================= */
{
const s = p.addSlide(); s.background={color:WHITE};
eyebrow(s,'PAID DISPLAY  ·  52 WEEKS  ·  £888K  ·  TRADEDESK + GOOGLE UAC');
title(s,'Three rulers, three answers — and only one should set the budget',0.52,26);
kicker(s,[
 {text:'The three systems disagree by ',options:{}},
 {text:'−29% to +30%',options:{bold:true,color:INK}},
 {text:' on the same registrations. That gap is structural and stable — and therefore ',options:{}},
 {text:'usable',options:{bold:true,color:INK}},
 {text:' — until Q4, when it breaks.',options:{}}
],1.14,0.32);

s.addImage({path:'fig1_divergence.png',x:M,y:1.52,w:CW,h:CW/3.227});   // h = 3.82

const cy=5.44, ch=1.62, gap=0.26, cw=(CW-2*gap)/3;
const cards=[
 {n:'1',col:AQUA,t:'The gap is stable — so it is a conversion factor, not a crisis',
  b:[{text:'For 10 months the ratios barely move: Vendor UI runs ',o:{}},
     {text:'+22%',o:{bold:true}},{text:' above MMM, rule-based ',o:{}},{text:'−29%',o:{bold:true}},
     {text:' below. Per vendor they are near-constant (UI/MMM: Tradedesk 1.17, Google 1.28). A predictable bias can be calibrated away.',o:{}}]},
 {n:'2',col:ORANGE,t:'Rule-based structurally breaks programmatic display',
  b:[{text:'First-touch click tracking sees only ',o:{}},{text:'47%',o:{bold:true}},
     {text:' of Tradedesk’s incremental registrations → reported CPA ',o:{}},
     {text:'£44.31 vs £18.77',o:{bold:true}},
     {text:'. Governing on it would cut the vendor delivering 57% of incremental growth — and it failed silently for 4 weeks in May.',o:{}}]},
 {n:'3',col:BLUE,t:'In Q4 the signals invert — and the vendor UI points the wrong way',
  b:[{text:'Vendor UI CPA ',o:{}},{text:'improves',o:{bold:true}},
     {text:' (Google −13%, Tradedesk −22%) while MMM says Google got ',o:{}},
     {text:'23% worse',o:{bold:true}},
     {text:'. At identical spend Q4 costs +20% per incremental registration: seasonality, not saturation.',o:{}}]}
];
cards.forEach((c,i)=>{
  const x=M+i*(cw+gap);
  card(s,x,cy,cw,ch);
  badge(s,x+0.18,cy+0.17,0.30,c.n,c.col,WHITE);
  s.addText(c.t,{x:x+0.56,y:cy+0.14,w:cw-0.74,h:0.40,isTextBox:true,margin:0,fontFace:B,
    fontSize:11,bold:true,color:INK,lineSpacing:13.5,valign:'top'});
  s.addText(c.b.map(r=>({text:r.text,options:{...r.o}})),
    {x:x+0.18,y:cy+0.60,w:cw-0.36,h:ch-0.72,isTextBox:true,margin:0,fontFace:B,fontSize:9.3,
     color:BODY,lineSpacing:11.8,valign:'top'});
});
pageNo(s,1);
s.addNotes('Top 3 insights. Data cleaned first: 3 exact duplicate rows removed (2022-09-19 and 2022-10-17 Tradedesk, 2022-10-24 Google UAC); 4 May weeks with rule_based=0 treated as a tag outage, not zero performance.');
}

/* =======================================================================
   SLIDE 2 - Why the approaches differ   (question b)
   ======================================================================= */
{
const s = p.addSlide(); s.background={color:WHITE};
eyebrow(s,'B  ·  WHAT EXPLAINS THE DIFFERENCES, AND WHAT EACH METHOD IS GOOD FOR');
title(s,'They disagree because they answer three different questions',0.52,26);
kicker(s,[{text:'None is “the right one”: each has a different counting rule, field of view and definition of a conversion. The error is comparing them like-for-like.',options:{}}],1.14,0.32);

s.addImage({path:'fig2_cpa.png',x:M,y:1.58,w:5.45,h:5.45/1.890});      // h = 2.88

// gap drivers
const gx=6.25, gw=W-M-gx;   // 6.583
s.addText('THREE DRIVERS OF THE GAP',{x:gx,y:1.58,w:gw,h:0.24,isTextBox:true,margin:0,
  fontFace:B,fontSize:9.5,bold:true,charSpacing:1.2,color:MUTE});
const drv=[
 ['Counting rules','Each vendor marks its own homework: self-attributed view-through and cross-device conversions, different lookback windows, and no de-duplication between vendors. Summing two vendor UIs double-counts people both of them touched.'],
 ['Observability','Rule-based tracking only sees clicks it can cookie. That is survivable for in-app UAC (rule-based ≈ 1.06× MMM) and fatal for programmatic display (0.47×). First touch then over-credits whichever tracked channel appeared first.'],
 ['Incrementality','MMM is the only one that subtracts baseline demand. Neither tracking system asks “would this have happened anyway?”, so both inflate exactly when organic demand rises — which is what Q4 shows.']
];
let dy=1.90;
drv.forEach((d,i)=>{
  badge(s,gx,dy+0.02,0.26,String(i+1),FOREST,LIME);
  s.addText(d[0],{x:gx+0.38,y:dy,w:gw-0.38,h:0.24,isTextBox:true,margin:0,fontFace:B,
    fontSize:11,bold:true,color:INK});
  s.addText(d[1],{x:gx+0.38,y:dy+0.245,w:gw-0.38,h:0.62,isTextBox:true,margin:0,fontFace:B,
    fontSize:9.3,color:BODY,lineSpacing:11.8});
  dy+=0.93;
});

// comparison table
const ty=4.70, th=2.38, gapc=0.22, colw=(CW-2*gapc)/3;
const cols=[
 {h:'VENDOR UI',c:BLUE,q:'“Did my platform see a conversion after my ad?”',
  good:'Daily and granular (campaign, creative, audience, placement). The only signal the bidding algorithm can actually consume. No lag.',
  bad:'Self-reported and self-interested; inconsistent windows; no cross-vendor de-duplication; inflates when demand rises — Q4 premium jumped from +22% to +54%.'},
 {h:'RULE-BASED (1ST TOUCH)',c:ORANGE,q:'“Which tracked touchpoint came first?”',
  good:'One consistent rule across vendors, de-duplicated, user-level, independent of vendor incentives. Excellent as a tracking-health monitor.',
  bad:'Blind to view-through and to consent/ITP-limited paths; first touch over-credits upper funnel; breaks silently when tags break (May: £34.6k of spend read as zero).'},
 {h:'MMM',c:AQUA,q:'“What would not have happened without the spend?”',
  good:'Covers every channel including TV, OOH and podcast; privacy-durable; captures view-through and halo; separates baseline from incremental.',
  bad:'Weekly and aggregate, 2–4 week latency, wide confidence intervals, cannot optimise creative or audience, and needs real spend variation to identify effects.'}
];
cols.forEach((c,i)=>{
  const x=M+i*(colw+gapc);
  card(s,x,ty,colw,th);
  s.addShape(p.ShapeType.ellipse,{x:x+0.20,y:ty+0.245,w:0.13,h:0.13,fill:{color:c.c}});
  s.addText(c.h,{x:x+0.40,y:ty+0.20,w:colw-0.58,h:0.22,isTextBox:true,margin:0,fontFace:B,
    fontSize:10,bold:true,charSpacing:0.8,color:INK});
  s.addText(c.q,{x:x+0.18,y:ty+0.50,w:colw-0.36,h:0.42,isTextBox:true,margin:0,fontFace:B,
    fontSize:9.8,italic:true,color:c.c,lineSpacing:12});
  s.addText([{text:'Advantages  ',options:{bold:true,color:INK}},{text:c.good,options:{color:BODY}}],
    {x:x+0.18,y:ty+0.93,w:colw-0.36,h:0.63,isTextBox:true,margin:0,fontFace:B,fontSize:9.2,lineSpacing:11.5});
  s.addText([{text:'Limitations  ',options:{bold:true,color:INK}},{text:c.bad,options:{color:BODY}}],
    {x:x+0.18,y:ty+1.58,w:colw-0.36,h:0.72,isTextBox:true,margin:0,fontFace:B,fontSize:9.2,lineSpacing:11.5});
});
pageNo(s,2);
s.addNotes('b) Factors explaining the differences plus advantages and limitations of each approach.');
}

/* =======================================================================
   SLIDE 3 - How to use them for decisions   (question c)
   ======================================================================= */
{
const s = p.addSlide(); s.background={color:WHITE};
eyebrow(s,'C  ·  HOW THE PAID DISPLAY TEAM SHOULD USE THESE APPROACHES');
title(s,'MMM sets the budget, calibrated platform data steers it',0.52,26);
kicker(s,[{text:'Stop asking which number is right. Give each system the decision it can actually answer, then convert between them with an explicit, refreshed calibration factor.',options:{}}],1.14,0.32);

// three roles
const ry=1.58, rh=1.42, rg=0.24, rw=(CW-2*rg)/3;
const roles=[
 {k:'HOW MUCH?',t:'MMM',c:AQUA,
  d:'Sets the display budget envelope and the cross-channel split, and owns the one true CPA target. Refreshed quarterly. Never used for in-flight decisions.'},
 {k:'WHERE & WHAT?',t:'Calibrated vendor UI',c:BLUE,
  d:'Daily optimisation inside a vendor: creative, audience, bidding, pacing. Used at face value only within a vendor — never to compare vendors.'},
 {k:'IS IT TRUE?',t:'Experiments',c:ORANGE,
  d:'Geo-holdout or PSA tests per vendor, twice a year. Calibrates MMM, validates the k factors and settles disagreements between the other two.'}
];
roles.forEach((r,i)=>{
  const x=M+i*(rw+rg);
  card(s,x,ry,rw,rh);
  s.addText(r.k,{x:x+0.20,y:ry+0.16,w:rw-0.40,h:0.20,isTextBox:true,margin:0,fontFace:B,
    fontSize:9,bold:true,charSpacing:1.2,color:r.c});
  s.addText(r.t,{x:x+0.20,y:ry+0.37,w:rw-0.40,h:0.28,isTextBox:true,margin:0,fontFace:H,
    fontSize:15,bold:true,color:INK});
  s.addText(r.d,{x:x+0.20,y:ry+0.69,w:rw-0.40,h:0.62,isTextBox:true,margin:0,fontFace:B,
    fontSize:9.3,color:BODY,lineSpacing:11.8});
});

// calibration table
const tx=M, tw=6.05, tyy=3.28;
s.addText('THE CALIBRATION MECHANIC  ·  MULTIPLY THE PLATFORM NUMBER BY k',
  {x:tx,y:tyy,w:tw,h:0.22,isTextBox:true,margin:0,fontFace:B,fontSize:9.5,bold:true,
   charSpacing:1.1,color:MUTE});
s.addTable([
 [{text:'Vendor',options:{bold:true}},{text:'k  Jan–Oct',options:{bold:true,align:'center'}},
  {text:'k  Nov–Dec',options:{bold:true,align:'center'}},
  {text:'In-platform CPA target\nto hit a £18 true CPA',options:{bold:true,align:'center'}}],
 ['Google UAC',{text:'0.78',options:{align:'center'}},{text:'0.55',options:{align:'center',bold:true,color:ORANGE}},
  {text:'£14.09  →  £9.90',options:{align:'center'}}],
 ['Tradedesk',{text:'0.86',options:{align:'center'}},{text:'0.73',options:{align:'center',bold:true,color:ORANGE}},
  {text:'£15.39  →  £13.19',options:{align:'center'}}]
],{x:tx,y:tyy+0.28,w:tw,colW:[1.55,1.05,1.05,2.40],fontFace:B,fontSize:9.8,color:INK,
   border:{type:'solid',color:LINE,pt:0.75},fill:{color:WHITE},valign:'middle',
   rowH:[0.46,0.32,0.32],margin:[4,6,4,6]});
s.addText([{text:'Same nominal in-platform target buys 25–35% less real value in Q4. ',options:{bold:true,color:INK}},
 {text:'Applying the Jan–Oct factor to December would overstate incremental registrations by 27% overall — and by 42% on Google UAC.',options:{color:BODY}}],
 {x:tx,y:tyy+1.48,w:tw,h:0.50,isTextBox:true,margin:0,fontFace:B,fontSize:9.3,lineSpacing:11.8});

s.addImage({path:'fig3_reversal.png',x:tx,y:5.34,w:tw,h:tw/3.424});    // h = 1.77

// rules of engagement
const ox=6.90, ow=W-M-ox;  // 5.933
s.addText('RULES OF ENGAGEMENT',{x:ox,y:tyy,w:ow,h:0.22,isTextBox:true,margin:0,fontFace:B,
  fontSize:9.5,bold:true,charSpacing:1.1,color:MUTE});
const rules=[
 ['Set one target, translate it.','The business affords one true CPA. Convert it into a per-vendor, per-season in-platform target using k. Re-fit k every quarter on the latest 13 weeks.'],
 ['Never compare vendors on raw platform CPA.','In Q4 Google looks 20% cheaper than Tradedesk (£10.31 vs £12.81) and is actually 7% more expensive (£18.75 vs £17.48).'],
 ['Demote rule-based to diagnostics.','Keep it as a tag-health monitor, not a budget input. Alert when a vendor’s rule-based/MMM ratio shifts more than 20% week-on-week — that would have caught May’s outage on day one.'],
 ['When the rulers disagree, test — don’t argue.','If MMM and calibrated UI point in opposite directions for two consecutive weeks, that vendor goes into a geo holdout.']
];
let oy=tyy+0.30;
rules.forEach((r,i)=>{
  badge(s,ox,oy+0.01,0.26,String(i+1),LIME,FOREST);
  s.addText(r[0],{x:ox+0.38,y:oy-0.02,w:ow-0.38,h:0.22,isTextBox:true,margin:0,fontFace:B,
    fontSize:10.3,bold:true,color:INK});
  s.addText(r[1],{x:ox+0.38,y:oy+0.21,w:ow-0.38,h:0.66,isTextBox:true,margin:0,fontFace:B,
    fontSize:9.3,color:BODY,lineSpacing:11.8});
  oy+=0.93;
});
pageNo(s,3);
s.addNotes('c) Decision framework. k = MMM registrations / platform-reported registrations, fitted per vendor per period.');
}

/* =======================================================================
   SLIDE 4 - Risks, opportunities, what next   (questions d + e)
   ======================================================================= */
{
const s = p.addSlide(); s.background={color:FOREST};
eyebrow(s,'D + E  ·  RISKS, OPPORTUNITIES AND WHAT I WOULD BUILD NEXT','9BBE7E');
title(s,'The framework is only as good as the calibration behind it',0.52,26,WHITE);
s.addText([{text:'Two risks are specific to this dataset and would bite within a quarter; the biggest opportunity is that display shows ',options:{color:'CFE3BC'}},
 {text:'no measurable saturation yet',options:{bold:true,color:LIME}},
 {text:'.',options:{color:'CFE3BC'}}],
 {x:M,y:1.14,w:CW,h:0.34,isTextBox:true,margin:0,fontFace:B,fontSize:11.5,lineSpacing:16});

const cy=1.62, ch=5.35, g=0.24, cw=(CW-2*g)/3;
const cols=[
 {h:'RISKS',accent:'F2A38A',items:[
  ['Calibration decay','Q4 proved k is not constant. Using the Jan–Oct factor in December overstates incremental output by 27%. Mitigation: re-fit quarterly, hold out the last 8 weeks, alert on drift.'],
  ['MMM as a single point of failure','Wide confidence intervals, 2–4 week latency, blind to creative. Mitigation: never let it drive in-flight decisions, and always publish the interval next to the point estimate.'],
  ['Co-movement mistaken for causation','Spend and MMM registrations track each other almost perfectly here (elasticity ≈ 1.0, R² > 0.99). That is a shared trend, not proof. Only experiments can validate the level.'],
  ['Silent data failures','3 duplicate rows and 4 zero-rows in a 107-row file. At scale this is invisible. Mitigation: a data contract with automated duplicate, null and ratio-drift checks.']]},
 {h:'OPPORTUNITIES',accent:LIME,items:[
  ['Headroom to scale','Spend tripled (£35k → £122k per month) at elasticity 0.96–0.99 with flat MMM CPA. No saturation is visible yet — test where the ceiling is rather than assume one.'],
  ['One currency for every channel','One incremental registration lets display compete like-for-like with TV, podcast and OOH — channels that today are argued about rather than measured.'],
  ['Better bidding, not just better reporting','Calibrated targets fed back to the platforms change what the algorithms optimise towards, not just what the team reports.'],
  ['Cheaper learning over time','Two vendors with a stable k make future geo tests shorter and smaller — measurement gets cheaper as the framework matures.']]},
 {h:'WHAT I WOULD BUILD NEXT',accent:'A8D0F0',items:[
  ['1 · Geo-holdout experiments','Per vendor, 4–6 weeks. The only unbiased read, and the anchor that calibrates everything else. Start now.'],
  ['2 · A single calibrated KPI','Replace three competing numbers with one incremental registration figure, raw inputs visible underneath.'],
  ['3 · Server-side conversion feed','Improve the signal at source (Conversions API, SKAN-resilient tracking) so the gap narrows instead of being corrected afterwards.'],
  ['4 · MMM at vendor level, with curves','Adstock and saturation curves per vendor so budgets are set on marginal, not average, CPA.'],
  ['5 · Automated measurement health','Duplicate, zero-row and ratio-drift monitoring as part of the pipeline SLA.']]}
];
cols.forEach((c,i)=>{
  const x=M+i*(cw+g);
  s.addShape(p.ShapeType.roundRect,{x,y:cy,w:cw,h:ch,fill:{color:DEEPCARD},rectRadius:0.08,
    line:{color:'2E5C14',width:0.75}});
  s.addText(c.h,{x:x+0.22,y:cy+0.20,w:cw-0.44,h:0.24,isTextBox:true,margin:0,fontFace:B,
    fontSize:10.5,bold:true,charSpacing:1.4,color:c.accent});
  let yy=cy+0.58;
  c.items.forEach(it=>{
    s.addText(it[0],{x:x+0.22,y:yy,w:cw-0.44,h:0.22,isTextBox:true,margin:0,fontFace:B,
      fontSize:10.4,bold:true,color:WHITE});
    const lines=Math.ceil(it[1].length/46);
    const hh=Math.max(0.44,lines*0.170);
    s.addText(it[1],{x:x+0.22,y:yy+0.23,w:cw-0.44,h:hh,isTextBox:true,margin:0,fontFace:B,
      fontSize:9.2,color:'C6DCB2',lineSpacing:11.6});
    yy+=0.23+hh+0.18;
  });
});
pageNo(s,4,true);
s.addNotes('d) risks and opportunities of the proposed approach; e) additional approaches recommended, in priority order.');
}

p.writeFile({fileName:'Marketing_Science_Case_Study.pptx'}).then(f=>console.log('written',f));
