import pandas as pd, numpy as np
df=pd.read_csv('data.csv', parse_dates=['week'])
df['q4']=(df.week>='2022-11-01').astype(int)
print("=== ELASTICITY: log(MMM regs) ~ log(spend), by vendor ===")
for v in ['google_uac','tradedesk']:
    for lab,d in [('Base',df[(df.vendor==v)&(df.q4==0)]),('Q4',df[(df.vendor==v)&(df.q4==1)]),('All',df[df.vendor==v])]:
        x=np.log(d.spend); y=np.log(d.mmm)
        b,a=np.polyfit(x,y,1); r=np.corrcoef(x,y)[0,1]**2
        print(f"  {v:11s} {lab:5s} n={len(d):2d}  elasticity={b:.3f}  R2={r:.3f}")
print("\n  (elasticity ~1.0 => constant returns in observed spend range; <1 => diminishing returns)")

print("\n=== SPEND vs MMM-CPA correlation within period (saturation check) ===")
for v in ['google_uac','tradedesk']:
    for lab,d in [('Base',df[(df.vendor==v)&(df.q4==0)]),('Q4',df[(df.vendor==v)&(df.q4==1)])]:
        cpa=d.spend/d.mmm
        print(f"  {v:11s} {lab:5s} corr(spend, CPA_mmm)={np.corrcoef(d.spend,cpa)[0,1]:+.2f}  CPA range {cpa.min():.1f}-{cpa.max():.1f}")

print("\n=== BUSINESS IMPACT: cost of steering on vendor UI ===")
g=df.groupby('vendor')[['spend','vendor_ui','mmm']].sum()
tot_spend=g.spend.sum(); tot_mmm=g.mmm.sum()
print(f"Actual: spend GBP {tot_spend:,.0f} -> {tot_mmm:,.0f} incremental (MMM) regs, blended CPA GBP {tot_spend/tot_mmm:.2f}")
print(f"Vendor UIs claim {g.vendor_ui.sum():,.0f} regs -> if budgets/targets set on UI, you overstate output by {100*(g.vendor_ui.sum()/tot_mmm-1):.0f}%")
print(f"Rule-based claims {df.rule_based.sum():,.0f} -> understates by {100*(df.rule_based.sum()/tot_mmm-1):.0f}%")

print("\n=== Q4 REALLOCATION SCENARIO (MMM-based, constant marginal CPA within observed range) ===")
q=df[df.q4==1].groupby('vendor')[['spend','mmm']].sum()
q['cpa']=q.spend/q.mmm
print(q.round(2))
for shift_pct in [0.10,0.20]:
    amt=q.loc['google_uac','spend']*shift_pct
    delta=amt/q.loc['tradedesk','cpa'] - amt/q.loc['google_uac','cpa']
    print(f"  Shift {shift_pct:.0%} of Google Q4 spend (GBP {amt:,.0f}) to Tradedesk -> {delta:+,.0f} incremental regs "
          f"({100*delta/q.mmm.sum():+.1f}% of Q4 output) at flat budget")
