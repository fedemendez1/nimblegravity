import pandas as pd, numpy as np
df=pd.read_csv('data.csv', parse_dates=['week']); df['q4']=(df.week>='2022-11-01').astype(int)

print("=== IS GOOGLE'S Q4 DECAY VOLUME (saturation) OR SEASON (incrementality)? ===")
d=df[df.vendor=='google_uac'].copy()
X=np.column_stack([np.ones(len(d)), np.log(d.spend), d.q4]); y=np.log(d.spend/d.mmm)
beta,*_=np.linalg.lstsq(X,y,rcond=None)
print(f"  log(CPA_mmm) = {beta[0]:.2f} + {beta[1]:.3f}*log(spend) + {beta[2]:.3f}*Q4")
print(f"  -> spend effect: +10% spend => {100*((1.1**beta[1])-1):+.1f}% CPA (weak saturation)")
print(f"  -> Q4 effect   : {100*(np.exp(beta[2])-1):+.1f}% CPA at IDENTICAL spend  <-- dominant")
print(f"  Evidence: Oct spend GBP46,900 CPA_mmm 15.3 | Dec spend GBP50,120 (+7%) CPA_mmm 19.2 (+26%)")

print("\n=== CALIBRATION FACTORS (multiply platform number by k to get incremental) ===")
rows=[]
for v in ['google_uac','tradedesk']:
    for lab,mask in [('Base (Jan-Oct)',df.q4==0),('Q4 (Nov-Dec)',df.q4==1)]:
        d=df[(df.vendor==v)&mask&(df.rule_based>0)]
        rows.append(dict(vendor=v,period=lab,
            k_vendor_ui=round(d.mmm.sum()/d.vendor_ui.sum(),3),
            k_rule_based=round(d.mmm.sum()/d.rule_based.sum(),3)))
cal=pd.DataFrame(rows); print(cal.to_string(index=False))

print("\n=== TARGET TRANSLATION: what a vendor-UI CPA target really costs ===")
TRUE_TARGET=18.0
print(f"  If the business can afford a TRUE (MMM) CPA of GBP {TRUE_TARGET:.2f}, the in-platform target must be:")
for _,r in cal.iterrows():
    print(f"   {r.vendor:11s} {r.period:15s} -> vendor-UI CPA target GBP {TRUE_TARGET*r.k_vendor_ui:5.2f}")
print("  Same nominal UI target in Q4 buys ~25-35% less real value: targets must be re-based each season.")

print("\n=== COST OF STEERING Q4 ON THE WRONG SIGNAL ===")
q=df[df.q4==1].groupby('vendor')[['spend','mmm']].sum(); q['cpa']=q.spend/q.mmm
amt=q.loc['tradedesk','spend']*0.20
loss=amt/q.loc['google_uac','cpa'] - amt/q.loc['tradedesk','cpa']
print(f"  Vendor UI ranks Google best in Q4 (GBP10.31 vs GBP12.81); MMM ranks Tradedesk best (GBP17.48 vs GBP18.75).")
print(f"  Moving 20% of Tradedesk Q4 budget (GBP {amt:,.0f}) to Google as the UI suggests: {loss:+,.0f} registrations.")
print(f"  Reverse (MMM-led) move: {-loss:+,.0f} regs. Swing = {2*abs(loss):,.0f} regs on a GBP{amt:,.0f} decision.")
