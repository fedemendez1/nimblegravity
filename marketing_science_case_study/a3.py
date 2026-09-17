import pandas as pd, numpy as np
pd.set_option('display.width',250); pd.set_option('display.max_columns',50)
df=pd.read_csv('data.csv', parse_dates=['week'])
df['period']=np.where(df.week>='2022-11-01','Q4 (Nov-Dec)','Base (Jan-Oct)')

print("=== 1. TRACKING OUTAGE ===")
print(df[df.rule_based==0][['week','vendor','spend','vendor_ui','rule_based','mmm']].to_string(index=False))
o=df[df.rule_based==0]
print(f"Outage: {len(o)} weeks, spend at risk GBP {o.spend.sum():,}, MMM regs {o.mmm.sum():,}, rule-based regs 0")
print(f"-> Implied rule-based CPA in outage: INFINITE. May-22 Tradedesk reported CPA GBP 205.7 vs true ~19.3")
# estimate lost registrations vs normal ratio
norm=df[(df.vendor=='tradedesk')&(df.rule_based>0)&(df.period=='Base (Jan-Oct)')]
r=norm.rule_based.sum()/norm.mmm.sum()
print(f"Normal Tradedesk rb/mmm = {r:.3f}; expected rb during outage = {r*o.mmm.sum():.0f} registrations lost from tracking")

print("\n=== 2. STRUCTURAL BIAS, stable base period (Jan-Oct) ===")
b=df[df.period=='Base (Jan-Oct)'].groupby('vendor')[['spend','vendor_ui','rule_based','mmm']].sum()
b2=df[(df.period=='Base (Jan-Oct)')&(df.rule_based>0)].groupby('vendor')[['spend','vendor_ui','rule_based','mmm']].sum()
print('UI/MMM :', (b.vendor_ui/b.mmm).round(3).to_dict())
print('RB/MMM (excl. outage weeks):', (b2.rule_based/b2.mmm).round(3).to_dict())

print("\n=== 3. Q4 REGIME BREAK: CPA by method, Base vs Q4 ===")
g=df.groupby(['vendor','period'])[['spend','vendor_ui','rule_based','mmm']].sum()
for m,lab in [('vendor_ui','Vendor UI'),('rule_based','Rule-based'),('mmm','MMM')]:
    g['CPA '+lab]=(g.spend/g[m]).round(2)
tab=g[['spend','CPA Vendor UI','CPA Rule-based','CPA MMM']]
print(tab)
print()
for v in ['google_uac','tradedesk']:
    base=tab.loc[(v,'Base (Jan-Oct)')]; q4=tab.loc[(v,'Q4 (Nov-Dec)')]
    print(f"{v}: Vendor-UI CPA {base['CPA Vendor UI']:.2f} -> {q4['CPA Vendor UI']:.2f} ({100*(q4['CPA Vendor UI']/base['CPA Vendor UI']-1):+.0f}%) | "
          f"MMM CPA {base['CPA MMM']:.2f} -> {q4['CPA MMM']:.2f} ({100*(q4['CPA MMM']/base['CPA MMM']-1):+.0f}%)")

print("\n=== 4. RANKING REVERSAL (who is the better buy?) ===")
for p in ['Base (Jan-Oct)','Q4 (Nov-Dec)']:
    s=g.xs(p,level='period')
    print(p)
    for lab in ['CPA Vendor UI','CPA Rule-based','CPA MMM']:
        best=s[lab].idxmin()
        print(f"   {lab:16s}: google {s.loc['google_uac',lab]:7.2f} | tradedesk {s.loc['tradedesk',lab]:7.2f}  -> favours {best}")
