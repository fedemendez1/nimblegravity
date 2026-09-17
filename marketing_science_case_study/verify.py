import pandas as pd, numpy as np
df=pd.read_csv('data.csv',parse_dates=['week']); df['q4']=df.week>='2022-11-01'
T=df[['spend','vendor_ui','rule_based','mmm']].sum()
print(f"A. spend GBP{T.spend:,.0f} | UI {T.vendor_ui:,} | RB {T.rule_based:,} | MMM {T.mmm:,}")
print(f"   UI vs MMM {100*(T.vendor_ui/T.mmm-1):+.0f}% | RB vs MMM {100*(T.rule_based/T.mmm-1):+.0f}%")
g=df.groupby('q4')[['spend','vendor_ui','rule_based','mmm']].sum()
print(f"B. UI premium: base {100*(g.loc[False,'vendor_ui']/g.loc[False,'mmm']-1):.0f}% -> Q4 {100*(g.loc[True,'vendor_ui']/g.loc[True,'mmm']-1):.0f}%")
t=df.groupby('vendor')[['spend','vendor_ui','rule_based','mmm']].sum()
print(f"C. TTD share of incremental regs: {100*t.loc['tradedesk','mmm']/T.mmm:.0f}% | of spend {100*t.loc['tradedesk','spend']/T.spend:.0f}%")
b=df[(~df.q4)&(df.rule_based>0)].groupby('vendor')[['rule_based','mmm','vendor_ui']].sum()
print(f"D. rb/mmm base: TTD {b.loc['tradedesk','rule_based']/b.loc['tradedesk','mmm']:.2f} | GOOG {b.loc['google_uac','rule_based']/b.loc['google_uac','mmm']:.2f}")
print(f"   ui/mmm base: TTD {b.loc['tradedesk','vendor_ui']/b.loc['tradedesk','mmm']:.2f} | GOOG {b.loc['google_uac','vendor_ui']/b.loc['google_uac','mmm']:.2f}")
print(f"E. TTD full-yr CPA rb GBP{t.loc['tradedesk','spend']/t.loc['tradedesk','rule_based']:.2f} vs mmm GBP{t.loc['tradedesk','spend']/t.loc['tradedesk','mmm']:.2f}")
# k drift
kb=g.loc[False,'mmm']/g.loc[False,'vendor_ui']; kq=g.loc[True,'mmm']/g.loc[True,'vendor_ui']
print(f"F. blended k base {kb:.3f} -> Q4 {kq:.3f}; using base k in Q4 overstates incremental by {100*(kb/kq-1):.0f}%")
for v in ['google_uac','tradedesk']:
    d=df[df.vendor==v]; a=d[~d.q4]; c=d[d.q4]
    k1=a.mmm.sum()/a.vendor_ui.sum(); k2=c.mmm.sum()/c.vendor_ui.sum()
    print(f"   {v}: k {k1:.2f} -> {k2:.2f}, overstatement {100*(k1/k2-1):.0f}%")
print(f"G. spend Jan GBP{df[df.week.dt.month==1].spend.sum():,} -> Dec GBP{df[df.week.dt.month==12].spend.sum():,} = {df[df.week.dt.month==12].spend.sum()/df[df.week.dt.month==1].spend.sum():.1f}x")
for v in ['google_uac','tradedesk']:
    d=df[(df.vendor==v)&(~df.q4)]
    print(f"H. elasticity base {v}: {np.polyfit(np.log(d.spend),np.log(d.mmm),1)[0]:.2f} (R2 {np.corrcoef(np.log(d.spend),np.log(d.mmm))[0,1]**2:.3f})")
print(f"I. blended true CPA GBP{T.spend/T.mmm:.2f}; UI-implied CPA GBP{T.spend/T.vendor_ui:.2f}")
