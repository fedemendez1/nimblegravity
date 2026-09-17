import re, csv
rows=[]
for line in open('dataset_raw.txt'):
    m=re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\S+)\s+(tradedesk|google uac)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$', line.strip())
    if m:
        rows.append(dict(week=m.group(1), channel=m.group(2), vendor=m.group(3).replace(' ','_'),
                         vendor_ui=int(m.group(4)), rule_based=int(m.group(5)),
                         mmm=int(m.group(6)), spend=int(m.group(7))))
print('parsed rows:', len(rows))
# duplicates
seen={}
dups=[]
for r in rows:
    k=(r['week'],r['vendor'])
    if k in seen: dups.append((k, r==seen[k]))
    seen[k]=r
print('duplicate (week,vendor) keys:', dups)
clean=list(seen.values())
clean.sort(key=lambda r:(r['week'],r['vendor']))
print('clean rows:', len(clean), 'weeks:', len(set(r['week'] for r in clean)))
with open('data.csv','w',newline='') as f:
    w=csv.DictWriter(f, fieldnames=['week','channel','vendor','spend','vendor_ui','rule_based','mmm'])
    w.writeheader()
    for r in clean: w.writerow({k:r[k] for k in w.fieldnames})
