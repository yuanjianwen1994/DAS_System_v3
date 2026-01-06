import re
import datetime

with open('logs/ganache_baseline_9999.log', 'r', encoding='utf-8') as f:
    content = f.read()

# pattern for "Block time: ..."
pattern = r'Block time: (.+?)\\n'
matches = re.findall(pattern, content)
print(f'Found {len(matches)} block times')
for i, match in enumerate(matches):
    print(f'{i}: {match}')
    if i > 0:
        # parse timestamps
        try:
            dt1 = datetime.datetime.strptime(matches[i-1], '%a %b %d %Y %H:%M:%S GMT%z')
            dt2 = datetime.datetime.strptime(match, '%a %b %d %Y %H:%M:%S GMT%z')
            diff = (dt2 - dt1).total_seconds()
            print(f'  Diff from previous: {diff:.2f} s')
        except Exception as e:
            print(f'  Error parsing: {e}')