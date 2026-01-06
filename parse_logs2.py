import re

with open('logs/ganache_baseline_9999.log', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for line in lines:
    if 'Block time:' in line:
        print(line.strip())
        # extract timestamp
        # pattern: Block time: (.+?) GMT
        match = re.search(r'Block time: (.+?) GMT', line)
        if match:
            print('  ->', match.group(1))