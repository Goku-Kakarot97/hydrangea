#!/usr/bin/env python3
"""Aggregates benchmark/k8s-results/*.log (written by run-k8s-compare.sh) into a median table. Rounds with 0 ops are discarded as invalid."""
import re, glob, statistics, sys
def parse(f):
    res = {}; cur = None
    for l in open(f):
        if l.startswith('## '):
            m = re.search(r'(http|ws|sse) 8080 (\d+) .*?(?:path=(\S+))?(?: |$)', l)
            cur = re.sub(r'host=\S+', '', l[3:]).strip()
        m = re.match(r'RESULT (.*)', l)
        if m and cur:
            res[cur] = {k: float(v) for k, v in (x.split('=') for x in m.group(1).split())}; cur = None
    return res
def label(c):
    p = re.search(r'path=(\S+)', c).group(1) if 'path=' in c else ''
    conns = c.split()[2]
    kind = 'WebSocket echo 64 B' if c.startswith('ws') else ('POST /echo 1 KiB' if 'method=POST' in c else 'GET ' + p)
    return f"{kind}, {conns} conn"
impls = ['hydrangea', 'netty']
data = {i: [parse(f) for f in sorted(glob.glob(f'k8s-results/{i}-r*.log'))] for i in impls}
scen = [k for k in data['hydrangea'][0]]
out = ["| scenario | server | req/s | p50 µs | p99 µs | p99.9 µs | errors | valid rounds |", "|---|---|---:|---:|---:|---:|---:|---:|"]
raw = []
for s in scen:
    for i in impls:
        rows = [r[s] for r in data[i] if s in r]
        valid = [r for r in rows if r['ops'] > 0]
        errors = int(sum(r['errors'] for r in rows))
        if not valid:
            out.append(f"| {label(s)} | {i} | - | - | - | - | {errors} | 0/{len(rows)} |"); continue
        valid.sort(key=lambda r: r['ops']); m = valid[len(valid)//2]
        out.append(f"| {label(s)} | {i} | {m['ops']:.0f} | {m['p50']:.0f} | {m['p99']:.0f} | {m['p999']:.0f} | {errors} | {len(valid)}/{len(rows)} |")
        raw.append((label(s), i, [round(r['ops']) for r in rows]))
print('\n'.join(out)); print()
print("per-round throughput (req/s), to judge the noise:")
for l, i, v in raw: print(f"  {l:42s} {i:10s} {v}")
