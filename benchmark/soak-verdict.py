#!/usr/bin/env python3
"""Usage: soak-verdict.py server.log load.log [pods.csv]  -- verdict of a gRPC/HTTP2 soak (k8s/soak-*.yaml, SoakServer/SoakLoad)."""
import re, sys, statistics as st

def health(path):
    rows = []
    for l in open(path):
        if l.startswith('health '):
            rows.append({k: int(v) for k, v in (t.split('=') for t in l[7:].split())})
    return rows

h = health(sys.argv[1])
load = open(sys.argv[2]).read().splitlines()
summ = [l for l in load if l.startswith('SUMMARY')]
errors = sum(int(re.search(r'errors=(\d+)', l).group(1)) for l in summ)
crashes = [l for l in load if l.startswith('WORKER-CRASH')]
idle = [r for r in h if r['http'] == 0 and r['t'] <= 150]
busy = [r for r in h if r['http'] > 0 and r['active'] > 0]
tail = h[-4:]
out = []
p = out.append
p('# gRPC + HTTP/2 soak (Kubernetes)\n')
p('Server samples every 30 s: %d, from t=%d s to t=%d s.\n' % (len(h), h[0]['t'], h[-1]['t']))
p('## Client side (verified workloads)\n')
for l in summ: p('- ' + l[len('SUMMARY '):])
p('\nTotal errors: **%d**; worker crashes: %d.\n' % (errors, len(crashes)))
base = idle[-1] if idle else h[0]
n = len(busy)
first, last = busy[: max(1, n // 6)], busy[-max(1, n // 6):]
def avg(rows, k): return st.mean(r[k] for r in rows)
p('## Server health\n')
p('| | idle before | first sixth under load | last sixth under load | after the load (last sample) |')
p('|---|---:|---:|---:|---:|')
for k, label in [('oldGenAfterGcMB', 'live heap after GC (MB)'), ('threads', 'threads'), ('fds', 'file descriptors'), ('active', 'active connections')]:
    p('| %s | %d | %.1f | %.1f | %d |' % (label, base[k], avg(first, k), avg(last, k), tail[-1][k]))
p('| max event-loop block (ms) | %d | max %d | max %d | %d |' % (base['maxBlockMs'], max(r['maxBlockMs'] for r in first), max(r['maxBlockMs'] for r in last), tail[-1]['maxBlockMs']))
p('')
p('Connections opened %d, closed %d (difference %d, those still open); rejected %d; HTTP responses %d; calls the server saw cancelled %d.\n' % (
    h[-1]['opened'], h[-1]['closed'], h[-1]['opened'] - h[-1]['closed'], h[-1]['rejected'], h[-1]['http'], h[-1]['cancelled']))
live = [r['oldGenAfterGcMB'] for r in busy]
slope = (avg(last, 'oldGenAfterGcMB') - avg(first, 'oldGenAfterGcMB'))
p('Live heap under load: min %d, max %d, change between the first and last sixth %+.1f MB.\n' % (min(live), max(live), slope))
checks = [
    ('0 errors in every verified workload', errors == 0 and not crashes),
    ('connections back to 0 after the load', tail[-1]['active'] == 0),
    ('descriptors back within 3 of the idle baseline', tail[-1]['fds'] <= base['fds'] + 3),
    ('threads back within 3 of the idle baseline', tail[-1]['threads'] <= base['threads'] + 3),
    ('live heap did not grow (last sixth - first sixth < 25 MB)', slope < 25),
    ('opened - closed <= 3 after the load', h[-1]['opened'] - h[-1]['closed'] <= 3),
]
p('## Verdict\n')
for name, ok in checks: p('- [%s] %s' % ('x' if ok else ' ', name))
if len(sys.argv) > 3:
    rows = [l.strip().split(',') for l in open(sys.argv[3]).read().splitlines()[1:] if l.strip()]
    srv = [r for r in rows if 'server' in r[1]]
    if srv:
        p('\nServer container memory (MiB, every 2 minutes): min %s, max %s; restarts: %s.' % (min(int(r[3]) for r in srv), max(int(r[3]) for r in srv), srv[-1][4]))
print('\n'.join(out))
sys.exit(0 if all(ok for _, ok in checks) else 1)
