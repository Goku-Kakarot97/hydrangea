#!/usr/bin/env python3
"""Medians over the rounds of redis-bench.sh: aggregate-redis.py OUT_DIR"""
import sys, glob, re, statistics, collections
rows = collections.defaultdict(lambda: collections.defaultdict(list))
for f in sorted(glob.glob(sys.argv[1] + "/round*.txt")):
    for line in open(f):
        if not line.startswith("RESULT"):
            continue
        kv = dict(p.split("=", 1) for p in line.split()[1:])
        sc = kv.pop("scenario"); lib = kv.pop("lib")
        key = sc + "".join(" %s=%s" % (k, kv[k]) for k in ("threads", "window", "size_kb", "connections") if k in kv)
        for k, v in kv.items():
            try: rows[(key, lib)][k].append(float(v))
            except ValueError: pass
keys = sorted({k for k, _ in rows})
for key in keys:
    print("### " + key)
    libs = [l for l in ("hydrangea", "hydrangea-conns", "jedis") if (key, l) in rows]
    metrics = [m for m in rows[(key, libs[0])] if m not in ("threads", "window", "size_kb", "connections")]
    print("| metric | " + " | ".join(libs) + " |\n|---|" + "---|" * len(libs))
    for m in metrics:
        vals = [statistics.median(rows[(key, l)][m]) if rows[(key, l)].get(m) else float("nan") for l in libs]
        print("| %s | %s |" % (m, " | ".join("%g" % v for v in vals)))
    print()
