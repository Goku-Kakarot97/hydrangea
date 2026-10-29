#!/usr/bin/env python3
"""Medians over the rounds of kafka-bench.sh: aggregate-kafka.py OUT_DIR"""
import sys, glob, statistics, collections
rows = collections.defaultdict(lambda: collections.defaultdict(list))
KEYS = ("size", "acks", "idem", "inflight", "partitions", "threads", "records", "samples")
for f in sorted(glob.glob(sys.argv[1] + "/round*.txt")):
    for line in open(f):
        if not line.startswith("RESULT"):
            continue
        kv = dict(p.split("=", 1) for p in line.split()[1:])
        sc = kv.pop("scenario"); lib = kv.pop("lib")
        key = sc + "".join(" %s=%s" % (k, kv[k]) for k in KEYS if k in kv)
        for k, v in kv.items():
            if k in KEYS: continue
            try: rows[(key, lib)][k].append(float(v))
            except ValueError: pass
for key in sorted({k for k, _ in rows}):
    print("### " + key)
    libs = [l for l in ("hydrangea", "kafka-clients") if (key, l) in rows]
    metrics = list(rows[(key, libs[0])])
    print("| metric | " + " | ".join(libs) + " | runs |\n|---|" + "---|" * (len(libs) + 1))
    for m in metrics:
        vals = [statistics.median(rows[(key, l)][m]) if rows[(key, l)].get(m) else float("nan") for l in libs]
        runs = "/".join(str(len(rows[(key, l)][m])) for l in libs)
        print("| %s | %s | %s |" % (m, " | ".join("%g" % v for v in vals), runs))
    print()
