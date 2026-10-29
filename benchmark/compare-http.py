#!/usr/bin/env python3
"""Compares two HttpDriver result files (RESULTS-HTTP-*.md): per scenario and server, throughput and CPU per op, plus Hydrangea's ratio to Netty in each file."""
import re, sys

def parse(path):
    out, scn = {}, None
    for line in open(path):
        if line.startswith("## "):
            scn = line[3:].strip()
        elif line.startswith("| ") and scn and not line.startswith("| server") and not line.startswith("|---"):
            c = [x.strip() for x in line.strip().strip("|").split("|")]
            try:
                out.setdefault(scn, {})[c[0]] = (float(c[1]), float(c[3]), float(c[6]))   # rate, p99, cpu
            except (ValueError, IndexError):
                pass
    return out

old, new = parse(sys.argv[1]), parse(sys.argv[2])
print("| scenario | hydrangea before | hydrangea now | now/before | netty before | netty now | now/before | hydrangea/netty before | hydrangea/netty now |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for s in new:
    if s not in old or "hydrangea" not in new[s] or "netty" not in new[s] or "hydrangea" not in old[s] or "netty" not in old[s]:
        continue
    hb, hn, nb, nn = old[s]["hydrangea"][0], new[s]["hydrangea"][0], old[s]["netty"][0], new[s]["netty"][0]
    print(f"| {s} | {hb:,.0f} | {hn:,.0f} | {hn/hb:.2f} | {nb:,.0f} | {nn:,.0f} | {nn/nb:.2f} | {hb/nb:.2f} | {hn/nn:.2f} |")
