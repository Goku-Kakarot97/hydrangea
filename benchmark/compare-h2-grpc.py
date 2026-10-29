#!/usr/bin/env python3
"""Compares two runs of run-h2.sh (arguments: h2 old, h2 new) and run-grpc.sh (grpc old, grpc new) logs, as markdown."""
import re, sys

def h2(p):
    d = {}
    for l in open(p):
        m = re.match(r"(hydrangea|netty)\s+scenario=(\w+) conns=(\d+) streams=(\d+) rps=(\d+) .*?serverCpuUsPerReq=([\d.]+)", l)
        if m:
            d[(m[2], m[3], m[4], m[1])] = (int(m[5]), float(m[6]))   # the last line of a key wins (the script measures twice)
    return d

def grpc(p):
    d, cur = {}, None
    for l in open(p):
        m = re.match(r"# (\S+) inflight/channels=(\d+ \d+) client=(\w+)", l)
        if m:
            cur = (m[1], m[2], m[3])
            continue
        m = re.match(r"(hydrangea|java)\s+<- impl=\w+ scenario=\S+ inflight=\d+ channels=\d+ rps=(\d+).*?serverCpuUsPerOp=([\d.]+)", l)
        if m and cur:
            d[cur + (m[1],)] = (int(m[2]), float(m[3]))
    return d

o, n = h2(sys.argv[1]), h2(sys.argv[2])
print("### HTTP/2 (cleartext, prior knowledge), requests/s\n")
print("| scenario | hydrangea before | now | now/before | netty before | now | now/before | hydrangea/netty before | now | hydrangea server CPU us/req before -> now |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
for (s, c, st, impl) in sorted(n):
    if impl != "hydrangea":
        continue
    k = (s, c, st)
    try:
        hb, hn, nb, nn = o[k + ("hydrangea",)], n[k + ("hydrangea",)], o[k + ("netty",)], n[k + ("netty",)]
    except KeyError:
        continue
    print(f"| {s} {c} connections x {st} streams | {hb[0]:,} | {hn[0]:,} | {hn[0]/hb[0]:.2f} | {nb[0]:,} | {nn[0]:,} | {nn[0]/nb[0]:.2f} | {hb[0]/nb[0]:.2f} | {hn[0]/nn[0]:.2f} | {hb[1]} -> {hn[1]} |")

o, n = grpc(sys.argv[3]), grpc(sys.argv[4])
print("\n### gRPC, requests/s (server under test; the load generator is named in the last column of the key)\n")
print("| scenario, inflight/channels, load generator | hydrangea server before | now | now/before | grpc-java server before | now | now/before | hydrangea/grpc-java before | now |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
for k in sorted(n, key=lambda k: (k[2] != "java", k)):
    if k[3] != "hydrangea":
        continue
    kj = k[:3] + ("java",)
    if k not in o or kj not in o or kj not in n:
        continue
    hb, hn, jb, jn = o[k], n[k], o[kj], n[kj]
    print(f"| {k[0]}, {k[1].replace(' ', '/')}, {k[2]} client | {hb[0]:,} | {hn[0]:,} | {hn[0]/hb[0]:.2f} | {jb[0]:,} | {jn[0]:,} | {jn[0]/jb[0]:.2f} | {hb[0]/jb[0]:.2f} | {hn[0]/jn[0]:.2f} |")
