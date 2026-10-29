#!/usr/bin/env python3
"""Summarises benchmark/mqtt-results/ (or the directory given as the first argument): median over rounds per scenario and library, as markdown,
one set of tables for each group of libraries found: MQTT 3.1.1 over TCP, MQTT 5 over TCP, MQTT 5 over WebSocket."""
import glob, os, re, statistics, sys

d = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "mqtt-results")
LIB = r"(hydrangea5-ws|hydrangea5|hydrangea|paho5-ws-nodelay|paho5-ws|paho5-nodelay|paho5|paho-nodelay|paho)"
GROUPS = [("MQTT 3.1.1 over TCP", "hydrangea", "paho", "paho-nodelay", 1000, "Paho org.eclipse.paho.client.mqttv3 1.2.5"),
          ("MQTT 5 over TCP", "hydrangea5", "paho5", "paho5-nodelay", 100, "Paho org.eclipse.paho.mqttv5.client 1.2.5"),
          ("MQTT 5 over WebSocket", "hydrangea5-ws", "paho5-ws", "paho5-ws-nodelay", 100, "Paho org.eclipse.paho.mqttv5.client 1.2.5, ws://")]
runs = {}   # (scenario name, lib) -> list of dicts
failed = []
for f in sorted(glob.glob(os.path.join(d, "r*-*.log"))):
    m = re.match(r"r(\d+)-" + LIB + r"-(.+)\.log$", os.path.basename(f))
    if not m:
        continue
    rnd, lib, name = int(m.group(1)), m.group(2), m.group(3)
    text = open(f).read()
    line = next((l for l in text.splitlines() if l.startswith("RESULT ")), None)
    if line is None:
        failed.append(os.path.basename(f))
        continue
    kv = dict(p.split("=", 1) for p in line.split()[1:])
    cpu = [float(x.split()[1].rstrip("%")) for x in open(f + ".cpu").read().splitlines() if x.startswith("BROKERCPU")] if os.path.exists(f + ".cpu") else []
    kv["broker_cpu_pct"] = str(round(statistics.mean(cpu), 0)) if cpu else "-"
    kv["round"] = rnd
    runs.setdefault((name, lib), []).append(kv)

def vals(name, lib, key):
    out = []
    for r in runs.get((name, lib), []):
        try:
            out.append(float(r[key]))
        except (KeyError, ValueError):
            pass
    return out

def cell(v, fmt="{:,.0f}"):
    if not v:
        return "-"
    return fmt.format(statistics.median(v)) + " (" + " / ".join(fmt.format(x) for x in v) + ")"

def ratio(a, b):
    return f"{statistics.median(a) / statistics.median(b):.2f}" if a and b and statistics.median(b) else "-"

def table(title, name, rows, libs):
    h_, p_, n_ = libs
    print(f"\n### {title}\n")
    print("| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |")
    print("|---|---:|---:|---:|---:|---:|")
    for label, key, fmt in rows:
        h, o, n = vals(name, h_, key), vals(name, p_, key), vals(name, n_, key)
        print(f"| {label} | {cell(h, fmt)} | {cell(o, fmt)} | {cell(n, fmt)} | {ratio(h, o)} | {ratio(h, n)} |")

PUB = [("sender-finished msgs/s", "completed_per_s", "{:,.0f}"), ("subscriber-received msgs/s", "received_per_s", "{:,.0f}"),
       ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("lost (issued - received)", "lost", "{:,.0f}"), ("unfinished at end", "unfinished_at_end", "{:,.0f}"),
       ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")]
LAT = [("p50 us", "p50_us", "{:,.0f}"), ("p90 us", "p90_us", "{:,.0f}"), ("p99 us", "p99_us", "{:,.0f}"), ("p99.9 us", "p999_us", "{:,.0f}"), ("max us", "max_us", "{:,.0f}")]
CONN = [("delivered msgs/s", "delivered_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("time to open 100 connections s", "setup_s", "{:.1f}"),
        ("threads after setup", "threads_after_setup", "{:,.0f}"), ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")]

for title, h, p, n, w1, paho in GROUPS:
    if not any(lib in (h, p, n) for (_, lib) in runs):
        continue
    libs = (h, p, n)
    print(f"\n## {title} ({paho})")
    table("Publish QoS 0 (2 producers, 128 B, window 1000, one subscriber)", "publish-qos0", PUB, libs)
    table(f"Publish QoS 1 (2 producers, 128 B, window {w1}, at most 2000 undelivered, one subscriber)", "publish-qos1", PUB, libs)
    table("Publish QoS 1, 64 KiB messages (1 producer, window 16, at most 64 undelivered)", "publish-qos1-64k", PUB[:2] + [("MB/s sent", "mb_per_s", "{:,.0f}")] + PUB[2:], libs)
    table("Publish QoS 2 (2 producers, 128 B, window 100, at most 400 undelivered)", "publish-qos2", PUB, libs)
    table("Latency QoS 0, one message at a time (publish -> delivery, 20k samples)", "latency-qos0", LAT, libs)
    table("Latency QoS 1, one message at a time (publish -> delivery, 20k samples)", "latency-qos1", LAT, libs)
    table("100 connections, each subscribed to its own topic and publishing QoS 1 to it", "connections-100", CONN, libs)
if failed:
    print("\nRuns without a result line: " + ", ".join(failed))
