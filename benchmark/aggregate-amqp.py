#!/usr/bin/env python3
"""Summarises benchmark/amqp-results/: median over rounds per scenario and library, as markdown."""
import glob, os, re, statistics, sys

d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "amqp-results")
runs = {}   # (scenario name, lib) -> list of dicts
failed = []
for f in sorted(glob.glob(os.path.join(d, "r*-*.log"))):
    m = re.match(r"r(\d+)-(\w+)-(.+)\.log$", os.path.basename(f))
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

def table(title, name, rows, higher_is_better):
    print(f"\n### {title}\n")
    print("| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |")
    print("|---|---:|---:|---:|")
    for label, key, fmt in rows:
        h, o = vals(name, "hydrangea", key), vals(name, "official", key)
        print(f"| {label} | {cell(h, fmt)} | {cell(o, fmt)} | {ratio(h, o)} |")

table("Publish, no confirmations (2 producers, 128 B)", "publish-noconfirm", [
    ("messages/s", "msgs_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("lost (broker count vs sent)", "lost", "{:,.0f}"),
    ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")], True)
table("Publish with confirmations, window 1000 per producer (2 producers, 128 B)", "publish-confirm", [
    ("confirmed msgs/s", "msgs_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("unconfirmed at end", "unconfirmed_at_end", "{:,.0f}"),
    ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")], True)
table("Publish with confirmations, 64 KiB messages (1 producer, window 16)", "publish-confirm-64k", [
    ("confirmed msgs/s", "msgs_per_s", "{:,.0f}"), ("MB/s", "mb_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"),
    ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")], True)
table("Consume with an acknowledgement per message (300k messages, 2 consumers, prefetch 250)", "consume", [
    ("messages/s", "msgs_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("left in queue", "left_in_queue", "{:,.0f}"),
    ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"), ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")], True)
table("Latency, one message at a time (publish -> delivery, 20k samples)", "latency", [
    ("p50 us", "p50_us", "{:,.0f}"), ("p90 us", "p90_us", "{:,.0f}"), ("p99 us", "p99_us", "{:,.0f}"), ("p99.9 us", "p999_us", "{:,.0f}"), ("max us", "max_us", "{:,.0f}")], False)
table("100 connections, each with a confirmed publisher and an acknowledging consumer", "connections-100", [
    ("delivered msgs/s", "delivered_per_s", "{:,.0f}"), ("client CPU-s per 100k msgs", "client_cpu_s_per_100k", "{:.2f}"), ("time to open 100 connections s", "setup_s", "{:.1f}"),
    ("threads after setup", "threads_after_setup", "{:,.0f}"), ("peak threads", "threads_peak", "{:,.0f}"), ("RSS MB", "rss_mb", "{:,.0f}"),
    ("broker CPU % (docker)", "broker_cpu_pct", "{:,.0f}")], True)
if failed:
    print("\nRuns without a result line: " + ", ".join(failed))
