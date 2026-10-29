# HTTP/2 vs HTTP/3 with the same client under delay and loss (Kubernetes)

Both servers are Hydrangea (`HydrangeaHttpServer` h2 over TLS 1.3 / `Http3Example`), 0.5 CPU, 1 I/O thread, one pod each, plus a client pod running `benchmark/src/h3/cmp_client.py`
(Python: `h2` library over asyncio+OpenSSL for HTTP/2, `aioquic` for HTTP/3; 1 MiB stream and connection windows for both). `tc netem` on the egress of all three pods (client, h2, h3), so delay and loss
apply in both directions: "rtt50" = 25 ms each way, "loss1"/"loss3" = rtt50 plus 1% / 3% random loss. Streams test: 10 concurrent GET of 100 000 bytes on one connection, 15 rounds.
Connect test: 12 fresh connections, time until the response to `GET /plain` arrives. Each cell is two repetitions with the protocol order swapped; warm-up before the first scenario. Raw data: `k8s-results/h3-netem/`
(`results.jsonl` is the valid run; `results-run1-flawed.jsonl` is a first run whose connect metric wrongly included the QUIC closing period and whose h2 client rebuilt the SSL context per connection).

| condition | | HTTP/2 | HTTP/3 |
|---|---|---:|---:|
| clean network | download MB/s | 119-173 | 8.0-8.4 |
| | connect p50 | 6 ms | 8 ms |
| rtt50 | download MB/s | 6.2-7.5 | 4.5-4.6 |
| | connect p50 | 260 ms | 210 ms |
| rtt50 + 1% loss | download MB/s | 0.51-0.83 | 0.27-0.29 |
| | per-stream p50 | 0.3-1.0 s | 2.5-2.6 s |
| | connect p50 | 260-266 ms | 310 ms |
| rtt50 + 3% loss | download MB/s | 0.21-0.22 | 0.16 |
| | per-stream p50 | 3.5-3.8 s | 4.6-4.8 s |
| | connect p50 | 364-370 ms | 210-446 ms |

## What it says
- **Fresh connection:** HTTP/3 is exactly one round trip faster at 50 ms RTT (210 vs 260 ms), as the protocol promises. With loss the two are close; the rare multi-second connects (5 s and 10 s maxima in both) are lost first packets waiting for a retransmission timer.
- **Throughput on a clean network:** HTTP/3 is far behind (8 vs 120+ MB/s). Some of this is the Python client (aioquic does the QUIC work in Python, h2 uses OpenSSL), so the gap is not all the server's, but the server also pays for per-packet user-space crypto and one syscall per datagram.
- **Under loss: HTTP/3 did not win here, it lost.** At 1% loss h2 moved roughly twice as much as h3; at 3% the gap shrank to about 30%. h2 numbers match the TCP throughput estimate for this RTT and loss (about 0.3-0.5 MB/s at 1%), i.e. the kernel TCP behaves as theory says. The Hydrangea QUIC
  sender recovers worse than Linux TCP (NewReno without SACK-like tuning, 1200-byte packets instead of 1448-byte segments, PTO-driven recovery, no pacing), which dominates over the head-of-line-blocking advantage QUIC is supposed to bring.
- **No visible head-of-line benefit in this test**, and this test is not built to show it: all ten streams are the same size and start together, so they finish together under either protocol. A fair HOL test needs streams of different sizes or a small request racing a large download.
- The conclusion "HTTP/3 is worse than HTTP/2" holds for this implementation and this client, not for QUIC as a protocol; the loss behaviour in particular points at the sender's loss recovery, which has not been tuned.

## Limits
Two repetitions per cell, one connection per run, Python on both sides, random (not bursty) loss from netem, one RTT, bodies of 100 KB, no pacing, no real mobile paths. Nothing here measures connection migration.
