# Hydrangea vs Undertow on Kubernetes at 0.5 CPU

Both servers: 1 I/O thread, heap 256 MB, requests = limits = 500m CPU, `eclipse-temurin:21-jre`, one running at a time, same two routes
(`GET /plain` -> "Hello, World!", `POST /echo` -> the body), handlers non-blocking on the I/O thread. Hydrangea `HydrangeaHttpServer -Dh2=true`;
Undertow 2.3.18.Final (`UndertowBenchServer`, `ENABLE_HTTP2`, jars from Maven Central via an init container), both with default settings otherwise.
Load: a separate Job pod (2 CPU) with load generators that depend on neither server (`bench.HttpLoad` plain NIO for HTTP/1.1, `h2.CapLoad` for h2c with prior
knowledge), closed loop, every reply checked, 5 s warm-up + 15 s measured, 3 rounds with the order alternating (logs: `k8s-results/http/`). 0 errors in all 42 runs.
The first attempt of this experiment failed because a jar list lacked a trailing newline (Undertow crashed with NoClassDefFoundError); the numbers below are from the clean rerun.

| scenario | Hydrangea req/s (r1/r2/r3) | Undertow req/s (r1/r2/r3) | Hydrangea / Undertow |
|---|---:|---:|---:|
| HTTP/1.1 GET, 16 conns | 47 110 / 47 937 / 49 478 | 49 582 / 46 674 / 46 193 | ~1.00 |
| HTTP/1.1 GET, 64 conns | 52 518 / 51 277 / 52 578 | 49 399 / 48 901 / 49 860 | ~1.05 |
| HTTP/1.1 GET, 256 conns | 41 778 / 43 071 / 45 591 | 43 080 / 42 285 / 44 051 | ~1.00 |
| HTTP/1.1 POST 1 KiB echo, 64 conns | 43 180 / 43 556 / 43 297 | 39 890 / 39 518 / 44 240 | ~1.04 |
| h2c GET, 64 streams on 2 conns | 150 262 / 144 350 / 150 003 | 42 356 / 41 160 / 40 872 | ~3.6 |
| h2c GET, 256 streams on 4 conns | 155 184 / 156 369 / 152 649 | 44 044 / 44 763 / 45 868 | ~3.5 |
| h2c POST 1 KiB echo, 64 streams | 102 568 / 101 931 / 102 867 | 12 798 / 12 819 / 12 774 | ~8.0 |

Reading it honestly:
- HTTP/1.1: a tie within noise (differences of 0-5% go either way between rounds). Both are limited by the same thing here, CPU spent per request on one thread. The ~49 ms p99
  at 64-256 connections in both columns is the CFS throttling of a 0.5 CPU quota, not queueing.
- h2c: Undertow is 3.5-8x slower in this setup. Part of that is probably configuration, not just implementation: I used Undertow's defaults (e.g. the 64 KiB initial
  stream window, which throttles a 1 KiB echo loop with a per-request round trip), and nobody tuned it for h2c on one I/O thread. A tuned Undertow would narrow the gap; I did not try.
  Undertow's HTTP/2 is mainly meant to be used behind TLS/ALPN, which this run does not exercise.
- Not measured: TLS, WebSocket/SSE, larger bodies, servlet or blocking handlers, more than 0.5 CPU, Undertow with `dispatch()` to its worker pool (the usual style for
  application code), memory.

## Second run: Undertow tuned, and Undertow the way application code uses it (logs: `k8s-results/http-tuned/`)

Same pod limits, load and method; 3 implementations rotated each round (9 runs of the whole scenario list, 0 errors). Averages of 3 rounds, requests per second:

| scenario | Hydrangea | Undertow tuned (handler on the I/O thread) | Undertow tuned + `dispatch()` to the worker pool |
|---|---:|---:|---:|
| HTTP/1.1 GET, 16 conns | 46 988 | 47 337 | 28 559 |
| HTTP/1.1 GET, 64 conns | 51 234 | 48 394 | 26 922 |
| HTTP/1.1 GET, 256 conns | 44 680 | 40 263 | 24 453 |
| HTTP/1.1 POST 1 KiB echo, 64 conns | 44 092 | 41 033 | 20 610 |
| h2c GET, 64 streams on 2 conns | 145 202 | 42 943 | 25 286 |
| h2c GET, 256 streams on 4 conns | 157 226 | 44 828 | 28 168 |
| h2c POST 1 KiB echo, 64 streams | 104 962 | 12 873 | 11 547 |

"Tuned" (`UndertowBenchServer h2,tuned`): 100 concurrent streams, push off, 16 KiB direct buffers, TCP_NODELAY, no request timeout. It changes almost nothing against the defaults of the first run
(HTTP/1.1 within a few percent, h2c the same), which says the first run was not a misconfiguration artefact for those settings. "Dispatch" runs the handler on Undertow's worker pool (default 8 threads
per I/O thread), which is how servlet and most application code is written; at 0.5 CPU the hand-off between threads costs about half the throughput, and Hydrangea has no such hand-off because handlers run on its loop.

- HTTP/1.1, handler on the I/O thread: Hydrangea is 0-11% ahead (largest at 256 connections and POST), a tie at 16 connections. Against the dispatching Undertow, Hydrangea is 1.7-2.1x ahead, but that compares
  two different threading models, not two parsers.
- h2c: Undertow stays 3.4x (GET) and 8x (echo) slower after tuning. One setting I could not use: `HTTP2_SETTINGS_INITIAL_WINDOW_SIZE` = 1 MiB. With it, Undertow 2.3.18 never sends a connection-level
  WINDOW_UPDATE (a raw-frame client received 0 credit after 100 POSTs of 1 KiB, against 101 376 bytes of credit with the default) and every connection stalls after 64 KiB of request bodies; with the
  default window the echo is bound by round trips on that 64 KiB window (p50 2.4 ms), which explains the 12.8k/s. So for concurrent uploads the gap comes from a window Undertow does not let you enlarge.
  The GET gap (no request body, no window involved) is plain per-request CPU cost at 0.5 CPU.
- Not covered, still: TLS (where Undertow's HTTP/2 is normally used), WebSocket/SSE, servlet API, other CPU limits, memory, anything an experienced Undertow tuner would do beyond the options above (for example
  a different XNIO worker layout or its own buffer pool sizing).
