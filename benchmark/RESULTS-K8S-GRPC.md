# gRPC on Kubernetes at 0.5 CPU: Hydrangea vs grpc-java

Both servers are `GrpcBench server` (benchmark/src/grpc/GrpcBench.java) with 1 I/O thread, heap 256 MB, requests = limits = 500m CPU, one running at a time
(`eclipse-temurin:21-jre`; grpc-java 1.81.0 netty-shaded, `directExecutor`, jars fetched from Maven Central by an init container).
The load is the same for both: `h2.CapLoad` (Hydrangea client, closed loop, every reply checked) in a separate Job pod (2 CPU limit), 5 s warm-up + 15 s measured.
3 rounds, order alternating per round (hydrangea first, java first, hydrangea first). 0 errors in all 24 runs. Raw logs: `k8s-results/grpc/`.
Latency in microseconds; p99 = 40 000 means "at or above 40 ms" (histogram cap).

| unary echo | load | Hydrangea req/s (r1/r2/r3) | grpc-java req/s (r1/r2/r3) | ratio | p50 H / J | p99 H / J |
|---|---|---:|---:|---:|---:|---:|
| 16 B | 16 in flight, 2 conns | 65 661 / 66 667 / 63 151 | 26 195 / 23 836 / 23 275 | 2.6x | 110 / 190-210 | ~500 / ~1 100-1 240 |
| 16 B | 64 in flight, 2 conns | 74 015 / 73 844 / 74 056 | 47 502 / 46 122 / 46 876 | 1.6x | 430 / 620-640 | ~2 100-2 500 / >=40 000 |
| 16 B | 256 in flight, 4 conns | 73 671 / 72 708 / 72 947 | 41 494 / 42 282 / 43 080 | 1.7x | 2 200-2 500 / ~3 100 | ~15 000-23 000 / >=40 000 |
| 4 KiB | 64 in flight, 2 conns | 24 093 / 24 220 / 22 718 | 23 212 / 23 013 / 23 652 | 1.0x | ~1 650-1 750 / ~1 700 | ~12 400-13 300 / ~12 400-13 000 |

Reading it honestly:
- Small calls: Hydrangea does 1.6-2.6x the requests per second of grpc-java at the same 0.5 CPU. The CPU is the limit, so this is mostly CPU cost per call
  (Hydrangea ~ half), made larger by grpc-java's JIT/GC/thread overhead sharing 0.5 CPU; on an 8-core laptop the gap was 1.0-1.2x (README, "Hiệu năng"). Do not
  extrapolate this ratio to bigger CPU limits.
- 4 KiB calls: the same within noise; copying bytes dominates for both.
- grpc-java is run with one event loop and `directExecutor`, the configuration the benchmark has always used (it favours grpc-java); the default executor would
  likely be slower on 0.5 CPU. Its worse p99 at 64-256 in flight comes from CFS throttling hitting its extra threads.
- The same Hydrangea server measured 62-65k req/s in `RESULTS-K8S-CAP.md` and 74k here: that earlier server (SoakServer) also serves HTTP routes and health
  reporting, and ran on a possibly different node; compare numbers only within one table.
- One node, no TLS, echo handler, Hydrangea client for both. Not a statement about grpc-java with a tuned executor or more CPU.
