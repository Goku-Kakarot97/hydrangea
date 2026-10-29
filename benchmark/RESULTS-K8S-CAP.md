# Capacity of one Hydrangea pod limited to 0.5 CPU (Kubernetes)

Server: `h2.SoakServer`, 1 I/O thread, heap 256 MB, requests = limits = 500m CPU, in-cluster Service, same node as the load pod.
Load: separate pod (2 CPU limit), closed loop, every reply checked, 5 s warm-up + 15 s measured, 2 rounds (log: `k8s-results/cap-0.5cpu.log`).
0 errors in all 24 runs. Latencies in microseconds; p99 = 40 000 means "at or above 40 ms" (histogram cap in CapLoad).

| scenario | load | req/s (round 1 / 2) | p50 | p99 |
|---|---|---:|---:|---:|
| HTTP/1.1 GET /plain | 16 conns | 46 224 / 47 566 | 146 | 500 |
| HTTP/1.1 GET /plain | 64 conns | 49 979 / 49 001 | 593 | ~49 000 |
| HTTP/1.1 GET /plain | 256 conns | 43 662 / 41 649 | 2 757 | ~56 000 |
| HTTP/1.1 POST /echo 1 KiB | 64 conns | 39 522 / 40 280 | 743 | ~49 000 |
| HTTP/2 GET /plain | 16 streams, 2 conns | 96 851 / 92 956 | 80 | 440 |
| HTTP/2 GET /plain | 64 streams, 2 conns | 115 080 / 115 698 | 270 | 1 110 |
| HTTP/2 GET /plain | 256 streams, 4 conns | 121 239 / 127 279 | 1 170 | >=40 000 |
| HTTP/2 POST /echo 1 KiB | 64 streams, 2 conns | 80 025 / 82 362 | 450 | 2 040 |
| gRPC unary 16 B | 16 in flight, 2 conns | 55 631 / 56 176 | 130 | 550 |
| gRPC unary 16 B | 64 in flight, 2 conns | 61 864 / 63 054 | 500 | 20 000-33 000 |
| gRPC unary 16 B | 256 in flight, 4 conns | 61 603 / 64 629 | 2 190 | >=40 000 |
| gRPC unary 4 KiB (echoed) | 64 in flight, 2 conns | 27 919 / 27 307 | 1 500 | ~11 800 |

Caveats: the HTTP/1.1 rows use the nio `HttpLoad` client and the HTTP/2 and gRPC rows use the Hydrangea client, so HTTP/1.1 vs HTTP/2 is not an
apples-to-apples comparison (HTTP/2 multiplexes many requests per syscall). The ~50 ms p99 in the HTTP/1.1 rows is CFS throttling at a 0.5 CPU
quota (50 ms per 100 ms period), not queueing. No Netty/grpc-java baseline on k8s; no TLS; one node, shared pod network; handlers do no work.
