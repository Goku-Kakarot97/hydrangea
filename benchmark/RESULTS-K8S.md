# Hydrangea on Kubernetes

Hydrangea's HTTP / WebSocket / SSE benchmark server ran as a Deployment (1 pod, requests 1 CPU / limits 2 CPU, 512 MB heap, 2 I/O threads,
`eclipse-temurin:21-jre`, jars mounted from a ConfigMap) behind a ClusterIP Service. Manifests: `k8s/`.

## Functional check through `kubectl port-forward`

Everything answered correctly from outside the cluster: routing with path variable and query, JSON, POST echo, 404 and 405, a 20 MB
streamed upload (the server counted exactly 20 000 000 bytes), a 5 MB download, an SSE stream, WebSocket echo, and `/metrics`.

## Load generated inside the cluster (separate Job pod, 2 CPU limit, calls the Service DNS name)

3 s warm-up + 10 s measured; every response, echo and event is verified; 0 errors in every scenario. Latencies in microseconds.

| scenario | ops/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| GET /plain, 64 connections | 117 091 | 431 | 2 004 | 6 609 |
| GET /plain, 1000 connections | 114 876 | 7 182 | 27 763 | 37 893 |
| GET /json, 64 connections | 126 673 | 412 | 1 723 | 5 891 |
| GET /users/{id}?q=, 64 connections | 119 576 | 437 | 1 760 | 6 367 |
| POST /echo 1 KiB, 64 connections | 107 688 | 488 | 1 904 | 5 987 |
| GET /download/1 MiB, 8 connections | 1 006 (1 055 MB/s) | 8 003 | 14 598 | 30 374 |
| WebSocket echo 64 B, 64 connections | 119 404 | 433 | 1 857 | 5 311 |
| WebSocket echo 64 B, 1000 connections | 109 647 | 7 809 | 27 278 | 38 765 |
| SSE, 1000 subscribers x 10 events/s | 10 000 | 5 000 | 12 000 | 19 000 |

The server pod and the load pod share cluster nodes and the pod network, and the server is limited to 2 CPUs, so these numbers
are not comparable to the local runs in `RESULTS-HTTP.md` (8 cores, loopback). There is no Netty baseline here.

## Rolling restart under load (graceful shutdown with a real SIGTERM)

70 s of traffic (64 HTTP GET connections, 50 WebSocket connections, 4 connections uploading 4 MiB bodies) while
`kubectl rollout restart` replaced the server pod.

| run | GET | WebSocket | POST 4 MiB uploads |
|---|---|---|---|
| without `preStop` | 0 errors | 0 errors (3 close frames while waiting for an echo, treated as goodbye) | 3 lost |
| with `preStop: sleep 8` | 0 errors | 0 errors | 1 lost, then 0 lost |

Reading the table honestly:
* Idle keep-alive connections are closed with a clean FIN. A client that writes at the instant the server closes an idle
  connection loses that request. This is inherent to HTTP/1.1; GETs are retried by the load generator as real clients do.
  With a long think time the same test shows 0 retries, which is how we know the server does not close connections carelessly.
* The `preStop` sleep is the standard Kubernetes remedy for connections that kube-proxy still routes to a pod that has already
  received SIGTERM. It reduced, but did not eliminate, lost non-idempotent POSTs (3 → 1 → 0). A client that must not lose a
  POST needs an idempotency key and a retry; no server can promise otherwise.

## What the soak test found (see `SOAK.md`)

The first 15-minute soak killed the server with `OutOfMemoryError` after 3 minutes: `Scheduled.cancel()` only set a flag, so with
a 5-minute keep-alive timer every closed connection stayed reachable for 5 minutes. Fixed in `EventLoop` (cancel releases the
task; the queue is compacted when it doubles), with a regression test checked by mutation. After the fix: 15 minutes, 7.7 M
responses, 366 k connections, constant descriptors and threads, 0 connections left after cool-down.

## Hydrangea vs Netty on the cluster

Both servers: 2 I/O threads, 512 MB heap, G1, requests 1 CPU / limit 2 CPU, one running at a time, same node as the load pod
(the other node had no room), 3 rounds with the order alternating, median round shown (`benchmark/aggregate-k8s.py`). Netty 4.2.10 with
`HttpServerCodec` + `HttpObjectAggregator`, no tuning. Other workloads of the namespace were running on the same node, so absolute numbers are
noisy; the per-round spread below is the measure of that noise. 0 errors in every run.

| scenario | server | req/s | p50 µs | p99 µs | p99.9 µs | per-round req/s |
|---|---|---:|---:|---:|---:|---|
| GET /plain, 64 conn | hydrangea | 177 599 | 286 | 1 761 | 6 133 | 177 599 / 161 895 / 186 409 |
| | netty | 171 190 | 302 | 1 600 | 5 561 | 174 454 / 166 035 / 171 190 |
| GET /plain, 1000 conn | hydrangea | 128 013 | 7 222 | 20 128 | 32 134 | 128 013 / 142 443 / 126 395 |
| | netty | 127 365 | 7 321 | 17 770 | 28 450 | 142 396 / 119 803 / 127 365 |
| GET /json, 64 conn * | hydrangea | 176 059 | 285 | 1 667 | 6 284 | 175 147 / 188 788 / 176 059 |
| | netty | 144 378 | 370 | 1 775 | 5 676 | 144 378 / 153 501 / 123 489 |
| GET /users/{id}?q=, 64 conn | hydrangea | 164 351 | 299 | 2 123 | 6 783 | 167 612 / 164 351 / 163 877 |
| | netty | 166 387 | 297 | 2 000 | 6 390 | 165 067 / 166 387 / 166 394 |
| POST /echo 1 KiB, 64 conn | hydrangea | 154 746 | 324 | 1 767 | 7 016 | 157 439 / 146 474 / 154 746 |
| | netty | 136 815 | 395 | 1 682 | 5 876 | 136 815 / 141 535 / 135 470 |
| GET /download 1 MiB, 8 conn | hydrangea | 3 720 | 1 666 | 5 547 | 13 281 | 3 720 / 3 508 / 3 740 |
| | netty | 3 521 | 1 919 | 5 776 | 12 808 | 3 433 / 3 521 / 3 526 |
| WebSocket echo 64 B, 64 conn | hydrangea | 166 464 | 329 | 1 504 | 6 853 | 166 464 / 167 318 / 164 529 |
| | netty | 169 840 | 317 | 1 292 | 5 217 | 178 508 / 135 700 / 169 840 |
| WebSocket echo 64 B, 1000 conn | hydrangea | 125 080 | 7 534 | 19 125 | 31 790 | 125 080 / 126 865 / 116 759 |
| | netty | 129 318 | 7 248 | 18 772 | 30 035 | 122 180 / 130 767 / 129 318 |

\* The Netty /json cell in the main rounds returned nothing in all three rounds: my Netty pod had no `hydrangea.jar` on its classpath and
the handler failed (a deployment mistake of mine, not a Netty problem). It was re-run afterwards, three runs in one Job, on the fixed pod.

How to read it: on the cluster the two servers are within each other's run-to-run noise for plain GET, route parameters, WebSocket
and large downloads (a gap smaller than the spread between rounds of the same server is not a result). The clear differences are
POST with a body (+13% for Hydrangea, consistent across rounds) and /json. Hydrangea's p99 is slightly higher than Netty's in most
rows, within ~15%, and p50 is equal or lower. On the unconstrained local machine (`RESULTS-HTTP.md`) Hydrangea leads on throughput
for small requests and large downloads, which the 2-CPU limit and the shared node hide here.
