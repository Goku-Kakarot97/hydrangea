# HTTP/3 on Kubernetes at 0.5 CPU

`Http3Example` (route `GET /plain`, 5-byte body) in `eclipse-temurin:21-jre`, 1 I/O thread, heap 256 MB, 500m CPU limit, ClusterIP UDP service, namespace `iris`
(everything labelled `app.kubernetes.io/part-of=hydrangea-bench`, deleted afterwards). The load is `benchmark/src/h3/h3_load.py`: aioquic (Python), closed loop, N processes x 4
connections x 8 outstanding requests, run in pods of 1 CPU request / 3 CPU limit (a 3-CPU request did not fit the cluster). Logs: `k8s-results/h3/`. Self-signed EC P-256 certificate, not verified by the client.

| run | requests/s |
|---|---:|
| 1 process, 1 connection, 1 outstanding (latency bound) | 2 814 |
| 2 processes (one pod) | 7 220 |
| 4 processes (one pod) | 11 861 |
| 6 processes (one pod) | 11 900 (the pod's 3 CPUs are the limit, not the server: server used 0.31 core) |
| 3 pods x 4 processes, started together | **24 150** (10 855 + 6 642 + 6 630) |

During the 3-pod run the server container used 11.67 CPU-seconds in 25 s (0.47 core of its 0.5 limit; the cgroup counted 44 throttled periods), i.e. the server was saturated:
about 19 microseconds of CPU per request, or roughly 24 thousand requests per second at 0.5 CPU. No client errors were observed in the logs, but the client counts every completed response without checking
the status (the interop test checks content).

## What was found and fixed on the way
- A request datagram produced two datagrams back: the response without an ACK, then an ACK-only packet, because the ACK flag was set after the handler had already sent. Handlers now
  send at the end of the datagram's processing, sharing a packet with the ACK. Datagrams sent per request went from 2.1 to 1.1; on a laptop (loopback, JFR on, same load) the server
  CPU went from 82 to 50 microseconds per request.
- Remaining cost is mostly syscalls: in the JFR profile `sendto` took about as many samples as all Java code together (`recvfrom` a third of that). Java has no `sendmmsg` or UDP GSO, so one datagram
  costs one syscall; this is the floor for the current design.

## How to read it
- Reference points at the same 0.5 CPU (different client, so not an A/B test): https HTTP/1.1 about 27 000 req/s and https h2 about 105 000 req/s (`RESULTS-K8S-TLS-UNDERTOW.md`). HTTP/3 lands next to
  HTTP/1.1 over TLS: one UDP datagram, one syscall pair and one AEAD operation per request, with no coalescing of several requests into one write as h2 does. This was expected, not tuned away.
- The client is Python and was the limit in every single-pod run; a stronger client could change the order of magnitude at the top, and the 24 thousand figure is what this setup could push.
- Not measured: large bodies and upload throughput, many connections (hundreds), handshake rate, loss or latency on the network path, other CPU limits, TLS-resumption or Retry costs.
- The same server code at the same limit with `withRetry(true)` was not run.
