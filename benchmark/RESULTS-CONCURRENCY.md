# HttpClient: event loop vs blocking I/O under many concurrent callers

Backend: JDK `HttpServer` on virtual threads, 20 ms fixed delay (`src/bench/SlowServer.java`), separate JVM, loopback.
Client (`src/bench/ConcBench.java`, fresh JVM per run, 8 s measured after 3 s warm-up, median of 3 interleaved runs, macOS, Java 25):

- `hy-async`: Hydrangea `HttpClient`, callers are future chains (no thread per caller), 2 event-loop threads
- `hy-block`: Hydrangea `HttpClient`, one platform thread per caller doing `send().join()`
- `jdk-plat` / `jdk-virt`: `java.net.http.HttpClient` (HTTP/1.1), blocking `send`, platform / virtual thread per caller; the connection cap is a fair `Semaphore`

Latency includes time queued for a connection. CPU = process CPU ms per request. `cap` = max connections to the host.

| callers | cap | mode | req/s | p50 ms | p99 ms | CPU ms/req | peak threads | RSS MB |
|---|---|---|---|---|---|---|---|---|
| 100 | 32 | hy-async | 1289 | 75 | 104 | 0.125 | 17 | 115 |
| 100 | 32 | hy-block | 1280 | 75 | 105 | 0.133 | 117 | 102 |
| 100 | 32 | jdk-plat | 1268 | 76 | 104 | 0.440 | 150 | 218 |
| 100 | 32 | jdk-virt | 1106 | 88 | 122 | 0.397 | 99 | 196 |
| 1000 | 32 | hy-async | 1152 | 871 | 940 | 0.112 | 17 | 102 |
| 1000 | 32 | hy-block | 1124 | 892 | 942 | 0.243 | 1017 | 162 |
| 1000 | 32 | jdk-plat | 1137 | 884 | 934 | 1.080 | 1046 | 310 |
| 1000 | 32 | jdk-virt | 1108 | 897 | 967 | 0.491 | 98 | 213 |
| 10000 | 32 | hy-async | 1144 | 7043 | 8742 | 0.097 | 17 | 121 |
| 10000 | 32 | hy-block | OutOfMemoryError after 4056 threads | | | | | |
| 10000 | 32 | jdk-plat | OutOfMemoryError after 4042 threads | | | | | |
| 10000 | 32 | jdk-virt | 1120 | 7026 | 9006 | 0.421 | 96 | 261 |
| 1000 | 1000 | hy-async | 41525 | 23.6 | 39 | 0.016 | 17 | 273 |
| 1000 | 1000 | hy-block | 32700 | 29.5 | 60 | 0.088 | 1017 | 303 |
| 1000 | 1000 | jdk-plat | 8710 | 85.7 | 426 | 0.438 | 1117 | 355 |
| 1000 | 1000 | jdk-virt | 22921 | 41.1 | 84 | 0.143 | 221 | 711 |

Rerun of the last four rows (3 interleaved runs, run 1 shown) after fixing the test backend: the earlier 39/303 errors in the
cap=1000 rows ("connection closed before the response was complete") came from the JDK test server, which closes idle connections
beyond `sun.net.httpserver.maxIdleConnections` (default 200). With `-Dsun.net.httpserver.maxIdleConnections=100000` there are 0 errors.
The earlier cap=1000 table rows were replaced by these.

Approximate context switches per request (involuntary, whole process run incl. warm-up, `/usr/bin/time -l`): hy-async 0.4,
hy-block 2.2, jdk-virt 6.7, jdk-plat 7.1.

JFR (hy-async, 1000 connections): 43% of CPU samples are in `HostPool.acquire`, iterating all connections in `all` to look for an
HTTP/2 connection with a free stream, on every request, under the pool lock. Irrelevant at 32 connections, a throughput limit at 1000.

Caveats: single machine, backend and client share CPUs; the OOM at 10000
platform threads is the macOS per-user thread limit, not heap. Linux not measured.

## h2c (HTTP/2 over cleartext, upgrade)

Backend: Hydrangea `HttpServer` with HTTP/2 (`src/bench/H2Server.java`, 4 loop threads, 20 ms delay via a timer, does not block the loop;
server tally confirmed every request arrived as HTTP/2). Clients (`src/bench/ConcBench2.java`) upgrade with `Upgrade: h2c` (Hydrangea:
`Http2ClientConfig(..., cleartextUpgrade=true)`; JDK: `Version.HTTP_2`, response version printed as HTTP_2). One warm-up request before load.
Hydrangea `cap` = 32 connections, client `maxStreamsPerConnection` = server max concurrent streams = `streams`. 3 interleaved runs, median.
The JDK client uses one connection per host and no extra cap.

| callers | streams | mode | req/s | p50 ms | p99 ms | CPU ms/req | peak threads | RSS MB | errors |
|---|---|---|---|---|---|---|---|---|---|
| 100 | 10000 | hy-async | 4049 | 25 | 29 | 0.029 | 10 | 124 | 0 |
| 100 | 10000 | hy-block | 4042 | 24 | 34 | 0.041 | 110 | 97 | 0 |
| 100 | 10000 | jdk-plat | 3999 | 24 | 58 | 0.105 | 110 | 226 | 0 |
| 100 | 10000 | jdk-virt | 3991 | 25 | 34 | 0.094 | 19 | 243 | 0 |
| 1000 | 10000 | hy-async | 36834 | 27 | 40 | 0.006 | 10 | 231 | 0 |
| 1000 | 10000 | hy-block | 31324 | 31 | 53 | 0.068 | 1010 | 265 | 0 |
| 1000 | 10000 | jdk-plat | 30558 | 30 | 66 | 0.120 | 1010 | 792 | 0 |
| 1000 | 10000 | jdk-virt | 39639 | 23 | 82 | 0.065 | 19 | 1265 | 0 |
| 10000 | 10000 | hy-async | 249900 | 36 | 113 | 0.004 | 10 | 972 | 0 |
| 10000 | 10000 | hy-block | OutOfMemoryError after 4062 threads | | | | | | |
| 10000 | 10000 | jdk-plat | OutOfMemoryError after 4063 threads | | | | | | |
| 10000 | 10000 | jdk-virt | 50578 | 176 | 591 | 0.067 | 19 | 1561 | 0 |
| 1000 | 100 | hy-async | 40428 | 24 | 44 | 0.007 | 15 | 280 | 0 |
| 1000 | 100 | hy-block | 38308 | 25 | 45 | 0.087 | 1014 | 356 | 0 |
| 1000 | 100 | jdk-plat | 4125 | 25 | 81 | 0.923 | 1010 | 537 | 930427 |
| 1000 | 100 | jdk-virt | 4064 | 26 | 48 | 1.004 | 19 | 721 | 1559163 |

- With the server's limit at 100 concurrent streams the JDK client does not queue: it fails the excess requests at once
  (`IOException: too many concurrent streams`), so the 4 k req/s are the successful requests of its 100 streams and the errors are
  fast failures; the CPU/req of those rows is not comparable. Hydrangea opens more connections (up to `cap`) and keeps going.
- hy-async at 10000 callers varied between 162 k and 250 k req/s across runs (server saw 2.04 M requests in one run, consistent with
  the client's count); client and server share the machine's CPUs, so this is not a clean client-only limit.
- The backend is Hydrangea's own server, which may favour the Hydrangea client; JDK vs Hydrangea was not tested against a third-party server.

## `HostPool.acquire` no longer scans every connection (HTTP/1.1)

The JFR of `hy-async` at 1000 connections showed 43% of the CPU samples in `HashMap$HashIterator.nextNode`: every request walked all the connections of the host looking for an
HTTP/2 connection with a free stream, which on an HTTP/1.1 host never finds one. `HostPool` now keeps a separate list of the connections that speak HTTP/2 (`markH2`, set where a
connection becomes HTTP/2, removed when it closes) and searches only that list, so an HTTP/1.1 host does no search at all. Behaviour is otherwise the same (411 tests of `hydrangea-http` pass).

Measured in one session against the previous code (HTTP/1.1, 1000 callers, 1000 connections, `ConcBench hy-async`, 4-5 interleaved runs each):

| backend delay | | req/s (runs) | p99 ms | client CPU µs/request |
|---|---|---|---|---|
| 20 ms | before | 40831 (37424-42031) | 62.0 | 18.5 |
| 20 ms | after | 40519 (33986-41947) | 46.6 | 14.5 |
| 1 ms | before | 53505 (33469-68238) | 94.1 | 23.0 |
| 1 ms | after | 49254 (27097-75009) | 149.5 | 18.0 |

* **CPU per request fell 22% in both settings** (18.5 to 14.5, 23.0 to 18.0 µs); at 100 connections it did not change (30.0 and 29.5 µs), as expected for a cost that grows with the number of connections.
* **Throughput did not change measurably.** With a 20 ms backend the rate is set by the delay (1000 callers / about 24 ms), and with a 1 ms backend the runs vary by a factor of two (the JDK test server and the
  client share the machine), so neither can show a gain in requests per second. The p99 of the 1 ms runs is worse after, inside that noise.
* A new JFR of the code after the change has no `HashMap$HashIterator.nextNode` among the top frames (it was 214 of 501 samples); the first is `HttpClientStage.parseHead`.
