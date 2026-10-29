# Re-run of the HTTP, HTTP/2 and gRPC benchmarks (2026-10-26)

Why: since the baselines (20-21 Oct) the repository was split into Maven modules, `SocketConnection` gained `whenWritable()` (a waiter list that is only touched when writability
flips or the connection closes), `Client.connect(EventLoop, ...)` was added, and earlier the gather-write corruption fix (a batch now ends at the first heap buffer over 256 KiB) went in.
None of it is on the request path of these benchmarks except the flush loop's extra check, so the expectation was "no change". This is the measurement of that expectation.

Same machine (Mac, aarch64, 8 cores, JDK 25.0.1), same scripts and parameters as the baselines: `benchmark/run-http.sh 3 3 5 4 4`, `benchmark/run-h2.sh 12 4 4`, `benchmark/run-grpc.sh 15 4`;
raw output `benchmark/RESULTS-HTTP-rerun.md`, `rerun-http.log`, `rerun-h2.log`, `rerun-grpc.log`. Baselines: `RESULTS-HTTP-final.md`, `h2-matrix.log`, `grpc-matrix.log`.

## Reading it

* **The machine was 3-8 % slower today for everyone**: Netty and the JDK server dropped by the same amount as Hydrangea in every HTTP/1.1 row. So compare the **ratio to the competitor in the same run**
  (the "hydrangea/netty" columns), not the absolute numbers across days. HTTP/1.1 and WebSocket ratios are equal or slightly better (1.04-1.12 before, 1.08-1.12 now). HTTP/2 ratios are mixed and
  within about 7 %: 1 MiB download 1.78 -> 1.66, 1 KiB echo 1.28 -> 1.22, plain 16x64 1.00 -> 0.98, plain 4x16 0.99 -> 1.11.
* This is one run of each matrix (HTTP: 3 rounds, median), on a machine that also runs an IDE and Docker: differences under about 5-8 % between two runs are noise here.
* I cannot attribute a difference to one change: all the changes above are in the "now" side together. What the data does show is that nothing regressed beyond that noise.

## HTTP/1.1, WebSocket (requests or messages per second)

| scenario | hydrangea before | hydrangea now | now/before | netty before | netty now | now/before | hydrangea/netty before | hydrangea/netty now |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| GET /plain (13 B), 64 connections | 202,268 | 197,081 | 0.97 | 187,101 | 182,377 | 0.97 | 1.08 | 1.08 |
| GET /plain (13 B), 1000 connections | 198,951 | 193,406 | 0.97 | 188,017 | 179,000 | 0.95 | 1.06 | 1.08 |
| GET /json (180 B), 64 connections | 201,070 | 194,328 | 0.97 | 180,207 | 172,818 | 0.96 | 1.12 | 1.12 |
| GET /users/{id}?q= (path variable + query), 64 connections | 196,357 | 192,719 | 0.98 | 188,437 | 172,811 | 0.92 | 1.04 | 1.12 |
| POST /echo 1 KiB, 64 connections | 193,613 | 185,754 | 0.96 | 174,605 | 166,675 | 0.95 | 1.11 | 1.11 |
| WebSocket echo 64 B, 64 connections | 201,864 | 197,176 | 0.98 | 186,073 | 178,602 | 0.96 | 1.08 | 1.10 |
| WebSocket echo 64 B, 1000 connections | 200,213 | 195,280 | 0.98 | 184,887 | 173,988 | 0.94 | 1.08 | 1.12 |

Two scenarios the baseline file does not have (it is a subset): `GET /download/1 MiB` x 8 connections now 9,067 MB/s for Hydrangea against 6,873 for Netty (1.32x, CPU 392 vs 493 us/req), and SSE with 1000
subscribers delivered its 10,000 events/s at 13 us/op CPU with p99 39 ms; I have no same-day older figure to compare those with.

### HTTP/2 (cleartext, prior knowledge), requests/s

| scenario | hydrangea before | now | now/before | netty before | now | now/before | hydrangea/netty before | now | hydrangea server CPU us/req before -> now |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| download1m 4 connections x 2 streams | 3,807 | 3,694 | 0.97 | 2,140 | 2,223 | 1.04 | 1.78 | 1.66 | 847.0 -> 894.7 |
| echo1k 4 connections x 16 streams | 353,452 | 341,950 | 0.97 | 275,547 | 281,060 | 1.02 | 1.28 | 1.22 | 3.6 -> 3.6 |
| plain 16 connections x 64 streams | 541,064 | 508,724 | 0.94 | 542,713 | 521,619 | 0.96 | 1.00 | 0.98 | 3.1 -> 3.2 |
| plain 4 connections x 16 streams | 380,844 | 404,580 | 1.06 | 385,235 | 366,086 | 0.95 | 0.99 | 1.11 | 3.2 -> 3.3 |

### gRPC, requests/s (server under test; the load generator is named in the last column of the key)

| scenario, inflight/channels, load generator | hydrangea server before | now | now/before | grpc-java server before | now | now/before | hydrangea/grpc-java before | now |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| stream-100x1k, 1/1, java client | 5,694 | 5,709 | 1.00 | 3,402 | 3,414 | 1.00 | 1.67 | 1.67 |
| stream-100x1k, 64/4, java client | 11,912 | 12,851 | 1.08 | 7,376 | 7,618 | 1.03 | 1.61 | 1.69 |
| unary-16, 1/1, java client | 29,707 | 28,725 | 0.97 | 21,314 | 25,688 | 1.21 | 1.39 | 1.12 |
| unary-16, 512/8, java client | 321,765 | 378,339 | 1.18 | 279,983 | 303,649 | 1.08 | 1.15 | 1.25 |
| unary-16, 64/4, java client | 276,723 | 281,208 | 1.02 | 249,061 | 254,805 | 1.02 | 1.11 | 1.10 |
| unary-4k, 1/1, java client | 26,287 | 26,874 | 1.02 | 23,974 | 24,158 | 1.01 | 1.10 | 1.11 |
| unary-4k, 512/8, java client | 174,008 | 189,141 | 1.09 | 142,182 | 155,245 | 1.09 | 1.22 | 1.22 |
| unary-4k, 64/4, java client | 178,329 | 185,906 | 1.04 | 161,881 | 166,844 | 1.03 | 1.10 | 1.11 |
| unary-64k, 1/1, java client | 10,395 | 10,540 | 1.01 | 10,472 | 10,350 | 0.99 | 0.99 | 1.02 |
| unary-64k, 512/8, java client | 18,590 | 20,877 | 1.12 | 20,197 | 20,884 | 1.03 | 0.92 | 1.00 |
| unary-64k, 64/4, java client | 19,790 | 21,673 | 1.10 | 20,980 | 21,785 | 1.04 | 0.94 | 0.99 |
| stream-100x1k, 1/1, hydrangea client | 7,643 | 8,051 | 1.05 | 3,982 | 4,165 | 1.05 | 1.92 | 1.93 |
| stream-100x1k, 64/4, hydrangea client | 16,890 | 17,960 | 1.06 | 8,035 | 8,382 | 1.04 | 2.10 | 2.14 |
| unary-16, 1/1, hydrangea client | 28,158 | 36,051 | 1.28 | 25,856 | 31,168 | 1.21 | 1.09 | 1.16 |
| unary-16, 512/8, hydrangea client | 148,940 | 502,818 | 3.38 | 154,750 | 356,143 | 2.30 | 0.96 | 1.41 |
| unary-16, 64/4, hydrangea client | 141,215 | 377,150 | 2.67 | 134,980 | 295,659 | 2.19 | 1.05 | 1.28 |
| unary-4k, 1/1, hydrangea client | 25,311 | 33,255 | 1.31 | 23,716 | 29,726 | 1.25 | 1.07 | 1.12 |
| unary-4k, 512/8, hydrangea client | 122,173 | 208,250 | 1.70 | 112,509 | 161,171 | 1.43 | 1.09 | 1.29 |
| unary-4k, 64/4, hydrangea client | 114,598 | 224,241 | 1.96 | 112,222 | 191,411 | 1.71 | 1.02 | 1.17 |
| unary-64k, 1/1, hydrangea client | 9,134 | 10,248 | 1.12 | 8,831 | 10,423 | 1.18 | 1.03 | 0.98 |
| unary-64k, 512/8, hydrangea client | 14,752 | 15,571 | 1.06 | 17,114 | 16,434 | 0.96 | 0.86 | 0.95 |
| unary-64k, 64/4, hydrangea client | 20,413 | 20,375 | 1.00 | 19,289 | 21,199 | 1.10 | 1.06 | 0.96 |

## What the gRPC table shows, and what it does not

* **Server rows with the grpc-java load generator** (`java client`, identical load on both servers): Hydrangea's server moves between 0.97x and 1.18x of its earlier throughput and grpc-java's server between 0.99x and 1.21x
  in the same rows, so neither server got slower; the ratio between them (last two columns) stays at 1.1-1.25x in Hydrangea's favour for small unary calls, about 1.7x for server streaming, and about 1.0x for 64 KiB.
* **Rows with the Hydrangea load generator are not comparable with the baseline**: both servers got 1.2-3.4x faster under it (grpc-java's server too), which means the *client* improved or the baseline file
  was recorded with an older client. I did not investigate which; use the `java client` rows for any server comparison.
* 64 KiB unary is transport-bound (about 20k rps either way); the differences there are noise.
