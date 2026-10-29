# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 202268 | 304 | 831 | 2045 | 26228 | 16.1 | 457 | 0 |
| netty | 187101 | 300 | 1031 | 2152 | 9357 | 17.4 | 478 | 0 |
| jdk-httpserver | 159941 | 365 | 1068 | 1816 | 6394 | 19.8 | 466 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 198951 | 4440 | 14353 | 29724 | 46457 | 16.5 | 471 | 0 |
| netty | 188017 | 4812 | 14078 | 21194 | 36583 | 16.1 | 487 | 0 |
| jdk-httpserver | 168700 | 1119 | 2935 | 6053 | 15017 | 21.3 | 477 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 201070 | 302 | 846 | 2178 | 47077 | 16.1 | 462 | 0 |
| netty | 180207 | 299 | 1170 | 2039 | 9012 | 17.4 | 488 | 0 |
| jdk-httpserver | 159972 | 367 | 1040 | 2022 | 7630 | 21.8 | 482 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 196357 | 301 | 893 | 2786 | 81823 | 15.4 | 383 | 0 |
| netty | 188437 | 294 | 1012 | 2124 | 16058 | 16.3 | 491 | 0 |
| jdk-httpserver | 155199 | 376 | 1072 | 2008 | 5355 | 22.3 | 484 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 193613 | 312 | 866 | 1923 | 16320 | 16.8 | 463 | 0 |
| netty | 174605 | 306 | 1226 | 2527 | 12039 | 18.2 | 492 | 0 |
| jdk-httpserver | 153239 | 370 | 1112 | 2059 | 11442 | 26.8 | 486 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 9768 | 370.4 | 545 | 0 |
| netty | 7261 | 484.0 | 565 | 0 |
| jdk-httpserver | 6438 | 790.6 | 554 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 201864 | 297 | 894 | 2067 | 31197 | 15.7 | 464 | 0 |
| netty | 186073 | 298 | 1082 | 1857 | 6513 | 16.5 | 564 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 200213 | 4939 | 9994 | 16260 | 31189 | 14.8 | 540 | 0 |
| netty | 184887 | 4869 | 14045 | 19939 | 32644 | 17.0 | 569 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9990 | 5000 | 18000 | 21000 | 22000 | 16.4 | 541 | 0 |

