# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 173941 | 298 | 1460 | 8447 | 95809 | 14.7 | 372 | 0 |
| netty | 180874 | 289 | 1186 | 4990 | 37916 | 16.8 | 394 | 0 |
| jdk-httpserver | 140620 | 388 | 1503 | 4039 | 14337 | 20.3 | 384 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 172415 | 4602 | 28555 | 77440 | 146500 | 13.6 | 365 | 0 |
| netty | 183952 | 4888 | 14305 | 61553 | 119473 | 16.1 | 396 | 0 |
| jdk-httpserver | 129519 | 1181 | 7514 | 33329 | 102394 | 22.7 | 395 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 194485 | 291 | 1120 | 4044 | 32119 | 14.6 | 366 | 0 |
| netty | 172931 | 319 | 1117 | 1984 | 14764 | 18.2 | 397 | 0 |
| jdk-httpserver | 139586 | 397 | 1426 | 3439 | 27030 | 21.2 | 357 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 179861 | 295 | 1489 | 5479 | 24713 | 13.9 | 362 | 0 |
| netty | 179876 | 297 | 1122 | 3038 | 50618 | 15.8 | 398 | 0 |
| jdk-httpserver | 143123 | 381 | 1459 | 5904 | 61185 | 19.9 | 369 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 182322 | 304 | 1151 | 5044 | 29737 | 16.6 | 366 | 0 |
| netty | 169516 | 314 | 1236 | 2771 | 23837 | 19.2 | 389 | 0 |
| jdk-httpserver | 128970 | 420 | 1669 | 5697 | 20195 | 25.1 | 382 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 9540 | 351.9 | 444 | 0 |
| netty | 6689 | 456.0 | 464 | 0 |
| jdk-httpserver | 5921 | 684.7 | 460 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 195126 | 291 | 1011 | 3274 | 52826 | 15.2 | 451 | 0 |
| netty | 174927 | 306 | 1320 | 3719 | 28113 | 16.8 | 464 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 198249 | 4363 | 17581 | 49965 | 102091 | 15.2 | 445 | 0 |
| netty | 173119 | 4917 | 19605 | 85364 | 151135 | 16.3 | 390 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9993 | 4000 | 11000 | 12000 | 13000 | 17.4 | 377 | 0 |

