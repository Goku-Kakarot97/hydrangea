# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 173509 | 298 | 1373 | 9828 | 77334 | 14.6 | 452 | 0 |
| netty | 174655 | 304 | 1314 | 3617 | 34100 | 15.9 | 392 | 0 |
| jdk-httpserver | 137430 | 406 | 1471 | 4460 | 10731 | 20.5 | 389 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 158720 | 4865 | 30489 | 128942 | 197200 | 14.7 | 369 | 0 |
| netty | 171092 | 5174 | 18405 | 50479 | 118558 | 13.5 | 378 | 0 |
| jdk-httpserver | 120250 | 1329 | 6661 | 16178 | 49067 | 23.7 | 381 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 188988 | 300 | 1142 | 4775 | 17392 | 14.8 | 364 | 0 |
| netty | 163244 | 325 | 1306 | 3644 | 17052 | 18.8 | 402 | 0 |
| jdk-httpserver | 129096 | 409 | 1843 | 5178 | 24203 | 21.5 | 396 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 97079 | 470 | 3344 | 18021 | 141424 | 18.5 | 371 | 0 |
| netty | 174735 | 296 | 1453 | 4397 | 18061 | 15.5 | 401 | 0 |
| jdk-httpserver | 142721 | 403 | 1245 | 2164 | 12568 | 21.4 | 400 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 145808 | 335 | 2075 | 11241 | 87230 | 13.3 | 364 | 0 |
| netty | 156899 | 310 | 1797 | 5400 | 51184 | 16.5 | 386 | 0 |
| jdk-httpserver | 125237 | 434 | 1736 | 4080 | 12848 | 24.6 | 396 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 7386 | 424.7 | 445 | 0 |
| netty | 6807 | 482.7 | 469 | 0 |
| jdk-httpserver | 4375 | 764.0 | 454 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 184297 | 290 | 1469 | 5216 | 35979 | 13.7 | 446 | 0 |
| netty | 183780 | 294 | 1162 | 2658 | 13427 | 16.6 | 488 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 172911 | 4485 | 29230 | 64480 | 153834 | 13.5 | 367 | 0 |
| netty | 172805 | 5253 | 14515 | 26714 | 43642 | 16.4 | 480 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9990 | 4000 | 13000 | 17000 | 17000 | 17.3 | 364 | 0 |

