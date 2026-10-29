# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 197081 | 305 | 848 | 3080 | 40694 | 15.8 | 452 | 0 |
| netty | 182377 | 296 | 1133 | 3771 | 24611 | 16.8 | 445 | 0 |
| jdk-httpserver | 151857 | 378 | 1171 | 2968 | 11368 | 21.7 | 472 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 193406 | 4430 | 17974 | 43916 | 83752 | 15.1 | 381 | 0 |
| netty | 179000 | 4904 | 17583 | 29172 | 65606 | 17.2 | 470 | 0 |
| jdk-httpserver | 159853 | 1138 | 3301 | 6946 | 68212 | 22.5 | 461 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 194328 | 309 | 915 | 2413 | 29416 | 15.7 | 454 | 0 |
| netty | 172818 | 312 | 1151 | 2366 | 42658 | 18.6 | 400 | 0 |
| jdk-httpserver | 150515 | 374 | 1248 | 2997 | 16177 | 22.4 | 395 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 192719 | 307 | 966 | 2926 | 24673 | 15.3 | 389 | 0 |
| netty | 172811 | 303 | 1367 | 3955 | 48532 | 15.7 | 418 | 0 |
| jdk-httpserver | 149443 | 389 | 1069 | 2320 | 26067 | 21.9 | 397 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 185754 | 312 | 1064 | 3448 | 32577 | 16.5 | 376 | 0 |
| netty | 166675 | 320 | 1243 | 2731 | 18476 | 18.8 | 406 | 0 |
| jdk-httpserver | 138472 | 392 | 1360 | 2623 | 20954 | 28.1 | 406 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 9067 | 392.2 | 450 | 0 |
| netty | 6873 | 492.6 | 481 | 0 |
| jdk-httpserver | 5938 | 706.7 | 472 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 197176 | 300 | 867 | 1841 | 43186 | 16.2 | 452 | 0 |
| netty | 178602 | 302 | 1138 | 2707 | 18135 | 17.8 | 487 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 195280 | 4557 | 16660 | 44097 | 64451 | 15.1 | 453 | 0 |
| netty | 173988 | 5117 | 18540 | 65232 | 111483 | 16.0 | 472 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9992 | 4000 | 39000 | 45000 | 46000 | 13.0 | 452 | 0 |

