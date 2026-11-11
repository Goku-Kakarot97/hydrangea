# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 185565 | 299 | 1291 | 5307 | 60267 | 14.2 | 365 | 0 |
| netty | 179770 | 300 | 1167 | 2652 | 42712 | 16.5 | 393 | 0 |
| jdk-httpserver | 147689 | 382 | 1352 | 2822 | 16342 | 20.9 | 395 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 180523 | 4674 | 23167 | 49282 | 111597 | 15.0 | 360 | 0 |
| netty | 166487 | 5034 | 23084 | 105795 | 122562 | 17.3 | 398 | 0 |
| jdk-httpserver | 150532 | 1195 | 3988 | 9010 | 31211 | 22.3 | 388 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 180598 | 279 | 1653 | 7122 | 81938 | 13.4 | 359 | 0 |
| netty | 161197 | 328 | 1415 | 3077 | 16005 | 18.9 | 397 | 0 |
| jdk-httpserver | 133368 | 405 | 1827 | 4699 | 19470 | 22.1 | 381 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 169955 | 304 | 1593 | 6230 | 40328 | 14.8 | 361 | 0 |
| netty | 169946 | 304 | 1494 | 4347 | 18702 | 15.8 | 392 | 0 |
| jdk-httpserver | 137496 | 408 | 1474 | 4059 | 17930 | 20.4 | 358 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 176499 | 303 | 1305 | 3358 | 99332 | 16.7 | 366 | 0 |
| netty | 154846 | 333 | 1475 | 3879 | 29060 | 19.0 | 377 | 0 |
| jdk-httpserver | 113582 | 454 | 2189 | 5583 | 13498 | 22.0 | 386 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 8581 | 370.3 | 443 | 0 |
| netty | 6492 | 493.7 | 463 | 0 |
| jdk-httpserver | 5545 | 587.9 | 449 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 199098 | 285 | 1075 | 3285 | 42265 | 14.7 | 448 | 0 |
| netty | 174857 | 305 | 1290 | 3253 | 21703 | 16.9 | 404 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 193558 | 4137 | 24385 | 67190 | 134399 | 13.0 | 369 | 0 |
| netty | 175821 | 5171 | 14784 | 26043 | 53466 | 16.9 | 467 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9990 | 5000 | 13000 | 14000 | 14000 | 18.4 | 383 | 0 |

