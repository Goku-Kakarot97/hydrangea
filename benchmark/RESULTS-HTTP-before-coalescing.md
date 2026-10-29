# Benchmark: HTTP, WebSocket and SSE

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine).
3 rounds, 3s warm-up + 5s measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.

## GET /plain (13 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 190246 | 283 | 1423 | 5590 | 19499 | 12.9 | 375 | 0 |
| netty | 183126 | 290 | 1125 | 3242 | 26967 | 16.2 | 394 | 0 |
| jdk-httpserver | 148046 | 387 | 1160 | 3507 | 23874 | 20.4 | 388 | 0 |

## GET /plain (13 B), 1000 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 189658 | 4456 | 20013 | 42872 | 73567 | 15.0 | 362 | 0 |
| netty | 172549 | 5031 | 21652 | 73769 | 111959 | 16.0 | 386 | 0 |
| jdk-httpserver | 148287 | 1175 | 4180 | 16570 | 76373 | 22.4 | 395 | 0 |

## GET /json (180 B), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 198981 | 296 | 898 | 2172 | 35761 | 14.8 | 364 | 0 |
| netty | 167445 | 311 | 1328 | 4022 | 27574 | 19.2 | 407 | 0 |
| jdk-httpserver | 136522 | 399 | 1432 | 5076 | 61017 | 23.6 | 380 | 0 |

## GET /users/{id}?q= (path variable + query), 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 187385 | 299 | 1114 | 4072 | 45804 | 15.4 | 362 | 0 |
| netty | 179434 | 304 | 1092 | 2423 | 36780 | 16.2 | 386 | 0 |
| jdk-httpserver | 128276 | 419 | 1682 | 7010 | 28431 | 21.0 | 378 | 0 |

## POST /echo 1 KiB, 64 connections

| server | req/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 193252 | 293 | 984 | 2669 | 17322 | 16.3 | 368 | 0 |
| netty | 158239 | 318 | 1518 | 5580 | 33924 | 19.9 | 392 | 0 |
| jdk-httpserver | 117897 | 461 | 1765 | 3433 | 15347 | 23.0 | 380 | 0 |

## GET /download/1 MiB, 8 connections

| server | MB/s | CPU µs/req | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| hydrangea | 9426 | 365.7 | 443 | 0 |
| netty | 6752 | 481.8 | 463 | 0 |
| jdk-httpserver | 5198 | 703.0 | 457 | 0 |

## WebSocket echo 64 B, 64 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 206943 | 288 | 861 | 2010 | 33710 | 14.6 | 450 | 0 |
| netty | 167371 | 308 | 1430 | 3587 | 29652 | 16.1 | 396 | 0 |

## WebSocket echo 64 B, 1000 connections

| server | msg/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 193885 | 4152 | 24203 | 60184 | 150340 | 14.8 | 444 | 0 |
| netty | 176307 | 5169 | 15010 | 36720 | 54492 | 15.9 | 407 | 0 |

## SSE: 1000 subscribers, 10 events/s each

| server | events/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| hydrangea | 9992 | 4000 | 11000 | 13000 | 14000 | 16.5 | 459 | 0 |

