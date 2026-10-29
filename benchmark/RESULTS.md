# Benchmark: Hydrangea vs XNIO

Mac OS X, aarch64, 8 cores; JDK 25.0.1; server 4 I/O threads, load generator 4 threads (same machine);
1 rounds, 1s warmup + 3s measured per scenario; cell = median round by throughput.

## ping-pong 1 conn, 64 B

| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| xnio | 52268 | 18 | 44 | 137 | 19059 | 6.6 | 76 | 0 |
| hydrangea | 53422 | 18 | 41 | 95 | 1859 | 7.0 | 260 | 0 |
| hydrangea+pool | 53940 | 18 | 40 | 79 | 429 | 7.1 | 128 | 0 |

## ping-pong 100 conns, 64 B

| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| xnio | 208859 | 443 | 1458 | 4138 | 22463 | 13.5 | 87 | 0 |
| hydrangea | 209202 | 446 | 1470 | 3902 | 12448 | 14.2 | 210 | 0 |
| hydrangea+pool | 198125 | 421 | 2098 | 8430 | 35182 | 14.5 | 168 | 0 |

## ping-pong 1000 conns, 64 B

| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| xnio | 200815 | 4156 | 18006 | 59242 | 116456 | 14.9 | 176 | 0 |
| hydrangea | 203633 | 4052 | 19994 | 45576 | 69331 | 14.1 | 351 | 0 |
| hydrangea+pool | 204811 | 4074 | 18457 | 47275 | 73185 | 15.2 | 177 | 0 |

## ping-pong 100 conns, 16 KiB

| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| xnio | 168760 | 548 | 1513 | 3278 | 11943 | 15.9 | 178 | 0 |
| hydrangea | 158610 | 585 | 1668 | 4151 | 19899 | 16.9 | 354 | 0 |
| hydrangea+pool | 165706 | 562 | 1522 | 4939 | 39665 | 16.7 | 218 | 0 |

## stream 16 conns (bulk MB/s)

| impl | MB/s | CPU ms per MB | peak RSS MB | errors |
|---|---:|---:|---:|---:|
| xnio | 7050 | 0.26 | 162 | 0 |
| hydrangea | 5565 | 0.44 | 357 | 0 |
| hydrangea+pool | 5756 | 0.40 | 255 | 0 |

## connect churn 50 slots

| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| xnio | 35500 | 596 | 3018 | 9106 | 234476 | 60.1 | 680 | 0 |
| hydrangea | 35940 | 560 | 2438 | 15989 | 231262 | 42.6 | 349 | 0 |
| hydrangea+pool | 39677 | 536 | 1393 | 22257 | 111294 | 43.4 | 348 | 0 |

