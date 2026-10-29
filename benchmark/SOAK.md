# Soak test

15 minutes of mixed traffic against the Hydrangea benchmark server (local JVM, 512 MB heap, 4 I/O threads), then a 45 s cool-down with no load.

## Traffic and results

| workload | result |
|---|---|
| HTTP keep-alive, routing + query (64 conns, 5 ms think) | `ops=8038 mbps=0.1 p50=452 p90=1227 p99=4744 p999=15990 max=356725 errors=0 connected=64` |
| HTTP connection per request (8 conns, 20 ms think) | `ops=358 mbps=0.0 p50=293 p90=970 p99=3343 p999=33953 max=258605 errors=0 connected=16` |
| WebSocket echo 200 B (200 conns, 100 ms think) | `ops=1929 mbps=0.4 p50=1137 p90=2561 p99=8805 p999=73573 max=341424 errors=0 connected=200` |
| SSE subscribers (300, reconnect) | `ops=3000 mbps=0.0 p50=2000 p90=4000 p99=11000 p999=96000 max=284000 errors=0 connected=300` |
| streamed upload 1 MiB (4 conns, 200 ms think) | `ops=20 mbps=0.0 p50=1122 p90=2764 p99=9627 p999=79610 max=182821 errors=0 connected=4` |
| download 512 KiB (4 conns, 100 ms think) | `ops=39 mbps=20.3 p50=777 p90=2067 p99=8910 p999=76293 max=214999 errors=0 connected=4` |
| echo 64 KiB (4 conns, 50 ms think) | `ops=76 mbps=5.0 p50=386 p90=1243 p99=7516 p999=50928 max=309873 errors=0 connected=4` |
| chaos clients (vanish mid-request / upload / handshake / stream) | `CHAOS attempts=44129 refused=0` |

## Server health over time

| t (s) | live heap after GC (MB) | heap used (MB) | threads | descriptors | active connections | opened | closed | http responses | max loop block (ms) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0 | 128 | 12 | 598 | 578 | 3251 | 2674 | 58262 | 76 |
| 40 | 0 | 171 | 12 | 598 | 578 | 15325 | 14747 | 313499 | 14 |
| 70 | 0 | 77 | 12 | 600 | 580 | 27490 | 26910 | 568649 | 17 |
| 100 | 0 | 193 | 12 | 597 | 577 | 39698 | 39121 | 824056 | 5 |
| 130 | 0 | 351 | 12 | 597 | 577 | 51833 | 51256 | 1078408 | 157 |
| 160 | 0 | 199 | 12 | 600 | 580 | 64102 | 63522 | 1336088 | 4 |
| 190 | 0 | 243 | 12 | 596 | 576 | 76341 | 75765 | 1592274 | 79 |
| 220 | 0 | 340 | 12 | 599 | 578 | 88530 | 87952 | 1848378 | 32 |
| 250 | 0 | 143 | 12 | 597 | 577 | 100742 | 100165 | 2104709 | 41 |
| 280 | 0 | 137 | 12 | 599 | 579 | 112742 | 112163 | 2356737 | 131 |
| 310 | 0 | 239 | 12 | 597 | 577 | 125038 | 124461 | 2615337 | 8 |
| 340 | 0 | 390 | 12 | 598 | 578 | 137291 | 136712 | 2872108 | 26 |
| 370 | 0 | 277 | 12 | 597 | 577 | 149283 | 148706 | 3122697 | 8 |
| 400 | 0 | 105 | 12 | 598 | 578 | 161611 | 161033 | 3380920 | 7 |
| 430 | 0 | 299 | 12 | 599 | 579 | 173697 | 173118 | 3632645 | 28 |
| 460 | 0 | 129 | 12 | 597 | 577 | 185961 | 185384 | 3891676 | 19 |
| 490 | 0 | 297 | 12 | 597 | 577 | 198086 | 197509 | 4144855 | 3 |
| 520 | 0 | 164 | 12 | 598 | 578 | 210318 | 209736 | 4402385 | 13 |
| 550 | 0 | 235 | 12 | 596 | 576 | 222345 | 221769 | 4653519 | 37 |
| 580 | 0 | 370 | 12 | 599 | 579 | 234554 | 233975 | 4911139 | 6 |
| 610 | 0 | 86 | 12 | 598 | 578 | 246576 | 245998 | 5163503 | 11 |
| 640 | 0 | 139 | 12 | 598 | 578 | 258796 | 258218 | 5423220 | 15 |
| 670 | 0 | 194 | 12 | 599 | 579 | 270931 | 270352 | 5677972 | 13 |
| 700 | 0 | 302 | 12 | 598 | 578 | 283213 | 282635 | 5937161 | 4 |
| 730 | 0 | 91 | 12 | 599 | 579 | 295575 | 294996 | 6197100 | 4 |
| 760 | 0 | 389 | 12 | 598 | 578 | 307753 | 307175 | 6455484 | 7 |
| 790 | 0 | 99 | 12 | 597 | 577 | 320085 | 319508 | 6716822 | 6 |
| 820 | 0 | 342 | 12 | 597 | 577 | 332314 | 331737 | 6973500 | 5 |
| 850 | 0 | 319 | 12 | 599 | 579 | 344690 | 344111 | 7234006 | 5 |
| 880 | 0 | 224 | 12 | 597 | 577 | 357019 | 356442 | 7494809 | 4 |
| 910 | 0 | 166 | 12 | 20 | 0 | 366356 | 366356 | 7692398 | 4 |
| 940 | 0 | 166 | 12 | 20 | 0 | 366356 | 366356 | 7692398 | 0 |

## Verdict

- PASS after cool-down no connection is left (active = 0)
- PASS after cool-down descriptors are back near the start (descriptors: 597 under early load, 598 under late load, 20 after cool-down)
- PASS live heap does not grow (last quarter vs first quarter, +25% allowed) (0 MB -> 0 MB)
- PASS thread count is stable (min 12, max 12)
- PASS descriptors under load do not creep (+10% allowed) (597 -> 598)
- PASS the event loops were never blocked for long (longest block 157 ms)
- PASS verified workloads report no errors (0 errors)
- PASS opened == closed + active at the end (366356 opened, 366356 closed, 0 active)
