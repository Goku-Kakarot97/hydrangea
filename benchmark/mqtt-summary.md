
### Publish QoS 0 (2 producers, 128 B, window 1000, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 546,360 (543,229 / 546,360 / 550,412) | 858,785 (858,785 / 862,228 / 833,929) | 535,256 (524,187 / 536,466 / 535,256) | 0.64 | 1.02 |
| subscriber-received msgs/s | 545,750 (544,191 / 545,750 / 551,350) | 132,462 (120,310 / 132,462 / 135,974) | 103,443 (92,140 / 103,443 / 106,890) | 4.12 | 5.28 |
| client CPU-s per 100k msgs | 0.10 (0.10 / 0.10 / 0.10) | 1.10 (1.10 / 1.10 / 1.10) | 1.50 (1.60 / 1.50 / 1.50) | 0.09 | 0.07 |
| lost (issued - received) | 0 (0 / 0 / 0) | 7,840,780 (7,920,139 / 7,840,780 / 7,683,827) | 4,388,094 (4,364,856 / 4,481,975 / 4,388,094) | 0.00 | 0.00 |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 13 (13 / 13 / 13) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.65 | 0.65 |
| RSS MB | 269 (324 / 269 / 259) | 363 (321 / 367 / 363) | 287 (319 / 285 / 287) | 0.74 | 0.94 |
| broker CPU % (docker) | 86 (86 / 86 / 95) | 57 (59 / 57 / 57) | 59 (59 / 59 / 58) | 1.51 | 1.46 |

### Publish QoS 1 (2 producers, 128 B, window 1000, at most 2000 undelivered, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 257,668 (256,075 / 260,642 / 257,668) | 67,272 (67,272 / 63,754 / 68,723) | 56,682 (56,682 / 59,931 / 55,676) | 3.83 | 4.55 |
| subscriber-received msgs/s | 257,668 (256,074 / 260,641 / 257,668) | 67,272 (67,272 / 63,754 / 68,724) | 56,682 (56,682 / 59,931 / 55,674) | 3.83 | 4.55 |
| client CPU-s per 100k msgs | 0.60 (0.60 / 0.50 / 0.60) | 4.60 (4.60 / 4.90 / 4.50) | 5.30 (5.30 / 5.10 / 5.60) | 0.13 | 0.11 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 13 (13 / 13 / 13) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.65 | 0.65 |
| RSS MB | 238 (240 / 238 / 234) | 237 (242 / 237 / 231) | 236 (236 / 255 / 236) | 1.00 | 1.01 |
| broker CPU % (docker) | 88 (88 / 83 / 89) | 42 (42 / 53 / 42) | 42 (41 / 51 / 42) | 2.10 | 2.10 |

### Publish QoS 1, 64 KiB messages (1 producer, window 16, at most 64 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 17,686 (17,221 / 17,686 / 17,839) | 735 (705 / 735 / 748) | 745 (715 / 747 / 745) | 24.05 | 23.73 |
| subscriber-received msgs/s | 17,686 (17,221 / 17,686 / 17,839) | 736 (705 / 736 / 748) | 745 (715 / 747 / 745) | 24.05 | 23.73 |
| MB/s sent | 1,159 (1,129 / 1,159 / 1,169) | 48 (46 / 48 / 49) | 49 (47 / 49 / 49) | 24.05 | 23.75 |
| client CPU-s per 100k msgs | 6.60 (6.60 / 6.70 / 6.40) | 158.80 (170.10 / 158.80 / 156.00) | 156.60 (169.20 / 156.40 / 156.60) | 0.04 | 0.04 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 12 (12 / 12 / 12) | 15 (15 / 15 / 15) | 15 (15 / 15 / 15) | 0.80 | 0.80 |
| RSS MB | 338 (449 / 328 / 338) | 211 (211 / 211 / 215) | 205 (205 / 207 / 205) | 1.60 | 1.65 |
| broker CPU % (docker) | 88 (88 / 85 / 88) | 2 (3 / 2 / 2) | 3 (3 / 3 / 3) | 44.00 | 29.33 |

### Publish QoS 2 (2 producers, 128 B, window 100, at most 400 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 168,914 (168,527 / 168,914 / 169,817) | 42,492 (42,492 / 39,196 / 43,431) | 32,862 (32,565 / 32,862 / 33,403) | 3.98 | 5.14 |
| subscriber-received msgs/s | 168,915 (168,524 / 168,915 / 169,819) | 42,490 (42,490 / 39,193 / 43,432) | 32,862 (32,566 / 32,862 / 33,403) | 3.98 | 5.14 |
| client CPU-s per 100k msgs | 0.60 (0.60 / 0.60 / 0.60) | 7.30 (7.30 / 8.10 / 7.10) | 9.40 (9.50 / 9.40 / 9.30) | 0.08 | 0.06 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 13 (13 / 13 / 13) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.65 | 0.65 |
| RSS MB | 210 (206 / 210 / 212) | 234 (232 / 234 / 236) | 234 (234 / 244 / 233) | 0.90 | 0.90 |
| broker CPU % (docker) | 86 (86 / 86 / 80) | 47 (49 / 47 / 46) | 45 (44 / 45 / 45) | 1.83 | 1.91 |

### Latency QoS 0, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 68 (68 / 68 / 67) | 101 (101 / 100 / 101) | 101 (102 / 99 / 101) | 0.67 | 0.67 |
| p90 us | 80 (79 / 80 / 87) | 120 (120 / 120 / 122) | 121 (124 / 121 / 121) | 0.67 | 0.66 |
| p99 us | 132 (107 / 144 / 132) | 217 (148 / 230 / 217) | 187 (187 / 191 / 161) | 0.61 | 0.71 |
| p99.9 us | 247 (247 / 601 / 239) | 498 (300 / 682 / 498) | 373 (373 / 513 / 370) | 0.50 | 0.66 |
| max us | 3,855 (4,074 / 3,855 / 3,440) | 14,493 (14,493 / 35,223 / 3,149) | 8,574 (36,230 / 8,574 / 3,233) | 0.27 | 0.45 |

### Latency QoS 1, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 70 (70 / 70 / 70) | 85 (85 / 86 / 85) | 85 (85 / 84 / 88) | 0.82 | 0.82 |
| p90 us | 80 (88 / 80 / 80) | 108 (108 / 107 / 108) | 108 (108 / 106 / 112) | 0.74 | 0.74 |
| p99 us | 160 (160 / 118 / 168) | 177 (177 / 168 / 203) | 186 (186 / 155 / 199) | 0.90 | 0.86 |
| p99.9 us | 354 (347 / 354 / 553) | 384 (322 / 384 / 459) | 326 (523 / 316 / 326) | 0.92 | 1.09 |
| max us | 41,621 (43,869 / 2,683 / 41,621) | 41,362 (41,362 / 16,236 / 47,561) | 1,873 (3,419 / 1,575 / 1,873) | 1.01 | 22.22 |

### 100 connections, each subscribed to its own topic and publishing QoS 1 to it

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| delivered msgs/s | 232,962 (230,655 / 232,962 / 236,834) | 24,929 (25,024 / 24,874 / 24,929) | 173,696 (183,685 / 169,181 / 173,696) | 9.35 | 1.34 |
| client CPU-s per 100k msgs | 2.20 (2.20 / 2.20 / 2.20) | 20.00 (20.00 / 20.10 / 19.90) | 3.60 (3.50 / 3.70 / 3.60) | 0.11 | 0.61 |
| time to open 100 connections s | 0.1 (0.1 / 0.1 / 0.1) | 31.8 (31.9 / 31.5 / 31.8) | 31.9 (32.0 / 31.5 / 31.9) | 0.00 | 0.00 |
| threads after setup | 11 (11 / 11 / 11) | 406 (406 / 406 / 406) | 406 (406 / 406 / 406) | 0.03 | 0.03 |
| peak threads | 15 (15 / 15 / 15) | 410 (410 / 410 / 410) | 410 (410 / 410 / 410) | 0.04 | 0.04 |
| RSS MB | 242 (220 / 242 / 245) | 323 (323 / 323 / 327) | 419 (411 / 427 / 419) | 0.75 | 0.58 |
| broker CPU % (docker) | 86 (84 / 86 / 89) | 9 (9 / 9 / 9) | 29 (29 / 28 / 29) | 9.56 | 2.97 |
