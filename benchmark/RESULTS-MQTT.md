# Hydrangea MQTT client vs Eclipse Paho (`org.eclipse.paho.client.mqttv3` for 3.1.1, `org.eclipse.paho.mqttv5.client` for MQTT 5 and MQTT over WebSocket)

Both clients talk to the **same** Mosquitto 2.1.2 (Docker Desktop 28.4.0, container `hydrangea-mqtt-test`; `max_inflight_messages 100`, `max_queued_messages 1000000`,
`message_size_limit 100000000`, anonymous, plain TCP). The client JVM runs in a **second container sharing the broker's network namespace** (Temurin JRE 21, Linux, loopback
inside the Docker VM; the VM has 8 CPUs, shared with the broker). Hydrangea: `io.hydrangea.mqtt.MqttClient`, `EventLoopGroup(2)`, nothing tuned, reconnect off. Paho: **1.2.5**
(the version in the local Maven repository), its asynchronous `MqttAsyncClient`, `MemoryPersistence`, automatic reconnect off, `maxInflight` set to the scenario's window, in two
variants: **as it ships**, and **with `TCP_NODELAY`** set through a custom `SocketFactory` (Paho has no option for it and leaves Nagle's algorithm on).

One JVM per run; every run does identical work through one small interface (`benchmark/src/mqtt/MqttBench.java`). 3 rounds, library order rotating between rounds; the cells show the
median and the three rounds. Reproduce: `sh hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh && mvn -q compile && sh benchmark/mqtt-bench.sh 3 && python3 benchmark/aggregate-mqtt.py`
(raw logs: `benchmark/mqtt-results/`). Needs the broker, a compiled project and Paho in `~/.m2`.

How a publisher is limited (the same for both libraries): at most `window` publishes unfinished (written at QoS 0, PUBACK at QoS 1, PUBCOMP at QoS 2: what Paho's own in-flight limit counts),
and at QoS 1 and 2 at most `system` messages published but not yet received by the subscriber, all producers together. That closed loop keeps the broker's queue bounded and nothing is
dropped, so the QoS 1 and 2 numbers are end-to-end throughput. QoS 0 has no second limit: Mosquitto drops QoS 0 for a subscriber that cannot keep up, and `lost` says how many.
Hydrangea producers also wait on `whenWritable()` when the socket queue is full; Paho has nothing like it. "Client CPU-s per 100k msgs" is the whole JVM's process CPU (publishers and
subscriber, JIT and GC included) over the measured window divided by messages; `broker CPU %` is `docker stats` (100 % = one core) over the whole run.

## What to take from it

* **QoS 1 and 2, one broker connection pair**: Hydrangea delivers about 3.8x (QoS 1) and 4.0-5.1x (QoS 2) the messages per second of Paho, with 8-17 % of the client CPU per message and
  fewer threads (13 vs 20). Mosquitto is single-threaded and runs at 85-88 % of a core for Hydrangea against about 42-47 % for Paho, i.e. **the broker is close to its limit in the Hydrangea
  runs**; a faster broker could widen the gap, a different one could narrow it.
* **64 KiB messages (QoS 1)**: 24x, and 4 % of the CPU per message. Paho spends about 1.6 ms of CPU per 64 KiB message, almost all in its receive thread
  (`MqttInputStream.readMqttWireMessage`, seen in thread dumps). I did **not** find out why, so treat the factor as "what this Paho 1.2.5 setup does", not as a general property of Paho.
* **100 connections**: Hydrangea opens them in 0.1 s on 11 threads and delivers 233k msgs/s; Paho needs 406 threads, **about 320 ms per connection** to connect (31.8 s for 100, with or without
  NODELAY; I did not find the cause), and delivers 25k msgs/s as shipped. With NODELAY, Paho's throughput rises to 174k msgs/s (the Nagle algorithm was costing it 7x there), and
  Hydrangea is still 1.34x ahead and uses 0.61x the CPU per message.
* **Latency, one message at a time**: Hydrangea is lower at every percentile in these runs (QoS 0 p50 68 vs 101 us; QoS 1 p50 70 vs 85 us), but the differences are tens of microseconds on
  20k samples and the max values (milliseconds) are noise.
* **QoS 0**: the "sender finished" row is *not* a win for Hydrangea. Paho as shipped writes 859k msgs/s against Hydrangea's 546k (Paho with NODELAY: 535k), because Paho keeps writing at
  a subscriber that cannot keep up and Mosquitto **drops** the excess: in round 1 Paho issued 10.5M messages and 7.9M never arrived (with NODELAY 6.6M issued, 4.4M lost), Hydrangea issued
  7.1M and lost none, because its producers wait on `whenWritable()`. The row that means something is what the subscriber received: 546k msgs/s for Hydrangea, 132k (Paho) and 103k (Paho +
  NODELAY). That depends partly on the subscriber being the same library, and Paho's callback thread being the slower one.

## Caveats, in order of importance

1. **Where the client runs matters.** My first runs executed the clients on the Mac itself and reached Mosquitto through Docker Desktop's port forwarding. There, a one-message-at-a-time
   QoS 1 exchange showed a 40 ms stall in 10-40 % of the samples for Hydrangea (p90 about 41 ms) and rarely for Paho (p90 0.4 ms); a packet capture through a relay showed the stall between
   the broker's PUBACK and the delivery, which is the Nagle/delayed-ACK pattern in the forwarder. Inside the Docker VM (these results) the effect is gone and the two libraries are close.
   **I do not know why Hydrangea was more sensitive on the macOS path**; my guess is its write pattern (the PUBACK for a delivery and the next PUBLISH leave in one segment) but I did not prove it.
   If you run a Hydrangea MQTT client against a broker through such a forwarder, expect this.
2. One Mac, one broker, one run of the matrix (three rounds), 128-byte payloads except where stated. Mosquitto's `max_inflight_messages 100` and its receive-maximum quota apply to both.
   A first attempt used a QoS 2 window of 200: Mosquitto dropped the excess ("exceeded its receive-maximum quota"), which looked like message loss in both libraries; the window is now 100.
   Mosquitto 2.1.2 here also drops clients that send roughly 2 MB or more in a single packet (`mosquitto_pub` too), so large-message numbers stop at 64 KiB.
3. Paho is measured through `MqttAsyncClient`; the blocking `MqttClient` is slower still for pipelined work and is not shown. Paho 1.2.5 is old (the newer MQTT 5 client is
   `org.eclipse.paho.mqttv5.client`, not compared).
4. Hydrangea speaks MQTT 3.1.1 only here; the broker and Paho do the same, so nothing in the comparison depends on MQTT 5 features.

## Results


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


---

# MQTT 5 and MQTT over WebSocket

Same Mosquitto, same container layout, same scenarios and the same method (3 rounds, library order rotating), but with **Paho `org.eclipse.paho.mqttv5.client` 1.2.5** (downloaded from Maven Central by
`mqtt-bench.sh`) against Hydrangea's MQTT 5 client, once over TCP (`GROUP=v5`) and once over WebSocket (`GROUP=v5ws`, Mosquitto's `listener 9001` with `protocol websockets`, URI `ws://host:9001/mqtt`
for both). Two differences from the 3.1.1 runs above, both forced by MQTT 5: the QoS 1 `window` is **100**, not 1000, because Mosquitto's CONNACK says receive maximum 100 and Paho's v5 client
throws beyond it (Hydrangea would have queued the rest, which is the same thing); and Paho's v5 API wants a callback per client or a subscription identifier, so the harness uses one callback per
connection. Reproduce: `GROUP=v5 sh benchmark/mqtt-bench.sh 3; GROUP=v5ws sh benchmark/mqtt-bench.sh 3; python3 benchmark/aggregate-mqtt.py`. The three groups' raw logs share `benchmark/mqtt-results/`.

## MQTT 5 over TCP (Paho org.eclipse.paho.mqttv5.client 1.2.5)

### Publish QoS 0 (2 producers, 128 B, window 1000, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 531,031 (531,031 / 546,204 / 519,096) | 685,784 (597,722 / 723,386 / 685,784) | 439,065 (439,065 / 445,686 / 421,368) | 0.77 | 1.21 |
| subscriber-received msgs/s | 531,769 (531,769 / 546,591 / 519,149) | 59,294 (50,525 / 74,014 / 59,294) | 69,771 (69,771 / 73,663 / 69,545) | 8.97 | 7.62 |
| client CPU-s per 100k msgs | 0.10 (0.10 / 0.10 / 0.10) | 1.20 (1.30 / 1.20 / 1.20) | 1.80 (1.80 / 1.80 / 1.80) | 0.08 | 0.06 |
| lost (issued - received) | 0 (0 / 0 / 0) | 6,739,490 (5,953,999 / 6,967,418 / 6,739,490) | 3,593,817 (3,593,817 / 3,845,969 / 3,369,263) | 0.00 | 0.00 |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 16 (15 / 16 / 16) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.80 | 0.80 |
| RSS MB | 300 (313 / 298 / 300) | 386 (386 / 366 / 410) | 325 (337 / 325 / 324) | 0.78 | 0.92 |
| broker CPU % (docker) | 76 (76 / 76 / 77) | 45 (45 / 44 / 45) | 47 (47 / 51 / 47) | 1.69 | 1.62 |

### Publish QoS 1 (2 producers, 128 B, window 100, at most 2000 undelivered, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 251,838 (260,101 / 251,838 / 212,736) | 62,921 (64,591 / 61,237 / 62,921) | 54,498 (54,498 / 57,911 / 54,260) | 4.00 | 4.62 |
| subscriber-received msgs/s | 251,836 (260,102 / 251,836 / 212,736) | 62,920 (64,592 / 61,238 / 62,920) | 54,498 (54,498 / 57,911 / 54,259) | 4.00 | 4.62 |
| client CPU-s per 100k msgs | 0.60 (0.60 / 0.60 / 0.70) | 4.80 (4.70 / 4.90 / 4.80) | 5.70 (5.70 / 5.20 / 5.70) | 0.12 | 0.11 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 16 (16 / 16 / 16) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.80 | 0.80 |
| RSS MB | 240 (209 / 240 / 244) | 240 (234 / 258 / 240) | 234 (238 / 234 / 230) | 1.00 | 1.03 |
| broker CPU % (docker) | 87 (82 / 87 / 87) | 43 (42 / 43 / 43) | 41 (41 / 41 / 42) | 2.02 | 2.12 |

### Publish QoS 1, 64 KiB messages (1 producer, window 16, at most 64 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 17,494 (17,494 / 17,497 / 17,407) | 738 (738 / 738 / 734) | 738 (752 / 712 / 738) | 23.69 | 23.71 |
| subscriber-received msgs/s | 17,494 (17,494 / 17,497 / 17,407) | 738 (738 / 738 / 734) | 738 (752 / 712 / 738) | 23.69 | 23.71 |
| MB/s sent | 1,146 (1,146 / 1,147 / 1,141) | 48 (48 / 48 / 48) | 48 (49 / 47 / 48) | 23.69 | 23.69 |
| client CPU-s per 100k msgs | 6.50 (6.90 / 6.40 / 6.50) | 161.90 (159.90 / 162.60 / 161.90) | 161.50 (158.70 / 170.80 / 161.50) | 0.04 | 0.04 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 15 (13 / 15 / 15) | 15 (15 / 15 / 15) | 15 (15 / 15 / 15) | 1.00 | 1.00 |
| RSS MB | 351 (351 / 369 / 319) | 209 (207 / 209 / 209) | 209 (207 / 209 / 209) | 1.68 | 1.68 |
| broker CPU % (docker) | 88 (88 / 87 / 88) | 2 (2 / 3 / 2) | 3 (3 / 4 / 3) | 44.00 | 29.33 |

### Publish QoS 2 (2 producers, 128 B, window 100, at most 400 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 168,832 (172,004 / 168,832 / 152,634) | 22,963 (23,008 / 22,113 / 22,963) | 18,987 (20,642 / 18,885 / 18,987) | 7.35 | 8.89 |
| subscriber-received msgs/s | 168,816 (172,003 / 168,816 / 152,635) | 22,961 (23,009 / 22,116 / 22,961) | 18,988 (20,639 / 18,883 / 18,988) | 7.35 | 8.89 |
| client CPU-s per 100k msgs | 0.60 (0.60 / 0.60 / 0.70) | 10.40 (10.00 / 10.40 / 10.60) | 12.50 (11.30 / 12.50 / 12.50) | 0.06 | 0.05 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 15 (15 / 16 / 15) | 20 (20 / 20 / 20) | 20 (20 / 20 / 20) | 0.75 | 0.75 |
| RSS MB | 211 (211 / 211 / 217) | 401 (418 / 388 / 401) | 397 (401 / 365 / 397) | 0.53 | 0.53 |
| broker CPU % (docker) | 85 (87 / 85 / 85) | 34 (33 / 34 / 36) | 33 (32 / 33 / 34) | 2.50 | 2.58 |

### Latency QoS 0, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 67 (67 / 70 / 57) | 106 (106 / 107 / 106) | 108 (105 / 108 / 109) | 0.63 | 0.62 |
| p90 us | 83 (77 / 83 / 111) | 126 (125 / 126 / 141) | 140 (125 / 140 / 157) | 0.66 | 0.59 |
| p99 us | 134 (91 / 134 / 409) | 165 (151 / 165 / 374) | 303 (161 / 303 / 407) | 0.81 | 0.44 |
| p99.9 us | 305 (173 / 305 / 2,922) | 419 (240 / 419 / 1,706) | 858 (523 / 858 / 2,080) | 0.73 | 0.36 |
| max us | 2,112 (2,112 / 1,985 / 28,590) | 10,026 (1,664 / 15,172 / 10,026) | 13,713 (20,210 / 12,182 / 13,713) | 0.21 | 0.15 |

### Latency QoS 1, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 69 (69 / 69 / 70) | 87 (86 / 87 / 90) | 85 (85 / 85 / 83) | 0.79 | 0.81 |
| p90 us | 79 (79 / 76 / 81) | 113 (122 / 107 / 113) | 119 (119 / 105 / 149) | 0.70 | 0.66 |
| p99 us | 128 (128 / 124 / 134) | 246 (246 / 279 / 212) | 284 (284 / 170 / 448) | 0.52 | 0.45 |
| p99.9 us | 422 (336 / 422 / 449) | 484 (484 / 1,337 / 478) | 1,350 (881 / 1,350 / 1,640) | 0.87 | 0.31 |
| max us | 13,635 (13,635 / 7,385 / 42,047) | 42,637 (41,330 / 42,637 / 42,815) | 16,159 (16,366 / 3,947 / 16,159) | 0.32 | 0.84 |

### 100 connections, each subscribed to its own topic and publishing QoS 1 to it

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| delivered msgs/s | 214,622 (199,947 / 214,622 / 222,261) | 24,905 (24,905 / 24,969 / 24,372) | 163,865 (163,523 / 166,348 / 163,865) | 8.62 | 1.31 |
| client CPU-s per 100k msgs | 2.40 (2.60 / 2.40 / 2.30) | 20.30 (20.30 / 20.20 / 20.50) | 3.80 (3.80 / 3.80 / 3.90) | 0.12 | 0.63 |
| time to open 100 connections s | 0.1 (0.1 / 0.1 / 0.1) | 31.6 (31.6 / 31.5 / 31.6) | 31.6 (31.6 / 31.5 / 31.7) | 0.00 | 0.00 |
| threads after setup | 13 (13 / 13 / 14) | 406 (406 / 406 / 406) | 406 (406 / 406 / 406) | 0.03 | 0.03 |
| peak threads | 17 (17 / 17 / 18) | 410 (410 / 410 / 410) | 410 (410 / 410 / 410) | 0.04 | 0.04 |
| RSS MB | 240 (268 / 238 / 240) | 330 (328 / 330 / 391) | 450 (451 / 450 / 439) | 0.73 | 0.53 |
| broker CPU % (docker) | 86 (86 / 84 / 97) | 8 (8 / 7 / 9) | 22 (23 / 22 / 22) | 10.75 | 3.91 |

## MQTT 5 over WebSocket (Paho org.eclipse.paho.mqttv5.client 1.2.5, ws://)

### Publish QoS 0 (2 producers, 128 B, window 1000, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 182,924 (182,924 / 180,403 / 190,751) | 309,728 (309,728) | 321,047 (336,371 / 304,032 / 321,047) | 0.59 | 0.57 |
| subscriber-received msgs/s | 182,276 (182,276 / 180,619 / 190,549) | 50,037 (50,037) | 55,339 (57,930 / 50,296 / 55,339) | 3.64 | 3.29 |
| client CPU-s per 100k msgs | 0.40 (0.40 / 0.40 / 0.40) | 1.80 (1.80) | 1.80 (1.70 / 1.80 / 1.80) | 0.22 | 0.22 |
| lost (issued - received) | 22,941 (26,828 / 22,941 / 0) | 2,221,821 (2,221,821) | 2,267,646 (2,412,630 / 2,094,037 / 2,267,646) | 0.01 | 0.01 |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 16 (16 / 16 / 15) | 23 (23) | 23 (23 / 23 / 23) | 0.70 | 0.70 |
| RSS MB | 258 (416 / 238 / 258) | 411 (411) | 351 (350 / 397 / 351) | 0.63 | 0.74 |
| broker CPU % (docker) | 77 (77 / 76 / 77) | 44 (44) | 49 (49 / 49 / 51) | 1.75 | 1.57 |

### Publish QoS 1 (2 producers, 128 B, window 100, at most 2000 undelivered, one subscriber)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 117,746 (119,733 / 111,705 / 117,746) | 45,704 (51,025 / 44,991 / 45,704) | 48,528 (52,418 / 48,528 / 46,248) | 2.58 | 2.43 |
| subscriber-received msgs/s | 117,745 (119,731 / 111,700 / 117,745) | 45,705 (51,026 / 44,991 / 45,705) | 48,529 (52,416 / 48,529 / 46,248) | 2.58 | 2.43 |
| client CPU-s per 100k msgs | 1.40 (1.30 / 1.40 / 1.40) | 6.30 (5.60 / 6.30 / 6.30) | 6.90 (6.40 / 6.90 / 6.90) | 0.22 | 0.20 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 15 (16 / 15 / 15) | 23 (23 / 23 / 23) | 23 (23 / 23 / 23) | 0.65 | 0.65 |
| RSS MB | 216 (216 / 210 / 218) | 242 (245 / 242 / 238) | 246 (246 / 243 / 269) | 0.89 | 0.88 |
| broker CPU % (docker) | 89 (81 / 90 / 89) | 42 (42 / 43 / 40) | 49 (49 / 50 / 49) | 2.12 | 1.82 |

### Publish QoS 1, 64 KiB messages (1 producer, window 16, at most 64 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 7,625 (7,625 / 7,259 / 9,167) | 230 (230 / 221 / 231) | 228 (228 / 225 / 232) | 33.08 | 33.41 |
| subscriber-received msgs/s | 7,625 (7,625 / 7,260 / 9,167) | 231 (231 / 221 / 231) | 228 (228 / 225 / 232) | 33.06 | 33.41 |
| MB/s sent | 500 (500 / 476 / 601) | 15 (15 / 14 / 15) | 15 (15 / 15 / 15) | 33.09 | 33.31 |
| client CPU-s per 100k msgs | 15.90 (15.90 / 16.80 / 13.10) | 392.50 (392.50 / 413.50 / 391.10) | 399.50 (399.50 / 412.10 / 391.00) | 0.04 | 0.04 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 15 (14 / 15 / 15) | 17 (17 / 17 / 17) | 17 (17 / 17 / 17) | 0.88 | 0.88 |
| RSS MB | 381 (381 / 439 / 371) | 214 (214 / 214 / 214) | 214 (214 / 214 / 214) | 1.78 | 1.78 |
| broker CPU % (docker) | 78 (78 / 75 / 85) | 2 (2 / 2 / 2) | 2 (2 / 2 / 2) | 39.00 | 39.00 |

### Publish QoS 2 (2 producers, 128 B, window 100, at most 400 undelivered)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| sender-finished msgs/s | 58,780 (59,591 / 58,780 / 57,578) | 22,852 (20,974 / 23,208 / 22,852) | 25,228 (25,228 / 31,000 / 24,973) | 2.57 | 2.33 |
| subscriber-received msgs/s | 58,781 (59,588 / 58,781 / 57,577) | 22,853 (20,978 / 23,208 / 22,853) | 25,231 (25,231 / 31,003 / 24,976) | 2.57 | 2.33 |
| client CPU-s per 100k msgs | 2.60 (2.60 / 2.60 / 2.40) | 11.50 (12.20 / 11.10 / 11.50) | 12.00 (12.00 / 10.50 / 12.20) | 0.23 | 0.22 |
| lost (issued - received) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| unfinished at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - | - |
| peak threads | 15 (15 / 15 / 16) | 23 (23 / 23 / 23) | 23 (23 / 23 / 23) | 0.65 | 0.65 |
| RSS MB | 218 (218 / 220 / 218) | 468 (452 / 468 / 480) | 505 (505 / 549 / 504) | 0.47 | 0.43 |
| broker CPU % (docker) | 88 (88 / 88 / 86) | 43 (40 / 43 / 44) | 51 (47 / 52 / 51) | 2.05 | 1.73 |

### Latency QoS 0, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 72 (72 / 73 / 72) | 130 (130 / 134 / 129) | 131 (130 / 133 / 131) | 0.55 | 0.55 |
| p90 us | 85 (82 / 89 / 85) | 154 (154 / 168 / 153) | 157 (152 / 167 / 157) | 0.55 | 0.54 |
| p99 us | 167 (115 / 167 / 180) | 252 (239 / 360 / 252) | 268 (268 / 366 / 230) | 0.66 | 0.62 |
| p99.9 us | 373 (294 / 373 / 582) | 924 (924 / 1,159 / 917) | 1,090 (1,257 / 1,090 / 878) | 0.40 | 0.34 |
| max us | 16,396 (16,396 / 2,452 / 40,124) | 9,404 (16,645 / 9,404 / 4,059) | 20,705 (24,342 / 6,038 / 20,705) | 1.74 | 0.79 |

### Latency QoS 1, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| p50 us | 74 (72 / 74 / 74) | 108 (100 / 108 / 109) | 105 (104 / 110 / 105) | 0.69 | 0.70 |
| p90 us | 83 (81 / 87 / 83) | 134 (132 / 152 / 134) | 156 (161 / 156 / 133) | 0.62 | 0.53 |
| p99 us | 154 (154 / 154 / 149) | 285 (285 / 294 / 280) | 414 (418 / 414 / 223) | 0.54 | 0.37 |
| p99.9 us | 418 (574 / 295 / 418) | 702 (1,463 / 674 / 702) | 1,405 (1,405 / 2,249 / 403) | 0.60 | 0.30 |
| max us | 30,334 (9,823 / 41,795 / 30,334) | 41,595 (41,470 / 41,595 / 83,424) | 37,774 (58,145 / 37,774 / 2,069) | 0.73 | 0.80 |

### 100 connections, each subscribed to its own topic and publishing QoS 1 to it

| metric | Hydrangea median (rounds) | Paho median (rounds) | Paho + TCP_NODELAY median (rounds) | Hydrangea / Paho | Hydrangea / Paho+NODELAY |
|---|---:|---:|---:|---:|---:|
| delivered msgs/s | 93,867 (93,867 / 85,000 / 97,570) | 43,450 (38,526 / 43,450 / 52,187) | 83,983 (79,333 / 83,983 / 94,791) | 2.16 | 1.12 |
| client CPU-s per 100k msgs | 5.90 (5.90 / 6.50 / 5.80) | 13.80 (15.30 / 13.80 / 12.00) | 7.50 (7.80 / 7.50 / 6.90) | 0.43 | 0.79 |
| time to open 100 connections s | 0.2 (0.2 / 0.2 / 0.2) | 31.7 (31.7 / 31.7 / 31.7) | 31.6 (31.6 / 31.6 / 31.4) | 0.01 | 0.01 |
| threads after setup | 14 (15 / 14 / 13) | 506 (506 / 506 / 506) | 506 (506 / 506 / 506) | 0.03 | 0.03 |
| peak threads | 18 (19 / 18 / 17) | 510 (510 / 510 / 510) | 510 (510 / 510 / 510) | 0.04 | 0.04 |
| RSS MB | 241 (216 / 244 / 241) | 395 (389 / 395 / 429) | 431 (431 / 431 / 435) | 0.61 | 0.56 |
| broker CPU % (docker) | 85 (85 / 85 / 84) | 17 (18 / 17 / 17) | 22 (22 / 23 / 20) | 5.00 | 3.86 |

Runs without a result line: r2-paho5-ws-publish-qos0.log, r3-paho5-ws-publish-qos0.log

## What to take from the MQTT 5 and WebSocket runs

* **MQTT 5 over TCP is the 3.1.1 result again**: QoS 1 4.0x Paho (3.8x with 3.1.1), 12 % of the CPU per message; 64 KiB 23.7x; 100 connections 8.6x as Paho ships and 1.31x against Paho with NODELAY.
  **QoS 2 is the exception: 7.4x** (4.0x with 3.1.1), because Paho's v5 client is itself slower at QoS 2 (23k msgs/s against 42k with its 3.1.1 client); Hydrangea's QoS 2 did not change (169k).
* **WebSocket costs both of them throughput**, Hydrangea less in relative terms: QoS 1 118k msgs/s against 252k over TCP (47 %), Paho 46k against 63k (73 %). So the ratio shrinks to 2.6x. WebSocket framing
  (a 2-6 byte header and a mask on every client frame, an extra copy per message) is the whole difference; the masking is done per byte in this client and is the first place to look if it matters.
* **64 KiB over WebSocket: 33x.** Hydrangea 7.6k msgs/s (about half its TCP speed, 16 CPU-s per 100k messages against 6.5), Paho 230.
* **QoS 0 "sender finished" is again not a win for Paho**: it finishes 1.3-1.7x more publishes, and the subscriber gets 4-9x less, because Paho overruns the broker, which drops what it cannot deliver.
* **Latency (one message at a time)**: p50 is 20-45 % lower for Hydrangea in every group (over TCP 67-69 us against 85-108; over WebSocket 72-74 against 108-131), the p99 gap is wider than with 3.1.1.
* **Run to run**: the three rounds of each cell are within a few percent except QoS 0 (the broker drops), as before. Two runs (`paho5-ws`, QoS 0, rounds 2 and 3) crashed in `Paho5Lib.close()`
  after printing nothing useful and have no result; the table uses the remaining round for that cell. This is the harness' or Paho's close path, not Hydrangea's.

## Why Paho needs 1.6 ms of CPU for a 64 KiB message (found)

The earlier text said the cause was not found. It is in Paho's receive path, and it is the same in 1.2.5 of both clients (`mqttv3` and `mqttv5`): a Java Flight Recorder profile of the 64 KiB run (1 producer, QoS 1) puts
**100 % of the samples in two places**, `MqttWireMessage.createWireMessage` (line 201, `DataInputStream.readFully`) and `MqttPublish.<init>` (line 69, the same call). Both read the payload through
`CountingInputStream`, which implements only `read()`, so `InputStream.read(byte[], int, int)` falls back to **one virtual call per byte**, and the payload is read twice per received message (once to cut the packet,
once to build the message). Measured alone on the same machine: 8.9 ns per byte, **580 us per 64 KiB read, against 0.9 us for the same read from a `ByteArrayInputStream`**. Two passes are 1.2 ms of the 1.6 ms;
the rest is the ordinary work. Consequences: the cost is **per received byte**, so it hits the subscriber side; a publisher-only program is not affected. Messages of 128 B pay 1.1 us for it. A larger factor than the measured 24x
on a faster CPU or with a different JIT is possible, a smaller one too. It is a property of this Paho version, not of Paho in general: it can be fixed by overriding `read(byte[], int, int)` in that one class.

## 3.1.1 checked again after the MQTT 5 work

The 3.1.1 numbers above were re-run (QoS 0, 1, 2 and latency QoS 1; `benchmark/mqtt-results-v3-recheck/`) after AUTH, topic aliases, message expiry and request/response were added to the shared client:
QoS 1 3.67x Paho (3.83x before), QoS 2 4.05x (3.98x), QoS 1 latency p50 69 us (70). That is within the noise of the two runs; the 64 KiB, QoS 0 latency and 100-connection scenarios were not repeated.
