# Hydrangea AMQP client vs the official `amqp-client` (RabbitMQ)

Both clients talk to the **same** RabbitMQ 3.13.7 (Docker Desktop 28.4.0, container `hydrangea-amqp-test`, plain AMQP on loopback), on one Mac with 8 CPUs that is also
running the client JVM, the Docker VM and an IDE. Hydrangea: `io.hydrangea.amqp.AmqpConnection` (this repository), `EventLoopGroup(2)`, nothing tuned. Official:
`com.rabbitmq:amqp-client` **5.26.0** (the version in the local Maven repository; newer ones exist), `ConnectionFactory` defaults except that automatic and
topology recovery are off (Hydrangea's `AmqpConnection` does not reconnect either; `RecoveringConnection` is not measured). JDK 25.0.1.

One JVM per run; every run does identical work through one small interface (`benchmark/src/amqp/AmqpBench.java`, so the two libraries cannot differ in what is asked
of them). 3 rounds, order alternating between rounds (Hydrangea first in rounds 1 and 3, official first in round 2); the cells show the median and the three rounds.
Reproduce: `sh hydrangea-amqp/src/test/resources/amqp/start-broker.sh && sh benchmark/amqp-bench.sh 3 && python3 benchmark/aggregate-amqp.py` (raw logs: `benchmark/amqp-results/`).
Runs are checked against the broker where the scenario allows it: `lost` (publish without confirms), `left in queue` (consume) and `unconfirmed at end` (confirm
scenarios) are 0 in every run; latency and the 100-connection scenario have no such check beyond completing.

How a producer waits when the connection is backed up: Hydrangea producers use `AmqpConnection.whenWritable()` (the write-ready callback, see below); the official
client's write simply blocks. "Client CPU-s per 100k msgs" is the JVM's process CPU time over the measured window divided by messages; it includes JIT and GC, and is
printed with one decimal, so differences of 0.1 are noise. `broker CPU %` is `docker stats` (100 % = one core), averaged over the whole run including set-up.

This is the second complete run of the matrix. The first one polled `isWritable()` with a 20 us park instead of using the callback; its numbers were close
(confirmed 1.19x, consume 1.17x, 100 connections 1.50x, publish without confirms 0.95x), except that its p99 latency advantage (0.71x) **did not reproduce** here (0.90x, rounds overlap).


### Publish, no confirmations (2 producers, 128 B)

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| messages/s | 125,304 (125,304 / 115,496 / 137,314) | 108,520 (87,035 / 126,544 / 108,520) | 1.15 |
| client CPU-s per 100k msgs | 0.10 (0.10 / 0.10 / 0.10) | 0.30 (0.30 / 0.30 / 0.50) | 0.33 |
| lost (broker count vs sent) | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - |
| peak threads | 10 (10 / 10 / 10) | 12 (12 / 12 / 12) | 0.83 |
| RSS MB | 193 (196 / 193 / 148) | 194 (193 / 206 / 194) | 0.99 |
| broker CPU % (docker) | 197 (198 / 191 / 197) | 219 (229 / 211 / 219) | 0.90 |

### Publish with confirmations, window 1000 per producer (2 producers, 128 B)

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| confirmed msgs/s | 91,460 (100,492 / 88,043 / 91,460) | 83,271 (83,271 / 84,559 / 78,948) | 1.10 |
| client CPU-s per 100k msgs | 0.30 (0.30 / 0.30 / 0.30) | 0.70 (0.70 / 0.70 / 0.70) | 0.43 |
| unconfirmed at end | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - |
| peak threads | 10 (10 / 10 / 10) | 12 (12 / 12 / 12) | 0.83 |
| RSS MB | 197 (208 / 197 / 95) | 189 (189 / 149 / 203) | 1.04 |
| broker CPU % (docker) | 251 (246 / 251 / 256) | 242 (247 / 241 / 242) | 1.04 |

### Publish with confirmations, 64 KiB messages (1 producer, window 16)

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| confirmed msgs/s | 1,940 (1,776 / 2,084 / 1,940) | 2,007 (2,021 / 2,007 / 1,747) | 0.97 |
| MB/s | 127 (116 / 137 / 127) | 132 (132 / 132 / 114) | 0.97 |
| client CPU-s per 100k msgs | 9.10 (10.70 / 7.60 / 9.10) | 9.40 (9.70 / 8.00 / 9.40) | 0.97 |
| peak threads | 9 (9 / 9 / 9) | 9 (9 / 9 / 9) | 1.00 |
| RSS MB | 208 (208 / 210 / 199) | 206 (206 / 207 / 197) | 1.01 |
| broker CPU % (docker) | 63 (67 / 60 / 63) | 61 (61 / 58 / 64) | 1.03 |

### Consume with an acknowledgement per message (300k messages, 2 consumers, prefetch 250)

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| messages/s | 83,241 (78,529 / 83,241 / 83,778) | 73,919 (69,256 / 74,570 / 73,919) | 1.13 |
| client CPU-s per 100k msgs | 0.40 (0.40 / 0.40 / 0.40) | 1.10 (1.10 / 1.10 / 1.00) | 0.36 |
| left in queue | 0 (0 / 0 / 0) | 0 (0 / 0 / 0) | - |
| peak threads | 8 (8 / 8 / 8) | 28 (28 / 28 / 28) | 0.29 |
| RSS MB | 220 (219 / 220 / 220) | 230 (231 / 229 / 230) | 0.96 |
| broker CPU % (docker) | 162 (162 / 158 / 169) | 159 (163 / 159 / 154) | 1.02 |

### Latency, one message at a time (publish -> delivery, 20k samples)

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| p50 us | 346 (351 / 334 / 346) | 354 (354 / 358 / 354) | 0.98 |
| p90 us | 425 (537 / 423 / 425) | 498 (498 / 530 / 461) | 0.85 |
| p99 us | 1,545 (1,994 / 1,545 / 1,376) | 1,711 (1,562 / 1,758 / 1,711) | 0.90 |
| p99.9 us | 8,499 (8,710 / 8,499 / 1,891) | 7,014 (6,859 / 8,355 / 7,014) | 1.21 |
| max us | 30,195 (37,783 / 30,195 / 9,562) | 33,555 (24,555 / 33,555 / 36,732) | 0.90 |

### 100 connections, each with a confirmed publisher and an acknowledging consumer

| metric | Hydrangea median (rounds) | amqp-client median (rounds) | Hydrangea / amqp-client |
|---|---:|---:|---:|
| delivered msgs/s | 60,024 (53,690 / 65,482 / 60,024) | 38,787 (38,481 / 38,787 / 48,310) | 1.55 |
| client CPU-s per 100k msgs | 1.20 (1.50 / 1.20 / 1.20) | 3.70 (3.80 / 3.70 / 3.60) | 0.32 |
| time to open 100 connections s | 0.9 (0.9 / 0.8 / 0.9) | 0.8 (0.8 / 0.9 / 0.8) | 1.12 |
| threads after setup | 8 (8 / 8 / 8) | 306 (306 / 306 / 306) | 0.03 |
| peak threads | 12 (12 / 12 / 12) | 1,010 (1,010 / 1,010 / 1,010) | 0.01 |
| RSS MB | 197 (197 / 205 / 196) | 249 (243 / 249 / 251) | 0.79 |
| broker CPU % (docker) | 464 (468 / 464 / 425) | 430 (483 / 430 / 414) | 1.08 |

## Reading it honestly

- **Where Hydrangea is clearly ahead, in both runs: many connections and consumers.** With 100 connections the official client has 306 threads after set-up and peaks at
  1 010; Hydrangea stays at 8 and 12. That goes with about 1.5x the delivered throughput, about a third of the CPU per message, and 21 % less RSS. The cause is structural (a
  thread or more per connection and per channel versus two event loops), not a faster codec. The same shows in the consume scenario (28 threads vs 8, 1.13x throughput, 0.36x CPU;
  the three rounds of each do not overlap).
- **Publisher confirms with small messages: Hydrangea 1.10x** (91k vs 83k confirmed/s; 1.19x in the first run) at 0.43x the CPU. The rounds do not overlap, but the margin is modest and it moved by 9 points between two runs.
- **A tie, within noise:** publishing without confirms (125k vs 109k medians, but the official's three rounds span 87k-127k, which contains Hydrangea's), 64 KiB messages (both ~130 MB/s;
  the broker is the limit there), and all latency percentiles. Where the numbers are equal, the broker or the Docker loopback path is the bottleneck, not either client.
  CPU per message in the no-confirm case is 0.1 vs 0.3, a gap at the resolution of the measurement.
- The broker was using 2-4.6 cores in the busy scenarios (`broker CPU %`), on the same 8 cores as the client: client differences are measured under shared CPU, and a faster
  client mostly hands the broker more work.

## Back-pressure: how it was measured and what was added

`AmqpConnection.basicPublish` never blocks, so a producer that ignores `isWritable()` queues without bound. Three ways of waiting were tried in this harness:
- *Spinning* on `isWritable()` looked like a 30 % throughput loss and about 5x the CPU per message; it was the harness (spinning burns CPU the benchmark then counts).
- *Polling* with a 20 us park matched the official client (first run).
- *No waiting at all*: a 5-second publish loop ran far ahead of the socket and closing the connection afterwards timed out (30 s). I did not measure the memory it used.

There was no way to be told "writable again"; the answer was this change: `AmqpConnection.whenWritable()` (and the same on `AmqpChannel` / `RecoveringChannel`) returns a future
that completes when the queued data drains below the low-water mark and **fails if the connection closes first**, so a waiting producer cannot hang; `onWritabilityChanged(Consumer<Boolean>)`
reports every transition. The numbers above use it. Tests: `AmqpTest.backPressureIsSignalledAndLifted`, `aProducerDrivenByWhenWritableNeverQueuesMoreThanAFewMessages`,
`HostileBrokerTest.whenWritableWaitsForABrokerThatStopsReadingAndFailsWhenTheConnectionDies`, `RecoveryTest.whenWritableFailsWhileDownAndWorksAgainAfterRecovery`.
The high-water mark counts bytes queued in the library; the operating system's socket buffer takes a few MiB more first, so a producer driven by the callback publishes a
short burst, not one message, between waits.

## Limits of what this proves

- **Callbacks run on the event loop in Hydrangea; on a worker pool in the official client.** The handler here is trivial. A consumer that does real or blocking work stalls
  the loop (and every connection on it) in Hydrangea, while the official client merely occupies one of its own threads. That is a different trade-off, not measured here.
- Not measured: TLS, durable queues / persistent messages (disk-bound, would hide client differences), quorum queues or clusters, many channels per connection, a tuned
  official client (custom executors, `NioParams`), GC pauses or a peak-memory profile, a machine other than this shared 8-CPU Mac.
- Only 3 rounds on a machine that is not otherwise idle: the spread inside a cell (e.g. 87k-127k) is as large as some of the differences called "ties" above, and two complete runs
  disagreed on the p99 latency.
- Maturity is not a number: the official client has 15 years of production use, RPC helpers, many SASL mechanisms and extensive documentation; this client has the tests in
  `hydrangea-amqp/src/test/java/io/hydrangea/amqp/` and nothing else behind it.
