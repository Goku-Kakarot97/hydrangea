# Hydrangea MySQL client against MySQL Connector/J

`hydrangea-mysql` (`MyPool`) against Connector/J 9.4.0 (+ HikariCP 7.0.2) on the same MySQL 8.4.13, both in Docker on a macOS laptop (Apple silicon, Docker VM with 8 CPUs):
the client in an `eclipse-temurin:21-jre` container with `--cpus=4`, the server container with `--cpus=4`, a bridge network between them, the server with its defaults
(`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, so every commit reaches the disk, which is the virtualised disk of the VM). Program: `benchmark/src/mysql/MyBench.java`,
driver: `benchmark/my-bench-docker.sh`. Raw results: `benchmark/my-results/linux-round{1,2,3}.txt` (204 runs, 0 errors). Numbers below are **medians of 3 runs** of 5 s after 2 s of
warm-up, one JVM per run, the libraries in rotating order.

Libraries: `hydrangea` (`MyPool`, callers are future chains, calls with parameters are server-prepared, binary rows), `hydrangea-block` (the same with a platform thread per caller doing
`join()`), `mysqlj` (HikariCP, Connector/J with its defaults: client-side prepared statements, the parameters put into the SQL text), `mysqlj-server` (`useServerPrepStmts=true`,
`cachePrepStmts=true`: the same protocol as Hydrangea), `mysqlj-virtual` (as `mysqlj` on virtual threads), `mysqlj-dedicated` (Connector/J server-prepared, one connection per
thread, no pool). `conns:callers` = pool size : concurrent callers. The Hydrangea pool has one event loop for each connection (`loops=conns`; see the investigation below for why).

Scenarios: `select1` (`SELECT 1`, text protocol for Hydrangea), `point` (one row by primary key out of 100 000, with a parameter), `insert` (one row into a table with an
AUTO_INCREMENT key), `tx` (`START TRANSACTION`, `UPDATE` by key, `SELECT` by key, `COMMIT`).

## Reads (queries per second; p99 in ms; CPU µs per query)

| scenario | conns:callers | hydrangea | hydrangea-block | mysqlj | mysqlj-server | mysqlj-virtual | mysqlj-dedicated | hydrangea / best Connector/J | p99 ms: hydrangea / best | CPU µs: hydrangea / best |
|---|---|---|---|---|---|---|---|---|---|---|
| select1 | 8:8 | 143 632 | 36 407 | 110 496 | 179 704 | 33 964 | 192 374 | 0.75 (0.80 against the pooled best) | 0.3 / 0.2 | 17.7 / 19.1 |
| select1 | 8:64 | **188 238** | 95 275 | 59 550 | 73 114 | 27 505 | - | **2.6** | 2.8 / 11.5 | 11.5 / 49.0 |
| select1 | 32:32 | 162 714 | 73 557 | 146 349 | 147 122 | 72 050 | 174 538 | 0.93 (1.1 against the pooled best) | 2.0 / 1.9 | 17.3 / 21.7 |
| point | 8:8 | 132 450 | 28 557 | 90 509 | 121 847 | 37 355 | 116 211 | 1.09 | 0.3 / 0.3 | 16.0 / 24.1 |
| point | 8:64 | **139 536** | 93 111 | 57 630 | 68 290 | 33 796 | - | **2.0** | 3.3 / 12.6 | 12.7 / 48.4 |
| point | 32:32 | 88 175 | 51 683 | 70 323 | 100 673 | 44 011 | 93 437 | 0.88 | 4.7 / 3.5 | 29.5 / 31.6 |

Ranges of the three runs at 8:64 (the cells that matter most): `select1` Hydrangea 177-217 k against `mysqlj-server` 70-92 k; `point` 126-148 k against 61-70 k. The ranges do not overlap.

* **With more callers than connections Hydrangea is 2-2.6 times ahead**, for the same reason as with PostgreSQL: the calls of 64 callers are pipelined on 8 connections (several in
  flight on each), while a JDBC caller holds a connection for its whole round trip and the others queue in the pool. The p99 is 4 times lower (2.8-3.3 ms against 11.5-12.6 ms) and
  the CPU per query 4 times lower (11.5-12.7 µs against 48-49 µs). Callers are served evenly (11 317-19 553 completions per caller in one run, against 2 050-3 865 for `mysqlj`).
* **With a connection for every caller the two are level**: Hydrangea is within -25% to +9% of the best Connector/J variant, with 10-30% less CPU per query. At 8:8 `select1` the best
  Connector/J variant is the one without a pool (192 k; the pooled best, `mysqlj-server`, is 180 k), and Hydrangea 144 k: this cell is bound by the wake-up latency of the VM (see
  below), where a thread blocked in `read()` on its own connection is the shape that does best.
* **Connector/J's default (client-side prepared statements) is the slowest JDBC variant** except for virtual threads: `useServerPrepStmts=true` is 1.3-1.6 times faster at 8:8 and
  a little faster at 8:64. Virtual threads on HikariCP are the slowest variant everywhere (27-72 k), as with pgjdbc.
* `hydrangea-block` (a platform thread per caller, `join()`) costs a second thread hand-off per query and is 2-4 times slower than the future chains at 8:8; at 8:64 it is still 1.3-1.6
  times ahead of the best Connector/J.

## Writes (queries per second; the server flushes every commit to the VM's disk)

| scenario | conns:callers | hydrangea | hydrangea-block | mysqlj | mysqlj-server | mysqlj-virtual | mysqlj-dedicated |
|---|---|---|---|---|---|---|---|
| insert | 8:8 | 1 525 (1 030-4 701) | 2 582 | 4 703 | 5 017 | 4 305 | 4 709 |
| insert | 8:64 | 4 887 (3 598-5 548) | 4 784 | 4 091 (425-4 543) | 1 162 (634-4 472) | 3 986 | - |
| insert | 32:32 | 9 335 | 8 960 | 8 741 | 7 958 | 7 915 | 9 200 |
| tx | 8:8 | 2 910 (2 612-3 107) | 3 112 | 3 060 | 2 661 (390-2 678) | 2 103 | 682 (619-2 981) |
| tx | 8:64 | 3 661 (3 311-4 414) | 3 346 | 2 742 | 2 281 | 2 910 | - |
| tx | 32:32 | 6 130 | 5 106 | 6 551 | 6 071 | 3 775 | 6 708 |

* **These numbers are not a comparison of clients.** With `innodb_flush_log_at_trx_commit=1` and `sync_binlog=1` every commit waits for two fsyncs on the virtualised disk of the
  Docker VM, which stalls at random: single runs of the same configuration differ by 3-10 times (`mysqlj-server` `tx` 8:8: 390, 2 678, 2 661; `hydrangea` `insert` 8:8: 1 030, 1 525,
  4 701; `mysqlj-dedicated` `tx` 8:8: 619, 682, 2 981), and the p99 of every library is tens to hundreds of ms. The medians are what they are; the ranges say they mean little.
* What can be said: at 8:64 Hydrangea's three runs (3.3-4.4 k `tx`, 3.6-5.5 k `insert`) are all above every Connector/J run (2.3-3.6 k `tx`, 0.4-4.9 k `insert`), with a p99 of
  30-105 ms against 350-900 ms and a quarter to a third of the CPU per statement; at 32:32 everything is within the noise. The PostgreSQL measurements with `fsync` on
  (`RESULTS-POSTGRES.md`, Linux durable writes) are the better source for what pipelining is worth when the server has to flush: the advantage shrinks to 1.3 times.
* A measurement of writes that means something needs a real disk (a Linux host, or the VM's disk with `innodb_flush_log_at_trx_commit=2` for a client-only comparison).

## Bulk loads and reads over one connection (Linux in Docker, 200 000 rows, chunks of 1 000, medians of 3 timed runs; `my-results/linux-bulk.txt`)

Every chunk is one transaction for every way (one commit per 1 000 rows, so the disk is paid the same by all). Program: `benchmark/src/mysql/MyBulkBench.java`, driver `my-bulk-docker.sh`.

| way | rows/s | CPU µs/row | what it is |
|---|---|---|---|
| `hydrangea-batch` | **40 059** | 8.2 | `executeBatch`: one PREPARE, the EXECUTEs written back to back |
| `mysqlj-batch` | 16 162 | 23.0 | `addBatch`/`executeBatch` with server-side prepared statements (one round trip per row: the driver does not pipeline) |
| `hydrangea-pipelined` | 38 589 | 11.7 | one `query` per row, a chunk in flight |
| `mysqlj-rewrite` | 149 706 | 1.6 | `rewriteBatchedStatements=true`: the driver turns the batch into multi-row INSERTs (client-side prepared statements) |
| `hydrangea-multirow` | 221 005 | 0.7 | one `INSERT ... VALUES (?,?,?),(?,?,?)...` per chunk |
| `mysqlj-multirow` | 230 495 | 0.8 | the same with Connector/J |
| `hydrangea-stream` | 1 047 884 / 1 205 415 | 0.4 / 0.3 | read everything back with `stream(1000)` (server cursor, `COM_STMT_FETCH`); two runs |
| `mysqlj-cursor` | 1 019 996 / 955 616 | 0.5 | `useCursorFetch=true`, fetch size 1 000; two runs |
| `hydrangea-whole` | 1 764 777 | 1.0 | one `query`, the result held whole |
| `mysqlj-whole` | 1 359 161 | 1.3 | the default: the result held whole |

* **Row-by-row batches**: Hydrangea's `executeBatch` is 2.5 times faster than Connector/J's server-prepared batch, because the EXECUTEs go out back to back instead of one round trip each,
  with a third of the CPU. It is still the slow way to load MySQL: **multi-row INSERTs are 5-6 times faster again**, for both libraries alike (221 k against 230 k rows/s, within noise), and
  Connector/J's `rewriteBatchedStatements` gets most of that automatically (150 k). Hydrangea has no equivalent switch: write the multi-row statement yourself, or accept the 40 k of a batch.
  There is no `LOAD DATA LOCAL INFILE` in the client (deliberately), which would be faster still.
* **Reads** are level: a stream and a cursor fetch both read about a million rows per second over one connection, holding one piece in memory; reading whole is 1.3-1.8 million, Hydrangea
  a little ahead (binary rows read lazily). Nothing here is a difference a user would notice.

## Soak and fuzz (`my-results/soak.txt`, `MyFuzzTest`)

`benchmark/src/mysql/MySoak.java`: 32 virtual-thread workers on a pool of 8 connections (statement cache 64, 300 distinct statements so it churns, connections replaced every 25 s and
pinged) picking at random from queries, inserts, batches inside transactions, streams, calls with a 50 ms limit that often expires (and kills), transactions with and without a deliberate
failure, and 20-120 KB blobs. 180 s: **74 413 operations, 0 unexpected errors, heap flat at 4 MB after GC, 24 threads throughout, at most 64 prepared statements on a connection** (the
cache size). 379 of the 7 437 timed-out calls had their `KILL QUERY` land on the next call of the connection (error 1317, counted apart: the kill is sent when the timed-out call is the one
the server is running, and the server may have just finished it), which is the documented limit of the time limit, the same as with PostgreSQL's cancel.

The first run found a defect the tests had not: **streams leaked prepared statements under concurrency** (2 045 on one connection, until the server refused with
`max_prepared_stmt_count`), because the connection became the stream's only once its cursor was open, so two streams prepared on the same connection at the same time overwrote each
other's claim. Fixed: a stream takes the connection when its statement is written. Running the soak with one kind of operation at a time (`only=`) is what isolated it.

`MyFuzzTest`: a fake server that logs the client in and then answers the query with rubbish (900 iterations: random bytes, a valid answer with bytes flipped, cut, repeated, random packets
with honest lengths, valid packets with lying lengths). Every iteration must end with a clean `MyException`. The first run found **140 cases of `StringIndexOutOfBoundsException` escaping
the parser** (the parser caught `ArrayIndexOutOfBoundsException` but not its sibling, raised by `new String(bytes, offset, length)` on a length that lies). Fixed; now 191 "malformed
packet", 65 other protocol errors, 547 connections lost, 91 answered, 6 server errors, no finding.

## The investigation of the 8:8 cell (`benchmark/my-results/linux-8x8-investigation.txt`)

The first measurement (`my-results/linux-before-fix-4loops.txt`, with the pool as it was and 4 event loops) had Hydrangea at 64 k `SELECT 1`/s at 8:8 against 204 k for Connector/J,
with the same CPU per query (22 against 21 µs): a latency problem, not a cost. Three causes, found by measurement; none of them is a per-query cost of the client:

1. **The pool handed the next query to another loop.** A caller's next query is made on the loop that completed the previous one, and `pickSlot` chose the least loaded connection,
   which was usually on another loop: a queue, a `selector.wakeup()` and a thread switch for 7 queries out of 8. **Fixed** in `MyPool.pickSlot` and `PgPool.pickSlot`: an idle
   connection of the loop the call is made on is preferred (`MyClient.onOwnLoop`). With that and one loop for each connection: 92 k → 175-179 k at 8:8, 22 → 15 µs of CPU per
   query. Verified by the `loop.tasks` metric: 39-63 cross-thread tasks per run, all of them the pool's maintenance timer.
2. **The VM sleeps its CPUs.** With few busy threads every round trip pays a wake-up: Connector/J with one dedicated connection needs 65 µs for a `SELECT 1` that takes 20 µs when
   8 threads are busy; keeping the CPUs busy with 4 spinning threads in another container brought its 4-connection run from 69 to 20 µs. So the low-concurrency cells of every
   client measure the environment.
3. **A loop driving exactly two connections of a thread-per-connection server is slow on this VM**: 13 k/s for the loop against 22 k/s with one connection and 30 k/s with four, for
   MySQL and PostgreSQL alike, with or without the pool (callers bound to their own `MyClient`), with or without a CPU limit on the server. It does **not** happen with an echo server
   and client both on `hydrangea-core` (two connections per loop give twice the throughput: 4 loops with 4, 8, 16 connections give 51 k, 101 k, 364 k), and not from macOS over a
   0.5 ms link. The client writes the two queries back to back and the two server threads wake together; what the kernel of the VM does then was not investigated. A spin before
   `epoll_wait` (tried, removed) helped only partly and burns CPU.

Consequences: the benchmarks use one loop for each connection (`loops=conns`), and a pool on a fast link with latency-bound work should be given as many loops as connections.
The same findings re-read the PostgreSQL `tx` 8:64 cell with `fsync` off, where pgjdbc + HikariCP was ahead with 4 loops: see the note in `RESULTS-POSTGRES.md`.

## What was not measured

* MariaDB 11 and ProxySQL 2.7: the whole feature set runs on both (`MyCompatTest`), nothing measured. Through ProxySQL the client must run with `withMaxInFlight(1)` (ProxySQL drops a
  client that pipelines), so the 8:64 advantage above does not exist there.
* `LOAD DATA LOCAL INFILE` (not implemented), TLS throughput (TLS works: 3 tests), the cost of `KILL QUERY` under load, reconnection under load.
* Writes on a real disk (see above).
