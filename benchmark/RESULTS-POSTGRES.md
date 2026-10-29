# PostgreSQL: Hydrangea client vs pgjdbc

`hydrangea-postgres` (`PgPool`) against pgjdbc 42.7.8 (+ HikariCP 7.0.2) on the same PostgreSQL 16 (Docker, `hydrangea-postgres/src/test/resources/postgres/start-postgres.sh`,
fsync off), Java 25, macOS with 8 cores shared by the client JVM and the Docker VM. One library and scenario per JVM, 2 s warm-up and 5 s measured, 3 rounds with the
library order rotated, median of the 3. Tool: `benchmark/src/postgres/PgBench.java`, driver `benchmark/pg-bench.sh`, raw lines in `benchmark/pg-results/`.

Libraries: `hydrangea` (PgPool, callers are future chains, no thread per caller), `hydrangea-block` (PgPool, a platform thread per caller doing `query().join()`),
`pgjdbc` (HikariCP, a platform thread per caller, a connection taken from the pool for each query), `pgjdbc-virtual` (the same on virtual threads),
`pgjdbc-dedicated` (one connection per caller thread, no pool; only where callers = connections). Library defaults are kept: pgjdbc prepares on the server after
5 uses and uses binary format for some types; Hydrangea prepares on first use and uses text format. Every column of every row is read.

Scenarios: `select1` (`SELECT 1`), `point` (primary-key lookup in 100 000 rows), `rows` (100 rows by key range), `insert` (one row), `tx` (`UPDATE` + `SELECT` in a
transaction). `CPU µs/query` is the client JVM's process CPU time per completed query (the server is not included). Latency includes waiting for a connection.

### A. 8 connections, 8 callers (no queueing)

| scenario | library | queries/s | p50 µs | p99 µs | CPU µs/query | peak threads |
|---|---|---|---|---|---|---|
| select1 | hydrangea | 11159 | 557 | 2097 | 23.8 | 20 |
| select1 | hydrangea-block | 10615 | 589 | 2359 | 43.0 | 28 |
| select1 | pgjdbc | 10785 | 557 | 2490 | 27.5 | 17 |
| select1 | pgjdbc-virtual | 9166 | 589 | 4194 | 44.5 | 21 |
| select1 | pgjdbc-dedicated | 11484 | 524 | 2031 | 26.1 | 15 |
| point | hydrangea | 10410 | 589 | 3538 | 27.8 | 20 |
| point | hydrangea-block | 10772 | 589 | 2097 | 43.9 | 28 |
| point | pgjdbc | 11380 | 557 | 2031 | 28.3 | 17 |
| point | pgjdbc-virtual | 11042 | 557 | 1966 | 46.5 | 21 |
| point | pgjdbc-dedicated | 10862 | 557 | 2097 | 27.0 | 15 |
| rows | hydrangea | 7308 | 819 | 5505 | 55.1 | 20 |
| rows | hydrangea-block | 7561 | 819 | 4718 | 73.3 | 28 |
| rows | pgjdbc | 7453 | 786 | 4980 | 52.2 | 17 |
| rows | pgjdbc-virtual | 7347 | 819 | 5767 | 73.3 | 21 |
| rows | pgjdbc-dedicated | 6651 | 851 | 6291 | 54.4 | 15 |
| insert | hydrangea | 10091 | 622 | 2883 | 27.3 | 20 |
| insert | hydrangea-block | 10169 | 622 | 2621 | 44.9 | 28 |
| insert | pgjdbc | 11110 | 589 | 2097 | 28.6 | 17 |
| insert | pgjdbc-virtual | 10153 | 622 | 2228 | 47.3 | 21 |
| insert | pgjdbc-dedicated | 10289 | 589 | 2752 | 28.2 | 15 |
| tx | hydrangea | 2534 | 2621 | 11010 | 101.6 | 20 |
| tx | hydrangea-block | 2579 | 2752 | 9961 | 120.6 | 28 |
| tx | pgjdbc | 3063 | 2228 | 8912 | 100.5 | 17 |
| tx | pgjdbc-virtual | 3218 | 2097 | 10485 | 148.6 | 21 |
| tx | pgjdbc-dedicated | 3364 | 2031 | 8126 | 95.6 | 15 |

### B. 8 connections, 64 callers (callers wait for a connection)

| scenario | library | queries/s | p50 µs | p99 µs | CPU µs/query | peak threads |
|---|---|---|---|---|---|---|
| select1 | hydrangea | 61273 | 819 | 4456 | 6.3 | 20 |
| select1 | hydrangea-block | 48751 | 1048 | 6291 | 18.5 | 84 |
| select1 | pgjdbc | 10793 | 557 | 109051 | 48.3 | 73 |
| select1 | pgjdbc-virtual | 8528 | 753 | 134217 | 71.9 | 21 |
| point | hydrangea | 52782 | 983 | 5505 | 7.4 | 20 |
| point | hydrangea-block | 46708 | 1048 | 6029 | 19.5 | 84 |
| point | pgjdbc | 10569 | 589 | 75497 | 53.3 | 73 |
| point | pgjdbc-virtual | 10361 | 655 | 104857 | 65.8 | 21 |
| rows | hydrangea | 15131 | 3014 | 16777 | 35.8 | 20 |
| rows | hydrangea-block | 13841 | 3407 | 16252 | 50.5 | 84 |
| rows | pgjdbc | 6853 | 851 | 218103 | 78.3 | 73 |
| rows | pgjdbc-virtual | 6931 | 917 | 192937 | 98.5 | 21 |
| insert | hydrangea | 45384 | 1114 | 7077 | 7.9 | 20 |
| insert | hydrangea-block | 38485 | 1245 | 8388 | 20.3 | 84 |
| insert | pgjdbc | 9717 | 622 | 121634 | 48.7 | 73 |
| insert | pgjdbc-virtual | 10043 | 688 | 109051 | 67.0 | 21 |
| tx | hydrangea | 2619 | 20971 | 60817 | 95.8 | 20 |
| tx | hydrangea-block | 2582 | 22020 | 54525 | 113.5 | 84 |
| tx | pgjdbc | 3408 | 2097 | 402653 | 122.8 | 73 |
| tx | pgjdbc-virtual | 3152 | 2228 | 318767 | 185.4 | 21 |

### C. 32 connections, 32 callers

| scenario | library | queries/s | p50 µs | p99 µs | CPU µs/query | peak threads |
|---|---|---|---|---|---|---|
| select1 | hydrangea | 21580 | 1114 | 8126 | 19.3 | 20 |
| select1 | hydrangea-block | 20207 | 1179 | 8126 | 33.4 | 52 |
| select1 | pgjdbc | 21136 | 1114 | 8912 | 24.3 | 41 |
| select1 | pgjdbc-virtual | 20226 | 1179 | 8388 | 33.3 | 21 |
| select1 | pgjdbc-dedicated | 22517 | 1114 | 6815 | 25.6 | 39 |
| point | hydrangea | 20538 | 1179 | 8388 | 20.3 | 20 |
| point | hydrangea-block | 18243 | 1245 | 11010 | 35.3 | 52 |
| point | pgjdbc | 18177 | 1245 | 9961 | 27.1 | 41 |
| point | pgjdbc-virtual | 15346 | 1376 | 11534 | 35.9 | 21 |
| point | pgjdbc-dedicated | 17344 | 1245 | 11010 | 25.8 | 39 |
| rows | hydrangea | 9306 | 2359 | 15204 | 51.0 | 20 |
| rows | hydrangea-block | 10413 | 2228 | 12582 | 62.3 | 52 |
| rows | pgjdbc | 10106 | 2228 | 14155 | 53.7 | 41 |
| rows | pgjdbc-virtual | 10145 | 2228 | 13107 | 66.2 | 21 |
| rows | pgjdbc-dedicated | 7910 | 2490 | 23068 | 52.2 | 39 |
| insert | hydrangea | 16260 | 1245 | 10485 | 19.8 | 20 |
| insert | hydrangea-block | 17365 | 1376 | 9437 | 28.6 | 52 |
| insert | pgjdbc | 20733 | 1245 | 6815 | 23.5 | 41 |
| insert | pgjdbc-virtual | 19816 | 1245 | 7340 | 34.2 | 21 |
| insert | pgjdbc-dedicated | 19531 | 1245 | 9961 | 23.0 | 39 |
| tx | hydrangea | 5125 | 4980 | 18874 | 72.3 | 20 |
| tx | hydrangea-block | 5209 | 4980 | 18874 | 93.2 | 52 |
| tx | pgjdbc | 6261 | 4063 | 18874 | 79.1 | 41 |
| tx | pgjdbc-virtual | 5653 | 4456 | 18874 | 114.8 | 21 |
| tx | pgjdbc-dedicated | 5986 | 4063 | 16777 | 81.4 | 39 |

## What the numbers say

* **With as many connections as callers (A, C) the clients are level.** Throughput is within the noise of the runs (single runs vary by 10-40%, see the raw files),
  and on loopback through Docker a query costs about 0.5 ms of round trip, so these rows measure the network path more than the client. CPU per query is similar:
  `hydrangea` 24-28 µs against `pgjdbc` 27-28 µs for `select1` and `point`, 55 against 52 for `rows`. Hydrangea is **not** faster than pgjdbc when each connection has one
  query at a time.
* **When callers outnumber connections (B), Hydrangea is 4-6 times faster** for `select1`, `point`, `insert` (61 k against 10.8 k, 53 k against 10.6 k, 45 k against 9.7 k
  queries/s) and 2.2 times for `rows`, with a p99 of 4-7 ms against 75-220 ms and 2-8 times less CPU per query (2.2 times for `rows`). The cause is **pipelining**, not the event loop: each
  Hydrangea connection takes the queued queries and writes them together, while a JDBC connection (taken from the pool for one query) runs one at a time. `hydrangea-block`, with
  platform threads instead of futures, gets nearly the same throughput (49 k, 47 k, 38 k), which shows the threading model is not what makes the difference. pgjdbc can
  pipeline only through its batch API, which was not used here. The p99 of 75-400 ms for pgjdbc is queueing for a pool connection among 64 threads.
* **Transactions were where Hydrangea was behind, until `BEGIN` was sent with the first query.** The first run measured `tx` 17-23% slower than pgjdbc
  (2.5 k against 3.1 k, 5.1 k against 6.3 k, 2.6 k against 3.4 k queries/s) because `PgPool.transaction` waited for `BEGIN` before it ran the body: 4 round trips
  against the 3 of pgjdbc (which, as far as I know, sends `BEGIN` with the first statement). `BEGIN` now rides in the same batch as the first query, before its `Sync`
  (if it fails the server skips that query), and a body that sends no query sends no `BEGIN` and no `COMMIT`. Re-measured in a second session (`pg-results/tx-after-begin-fix.txt`,
  3 rounds): Hydrangea 2.9 k / 2.9 k / 5.9 k queries/s (8:8, 8:64, 32:32) against pgjdbc 2.9 k / 2.9 k / 4.8 k, with 20-40% less CPU per query. The numbers of the two sessions
  cannot be compared directly (pgjdbc itself moved from 3.1 k to 2.9 k and from 6.3 k to 4.8 k, probably the state of the table and the machine), so the claim is only that
  in the same session Hydrangea is now level with pgjdbc at 8 connections and about 20% ahead at 32; Hydrangea's own rate rose 14-15% in the two configurations without queueing,
  about what one round trip of four would give.
* `hydrangea-block` and `pgjdbc-virtual` spend 1.5-2 times the CPU per query of their non-blocking / platform-thread counterparts in A (43 against 24-28 µs): handing a
  result from the event loop (or a platform thread) to a waiting thread costs more than the query itself at this size.

## Limits of this measurement

* One machine: the client, Docker's VM and PostgreSQL share 8 cores, and the network is loopback through Docker. A real network adds latency per round trip, which
  favours pipelining more, and the server and client no longer compete for CPU.
* `insert` ran with fsync off; with durable commits the server dominates and the clients would be even closer.
* Tables A-C were measured when Hydrangea used the text format only; binary transfer was added later and changes nothing for these queries (see "Binary transfer" below). Not measured with wide rows or large values.
* Measured later, further down: TLS, COPY, bulk insert, a server 12 ms away, a soak test, Linux in Kubernetes. Still not measured: r2dbc-postgresql, other pool sizes, Linux with durable writes (`fsync` on), and a macOS re-run of tables A-C on the current code.

## Over a network: a PostgreSQL 17.5 about 12 ms away

The same clients against an existing PostgreSQL 17.5 on another machine of the company network (round trip of `SELECT 1` about 11.8 ms, Hydrangea logged in with SCRAM-SHA-256),
a database shared with other users, so only read-only scenarios without tables were used (`calc` and `series`: the shapes of `point` and `rows` computed from `generate_series`; plus
`select1`), no table was created and nothing was written. 3 rounds, library order rotated, 5 s measured (`pg-bench.sh` with `HOST`, `PORT`, `SCENARIOS`, `EXTRA`; raw lines in
`pg-results/remote-round*.txt`). Median of 3. With a round trip this long every row is bounded by `callers / 12 ms`, and the noise is large (the ranges of the 3 runs are in the raw files).

| connections:callers | scenario | hydrangea | hydrangea-block | pgjdbc | pgjdbc-virtual | pgjdbc-dedicated |
|---|---|---|---|---|---|---|
| 8:8 | select1 | 624 | 688 | 615 | 648 | 530 |
| 8:8 | calc | 580 | 627 | 595 | 647 | 635 |
| 8:8 | series (100 rows) | 606 | 613 | 549 | 560 | 572 |
| 8:64 | select1 | **4686** | 5453 | 545 | 600 | - |
| 8:64 | calc | **4474** | 3981 | 601 | 559 | - |
| 8:64 | series (100 rows) | **1974** | 1854 | 532 | 609 | - |
| 32:32 | select1 | 2025 | 2004 | 1476 | 1449 | 2292 |
| 32:32 | calc | 1792 | 1266 | 1365 | 1324 | 1583 |
| 32:32 | series (100 rows) | 1832 | 1686 | 1190 | 1254 | 1599 |

(queries per second.) CPU µs per query of the client JVM, `select1`: 107 / 149 / 255 / 249 / 201 at 8:8, 23 / 37 / 303 / 372 at 8:64, 42 / 47 / 194 / 280 / 48 at 32:32.

* **8:8 (a connection for every caller): all the same**, about 8 / 12 ms = 650 queries/s; the network round trip is the whole cost. CPU per query is 2-3 times lower for Hydrangea,
  but at 600 queries/s that is 0.1 ms of CPU per query against 0.25 ms, small in absolute terms.
* **8:64: Hydrangea is 8-9 times faster** (4.7 k against 0.55 k queries/s for `select1`) because it keeps 64 queries in flight over 8 connections (about 64 / 12 ms = 5.3 k at best), where
  JDBC keeps 8. pgjdbc's p99 was 2.3-3.2 s against Hydrangea's 40-90 ms: 64 threads waiting for 8 pool connections. The longer the round trip, the more pipelining is worth.
* **32:32:** Hydrangea is level with pgjdbc without a pool (`pgjdbc-dedicated`, one connection per thread) and 1.2-1.5 times ahead of pgjdbc with HikariCP, within the noise of single runs
  (ranges as wide as 341-2046 queries/s for one library).
* Not tested here: writes (`insert`, `tx`), because the database is shared; so the transaction change was measured only on the local container.

A note on this network: a first big upload on a fresh connection to this server often stalls for 6-10 seconds, for both clients: with 12 new connections each sent 1 MB, 7 of 12
(Hydrangea) and 5 of 12 (pgjdbc) took 6-10 s and the others 0.12-0.27 s; on a warmed connection both take 0.11 s per MB. It is a property of the path to that server, not of either
client. The test `largeResultsAndLargeValues` therefore has a 120 s timeout when run with `-Dpg.remote=true`.

## TLS and COPY

Same machine and container image as above (PostgreSQL 16 in Docker, loopback), the TLS container of `start-postgres.sh` serving the runs with and without TLS so that only TLS differs
(both libraries verify the certificate against the host name; TLS 1.3). 3 interleaved rounds; raw lines in `pg-results/tls.txt` and `pg-results/copy.txt`; tools `PgBench` (`tls=/path/ca.pem`) and `PgCopyBench`.

### What TLS costs

| connections:callers | scenario | library | TLS off: queries/s, CPU µs/query | TLS on |
|---|---|---|---|---|
| 8:8 | select1 | hydrangea | 8107, 22.4 | 8199, 26.9 |
| 8:8 | select1 | pgjdbc | 7820, 31.5 | 8177, 28.1 |
| 8:8 | rows (100 rows) | hydrangea | 5070, 47.2 | 5651, 56.5 |
| 8:8 | rows | pgjdbc | 5348, 62.7 | 5142, 60.7 |
| 8:64 | select1 | hydrangea | 38974, 5.5 | 29730, 9.1 |
| 8:64 | select1 | pgjdbc | 7369, 51.2 | 8588, 53.7 |
| 8:64 | rows | hydrangea | 11081, 30.1 | 10941, 40.4 |
| 8:64 | rows | pgjdbc | 4864, 92.9 | 3446, 108.1 |

* Where the round trip is the limit (8:8) TLS changes nothing measurable in throughput; the CPU per query of Hydrangea rises by about 4-9 µs (+20%), within the noise for pgjdbc.
* Where Hydrangea is limited by its own CPU (8:64 `select1`, 39 k queries/s) the same 3.6 µs per query shows as a 24% lower rate (30 k). It is still 3.5 times pgjdbc with TLS (8.6 k).
* pgjdbc's p99 at 8:64 stays at 240-520 ms with and without TLS (the pool queue).

### COPY of 1 million rows (53 MB), one connection

| | Hydrangea | pgjdbc `CopyManager` |
|---|---|---|
| COPY FROM STDIN, plain | 101 MB/s (runs 95-107) | 69 MB/s (65-98) |
| COPY FROM STDIN, TLS | 88 MB/s (69-95) | 96 MB/s (80-97) |
| COPY TO STDOUT, plain | 65 MB/s (52-87) | 76 MB/s (73-82) |
| COPY TO STDOUT, TLS | 82 MB/s (76-89) | 79 MB/s (66-81) |

The two are level: the order of the winner changes between rows and the ranges of single runs overlap, so no difference should be read into these numbers. The server and the Docker network
are the limit at about 100 MB/s. Process CPU for the copy is similar (0.08 s for both on copy-in; 0.15-0.18 s against 0.14-0.16 s on copy-out, where Hydrangea hands every row to the sink as its own
`byte[]`).

Not measured: COPY with binary format, many small copies, COPY over a real network (the remote server of the previous section is shared, so no COPY of that size was sent to it; the COPY tests were run
there with temporary tables and passed).

## Bulk insert: batch, array parameters, COPY

`PgBatchBench`: 200 000 rows (`id int, name text, n bigint`) into an unlogged table over **one connection**, local PostgreSQL 16 in Docker (fsync off), the first of 6 runs
discarded, median of the rest, 3 JVMs per way in rotating order (`pg-results/batch.txt`, `pg-results/unnest.txt`). `chunk` = 1000 rows per batch or per statement.

| way | rows/s (median, range of the 3 JVMs) | client CPU µs/row |
|---|---|---|
| `hydrangea-pipelined`: one `query()` per row, all in flight | 45 000 (44 161-45 540) | 6.2 |
| `pgjdbc-batch`: `addBatch`/`executeBatch`, the driver's default | 141 000 (121 978-143 202) | 1.0 |
| `hydrangea-batch`: `executeBatch`, one Sync per chunk | 235 000 (231 858-238 695) | 0.9 |
| `hydrangea-unnest`: `INSERT ... SELECT FROM unnest($1::int[], $2::text[], $3::bigint[])` per chunk | 404 000 (320 441-463 049, 6 JVMs) | 0.75 |
| `pgjdbc-rewrite`: `reWriteBatchedInserts=true` | 494 000 (393 265-505 866) | 0.7 |
| `pgjdbc-unnest`: the same statement with `createArrayOf` | 509 000 (461 553-621 766, 6 JVMs) | 0.3 |
| `hydrangea-copy` | 2 031 000 (1 891 661-2 062 725) | ~0 |
| `pgjdbc-copy` (`CopyManager`) | 2 335 000 (2 300 426-3 049 088) | ~0 |

* `executeBatch` is **1.7 times faster than pgjdbc's default batch**, and a batch is 5 times faster than the same inserts sent one `query()` at a time: the per-row cost is the server's
  (a `Sync`, a commit and a `ReadyForQuery` for each statement), not the client's.
* **pgjdbc was faster than Hydrangea where it could use a better statement**: `reWriteBatchedInserts` is 2 times our `executeBatch`, and with `unnest` pgjdbc was 1.25-1.65 times faster in the
  first measurements. The next section shows how that gap was closed for `unnest` (it was the cost of building arrays, not their text format); the `executeBatch` gap to the rewrite stays.
* **COPY is the way to load many rows** with either library (2.0-2.3 million rows/s, pgjdbc a little ahead, the ranges overlap).
* Not measured: batches over a real network (where the round trips saved by a batch matter more), updates and deletes by batch, wider rows.

## Soak test and parser fuzz

`PgSoak` (`benchmark/src/postgres/PgSoak.java`) runs a mixed load for minutes: 32 virtual threads over a pool of 8 connections with a statement cache of 64, picking at random from queries over 300 different SQL
strings (more than the cache holds), inserts with array columns, batches, small COPYs, streams, queries with a 50 ms time limit that often expires (so cancels fly), transactions with and without a deliberate
failure (`transactionWithRetry`), and `= ANY($1)` queries; pool lifetime 25 s and ping 5 s so connections are replaced during the run. Last run, 180 s: **117 421 operations, 0 unexpected errors**, heap
after a GC flat at 2-3 MB from the first minute to the last, threads constant (18), 9-13 server sessions (the pool's 8, replaced now and then, and the probe), prepared statements on a connection at most 64
(the cache size, so none leaks on the server). 542 operations (0.5%) received the documented retriable error of a cancel meant for another query that cannot be retried transparently (a COPY, a stream, a
transaction); 8 283 operations were timeouts on purpose.

The soak found four defects that no test had, all fixed and each with a test that fails without the fix (`PgPoolRaceTest`, `PgWireTest`):

1. **A plain call could land in the middle of a transaction.** The pool chose a connection, a transaction took it before the plain query was queued, and the plain query, queued right after the
   transaction's `BEGIN`, carried it; when it failed it aborted the other caller's transaction (SQLSTATE 25P02). Fixed with a handshake per connection between choosing and queuing (`dispatching`) and
   the reservation of a transaction.
2. **A cancel that reached the wrong query** (the timed-out query finished at the same moment) failed it with an error that said nothing useful. It is now run again by the client when that cannot change
   anything (outside a transaction, nothing queued behind it that begins or ends a transaction: my first version retried without that check and moved a query into another caller's transaction, which
   made the soak worse before better) and otherwise reported as a retriable error that names the cause; `transactionWithRetry` also retries it.
3. **Retrying the query that carried a transaction's `BEGIN` dropped the `BEGIN`** when the cancel had hit the `BEGIN` itself, so the query ran outside the transaction. The retry now carries it again.
4. **A cancel that hit the `COMMIT` or `ROLLBACK`** that ended a transaction made it fail while the transaction stayed open, and the pool handed that connection to the next caller (25P02 for its first queries).
   The client now sends the ending statement again, and the pool closes (and replaces) any connection that is not outside a transaction when a transaction ends. Without both fixes the regression test sees
   384 such errors in 8 s; with them none.

Without timeouts (hence without cancels) the same mix ran 338 000 and 249 000 operations with no error at all before the last two fixes, which is how the cause was narrowed down.

`PgFuzzTest` feeds 900 connections random bytes, valid answers with bytes flipped, cut, repeated or interleaved, random sequences of message types and valid messages with false lengths (fixed seeds):
every query ended, 0 hung. It found that a message body shorter than its own counts raised `BufferUnderflowException` out of the parser (the client survived by closing the connection, but logged a stack
trace for each and called it an "internal error"); every such case is now a `PgException` "protocol error: malformed 'X' message". Outcomes of the 900: 593 other protocol errors, 222 lost connections,
34 malformed messages, 48 valid answers.

## Binary transfer, and what the profile said

`PgConfig.withBinaryTransfer` (on by default): once the server has described a statement, parameters whose Java value has a binary form for the type the server gave them are sent in binary (integers, booleans, floats,
`bytea` as its bytes, `uuid`, `numeric`, dates and times, and arrays of these), and results are requested in binary for the columns where the text form can be rebuilt exactly (booleans, integers, `numeric`, `uuid`, `bytea`, arrays
of them), so that nothing a caller can see depends on the format: floats, dates, times, json and the rest stay text, because a Java double prints differently from PostgreSQL's and a `timestamptz` depends on the session's
zone. The first run of a statement is always text (its types are not known yet). `PgBinaryTest` runs queries over several types, values and shapes (the edge values of every integer type, `numeric` from 0.00 to 1e1000 and NaN and
Infinity, `bytea` with zeros and of 300 kB, arrays with NULLs, quotes, several dimensions, lower bounds other than 1) on a client with binary transfer three times and on one without it once, and requires every row to be equal under
`get`, `getString` and `isNull`; with a wrong sign in the `numeric` decoder or the wrong byte order for `int8` it fails.

**The first measurement said binary did not help** (`pg-results/binary-bulk-before.txt`, `binary-query.txt`, 3 JVMs each, median):

| bulk insert, 200 000 rows, one connection | binary off | binary on |
|---|---|---|
| `hydrangea-unnest` | 471 k rows/s, 0.8 µs CPU/row | **384 k, 0.9** |
| `hydrangea-batch` | 262 k | 251 k |
| `pgjdbc-unnest` (for reference) | 634 k, 0.3 µs | |

and for queries returning rows (`point`, `rows`, 8 connections, 8 or 64 callers) binary on and off were within noise of each other (57 k against 59 k queries/s for `point` at 8:64, CPU 6.4 against 6.6 µs).
The guess that the text format was the bottleneck was wrong. A JFR of `hydrangea-unnest` showed where the client CPU went: building the text literal of each array on the caller's thread **even when the query would go in binary**
(twice the work), `itemsOf` copying every list three times (`ArrayList.grow`), and a `ByteBuffer` and a `byte[]` allocated for every element of a binary array.

Three changes, each from the profile: the text of a list or array is built only if the statement still needs it (`PgTypes.LAZY`, with a copy of the parameter taken at the call so that a caller who reuses its list or
`byte[]` right after `query()` returns cannot change what is sent, which `aCallerMayReuseItsListsArraysAndBytesTheMomentTheCallReturns` checks with 4 500 queued queries), lists are read without being copied, and
binary arrays are written straight into one buffer. After (`binary-bulk-after.txt`, 4 JVMs each, median):

| bulk insert, 200 000 rows, one connection | binary off | binary on |
|---|---|---|
| `hydrangea-unnest` | 557 k rows/s, 0.5 µs CPU/row | **645 k (625-669 k), 0.4** |
| `hydrangea-batch` | 270 k | 261 k |
| `pgjdbc-unnest` | 671 k (564-691 k), 0.25 µs | |
| `pgjdbc-rewrite` | 558 k, 0.6 µs | |

`hydrangea-unnest` is now within 4% of `pgjdbc-unnest` in rows per second (it was 40% behind) and ahead of pgjdbc's `reWriteBatchedInserts`; it still uses about 1.6 times the CPU per row of pgjdbc
(0.4 against 0.25 µs). The binary form is now 16% faster than the text form for this workload, where before it was slower; for queries returning a few integer columns it changes nothing measurable. `executeBatch` stays
at 260-270 k rows/s, half of pgjdbc's rewrite: the cost there is the server's per-statement work, which only a multi-row statement avoids, so for bulk loads the recommendation is `unnest` with array parameters (or COPY).
A multi-row rewrite of `executeBatch` was not built: it would lose the per-item counts, and `unnest` already beats it.

The soak test after these changes (120 s, 110 748 operations, 922 per second, against 652 per second before): 0 unexpected errors, heap flat at 3 MB, prepared statements at most 64 per connection.

## In Kubernetes: Linux, next to the database

The load generator ran as a Job in a Kubernetes pod (dev cluster, `eclipse-temurin:21-jre`, x86_64, **2 CPUs**, `benchmark/run-k8s-pg.sh`, `k8s/pg-load.yaml.tmpl`) against an existing PostgreSQL 17.5 on the same network:
`p50` of `SELECT 1` is about 0.1 ms. The database is shared with running applications, so only read-only scenarios that need no table were run (`select1`; `calc` and `series`: the shapes of `point` and `rows` computed from
`generate_series`), nothing was written, and never more than 32 connections were opened. 3 rounds, order rotated, 5 s measured per run, 126 runs, **0 errors**; median of 3; raw log `pg-results/k8s-pg.log`.
(Compare with the macOS tables above: there the round trip was 0.5 ms and the client shared 8 cores with Docker.)

| connections:callers | scenario | hydrangea q/s | hydrangea-block | pgjdbc (Hikari) | best pgjdbc variant | hydrangea / best | CPU µs/query: hydrangea / best | p99 ms: hydrangea / pgjdbc |
|---|---|---|---|---|---|---|---|---|
| 8:8 | select1 | 70 566 | 58 752 | 55 850 | 62 203 (`pgjdbc-dedicated`) | **1.1x** | 17 / 23 | 0.3 / 0.5 |
| 8:8 | calc | 65 697 | 55 697 | 55 506 | 60 445 (`pgjdbc-dedicated`) | **1.1x** | 18 / 24 | 0.3 / 0.5 |
| 8:8 | series | 33 556 | 29 892 | 25 529 | 29 035 (`pgjdbc-dedicated`) | **1.2x** | 41 / 52 | 0.6 / 0.9 |
| 8:64 | select1 | 180 033 | 109 781 | 30 531 | 39 353 (`pgjdbc-virtual`) | **4.6x** | 11 / 49 | 1.1 / 71.3 |
| 8:64 | calc | 161 872 | 99 100 | 29 446 | 37 187 (`pgjdbc-virtual`) | **4.4x** | 12 / 52 | 1.2 / 71.3 |
| 8:64 | series | 55 260 | 47 825 | 14 226 | 21 717 (`pgjdbc-virtual`) | **2.5x** | 36 / 88 | 7.3 / 142.6 |
| 32:32 | select1 | 99 908 | 72 649 | 26 036 | 49 020 (`pgjdbc-virtual`) | **2.0x** | 20 / 40 | 1.0 / 44.0 |
| 32:32 | calc | 95 456 | 71 580 | 25 029 | 49 815 (`pgjdbc-virtual`) | **1.9x** | 21 / 40 | 1.0 / 44.0 |
| 32:32 | series | 46 767 | 36 888 | 8 503 | 25 667 (`pgjdbc-dedicated`) | **1.8x** | 42 / 78 | 3.0 / 54.5 |

* **The pod has 2 CPUs and the client is the bottleneck**: 180 000 queries/s at 11 µs of CPU is 2 CPUs, and pgjdbc's 30 000 at 65 µs is 2 CPUs too. On a small pod what counts is the CPU each query costs,
  and Hydrangea costs 1.6-6 times less than pgjdbc with a pool (11-20 µs against 27-77 µs for `select1`; 36-42 against 67-235 for `series`) and 1.3-4.5 times less than pgjdbc's best variant.
* **Even with a connection for every caller (8:8) Hydrangea is ahead here** (+13% for `select1`, +16% for `series` over the best pgjdbc variant, and 1.2-1.3 times pgjdbc with a pool), unlike on macOS where
  the clients were level: with a 0.1 ms round trip and 2 CPUs the client's own cost shows. When callers outnumber connections (8:64) the gap is 2.5-4.6 times; with 32 connections for 32 callers 1.8-2 times.
* **pgjdbc with HikariCP is the slowest pgjdbc variant at 8:64 and 32:32** (p99 71 ms and 44 ms, 65-235 µs of CPU per query: 64 or 32 threads contending for a pool), `pgjdbc-virtual` and `pgjdbc-dedicated` do better. The
  comparison "best pgjdbc variant" is therefore the fair one; against the usual pooled setup the gap is larger.
* `hydrangea-block` (platform threads, `query().join()`) is 1.4-2.8 times pgjdbc's best variant at 8:64 and 32:32 and uses less CPU: the advantage is the client's efficiency as well as its pipelining.
* **Limits**: the 2-CPU limit decides these numbers (a bigger pod would raise the ceilings and could change the ratios); the database's hardware is unknown; read-only synthetic queries; no writes or transactions were
  measured here (they would create tables in a database that applications use); 5 s windows, 3 rounds.

## Linux, durable writes: client in a container, PostgreSQL with `fsync` on

Until now every write was measured with `fsync` off. Here the client runs in a Linux container (`eclipse-temurin:21-jre`, 4 CPUs, `benchmark/pg-bench-docker.sh`, `benchmark/pg-bulk-docker.sh`) on a Docker network next to a
`postgres:16` container (4 CPUs, aarch64 Linux in Docker Desktop's VM, shared_buffers 256 MB): **`pg-durable` with PostgreSQL's defaults (`fsync` and `synchronous_commit` on)** and, for comparison, `pg-fast`
(`fsync`, `synchronous_commit` and `full_page_writes` off). 3 rounds, order rotated, 5 s per run, median; raw lines in `pg-results/linux-{durable,fast}-{writes,bulk}.txt`. `insert` is one row into a logged table,
`tx` is `UPDATE` then `SELECT` in a transaction. Queries per second:

### With `fsync` on (the default of PostgreSQL)

| scenario | connections:callers | hydrangea | hydrangea-block | pgjdbc (Hikari) | pgjdbc-virtual | pgjdbc-dedicated | hydrangea / best pgjdbc | p99 ms: hydrangea / pgjdbc |
|---|---|---|---|---|---|---|---|---|
| insert | 8:8 | **21 775** | 17 667 | 19 104 | 18 530 | 20 279 | 1.07x | 0.9 / 1.2 |
| insert | 8:64 | **23 262** | 18 902 | 17 699 | 16 873 | - | 1.31x | 5.5 / 104.9 |
| insert | 32:32 | **38 684** | 32 783 | 38 270 | 36 180 | 38 005 | 1.01x | 3.3 / 3.3 |
| tx | 8:8 | **11 207** | 10 387 | 11 354 | 9 191 | 11 024 | 0.99x | 1.9 / 1.8 |
| tx | 8:64 | **11 651** | 10 949 | 10 855 | 9 334 | - | 1.07x | 12.6 / 167.8 |
| tx | 32:32 | **23 476** | 21 354 | 22 204 | 18 168 | 20 107 | 1.06x | 4.1 / 4.1 |

### With `fsync` off (a second container, for comparison)

| scenario | connections:callers | hydrangea | hydrangea-block | pgjdbc (Hikari) | pgjdbc-virtual | pgjdbc-dedicated | hydrangea / best pgjdbc |
|---|---|---|---|---|---|---|---|
| insert | 8:8 | **66 279** | 36 504 | 56 958 | 43 146 | 58 291 | 1.14x |
| insert | 8:64 | **147 306** | 104 493 | 74 936 | 41 299 | - | 1.97x |
| insert | 32:32 | **83 011** | 57 075 | 92 262 | 59 926 | 75 305 | 0.90x |
| tx | 8:8 | **23 739** | 19 407 | 19 837 | 13 114 | 18 756 | 1.20x |
| tx | 8:64 | **23 346** | 20 302 | 38 622 | 13 635 | - | 0.60x |
| tx | 32:32 | **31 358** | 33 638 | 37 655 | 21 608 | 32 381 | 0.83x |

* **When the server has to flush to disk the advantage of pipelining shrinks a lot**: for `insert` at 8 connections with 64 callers it is **1.3 times** with `fsync` on (23 k against 17.7 k queries/s) where it was 2 times with `fsync` off
  (147 k against 75 k) and 2.5-5 times in the earlier read-only measurements. With `fsync` on the group commit of the server is the limit (about 23 k single-row inserts per second on 8 connections, about 39 k on 32), and no client can
  push past it. With a connection for every caller (8:8, 32:32) all the clients are level for `insert` (21.8 k against 20.3 k; 38.7 k against 38.3 k), and Hydrangea still uses less CPU per insert (20-26 µs against 30-64 µs).
* **Transactions with `fsync` on are level to slightly ahead** (11.2 k against 11.4 k at 8:8, 11.7 k against 10.9 k at 8:64, 23.5 k against 22.2 k at 32:32: within the noise of single runs, 6-7% at most), with 59-89 µs of
  CPU per transaction against 90-139 µs and, at 8:64, a p99 of 12.6 ms against 168 ms.
* **With `fsync` off `tx` is where pgjdbc with a pool comes out ahead** at 8:64 (38.6 k against 23.3 k, in all three runs: 35-42 k against 23-25 k) and at 32:32 (37.7 k against 31.4 k), while Hydrangea is ahead at 8:8 (23.7 k against 19.8 k).
  **Fairness measured** (8 connections, 64 callers, 3 runs each, `caller_*` and `top10_share` in the RESULT lines, raw data `pg-results/linux-fairness-fsync-{off,on}.txt`): the benchmark now counts the operations each caller completed.
  Per caller, in one run (`tx`, `fsync` off): Hydrangea 2173-2195 (every caller within 1%); pgjdbc with HikariCP 857-3042 (best caller 3.5 times the worst; in the three runs min/median/max were 857/2738/3042,
  1613/3582/3984, 1496/3643/4193); pgjdbc-virtual 675-1834. The busiest 10% of callers complete 9.4% of the work for Hydrangea (the perfectly even value), 10.2-10.5% for pgjdbc with Hikari, 12.6-13% for pgjdbc-virtual.
  So the pool **is** unfair (a spread of 3-4 times between callers, p99 19-25 ms with a p50 of 0.11-0.14 ms), but **the earlier explanation was too strong**: no handful of threads monopolises the connections, and the throughput is
  not an artifact of what the statistics leave out, because q/s counts every completed operation.
  **Why Hydrangea was slower in this cell** (`pg-results/linux-tx-8x64-investigation.txt`, and the MySQL work that found the rest, `RESULTS-MYSQL.md`): three things, none of them a cost per transaction. Ruled out by measurement:
  the per-waiter acquire timer (turning it off changed nothing), CPU (58-64 µs per transaction against pgjdbc's 66-83 µs, loop threads 90% idle), and the event loop itself (an echo server
  and client both on `hydrangea-core` scale normally from one to two to four connections per loop).
  1. **The pool handed the next query to another loop.** A caller's next query ran on the loop that completed the previous one, and `pickSlot` chose the least loaded connection, which
     usually belonged to another loop: a queue, a `selector.wakeup()` and a thread switch for seven queries out of eight. Fixed: the pool now prefers an idle connection of the loop it is
     called on (`PgPool.pickSlot`, `MyPool.pickSlot`). With that and one loop for each connection, the MySQL client went from 92 k to 178 k `SELECT 1`/s at 8:8, level with Connector/J,
     and from 22 to 15 µs of CPU per query.
  2. **The VM sleeps its CPUs.** With few busy threads, every round trip costs a wake-up: Connector/J with one dedicated connection also needs 65 µs for a `SELECT 1` that takes 20 µs when
     eight threads are busy, and keeping the CPUs busy with four spinning threads in another container brought its 4-connection run from 69 to 20 µs. So the 8:8 and 4:4 cells of any
     client are latency-bound by the environment, not by the client.
  3. **A loop driving exactly two connections of a thread-per-connection server is slow on this VM**: 13 k/s for the loop against 22 k/s with one connection and 30 k/s with four, for
     both PostgreSQL and MySQL, with or without the pool, and not with the echo server (where two connections per loop give twice the throughput), and not from macOS over a 0.5 ms link.
     The client writes the two queries back to back and the two server threads wake together; what the kernel of the VM then does with them was not investigated further.
  So pgjdbc's lead in this cell came from the thread-per-connection shape matching the environment, and from the pool's hand-off, not from a cheaper transaction. **Defaults changed**:
  the benchmarks now use one event loop for each connection (`loops=conns`; the tables above were measured with 4 loops), and a pool should be given as many loops as connections when the
  link is fast and the work is latency-bound. The pinning experiment (client on CPUs 0-3, server on 4-7: pgjdbc + Hikari fell from 41 k to 17 k, Hydrangea stayed at 20 k) fits the same picture.
* **With `fsync` on the picture changes**: Hydrangea is level or ahead in throughput (`tx` 11.8-12.2 k against 10.5-11.4 k; `insert` 19-23 k against 16.6-19.3 k) and still even between callers (every caller within 1%), while pgjdbc
  with Hikari has 591-2290 completions per caller (3-4 times) and a p99 of 160-185 ms for `tx` and 100 ms for `insert`, against 5-14 ms for Hydrangea.

### Bulk insert on Linux (200 000 rows, one connection, an **unlogged** table, so `fsync` does not matter for these rows)

| way | | durable server: rows/s (range of 3 JVMs) | `fsync` off server | client CPU µs/row |
|---|---|---|---|---|
| hydrangea-pipelined | one `query()` per row | 70 k (68-70 k) | 74 k | 8.1 |
| pgjdbc-batch | `executeBatch`  default | 312 k (307-330 k) | 330 k | 0.5 |
| hydrangea-batch | `executeBatch` | 360 k (348-377 k) | 385 k | 0.5 |
| pgjdbc-rewrite | `reWriteBatchedInserts=true` | 1 121 k (1043-1142 k) | 1 110 k | 0.7 |
| hydrangea-unnest | `unnest` with array parameters | 1 459 k (1289-1472 k) | 1 390 k | 0.2 |
| pgjdbc-unnest | `unnest` with `createArrayOf` | 1 628 k (1465-1683 k) | 1 477 k | 0.2 |
| hydrangea-copy | COPY | 4 022 k (3955-4037 k) | 3 772 k | 0.0 |
| pgjdbc-copy | COPY (`CopyManager`) | 4 163 k (4144-4178 k) | 3 954 k | 0.0 |

* The bulk table is unlogged (it is the one of the earlier bulk measurements), so the two servers give the same numbers within noise; a bulk load into a logged table was not measured.
* On Linux and the Docker network everything is 2-3 times faster than on macOS, and the order is the same: COPY is far ahead (4.0 and 4.2 million rows/s, pgjdbc 3-4% ahead); then `unnest` (Hydrangea 1.46 million against
  pgjdbc 1.63 million: **pgjdbc about 10% ahead here, the ranges do not overlap**, where on macOS Hydrangea was 4% behind inside the noise); then pgjdbc's rewrite (1.12 million) and `executeBatch` (Hydrangea 360 k,
  **16% ahead of pgjdbc's 312 k**); a `query()` per row is the slowest (70 k) because of the server's per-statement work.
