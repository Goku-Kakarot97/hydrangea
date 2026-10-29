# Hydrangea

[![Maven Central](https://img.shields.io/maven-central/v/io.github.goku-kakarot97/hydrangea-core?label=Maven%20Central)](https://central.sonatype.com/namespace/io.github.goku-kakarot97) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE) ![Java](https://img.shields.io/badge/Java-21-orange)

A small non-blocking networking framework for Java 21 with **no third-party dependencies**: an event loop on `java.nio`, and on top of it servers for HTTP/1.1, HTTP/2, HTTP/3 (experimental), WebSocket, SSE and gRPC, and clients for HTTP, gRPC, Redis, Kafka, MQTT, AMQP, PostgreSQL and MySQL. Every protocol is its own Maven module; the whole thing is about the size of one mid-sized library.

Vietnamese documentation, with the full detail of every feature and every measurement: [`README-VI.md`](README-VI.md).

```java
try (var group = new EventLoopGroup()) {
    Server.bind(group, new InetSocketAddress(7000), Options.defaults(), () -> new Handler() {
        public void onRead(Connection c, ByteBuffer data) {
            c.write(ByteBuffer.allocate(data.remaining()).put(data).flip());   // echo; `data` is valid only inside this call
        }
    });
    Thread.currentThread().join();
}
```

## Why it exists

- **One thread model everywhere.** A connection belongs to one event-loop thread; handlers, pipeline stages and `CompletableFuture` continuations run there, so protocol code has no locks. The same rule holds for every client: a Redis, Kafka, PostgreSQL or MySQL connection is used from any number of threads, and what they send is **written together and answered in order** (automatic pipelining).
- **Honest numbers.** Every benchmark below was run against the established library for that protocol, on the same machine or the same Kubernetes pod, with fresh JVMs, rotating order and every reply verified. The result files say what was measured, what was not, and where Hydrangea loses.
- **Nothing to pull in.** The library modules import only the JDK (`ArchitectureTest` enforces it). `hydrangea-micrometer` is the one exception, for exporting metrics.

## Getting it

Maven Central, group `io.github.goku-kakarot97` (the Java packages stay `io.hydrangea.*`), one artifact per module (pick what you use; each pulls in `hydrangea-core`):

```xml
<dependency>
    <groupId>io.github.goku-kakarot97</groupId>
    <artifactId>hydrangea-http</artifactId>      <!-- or hydrangea-grpc, -redis, -kafka, -mqtt, -amqp, -postgres, -mysql, -sql, -micrometer -->
    <version>1.0.1</version>
</dependency>
```

Or straight from a tag through [JitPack](https://jitpack.io/#Goku-Kakarot97/hydrangea) (`com.github.Goku-Kakarot97.hydrangea:hydrangea-http:v1.0.1`).

## Modules

| Module | What it holds | Depends on |
|---|---|---|
| `hydrangea-core` | `EventLoop`, `EventLoopGroup`, `Server`, `Client`, `Connection`, `UdpSocket`, `BufferPool`, codecs, `Pipeline`/`Stage`, `TlsStage`, metrics, back-off | – |
| `hydrangea-http` | HTTP/1.1, HTTP/2 (HPACK) and HTTP/3 servers and clients, `HttpRouter`, WebSocket, Server-Sent Events, streaming uploads/downloads | core, quic |
| `hydrangea-grpc` | gRPC server and client (unary, server/client/bidi streaming, deadlines, interceptors) | core, http |
| `hydrangea-quic` | QUIC v1 server with its own minimal TLS 1.3 (experimental; the base of HTTP/3) | core |
| `hydrangea-redis` | Redis client: RESP2/RESP3, automatic pipelining, pub/sub, transactions, scripts, reconnect | core |
| `hydrangea-kafka` | Kafka client: idempotent producer, consumer groups, compression, no external dependency | core |
| `hydrangea-mqtt` | MQTT 3.1.1 and 5.0 client, also over WebSocket | core, http |
| `hydrangea-amqp` | AMQP 0-9-1 (RabbitMQ) client with publisher confirms and back-pressure | core |
| `hydrangea-postgres` | PostgreSQL client (wire protocol v3): pipelining, prepared-statement cache, binary transfer, COPY, streaming, batches, arrays, LISTEN/NOTIFY, SCRAM, TLS, reconnect, pool | core, sql |
| `hydrangea-mysql` | MySQL client (MySQL protocol): pipelining, server-side prepared statements (binary protocol), cursors, batches, `caching_sha2_password`, TLS, reconnect, pool; runs on MariaDB and behind ProxySQL | core, sql |
| `hydrangea-sql` | The interfaces the two database clients share (`SqlPool`, `SqlClient`, `SqlResult`, `SqlRow`, `SqlException`) | – |
| `hydrangea-micrometer` | `MetricsSink` for Micrometer (Prometheus, OTLP, Datadog, JMX…) | core |
| `hydrangea-examples` | Runnable examples and the cross-module tests | all |

## A taste of the API

**HTTP server with routing**

```java
HttpRouter router = new HttpRouter()
        .get("/users/{id}", req -> HttpResponse.ok("user " + req.pathInt("id")))     // a bad or missing id answers 400 by itself
        .post("/echo", req -> HttpResponse.ok(new String(req.body(), StandardCharsets.UTF_8)));
HttpServer.bind(group, new InetSocketAddress(8080), router);                        // HTTP/1.1 and h2c on the same port; the TLS overload adds HTTPS and h2
```

**HTTP client** (one `CompletableFuture` per request, connection pool per host, HTTP/2 multiplexing when the server speaks it)

```java
HttpClient client = new HttpClient(group, HttpClientConfig.defaults());
client.get("https://example.org/").thenAccept(resp -> System.out.println(resp.status()));
```

**PostgreSQL or MySQL through the shared interface** (placeholders are `?` for both; the PostgreSQL adapter turns them into `$n`)

```java
SqlPool db = PgPool.create(group, PgConfig.of(host, 5432, user, database).withPassword(pw), 8).join().asSql();
// or: MyPool.create(group, MyConfig.of(host, 3306, user, database).withPassword(pw), 8).join().asSql();

CompletableFuture<String> name = db.execute("SELECT name FROM users WHERE id = ?", 42).thenApply(r -> r.first().getString("name"));
CompletableFuture<Void> transfer = db.transaction(tx ->
        tx.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", amount, from)
          .thenCompose(r -> tx.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", amount, to))
          .thenApply(r -> null));
```

**Redis, Kafka, MQTT** follow the same shape: one connection shared by every thread, every call returns a `CompletableFuture`, callbacks run on the loop.

```java
RedisClient redis = RedisClient.connect(group, RedisConfig.of("localhost", 6379)).join();
redis.set("k", "v").thenCompose(ok -> redis.get("k"));

KafkaProducer producer = KafkaClient.create(group, KafkaConfig.of("localhost:9092")).producer(ProducerConfig.defaults());
producer.send("topic", key, value);                                   // idempotent, acks=all, batched, from any thread
```

Examples for every protocol are in `hydrangea-examples/src/main/java/io/hydrangea/examples` (`HttpExample`, `Http2Example`, `Http3Example`, `GrpcHelloExample`, `WebSocketChatExample`, `SseExample`, `RedisExample`, `KafkaExample`, `MqttExample`, `AmqpExample`, `PgExample`, `SqlExample`, …).

## Performance

All numbers are medians of several rounds with the order of the libraries rotating, one JVM per run, every response verified, 0 errors unless stated. "Local" is an 8-core Apple-silicon Mac; "Kubernetes" is a pod with the CPU limit given, load generated from a separate pod. Each line links to the file with the full tables, the method and the caveats. Where Hydrangea loses, it says so.

### Servers

| What | Against | Result | Details |
|---|---|---|---|
| HTTP/1.1 GET/POST/WebSocket, local, 64–1000 connections | Netty 4.2 | 1.06–1.12× the requests/s (e.g. 202k vs 187k GET, 194k vs 175k POST 1 KiB), 16 vs 17 µs CPU per request; download 1 MiB 9.8 vs 7.3 GB/s | [`RESULTS-HTTP-final.md`](benchmark/RESULTS-HTTP-final.md) |
| HTTP/1.1 on Kubernetes, 2 CPU | Netty 4.2 | level on GET, routing, WebSocket and large downloads (differences within round-to-round noise); +13% on POST with a body; Hydrangea's p99 is up to 15% higher | [`RESULTS-K8S.md`](benchmark/RESULTS-K8S.md) |
| HTTP/1.1 on Kubernetes, 0.5 CPU | Undertow 2.3 (tuned) | level (47–51k vs 47–48k GET); 1.7–2.1× when Undertow dispatches to its worker pool, which is how servlet code is usually written | [`RESULTS-K8S-HTTP-UNDERTOW.md`](benchmark/RESULTS-K8S-HTTP-UNDERTOW.md) |
| HTTP/2 (h2c) on Kubernetes, 0.5 CPU | Undertow 2.3 | 3.4–3.5× on GET (145k vs 43k), ~8× on POST echo 1 KiB; over TLS 2.8× and 4.9×. Undertow was left at its 64 KiB stream window, which is part of the gap | same file, [`RESULTS-K8S-TLS-UNDERTOW.md`](benchmark/RESULTS-K8S-TLS-UNDERTOW.md) |
| HTTP/2, local | Netty `Http2FrameCodec` | GET level (381k vs 385k, 541k vs 543k); POST echo 1 KiB 353k vs 276k; download 1 MiB 3.99 vs 2.24 GB/s; 3.1–3.6 vs 4.2–7.1 µs CPU per request | [`RESULTS-RERUN.md`](benchmark/RESULTS-RERUN.md) |
| gRPC server, local | grpc-java (same grpc-java client) | unary 16 B 277k vs 249k req/s, 4 KiB 174k vs 142k, server streaming 11.9k vs 7.4k streams/s; **64 KiB unary 6–8% slower** | [`README-VI.md`](README-VI.md) (Hiệu năng) |
| gRPC server on Kubernetes, 0.5 CPU | grpc-java 1.81 | 1.6–2.6× on small unary calls (CPU-bound: about half the CPU per call); level on 4 KiB | [`RESULTS-K8S-GRPC.md`](benchmark/RESULTS-K8S-GRPC.md) |
| TLS cost on 0.5 CPU | itself, cleartext | after profiling (one TLS record per flush, scratch buffers, a table-driven HPACK Huffman decoder) TLS costs 25–40% on small requests instead of 75–80%; the rest is the JDK's `SSLEngine` | [`RESULTS-K8S-TLS.md`](benchmark/RESULTS-K8S-TLS.md) |
| HTTP/3 vs HTTP/2 under 50 ms RTT and 1–3% loss | itself | connect one round trip faster (210 vs 260 ms); **throughput worse** (loss recovery of the QUIC sender is not tuned; 8 vs 120 MB/s on a clean network with a Python client) | [`RESULTS-K8S-H3-VS-H2.md`](benchmark/RESULTS-K8S-H3-VS-H2.md) |

### Clients

| What | Against | Result | Details |
|---|---|---|---|
| HTTP client, 1000 concurrent callers, 32 connections | `java.net.http.HttpClient` (platform and virtual threads) | same throughput (latency-bound), 17 threads instead of 1000, 0.11 vs 0.49–1.08 ms CPU per request; with 1000 connections 41.5k vs 8.7k (platform) / 22.9k (virtual) req/s; 10 000 callers: the thread-per-caller variants run out of memory | [`RESULTS-CONCURRENCY.md`](benchmark/RESULTS-CONCURRENCY.md) |
| Redis client | Jedis 5.2 | 32 threads sharing one connection: 2.1× (66k vs 31k ops/s, p99 halved); 1000–10 000 virtual threads on one connection: 10–20× with p99 in ms instead of seconds; pipeline of 1000: +26%; **one request at a time: 5% slower**, large multi-element replies 5–10% slower | [`RESULTS-REDIS.md`](benchmark/RESULTS-REDIS.md) |
| Kafka client | kafka-clients 3.9 | produce idempotent 3 partitions 1.93M vs 1.61M records/s at 40% of the CPU; 1000 virtual threads sharing a producer 2× with half the p99; consume 5.6M vs 3.9M records/s; **1 partition and the non-idempotent single-in-flight mode 13–17% slower** in that run (±40% run-to-run variance on Docker/macOS) | [`RESULTS-KAFKA.md`](benchmark/RESULTS-KAFKA.md) |
| MQTT client | Eclipse Paho 1.2.5 | QoS 1 end-to-end 3.8× (258k vs 67k msgs/s) at 13% of the CPU per message; QoS 2 4–5×; 100 connections 1.34× against Paho with `TCP_NODELAY` (9.4× as shipped) on 11 threads instead of 406; latency a few tens of µs lower; the broker (Mosquitto) was near its limit in the Hydrangea runs | [`RESULTS-MQTT.md`](benchmark/RESULTS-MQTT.md) |
| AMQP client | `amqp-client` 5.26 | 100 connections each publishing with confirms and consuming with acks: 1.5× (60k vs 39k msgs/s), a third of the CPU, 8 threads instead of 306; consume with acks 1.13×; publish with confirms 1.10×; level on unconfirmed publishes, 64 KiB messages and latency | [`RESULTS-AMQP.md`](benchmark/RESULTS-AMQP.md) |
| PostgreSQL client | pgjdbc 42.7 + HikariCP | more callers than connections (8:64): 2.5–4.6× on Kubernetes against a real PostgreSQL 17.5, with a quarter of the CPU and a p99 4× lower; one connection per caller: 1.1–1.2×; **with `fsync` on, writes are bound by the server's group commit and the advantage shrinks to 1.3×**; COPY level, `unnest` bulk insert **10% slower**, `executeBatch` +16% | [`RESULTS-POSTGRES.md`](benchmark/RESULTS-POSTGRES.md) |
| MySQL client | Connector/J 9.4 + HikariCP | 8 connections, 64 callers: 2–2.6× (188k vs 73k `SELECT 1`/s) with p99 and CPU 4× lower; one connection per caller: level (−25% to +9%); row-by-row batches 2.5× but **multi-row INSERTs are 5–6× faster for both and Connector/J builds them automatically** (`rewriteBatchedStatements`); reads through cursors level | [`RESULTS-MYSQL.md`](benchmark/RESULTS-MYSQL.md) |

Where the client advantage comes from: with more callers than connections, Hydrangea keeps several requests in flight on every connection (pipelining), while a JDBC or Jedis caller holds a connection for its whole round trip and the others queue in the pool. With a connection per caller the two models do the same work and the numbers are level; when the server is bound by disk or CPU, so is everyone.

The `RESULTS-MYSQL.md` investigation is worth reading before trusting any 8-connection number from a laptop: it found a real defect in the pool (fixed: a connection's next query went to another event loop), and two environment effects of the Docker VM that make low-concurrency cells measure the VM, not the client.

### Metrics overhead

Recording latency costs 39 ns per call when a sink is bound and 7 ns when none is (the hot path reads a clock and adds to a pre-allocated cell; a background thread folds the cells every 5 s). Measured end to end on Redis, HTTP server, HTTP client and Kafka: no difference beyond run-to-run noise ([`RESULTS-METRICS.md`](benchmark/RESULTS-METRICS.md)).

## Reliability

- **Soak tests**: 65 minutes of gRPC + HTTP/2 on Kubernetes (7.7 million responses, 554k connections opened and closed, cancellations and garbage from clients, 0 errors, live heap flat, descriptors and threads back to baseline: [`SOAK-H2.md`](benchmark/SOAK-H2.md)); 15 minutes of HTTP/1.1 + WebSocket + SSE with 44k clients vanishing mid-request ([`SOAK.md`](benchmark/SOAK.md)); mixed-load soaks of the PostgreSQL and MySQL clients. The soaks found real defects (a cancelled timer keeping connections alive, a pool/transaction race, a cancel hitting the wrong query, a stream leaking prepared statements); each has a regression test.
- **Fuzzing** in `mvn test`: frame and header decoders, HPACK, the protocol state machines through real sockets, the TLS stage, and the database clients against a server that sends rubbish (both found parser exceptions escaping as "internal error"; fixed).
- **Security tests** ([`SECURITY-TESTING.md`](SECURITY-TESTING.md)): HTTP request smuggling (CL+TE, conflicting CL, bare LF, obs-fold), header injection, WebSocket framing, and for the database clients a hostile server / man in the middle that asks for a local file (`LOAD DATA LOCAL`), switches to a clear-text password plugin, refuses TLS after it was requested, smuggles bytes before the TLS handshake, or fakes the SCRAM server signature. Two findings, fixed: PostgreSQL sent the password in clear text when the server asked without TLS, and MySQL sent it encrypted with a public key the server handed over (both now refuse by default).
- **Graceful shutdown** verified on Kubernetes with a real SIGTERM during a rolling restart.
- **On GitHub**: CI runs `mvn test` on every push (the tests that need a broker or a database skip themselves without one), CodeQL with the security-extended queries, and the OpenSSF Scorecard. Vulnerabilities: see [`SECURITY.md`](SECURITY.md).

## Metrics

`group.metrics()` counts connections, bytes, loop tasks and the longest a callback held a loop; every protocol adds its own (`http.server.duration`, `grpc.server.calls{method,code}`, `kafka.producer.records{outcome}`, `pg.queries{outcome}`, `pg.pool.waiting`, `my.query.duration`, `redis.commands.pending`, `mqtt.client.publish.duration`, …). Export with `group.metrics().bindTo(new MicrometerSink(registry))` from `hydrangea-micrometer`, or implement the four-method `MetricsSink` for anything else; values are read at scrape time, the loops pay nothing.

## Status and limits

- Java 21, Maven. Library modules have no dependencies; tests use JUnit, benchmarks use the compared libraries.
- **Experimental**: QUIC and HTTP/3 (no 0-RTT, migration, stateless reset or client; loss recovery not tuned: see the HTTP/3 results above).
- **HTTP**: client has no request pipelining, SOCKS or TLS to a proxy; server does not compress responses.
- **Databases**: not JDBC. PostgreSQL has no replication, composite/range types (returned as text), client certificates or Kerberos; MySQL has no `LOAD DATA LOCAL` (deliberately: the protocol lets the server ask for any file) and no multi-statement text queries. Both are new (weeks old) and have been run against real servers in Docker and Kubernetes, not yet in production by anyone.
- **Handlers run on the event loop**: a blocking or slow handler stalls every connection of that loop. Move heavy work to an executor.
- The benchmarks are what they say and nothing more: one machine or one cluster, specific versions, specific configurations. Expect different ratios elsewhere.

## Build and run

```
mvn test                                   # unit, protocol and fuzz tests; integration tests skip when their Docker service is absent
java -cp target/classes io.hydrangea.examples.EchoServer 7000
java -cp target/classes io.hydrangea.examples.Http2Example 8080
curl --http2-prior-knowledge http://localhost:8080/version
```

Scripts to start the Docker services the integration tests and benchmarks use are next to the tests (`hydrangea-*/src/test/resources/*/start-*.sh`); the benchmark drivers are in `benchmark/` (`run-http.sh`, `pg-bench-docker.sh`, `my-bench-docker.sh`, `mqtt-bench.sh`, `amqp-bench.sh`, …) and the Kubernetes manifests in `k8s/`.

## Reading the code

[`READING-GUIDE.md`](READING-GUIDE.md) is a path from the outside in (NIO, the event loop, a pipeline, one protocol); [`HTTP-CLIENT-READING.md`](HTTP-CLIENT-READING.md) follows one request through the HTTP client; [`NIO-INTRO.md`](NIO-INTRO.md) is the background; [`REVIEW.md`](REVIEW.md) explains the design choices against XNIO.

## License

[Apache License 2.0](LICENSE).
