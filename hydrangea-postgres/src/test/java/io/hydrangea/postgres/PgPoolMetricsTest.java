package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** The pool's own numbers: callers waiting, how long, waits that ran out, connections replaced, pings that failed. */
class PgPoolMetricsTest {

    static final class Capture implements MetricsSink {
        final Map<String, DoubleSupplier> meters = new HashMap<>();

        private static String key(String name, String[] tags) {
            return tags.length == 0 ? name : name + "{" + String.join(",", tags) + "}";
        }

        @Override
        public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
            meters.put(key(name, tags), total);
        }

        @Override
        public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
            meters.put(key(name, tags), value);
        }

        @Override
        public Counter counter(String name, String unit, String... tags) {
            return NOOP.counter(name, unit, tags);
        }

        @Override
        public Timer timer(String name, String... tags) {
            return NOOP.timer(name, tags);
        }

        double get(String key) {
            DoubleSupplier s = meters.get(key);
            assertNotNull(s, "no meter " + key + " among " + meters.keySet());
            return s.getAsDouble();
        }
    }

    static void eventually(java.util.function.BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    @Test
    void waitingCallersAreCountedAndTimed() throws Exception {
        Pg.require(Pg.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            PgPoolOptions options = PgPoolOptions.defaults().withAcquireTimeout(Duration.ofMillis(300));
            try (PgPool pool = PgPool.create(group, Pg.config(), 1, options, null).get(10, TimeUnit.SECONDS)) {
                CompletableFuture<Void> gate = new CompletableFuture<>();
                CompletableFuture<Integer> holder = pool.transaction(db -> gate.thenCompose(v -> db.query("SELECT 1")).thenApply(r -> r.first().getInt(0)));
                Thread.sleep(100);
                CompletableFuture<PgResult> plain = pool.query("SELECT 2");                      // waits: the one connection is in the transaction
                CompletableFuture<Integer> tx = pool.transaction(db -> db.query("SELECT 3").thenApply(r -> r.first().getInt(0)));
                eventually(() -> sink.get("pg.pool.waiting") == 2, "two callers waiting");
                assertEquals(0, sink.get("pg.pool.acquire.timeouts"));
                Thread.sleep(400);                                                                // past the acquire timeout: both give up
                ExecutionException e = assertThrows(ExecutionException.class, () -> plain.get(1, TimeUnit.SECONDS));
                assertTrue(((PgException) e.getCause()).retriable());
                assertThrows(ExecutionException.class, () -> tx.get(1, TimeUnit.SECONDS));
                eventually(() -> sink.get("pg.pool.acquire.timeouts") == 2, "the two timeouts");
                assertEquals(0, sink.get("pg.pool.waiting"));
                CompletableFuture<PgResult> served = pool.query("SELECT 4");                     // waits, then is served when the transaction ends
                eventually(() -> sink.get("pg.pool.waiting") == 1, "one caller waiting");
                gate.complete(null);
                assertEquals(1, holder.get(5, TimeUnit.SECONDS));
                assertEquals(4, served.get(5, TimeUnit.SECONDS).first().getInt(0));
                eventually(() -> sink.get("pg.pool.waiting") == 0, "nobody waiting");
                assertEquals(1, sink.get("pg.pool.acquire.duration.count"), "only the served wait is timed");
                assertTrue(sink.get("pg.pool.acquire.duration.sum") > 0);
                assertEquals(2, sink.get("pg.pool.acquire.timeouts"));
            }
        }
    }

    @Test
    void replacedConnectionsAndFailedPingsAreCounted() throws Exception {
        Pg.require(Pg.PORT);
        Pg.notOnRemote();
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            PgPoolOptions options = PgPoolOptions.defaults().withMaxLifetime(Duration.ofMillis(600)).withPing(Duration.ofMillis(200), Duration.ofMillis(100));
            try (PgPool pool = PgPool.create(group, Pg.config(), 2, options, null).get(10, TimeUnit.SECONDS);
                 PgClient admin = PgClient.connect(group, Pg.config()).get(10, TimeUnit.SECONDS)) {
                int pid = pool.query("SELECT pg_backend_pid()").get(5, TimeUnit.SECONDS).first().getInt(0);
                admin.query("SELECT pg_terminate_backend($1)", pid).get(5, TimeUnit.SECONDS);
                eventually(() -> sink.get("pg.pool.connections.replaced{reason,broken}") >= 1, "the broken connection to be replaced");
                eventually(() -> sink.get("pg.pool.connections.replaced{reason,lifetime}") >= 2, "the connections to be retired at the end of their life");
                List<CompletableFuture<PgResult>> fs = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    fs.add(pool.query("SELECT " + i));
                }
                for (CompletableFuture<PgResult> f : fs) {
                    f.get(5, TimeUnit.SECONDS);
                }
                assertEquals(0, sink.get("pg.pool.pings.failed"), "a healthy connection answers its pings");
                assertEquals(0, sink.get("pg.pool.waiting"));
            }
        }
    }
}
