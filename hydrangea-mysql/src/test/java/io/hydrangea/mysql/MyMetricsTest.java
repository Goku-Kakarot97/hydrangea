package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** The client's and the pool's numbers, as a {@link MetricsSink} sees them. */
class MyMetricsTest {

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

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    @Test
    void countsOutcomesTimesCallsWatchesThePendingQueueAndTheKills() throws Exception {
        My.require(My.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            try (MyClient c = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS)) {
                for (int i = 0; i < 20; i++) {
                    await(c.query("SELECT ? + 0", i));
                }
                assertThrows(ExecutionException.class, () -> await(c.query("SELECT * FROM no_such_table_metrics")));
                eventually(() -> sink.get("my.queries{outcome,error}") == 1, "the error to be counted");
                assertEquals(20, sink.get("my.queries{outcome,ok}"));
                assertEquals(21, sink.get("my.query.duration.count"), "errors are timed too");
                assertTrue(sink.get("my.query.duration.sum") > 0);
                assertEquals(0, sink.get("my.queries.pending"));

                CompletableFuture<MyResult> slow = c.query("SELECT SLEEP(0.3)");
                eventually(() -> sink.get("my.queries.pending") == 1, "one call waiting");
                await(slow);
                eventually(() -> sink.get("my.queries.pending") == 0, "the answer to be counted");

                assertThrows(ExecutionException.class, () -> await(c.query(Duration.ofMillis(100), "SELECT SLEEP(5)")));
                eventually(() -> sink.get("my.queries{outcome,timeout}") == 1, "the timeout to be counted");
                eventually(() -> sink.get("my.kills") == 1, "the KILL QUERY to be counted");
                assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
            }
            eventually(() -> sink.get("my.queries.pending") == 0, "nothing left");
            assertEquals(0, sink.get("my.connections.lost"), "a close we asked for is not a loss");
        }
    }

    @Test
    void aLostConnectionIsCountedAndSoIsAReconnect() throws Exception {
        My.require(My.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            MyConfig reconnecting = My.config().withReconnect(io.hydrangea.Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(100)));
            try (MyClient victim = MyClient.connect(group, reconnecting).get(10, TimeUnit.SECONDS); MyClient killer = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS)) {
                CompletableFuture<MyResult> slow = victim.query("SELECT SLEEP(30)");
                Thread.sleep(200);
                await(killer.query("KILL " + victim.threadId()));
                assertThrows(ExecutionException.class, () -> slow.get(5, TimeUnit.SECONDS));
                eventually(() -> sink.get("my.connections.lost") == 1, "the loss to be counted");
                eventually(() -> sink.get("my.reconnects") == 1, "the reconnect to be counted");
                assertTrue(sink.get("my.queries{outcome,failed}") + sink.get("my.queries{outcome,error}") >= 1);
                assertEquals(0, sink.get("my.queries.pending"), "the gauge went back down");
            }
        }
    }

    @Test
    void thePoolCountsWaitingCallersTimeoutsAndReplacedConnections() throws Exception {
        My.require(My.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            MyPoolOptions options = MyPoolOptions.defaults().withAcquireTimeout(Duration.ofMillis(300)).withMaxLifetime(Duration.ofMillis(800)).withPing(Duration.ofMillis(200), Duration.ofMillis(100));
            try (MyPool pool = MyPool.create(group, My.config(), 1, options).get(10, TimeUnit.SECONDS); MyClient admin = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS)) {
                CompletableFuture<Void> gate = new CompletableFuture<>();
                CompletableFuture<Integer> holder = pool.transaction(db -> gate.thenCompose(v -> db.query("SELECT 1")).thenApply(r -> r.first().getInt(0)));
                Thread.sleep(100);
                CompletableFuture<MyResult> plain = pool.query("SELECT 2");
                eventually(() -> sink.get("my.pool.waiting") == 1, "one caller waiting");
                Thread.sleep(400);
                assertThrows(ExecutionException.class, () -> plain.get(1, TimeUnit.SECONDS));
                eventually(() -> sink.get("my.pool.acquire.timeouts") == 1, "the timeout");
                CompletableFuture<MyResult> served = pool.query("SELECT 4");
                eventually(() -> sink.get("my.pool.waiting") == 1, "one caller waiting again");
                gate.complete(null);
                assertEquals(1, await(holder));
                assertEquals(4, await(served).first().getInt(0));
                eventually(() -> sink.get("my.pool.waiting") == 0, "nobody waiting");
                assertEquals(1, sink.get("my.pool.acquire.duration.count"));

                long id = await(pool.query("SELECT CONNECTION_ID()")).first().getLong(0);
                await(admin.query("KILL " + id));
                eventually(() -> sink.get("my.pool.connections.replaced{reason,broken}") >= 1, "the broken connection to be replaced");
                eventually(() -> sink.get("my.pool.connections.replaced{reason,lifetime}") >= 1, "a connection to be retired at the end of its life");
                assertEquals(1, await(pool.query("SELECT 1")).first().getInt(0));
                assertEquals(0, sink.get("my.pool.pings.failed"));
            }
        }
    }
}
