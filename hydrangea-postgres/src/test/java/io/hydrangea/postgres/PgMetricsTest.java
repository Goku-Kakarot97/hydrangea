package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgMetricsTest {

    private static final class Capture implements MetricsSink {
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
            return meters.get(key).getAsDouble();
        }
    }

    private static void eventually(java.util.function.BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    @Test
    void countsOutcomesTimesQueriesAndWatchesThePendingQueue() throws Exception {
        Pg.require(Pg.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            try (PgClient c = Pg.get(PgClient.connect(group, Pg.config()))) {
                for (int i = 0; i < 20; i++) {
                    Pg.get(c.query("SELECT $1::int", i));
                }
                try {
                    c.query("SELECT 1/0").get(5, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException expected) {
                    // counted as an error
                }
                eventually(() -> sink.get("pg.queries{outcome,error}") == 1, "the error to be counted");
                assertEquals(20, sink.get("pg.queries{outcome,ok}"));
                assertEquals(21, sink.get("pg.query.duration.count"), "errors are timed too");
                assertTrue(sink.get("pg.query.duration.sum") > 0);
                assertEquals(0, sink.get("pg.queries.pending"));

                var slow = c.query("SELECT pg_sleep(0.3)");
                eventually(() -> sink.get("pg.queries.pending") == 1, "one query waiting");
                Pg.get(slow);
                eventually(() -> sink.get("pg.queries.pending") == 0, "the answer to be counted");
            }
            eventually(() -> sink.get("pg.queries.pending") == 0, "nothing left");
            assertEquals(0, sink.get("pg.connections.lost"), "a close we asked for is not a loss");
        }
    }

    @Test
    void aLostConnectionCountsItsFailedQueries() throws Exception {
        Pg.require(Pg.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            try (PgClient victim = Pg.get(PgClient.connect(group, Pg.config())); PgClient killer = Pg.get(PgClient.connect(group, Pg.config()))) {
                var slow = victim.query("SELECT pg_sleep(30)");
                Thread.sleep(200);
                Pg.get(killer.query("SELECT pg_terminate_backend($1)", victim.backendPid()));
                try {
                    slow.get(5, TimeUnit.SECONDS);
                } catch (java.util.concurrent.ExecutionException expected) {
                    // the connection went away under it
                }
                eventually(() -> sink.get("pg.connections.lost") == 1, "the loss to be counted");
                assertTrue(sink.get("pg.queries{outcome,failed}") + sink.get("pg.queries{outcome,error}") >= 1);
                assertEquals(0, sink.get("pg.queries.pending") , "the gauge went back down");
            }
        }
    }
}
