package io.hydrangea;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyTest {

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

    @Test
    void bucketsAreOrderedAndTight() {
        int last = -1;
        for (long v = 0; v < 5_000_000; v += (v < 100 ? 1 : v / 50 + 1)) {
            int b = Latency.bucketOf(v);
            assertTrue(b >= last, "buckets never go down: " + v);
            last = b;
            long edge = Latency.upperEdge(b);
            assertTrue(edge > v, "upper edge " + edge + " must be above " + v);
            assertTrue(edge <= v + v / 7 + 2, "bucket for " + v + " ends at " + edge + ": more than 12.5% wide");
        }
        assertEquals(Latency.BUCKETS - 1, Latency.bucketOf(Long.MAX_VALUE));
        assertEquals(0, Latency.bucketOf(0));
    }

    @Test
    void doesNothingUntilASinkIsBound() {
        Metrics m = new Metrics();
        Latency l = m.latency("x");
        assertEquals(0, l.begin());
        l.record(5);
        l.end(0);
        assertEquals(0, l.count());
        m.bindTo(new Capture());
        assertTrue(l.begin() != 0);
        m.close();
    }

    @Test
    void quantilesAreOfTheLastWindowOnly() {
        Metrics m = new Metrics();
        Capture sink = new Capture();
        m.bindTo(sink);
        Latency l = m.latency("redis.command.duration", "k", "v");
        for (int i = 0; i < 990; i++) {
            l.record(TimeUnit.MILLISECONDS.toNanos(1));
        }
        for (int i = 0; i < 10; i++) {
            l.record(TimeUnit.MILLISECONDS.toNanos(100));
        }
        l.tick();
        assertEquals(1000, sink.get("redis.command.duration.count{k,v}"));
        assertEquals(990 * 0.001 + 10 * 0.1, sink.get("redis.command.duration.sum{k,v}"), 1e-9);
        double p50 = sink.get("redis.command.duration{k,v,quantile,0.5}");
        double p99 = sink.get("redis.command.duration{k,v,quantile,0.99}");
        double p999 = sink.get("redis.command.duration{k,v,quantile,0.999}");
        double max = sink.get("redis.command.duration{k,v,quantile,1}");
        assertTrue(p50 >= 0.001 && p50 <= 0.0012, "p50 " + p50);
        assertTrue(p99 >= 0.001 && p99 <= 0.0012, "p99 is still in the fast 99%: " + p99);
        assertTrue(p999 >= 0.1 && p999 <= 0.113, "p99.9 " + p999);
        assertTrue(max >= 0.1 && max <= 0.113, "max " + max);

        l.tick();   // a window with nothing in it
        assertTrue(Double.isNaN(sink.get("redis.command.duration{k,v,quantile,0.99}")));
        for (int i = 0; i < 100; i++) {
            l.record(TimeUnit.MILLISECONDS.toNanos(5));
        }
        l.tick();
        double p99b = sink.get("redis.command.duration{k,v,quantile,0.99}");
        assertTrue(p99b >= 0.005 && p99b <= 0.0057, "the old slow requests are out of the window: " + p99b);
        assertEquals(1100, sink.get("redis.command.duration.count{k,v}"), "the count keeps running");
        m.close();
    }

    @Test
    void sameNameAndTagsShareOneMeterAndLateSinksGetEverything() {
        Metrics m = new Metrics();
        Latency a = m.latency("n", "t", "1");
        assertSame(a, m.latency("n", "t", "1"));
        assertNotSame(a, m.latency("n", "t", "2"));
        Metrics.Count c = m.counter("c", null, "outcome", "ok");
        assertSame(c, m.counter("c", null, "outcome", "ok"));
        Metrics.Count g = m.gauge("g", null);
        Capture early = new Capture();
        m.bindTo(early);
        m.counter("late", null).add(7);      // created after the sink was bound
        c.add(3);
        g.add(5);
        g.decrement();
        assertEquals(3, early.get("c{outcome,ok}"));
        assertEquals(4, early.get("g"));
        assertEquals(7, early.get("late"));
        Capture late = new Capture();
        m.bindTo(late);
        assertEquals(3, late.get("c{outcome,ok}"));
        assertTrue(late.meters.containsKey("n.count{t,2}"));
        m.close();
    }

    @Test
    void manyThreadsLoseNothing() throws Exception {
        Metrics m = new Metrics();
        m.bindTo(new Capture());
        Latency l = m.latency("p");
        Thread[] ts = new Thread[6];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(() -> {
                for (int k = 0; k < 200_000; k++) {
                    l.record(1000 + k % 5000);
                }
            });
            ts[i].start();
        }
        for (Thread t : ts) {
            t.join();
        }
        assertEquals(6 * 200_000L, l.count());
        m.close();
    }
}
