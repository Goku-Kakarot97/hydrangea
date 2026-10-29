package io.hydrangea.redis;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisMetricsTest {

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
    void countsOutcomesTimesCommandsAndWatchesThePendingQueue() throws Exception {
        try (FakeRedis server = new FakeRedis(); EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            var f = RedisClient.connect(group, server.config().withReconnect(null));
            FakeRedis.Conn c = server.accept();
            c.loginResp3();
            RedisClient client = f.get(5, TimeUnit.SECONDS);

            for (int i = 0; i < 20; i++) {
                var r = client.command("PING");
                c.expect("PING");
                c.write("+PONG\r\n");
                r.get(5, TimeUnit.SECONDS);
            }
            var bad = client.command("FOO");
            c.expect("FOO");
            c.write("-ERR unknown command\r\n");
            try {
                bad.get(5, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException expected) {
                // counted as an error reply
            }
            assertEquals(20, sink.get("redis.commands{outcome,ok}"));
            assertEquals(1, sink.get("redis.commands{outcome,error}"));
            assertEquals(21, sink.get("redis.command.duration.count"), "errors are timed too");
            assertTrue(sink.get("redis.command.duration.sum") > 0);
            assertEquals(0, sink.get("redis.commands.pending"));

            var a = client.command("GET", "a");
            var b = client.command("GET", "b");
            var d = client.command("GET", "d");
            eventually(() -> sink.get("redis.commands.pending") == 3, "three commands waiting");
            c.expect("GET");
            c.expect("GET");
            c.expect("GET");
            c.write("$-1\r\n$-1\r\n$-1\r\n");
            a.get(5, TimeUnit.SECONDS);
            b.get(5, TimeUnit.SECONDS);
            d.get(5, TimeUnit.SECONDS);
            eventually(() -> sink.get("redis.commands.pending") == 0, "the replies to be counted");

            var lost = client.command("GET", "x");
            c.expect("GET");
            c.socket.close();
            try {
                lost.get(5, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException expected) {
                // the connection went away under it
            }
            assertEquals(1, sink.get("redis.commands{outcome,failed}"));
            assertEquals(1, sink.get("redis.connections.lost"));
            assertEquals(0, sink.get("redis.commands.pending"));
            assertEquals(24, sink.get("redis.commands{outcome,ok}") + sink.get("redis.commands{outcome,error}"), "the failed one is in neither");
        }
    }
}
