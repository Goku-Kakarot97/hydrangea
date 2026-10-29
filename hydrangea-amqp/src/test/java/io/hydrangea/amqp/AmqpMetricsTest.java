package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the AMQP client counts, against a real RabbitMQ (see {@link AmqpTest}). Skipped when none listens. */
class AmqpMetricsTest {

    private static final String HOST = System.getProperty("amqp.host", "127.0.0.1");
    private static final int PORT = Integer.getInteger("amqp.port", 25672);

    private static final class Capture implements MetricsSink {
        final Map<String, DoubleSupplier> meters = new ConcurrentHashMap<>();

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
            if (s == null) {
                throw new AssertionError("no meter " + key + "; have " + meters.keySet());
            }
            return s.getAsDouble();
        }
    }

    private static void eventually(BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void publishConfirmDeliveryAndAckAreCounted() throws Exception {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 500);
        } catch (Exception e) {
            Assumptions.abort("no RabbitMQ on " + HOST + ":" + PORT);
        }
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            AmqpConnection c = AmqpConnection.connect(group, AmqpConfig.of(HOST, PORT)).get(10, TimeUnit.SECONDS);
            try {
                AmqpChannel ch = c.openChannel().get(10, TimeUnit.SECONDS);
                String q = "hydrangea-metrics-" + UUID.randomUUID();
                ch.queueDeclare(q, false, true, true, null).get(10, TimeUnit.SECONDS);
                ch.confirmSelect().get(10, TimeUnit.SECONDS);
                CountDownLatch got = new CountDownLatch(20);
                ch.basicConsume(q, false, d -> {
                    ch.basicAck(d.deliveryTag(), false);
                    got.countDown();
                }).get(10, TimeUnit.SECONDS);
                for (int i = 0; i < 20; i++) {
                    ch.basicPublish("", q, false, BasicProperties.EMPTY, ("m" + i).getBytes(StandardCharsets.UTF_8)).get(10, TimeUnit.SECONDS);
                }
                assertTrue(got.await(10, TimeUnit.SECONDS));
                assertEquals(20, sink.get("amqp.messages{direction,published}"));
                assertEquals(20, sink.get("amqp.confirms{result,ack}"));
                assertEquals(0, sink.get("amqp.confirms{result,nack}"));
                assertEquals(0, sink.get("amqp.unconfirmed"));
                assertEquals(20, sink.get("amqp.confirm.duration.count"));
                assertTrue(sink.get("amqp.confirm.duration.sum") > 0);
                eventually(() -> sink.get("amqp.messages{direction,delivered}") == 20, "20 deliveries");
                assertEquals(20, sink.get("amqp.acks{type,ack}"));
                assertEquals(0, sink.get("amqp.publish.failures"));
            } finally {
                c.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
