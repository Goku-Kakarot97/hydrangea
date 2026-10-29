package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What the MQTT client counts, against a real Mosquitto (see {@code start-broker.sh}). Skipped when none listens. */
class MqttMetricsTest {

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
    void messagesByQosPublishTimeAndPendingAreCounted() throws Exception {
        Broker.require(Broker.PORT);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            Capture sink = new Capture();
            group.metrics().bindTo(sink);
            MqttClient pub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS);
            MqttClient sub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS);
            try {
                String topic = Broker.id("metrics");
                for (QoS qos : QoS.values()) {
                    BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
                    sub.subscribe(topic + "/" + qos.level(), qos, q::add).get(10, TimeUnit.SECONDS);
                    for (int i = 0; i < 5; i++) {
                        pub.publish(topic + "/" + qos.level(), "m".getBytes(StandardCharsets.UTF_8), qos).get(10, TimeUnit.SECONDS);
                    }
                    for (int i = 0; i < 5; i++) {
                        assertNotNull(q.poll(10, TimeUnit.SECONDS));
                    }
                }
                for (int q = 0; q < 3; q++) {
                    assertEquals(5, sink.get("mqtt.client.messages{direction,published,qos," + q + "}"), "published at QoS " + q);
                    assertEquals(5, sink.get("mqtt.client.messages{direction,received,qos," + q + "}"), "received at QoS " + q);
                }
                eventually(() -> sink.get("mqtt.client.pending") == 0, "every acknowledgement");
                assertEquals(10, sink.get("mqtt.client.publish.duration.count"), "only QoS 1 and 2 wait for an answer");
                assertTrue(sink.get("mqtt.client.publish.duration.sum") > 0);
                assertEquals(0, sink.get("mqtt.client.publish.failures"));
                assertEquals(0, sink.get("mqtt.client.connections.lost"));
            } finally {
                pub.close();
                sub.close();
            }
        }
    }
}
