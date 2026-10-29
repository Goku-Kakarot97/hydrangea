package io.hydrangea.mqtt;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.testing.ChaosProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Reconnecting against a real Mosquitto through a proxy that can cut the connection. */
class MqttReconnectBrokerTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        Broker.require(Broker.PORT);
        group = Broker.newGroup();
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final Backoff QUICK = Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(200)).withJitter(0);

    private static void await(java.util.function.BooleanSupplier c) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!c.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(c.getAsBoolean(), "timed out");
    }

    @Test
    void aCutConnectionIsReestablishedAndTheSubscriptionComesBack() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            List<String> events = new CopyOnWriteArrayList<>();
            MqttClient sub = MqttClient.connect(group, MqttConfig.of("127.0.0.1", proxy.port()).withClientId(Broker.id("sub")).withReconnect(QUICK),
                    new MqttListener() {
                        @Override
                        public void connected(boolean sp) {
                            events.add("up");
                        }

                        @Override
                        public void connectionLost(Throwable cause) {
                            events.add("down");
                        }
                    }).get(10, TimeUnit.SECONDS);
            MqttClient pub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS);
            try {
                String topic = Broker.id("rc");
                BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
                sub.subscribe(topic, QoS.AT_LEAST_ONCE, q::add).get(10, TimeUnit.SECONDS);
                for (int round = 1; round <= 3; round++) {
                    int round0 = round;
                    proxy.sever();
                    await(() -> events.stream().filter("down"::equals).count() == round0);          // the loss was noticed ...
                    await(() -> events.stream().filter("up"::equals).count() == round0 + 1 && sub.isConnected());   // ... and repaired
                    pub.publish(topic, b("round " + round), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
                    MqttMessage m = q.poll(10, TimeUnit.SECONDS);
                    assertNotNull(m, "round " + round + ": the subscription must have been restored");
                    assertEquals("round " + round, m.payloadAsString());
                }
            } finally {
                sub.close();
                pub.close();
            }
        }
    }

    @Test
    void qos1PublishesSurviveRepeatedCutsWithAPersistentSession() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            String topic = Broker.id("surv");
            MqttClient sub = MqttClient.connect(group, Broker.config().withClientId(Broker.id("sub")).withCleanSession(false)).get(10, TimeUnit.SECONDS);
            Set<String> got = new java.util.concurrent.ConcurrentSkipListSet<>();
            sub.subscribe(topic, QoS.AT_LEAST_ONCE, m -> got.add(m.payloadAsString())).get(10, TimeUnit.SECONDS);

            MqttClient pub = MqttClient.connect(group, MqttConfig.of("127.0.0.1", proxy.port()).withClientId(Broker.id("pub")).withCleanSession(false)
                    .withReconnect(QUICK).withLimits(50, 100_000)).get(10, TimeUnit.SECONDS);
            try {
                int n = 3000;
                List<CompletableFuture<Void>> fs = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    fs.add(pub.publish(topic, b("m" + i), QoS.AT_LEAST_ONCE));
                    if (i % 700 == 699) {
                        proxy.sever();
                    }
                }
                int failed = 0;
                for (CompletableFuture<Void> f : fs) {
                    try {
                        f.get(30, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.ExecutionException e) {
                        failed++;
                    }
                }
                assertEquals(0, failed, "with a persistent session nothing in flight may be lost");
                await(() -> got.size() == n);
                Set<String> expected = new HashSet<>();
                for (int i = 0; i < n; i++) expected.add("m" + i);
                assertEquals(expected, got, "at least once: every message arrived (duplicates are allowed, losses are not)");
            } finally {
                pub.close();
                sub.close();
            }
        }
    }

    @Test
    void messagesPublishedWhileDownAreSentAfterTheReconnect() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            String topic = Broker.id("down");
            MqttClient sub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS);
            BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
            sub.subscribe(topic, QoS.AT_LEAST_ONCE, q::add).get(10, TimeUnit.SECONDS);
            MqttClient pub = MqttClient.connect(group, MqttConfig.of("127.0.0.1", proxy.port()).withClientId(Broker.id("pub"))
                    .withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(300), Duration.ofMillis(300)).withJitter(0))).get(10, TimeUnit.SECONDS);
            try {
                proxy.refuse(true);
                proxy.sever();
                await(() -> !pub.isConnected());
                CompletableFuture<Void> f = pub.publish(topic, b("queued"), QoS.AT_LEAST_ONCE);
                Thread.sleep(500);
                assertFalse(f.isDone());
                proxy.refuse(false);
                f.get(10, TimeUnit.SECONDS);
                assertEquals("queued", q.poll(10, TimeUnit.SECONDS).payloadAsString());
            } finally {
                pub.close();
                sub.close();
            }
        }
    }

    @Test
    void aDeadNetworkIsNoticedThroughKeepAliveAndRecovered() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            List<String> events = new CopyOnWriteArrayList<>();
            MqttClient c = MqttClient.connect(group, MqttConfig.of("127.0.0.1", proxy.port()).withClientId(Broker.id("ka"))
                    .withKeepAlive(Duration.ofSeconds(1)).withReconnect(QUICK), new MqttListener() {
                @Override
                public void connectionLost(Throwable cause) {
                    events.add("lost");
                }

                @Override
                public void connected(boolean sp) {
                    events.add("up");
                }
            }).get(10, TimeUnit.SECONDS);
            try {
                proxy.blackhole(true);   // nothing is reset, nothing answers: only the keep-alive can tell
                long t0 = System.nanoTime();
                await(() -> events.contains("lost"));
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ms < 4000, "noticed after " + ms + " ms");
                proxy.blackhole(false);
                proxy.sever();
                await(() -> events.size() >= 3 && c.isConnected());
            } finally {
                c.close();
            }
        }
    }
}
