package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Against a real Mosquitto (see {@code hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh}). Skipped when none listens. */
class MqttBrokerTest {

    private static EventLoopGroup group;
    private final List<MqttClient> clients = new ArrayList<>();
    private final List<Throwable> losses = new CopyOnWriteArrayList<>();

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

    @AfterEach
    void closeClients() {
        clients.forEach(MqttClient::close);
        assertEquals(List.of(), losses, "no connection should have been lost in this test");
    }

    private MqttClient connect(MqttConfig cfg) throws Exception {
        MqttClient c = MqttClient.connect(group, cfg, new MqttListener() {
            @Override
            public void connectionLost(Throwable cause) {
                losses.add(cause);
            }
        }).get(10, TimeUnit.SECONDS);
        clients.add(c);
        return c;
    }

    private MqttClient connect() throws Exception {
        return connect(Broker.config());
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static MqttMessage next(BlockingQueue<MqttMessage> q) throws Exception {
        MqttMessage m = q.poll(10, TimeUnit.SECONDS);
        assertNotNull(m, "no message within 10 s");
        return m;
    }

    private static BlockingQueue<MqttMessage> collect(MqttClient c, String filter, QoS qos) throws Exception {
        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        c.subscribe(filter, qos, q::add).get(10, TimeUnit.SECONDS);
        return q;
    }

    @Test
    void everyQosLevelDeliversBetweenTwoClients() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("t");
        for (QoS qos : QoS.values()) {
            BlockingQueue<MqttMessage> q = collect(sub, topic + "/" + qos.level(), qos);
            pub.publish(topic + "/" + qos.level(), b("hello " + qos), qos).get(10, TimeUnit.SECONDS);
            MqttMessage m = next(q);
            assertEquals("hello " + qos, m.payloadAsString());
            assertEquals(qos, m.qos());
            assertFalse(m.retained());
        }
    }

    @Test
    void theBrokerGrantsAtMostWhatTheSubscriptionAsksAndMessagesAreDowngraded() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("t");
        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        assertEquals(QoS.AT_MOST_ONCE, sub.subscribe(topic, QoS.AT_MOST_ONCE, q::add).get(10, TimeUnit.SECONDS));
        pub.publish(topic, b("x"), QoS.EXACTLY_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals(QoS.AT_MOST_ONCE, next(q).qos(), "published at 2, subscribed at 0");
    }

    @Test
    void wildcardsSelectTheRightTopics() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String root = Broker.id("w");
        // the two filters do not overlap, so each message is sent once
        BlockingQueue<MqttMessage> plus = collect(sub, root + "/+/temp", QoS.AT_LEAST_ONCE);
        BlockingQueue<MqttMessage> other = collect(sub, root + "/other/#", QoS.AT_LEAST_ONCE);
        pub.publish(root + "/kitchen/temp", b("1"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/kitchen/humidity", b("2"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/a/b/temp", b("3"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/other/x/y", b("4"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/other", b("5"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("1", next(plus).payloadAsString());
        assertNull(plus.poll(300, TimeUnit.MILLISECONDS), "'+' is exactly one level");
        assertEquals("4", next(other).payloadAsString());
        assertEquals("5", next(other).payloadAsString(), "'other/#' also matches 'other'");
        assertNull(other.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    void retainedMessagesAreReplayedOnSubscribeAndCleared() throws Exception {
        MqttClient pub = connect();
        String topic = Broker.id("r");
        pub.publish(topic, b("kept"), QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
        MqttClient late = connect();
        BlockingQueue<MqttMessage> q = collect(late, topic, QoS.AT_LEAST_ONCE);
        MqttMessage m = next(q);
        assertEquals("kept", m.payloadAsString());
        assertTrue(m.retained());
        pub.publish(topic, new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);   // an empty retained payload clears it
        MqttClient later = connect();
        BlockingQueue<MqttMessage> q2 = collect(later, topic, QoS.AT_LEAST_ONCE);
        assertNull(q2.poll(500, TimeUnit.MILLISECONDS));
    }

    @Test
    void anEmptyPayloadIsDelivered() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("e");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        pub.publish(topic, new byte[0], QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals(0, next(q).payload().length);
    }

    @Test
    void unsubscribeStopsDelivery() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("u");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        pub.publish(topic, b("1"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        next(q);
        sub.unsubscribe(topic).get(10, TimeUnit.SECONDS);
        pub.publish(topic, b("2"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertNull(q.poll(500, TimeUnit.MILLISECONDS));
    }

    @Test
    void largeAndBinaryPayloadsSurvive() throws Exception {
        // This Mosquitto (2.1.2, in Docker) drops any client that sends about 2 MB or more, whatever the config: mosquitto_pub fails the same
        // way at 2 000 000 bytes. Larger packets, and the 4-byte remaining-length boundary, are covered by MqttProtocolTest and PacketsTest.
        MqttClient pub = connect();
        MqttClient sub = connect(Broker.config().withMaxPacketBytes(8 << 20));
        String topic = Broker.id("big");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        for (int size : new int[] {1, 127, 128, 16_383, 16_384, 65_535, 65_536, 1_000_000, 1_500_000}) {
            byte[] payload = new byte[size];
            new java.util.Random(size).nextBytes(payload);
            try {
                pub.publish(topic, payload, QoS.AT_LEAST_ONCE).get(20, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("size " + size + ", connection losses: " + losses, e);
            }
            assertArrayEquals(payload, next(q).payload(), "size " + size);
        }
    }

    @Test
    void unicodeTopicsAndPayloads() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("ü") + "/日本語/🙂";
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        pub.publish(topic, b("héllo 🙂"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        MqttMessage m = next(q);
        assertEquals(topic, m.topic());
        assertEquals("héllo 🙂", m.payloadAsString());
    }

    @Test
    void manyQos1MessagesPipelineAndArriveInOrder() throws Exception {
        MqttClient pub = connect(Broker.config().withLimits(50, 100_000));
        MqttClient sub = connect();
        String topic = Broker.id("n");
        int n = 5000;
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(pub.publish(topic, b(Integer.toString(i)), QoS.AT_LEAST_ONCE));
        }
        for (CompletableFuture<Void> f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        for (int i = 0; i < n; i++) {
            assertEquals(Integer.toString(i), next(q).payloadAsString());
        }
    }

    @Test
    void manyQos2MessagesArriveExactlyOnce() throws Exception {
        MqttClient pub = connect(Broker.config().withLimits(20, 100_000));
        MqttClient sub = connect();
        String topic = Broker.id("x");
        int n = 1000;
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.EXACTLY_ONCE);
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(pub.publish(topic, b(Integer.toString(i)), QoS.EXACTLY_ONCE));
        }
        for (CompletableFuture<Void> f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        for (int i = 0; i < n; i++) {
            assertEquals(Integer.toString(i), next(q).payloadAsString());
        }
        assertNull(q.poll(300, TimeUnit.MILLISECONDS), "no duplicate");
    }

    @Test
    void aClientHearsItsOwnMessages() throws Exception {
        MqttClient c = connect();
        String topic = Broker.id("self");
        BlockingQueue<MqttMessage> q = collect(c, topic, QoS.AT_LEAST_ONCE);
        c.publish(topic, b("me"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("me", next(q).payloadAsString());
    }

    @Test
    void aWillIsPublishedWhenTheClientVanishesButNotAfterDisconnect() throws Exception {
        MqttClient watcher = connect();
        String topic = Broker.id("will");
        BlockingQueue<MqttMessage> q = collect(watcher, topic, QoS.AT_LEAST_ONCE);

        MqttClient polite = connect(Broker.config().withWill(new MqttConfig.Will(topic, b("polite"), QoS.AT_LEAST_ONCE, false)));
        polite.disconnect().get(5, TimeUnit.SECONDS);
        assertNull(q.poll(700, TimeUnit.MILLISECONDS), "a clean DISCONNECT discards the will");

        try (io.hydrangea.testing.ChaosProxy proxy = new io.hydrangea.testing.ChaosProxy(Broker.HOST, Broker.PORT)) {
            MqttClient doomed = connect(MqttConfig.of("127.0.0.1", proxy.port()).withClientId(Broker.id("doomed")).withReconnect(null)
                    .withWill(new MqttConfig.Will(topic, b("vanished"), QoS.AT_LEAST_ONCE, false)));
            proxy.sever();
            assertEquals("vanished", next(q).payloadAsString());
            doomed.closeFuture().get(5, TimeUnit.SECONDS);
            losses.clear();   // the loss was the point of the test
        }
    }

    @Test
    void persistentSessionKeepsSubscriptionsAndQueuesQos1WhileOffline() throws Exception {
        String id = Broker.id("session");
        String topic = Broker.id("persist");
        MqttConfig cfg = Broker.config().withClientId(id).withCleanSession(false);
        MqttClient first = connect(cfg);
        assertFalse(first.sessionPresent());
        first.subscribe(topic, QoS.AT_LEAST_ONCE, m -> { }).get(10, TimeUnit.SECONDS);
        first.disconnect().get(5, TimeUnit.SECONDS);

        connect().publish(topic, b("while you were away"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);

        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        MqttClient second = MqttClient.connect(group, cfg, new MqttListener() {
            @Override
            public void unhandledMessage(MqttMessage m) {
                q.add(m);     // the broker replays before this client had a chance to subscribe again
            }
        }).get(10, TimeUnit.SECONDS);
        clients.add(second);
        assertTrue(second.sessionPresent());
        assertEquals("while you were away", next(q).payloadAsString());
        // clean up the session
        second.disconnect().get(5, TimeUnit.SECONDS);
        connect(cfg.withCleanSession(true)).disconnect().get(5, TimeUnit.SECONDS);
    }

    @Test
    void aSecondConnectionWithTheSameIdTakesOverAndTheFirstIsDropped() throws Exception {
        String id = Broker.id("dup");
        MqttClient first = connect(Broker.config().withClientId(id).withReconnect(null));
        MqttClient second = connect(Broker.config().withClientId(id).withReconnect(null));
        first.closeFuture().get(10, TimeUnit.SECONDS);
        assertTrue(second.isConnected());
        assertEquals(1, losses.size(), "the first client was dropped by the broker");
        losses.clear();
    }

    @Test
    void anIdleConnectionStaysUpOnKeepAlive() throws Exception {
        MqttClient c = connect(Broker.config().withKeepAlive(Duration.ofSeconds(1)).withReconnect(null));
        Thread.sleep(3500);
        assertTrue(c.isConnected());
        String topic = Broker.id("ka");
        BlockingQueue<MqttMessage> q = collect(c, topic, QoS.AT_LEAST_ONCE);
        c.publish(topic, b("still here"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("still here", next(q).payloadAsString());
    }

    @Test
    void anEmptyClientIdLetsTheBrokerChoose() throws Exception {
        MqttClient c = connect(Broker.config().withClientId(""));
        assertTrue(c.isConnected());
    }

    @Test
    void publishingFromManyThreadsAtOnce() throws Exception {
        MqttClient pub = connect(Broker.config().withLimits(100, 100_000));
        MqttClient sub = connect();
        String topic = Broker.id("mt");
        int threads = 8, each = 500;
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        List<Thread> ts = new ArrayList<>();
        List<CompletableFuture<Void>> fs = new CopyOnWriteArrayList<>();
        for (int t = 0; t < threads; t++) {
            int tt = t;
            Thread th = new Thread(() -> {
                for (int i = 0; i < each; i++) {
                    fs.add(pub.publish(topic, b(tt + ":" + i), QoS.AT_LEAST_ONCE));
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }
        for (CompletableFuture<Void> f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        int[] last = new int[threads];
        java.util.Arrays.fill(last, -1);
        for (int i = 0; i < threads * each; i++) {
            String[] p = next(q).payloadAsString().split(":");
            int t = Integer.parseInt(p[0]), seq = Integer.parseInt(p[1]);
            assertEquals(last[t] + 1, seq, "per-thread order is kept");
            last[t] = seq;
        }
    }

    @Test
    void whenWritableLetsAProducerPaceItselfAgainstAnOversizedBurst() throws Exception {
        MqttClient pub = connect(Broker.config().withMaxPacketBytes(8 << 20));
        MqttClient sub = connect(Broker.config().withMaxPacketBytes(8 << 20));
        String topic = Broker.id("bp");
        AtomicInteger got = new AtomicInteger();
        sub.subscribe(topic, QoS.AT_MOST_ONCE, m -> got.incrementAndGet()).get(10, TimeUnit.SECONDS);
        byte[] big = new byte[256 * 1024];
        int waits = 0;
        for (int i = 0; i < 200; i++) {
            if (!pub.isWritable()) {
                waits++;
                pub.whenWritable().get(10, TimeUnit.SECONDS);
            }
            pub.publish(topic, big, QoS.AT_MOST_ONCE);
        }
        assertTrue(waits > 0, "200 x 256 KiB must have filled the socket queue at least once");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (got.get() < 200 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(200, got.get(), "QoS 0 over a healthy connection loses nothing");
    }

    @Test
    void connectingToAClosedPortFailsFast() throws Exception {
        int port;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> MqttClient.connect(group, MqttConfig.of("127.0.0.1", port).withClientId("x")).get(10, TimeUnit.SECONDS));
        assertInstanceOf(java.io.IOException.class, e.getCause());
    }

    @Test
    void anUnresolvableHostFailsTheConnect() {
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> MqttClient.connect(group, MqttConfig.of("no-such-host.invalid", 1883).withClientId("x")).get(20, TimeUnit.SECONDS));
        assertInstanceOf(java.io.IOException.class, e.getCause());
    }
}
