package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.testing.ChaosProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** MQTT 5 against a real Mosquitto (see {@code hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh}). Skipped when none listens. */
class Mqtt5BrokerTest {

    private static EventLoopGroup group;
    private final List<MqttClient> clients = new ArrayList<>();

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
    }

    private MqttClient connect(MqttConfig cfg) throws Exception {
        MqttClient c = MqttClient.connect(group, cfg).get(10, TimeUnit.SECONDS);
        clients.add(c);
        return c;
    }

    private MqttClient connect() throws Exception {
        return connect(Broker.config5());
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

    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return fail("expected a failure");
    }

    @Test
    void everyQosDeliversAndTheBrokerDescribesItself() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        MqttServerInfo info = sub.serverInfo();
        assertTrue(info.receiveMaximum() > 0 && info.receiveMaximum() <= 65535);
        assertTrue(info.subscriptionIdentifiersAvailable());
        assertTrue(info.sharedSubscriptionAvailable());
        String topic = Broker.id("t");
        for (QoS qos : QoS.values()) {
            BlockingQueue<MqttMessage> q = collect(sub, topic + "/" + qos.level(), qos);
            pub.publish(topic + "/" + qos.level(), b("v5 " + qos), qos).get(10, TimeUnit.SECONDS);
            MqttMessage m = next(q);
            assertEquals("v5 " + qos, m.payloadAsString());
            assertEquals(qos, m.qos());
        }
    }

    @Test
    void messagePropertiesTravelThroughTheBroker() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("props");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        MqttProperties props = MqttProperties.NONE.withUtf8Payload().withContentType("application/json").withResponseTopic(topic + "/reply")
                .withCorrelationData(new byte[] {7, 8, 9}).withMessageExpiry(300).withUserProperty("trace", "abc").withUserProperty("trace", "def");
        pub.publish(topic, b("{\"a\":1}"), QoS.AT_LEAST_ONCE, false, props).get(10, TimeUnit.SECONDS);
        MqttProperties got = next(q).properties();
        assertEquals(1, got.payloadFormat());
        assertEquals("application/json", got.contentType());
        assertEquals(topic + "/reply", got.responseTopic());
        assertArrayEquals(new byte[] {7, 8, 9}, got.correlationData());
        assertTrue(got.messageExpirySeconds() != null && got.messageExpirySeconds() <= 300 && got.messageExpirySeconds() > 290, "the time left: " + got.messageExpirySeconds());
        assertEquals(List.of(new UserProperty("trace", "abc"), new UserProperty("trace", "def")), got.userProperties());
        assertFalse(got.subscriptionIds().isEmpty(), "the broker says which subscription matched");
    }

    @Test
    void requestAndResponse() throws Exception {
        MqttClient server = connect();
        MqttClient client = connect();
        String service = Broker.id("svc");
        server.subscribe(service, QoS.AT_LEAST_ONCE, m -> server.publish(m.properties().responseTopic(), b("echo:" + m.payloadAsString()), QoS.AT_LEAST_ONCE, false,
                MqttProperties.NONE.withCorrelationData(m.properties().correlationData()))).get(10, TimeUnit.SECONDS);
        String replyTopic = Broker.id("reply");
        BlockingQueue<MqttMessage> replies = collect(client, replyTopic, QoS.AT_LEAST_ONCE);
        client.publish(service, b("ping"), QoS.AT_LEAST_ONCE, false,
                MqttProperties.NONE.withResponseTopic(replyTopic).withCorrelationData(new byte[] {42})).get(10, TimeUnit.SECONDS);
        MqttMessage r = next(replies);
        assertEquals("echo:ping", r.payloadAsString());
        assertArrayEquals(new byte[] {42}, r.properties().correlationData());
    }

    @Test
    void sharedSubscriptionsSplitTheLoadAndNeverDuplicate() throws Exception {
        MqttClient pub = connect();
        MqttClient w1 = connect();
        MqttClient w2 = connect();
        String topic = Broker.id("jobs");
        String group = Broker.id("g");
        Set<String> seen1 = java.util.concurrent.ConcurrentHashMap.newKeySet();
        Set<String> seen2 = java.util.concurrent.ConcurrentHashMap.newKeySet();
        w1.subscribe("$share/" + group + "/" + topic, QoS.AT_LEAST_ONCE, m -> seen1.add(m.payloadAsString())).get(10, TimeUnit.SECONDS);
        w2.subscribe("$share/" + group + "/" + topic, QoS.AT_LEAST_ONCE, m -> seen2.add(m.payloadAsString())).get(10, TimeUnit.SECONDS);
        int n = 200;
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(pub.publish(topic, b("job" + i), QoS.AT_LEAST_ONCE));
        }
        for (CompletableFuture<Void> f : fs) {
            f.get(10, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen1.size() + seen2.size() < n && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(n, seen1.size() + seen2.size());
        Set<String> both = new java.util.HashSet<>(seen1);
        both.retainAll(seen2);
        assertEquals(Set.of(), both, "each job goes to one worker only");
        assertFalse(seen1.isEmpty());
        assertFalse(seen2.isEmpty());
    }

    @Test
    void noLocalKeepsAClientFromHearingItself() throws Exception {
        MqttClient a = connect();
        MqttClient other = connect();
        String topic = Broker.id("nl");
        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        a.subscribe(topic, SubscribeOptions.of(QoS.AT_LEAST_ONCE).withNoLocal(true), q::add).get(10, TimeUnit.SECONDS);
        a.publish(topic, b("mine"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertNull(q.poll(500, TimeUnit.MILLISECONDS));
        other.publish(topic, b("theirs"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("theirs", next(q).payloadAsString());
    }

    @Test
    void retainHandlingDecidesWhetherRetainedMessagesAreSent() throws Exception {
        MqttClient pub = connect();
        String topic = Broker.id("rh");
        pub.publish(topic, b("kept"), QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
        try {
            MqttClient c = connect();
            BlockingQueue<MqttMessage> never = new LinkedBlockingQueue<>();
            c.subscribe(topic, SubscribeOptions.of(QoS.AT_LEAST_ONCE).withRetainHandling(SubscribeOptions.RetainHandling.NEVER), never::add).get(10, TimeUnit.SECONDS);
            assertNull(never.poll(500, TimeUnit.MILLISECONDS), "NEVER");

            BlockingQueue<MqttMessage> once = new LinkedBlockingQueue<>();
            SubscribeOptions ifNew = SubscribeOptions.of(QoS.AT_LEAST_ONCE).withRetainHandling(SubscribeOptions.RetainHandling.SEND_IF_NEW);
            MqttClient d = connect();
            d.subscribe(topic, ifNew, once::add).get(10, TimeUnit.SECONDS);
            assertEquals("kept", next(once).payloadAsString());
            d.subscribe(topic, ifNew, once::add).get(10, TimeUnit.SECONDS);
            assertNull(once.poll(500, TimeUnit.MILLISECONDS), "SEND_IF_NEW: the subscription already existed");

            BlockingQueue<MqttMessage> always = new LinkedBlockingQueue<>();
            MqttClient e = connect();
            e.subscribe(topic, QoS.AT_LEAST_ONCE, always::add).get(10, TimeUnit.SECONDS);
            assertTrue(next(always).retained());
            e.subscribe(topic, QoS.AT_LEAST_ONCE, always::add).get(10, TimeUnit.SECONDS);
            assertEquals("kept", next(always).payloadAsString(), "SEND_ALWAYS replays on every subscribe");
        } finally {
            pub.publish(topic, new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void retainAsPublishedKeepsTheRetainFlagOnLiveDelivery() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String topic = Broker.id("rap");
        BlockingQueue<MqttMessage> plain = collect(sub, topic + "/a", QoS.AT_LEAST_ONCE);
        BlockingQueue<MqttMessage> rap = new LinkedBlockingQueue<>();
        sub.subscribe(topic + "/b", SubscribeOptions.of(QoS.AT_LEAST_ONCE).withRetainAsPublished(true), rap::add).get(10, TimeUnit.SECONDS);
        try {
            pub.publish(topic + "/a", b("x"), QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
            pub.publish(topic + "/b", b("x"), QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
            assertFalse(next(plain).retained(), "the broker clears the flag for a live subscriber");
            assertTrue(next(rap).retained());
        } finally {
            pub.publish(topic + "/a", new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
            pub.publish(topic + "/b", new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void subscriptionIdentifiersGiveEachOverlappingHandlerExactlyItsOwnMessages() throws Exception {
        MqttClient pub = connect();
        MqttClient sub = connect();
        String root = Broker.id("ov");
        List<String> hash = new CopyOnWriteArrayList<>();
        List<String> plus = new CopyOnWriteArrayList<>();
        sub.subscribe(root + "/#", QoS.AT_LEAST_ONCE, m -> hash.add(m.topic())).get(10, TimeUnit.SECONDS);
        sub.subscribe(root + "/+", QoS.AT_LEAST_ONCE, m -> plus.add(m.topic())).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/x", b("1"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        pub.publish(root + "/x/y", b("2"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while ((hash.size() < 2 || plus.size() < 1) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Thread.sleep(300);
        assertEquals(List.of(root + "/x", root + "/x/y"), hash, "no duplicates, unlike MQTT 3.1.1 where the broker sends one copy per subscription");
        assertEquals(List.of(root + "/x"), plus);
    }

    @Test
    void aclDenialsComeBackAsReasonCodes() throws Exception {
        Broker.require(Broker.AUTH_PORT);
        MqttClient c = connect(MqttConfig.of(Broker.HOST, Broker.AUTH_PORT).withVersion(5).withClientId(Broker.id("acl")).withCredentials("alice", "secret"));
        // Mosquitto 2.1 grants a subscription that the ACL denies (and never delivers on it); a refused SUBACK is covered by Mqtt5ProtocolTest
        c.subscribe("forbidden/x", QoS.AT_LEAST_ONCE, m -> { }).get(10, TimeUnit.SECONDS);
        MqttException pub = assertInstanceOf(MqttException.class, failure(c.publish("forbidden/x", b("no"), QoS.AT_LEAST_ONCE)));
        assertEquals(0x87, pub.reasonCode(), "PUBACK: not authorized");
        assertFalse(pub.retriable());
        assertTrue(c.isConnected(), "a refused publish or subscription does not end the connection");
        BlockingQueue<MqttMessage> q = collect(c, "allowed/x", QoS.AT_LEAST_ONCE);
        c.publish("allowed/x", b("yes"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("yes", next(q).payloadAsString());
        MqttException q2 = assertInstanceOf(MqttException.class, failure(c.publish("forbidden/y", b("no"), QoS.EXACTLY_ONCE)));
        assertEquals(0x87, q2.reasonCode(), "PUBREC: not authorized");
    }

    @Test
    void wrongCredentialsAreRefusedForGood() throws Exception {
        Broker.require(Broker.AUTH_PORT);
        MqttConfig cfg = MqttConfig.of(Broker.HOST, Broker.AUTH_PORT).withVersion(5).withClientId(Broker.id("bad")).withCredentials("alice", "wrong");
        ExecutionException e = assertThrows(ExecutionException.class, () -> MqttClient.connect(group, cfg).get(10, TimeUnit.SECONDS));
        MqttException m = assertInstanceOf(MqttException.class, e.getCause());
        assertTrue(m.reasonCode() == 0x86 || m.reasonCode() == 0x87, "reason 0x" + Integer.toHexString(m.reasonCode()));
        assertFalse(m.retriable());
    }

    @Test
    void sessionExpiryKeepsASessionAlive() throws Exception {
        String id = Broker.id("sess");
        String topic = Broker.id("persist");
        MqttConfig cfg = Broker.config5().withClientId(id).withCleanSession(false).withV5(MqttConfig.V5.DEFAULTS.withSessionExpiry(60));
        MqttClient first = connect(cfg);
        assertFalse(first.sessionPresent());
        first.subscribe(topic, QoS.AT_LEAST_ONCE, m -> { }).get(10, TimeUnit.SECONDS);
        first.disconnect().get(5, TimeUnit.SECONDS);

        connect().publish(topic, b("while away"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);

        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        MqttClient second = MqttClient.connect(group, cfg, new MqttListener() {
            @Override
            public void unhandledMessage(MqttMessage m) {
                q.add(m);
            }
        }).get(10, TimeUnit.SECONDS);
        clients.add(second);
        assertTrue(second.sessionPresent());
        assertEquals("while away", next(q).payloadAsString());
        second.disconnect().get(5, TimeUnit.SECONDS);
        connect(cfg.withCleanSession(true).withV5(MqttConfig.V5.DEFAULTS)).disconnect().get(5, TimeUnit.SECONDS);   // drop the session
    }

    @Test
    void withoutSessionExpiryTheSessionEndsWithTheConnection() throws Exception {
        String id = Broker.id("short");
        MqttConfig cfg = Broker.config5().withClientId(id).withCleanSession(false);   // expiry 0
        MqttClient first = connect(cfg);
        first.subscribe(Broker.id("x"), QoS.AT_LEAST_ONCE, m -> { }).get(10, TimeUnit.SECONDS);
        first.disconnect().get(5, TimeUnit.SECONDS);
        MqttClient second = connect(cfg);
        assertFalse(second.sessionPresent());
    }

    @Test
    void anEmptyClientIdGetsOneFromTheBrokerAndKeepsIt() throws Exception {
        MqttClient c = connect(Broker.config5().withClientId(""));
        assertNotNull(c.serverInfo().assignedClientId());
        assertEquals(c.serverInfo().assignedClientId(), c.clientId());
    }

    @Test
    void takingOverTheSessionEndsTheOldConnectionForGoodWithReason0x8E() throws Exception {
        String id = Broker.id("twin");
        List<Throwable> ended = new CopyOnWriteArrayList<>();
        MqttClient first = MqttClient.connect(group, Broker.config5().withClientId(id), new MqttListener() {
            @Override
            public void gaveUp(Throwable cause) {
                ended.add(cause);
            }
        }).get(10, TimeUnit.SECONDS);
        clients.add(first);
        connect(Broker.config5().withClientId(id).withReconnect(null));
        first.closeFuture().get(10, TimeUnit.SECONDS);
        assertEquals(1, ended.size());
        MqttException e = assertInstanceOf(MqttException.class, ended.get(0));
        assertEquals(0x8E, e.reasonCode());
        assertFalse(e.retriable(), "reconnecting would only take the session back");
    }

    @Test
    void messageExpiryDropsStaleRetainedMessages() throws Exception {
        MqttClient pub = connect();
        String topic = Broker.id("exp");
        pub.publish(topic, b("short lived"), QoS.AT_LEAST_ONCE, true, MqttProperties.NONE.withMessageExpiry(1)).get(10, TimeUnit.SECONDS);
        Thread.sleep(2200);
        BlockingQueue<MqttMessage> q = collect(connect(), topic, QoS.AT_LEAST_ONCE);
        assertNull(q.poll(500, TimeUnit.MILLISECONDS));
        pub.publish(topic, new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);
    }

    @Test
    void willDelayGivesTheClientTimeToComeBack() throws Exception {
        MqttClient watcher = connect();
        String topic = Broker.id("will");
        BlockingQueue<MqttMessage> q = collect(watcher, topic, QoS.AT_LEAST_ONCE);
        MqttConfig.Will will = new MqttConfig.Will(topic, b("gone"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withContentType("text/plain"), 2);
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            String id = Broker.id("delayed");
            MqttConfig cfg = MqttConfig.of("127.0.0.1", proxy.port()).withVersion(5).withClientId(id).withWill(will)
                    .withV5(MqttConfig.V5.DEFAULTS.withSessionExpiry(60)).withCleanSession(false).withReconnect(null);
            MqttClient doomed = connect(cfg);
            long t0 = System.nanoTime();
            proxy.sever();
            doomed.closeFuture().get(5, TimeUnit.SECONDS);
            MqttMessage m = next(q);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals("gone", m.payloadAsString());
            assertEquals("text/plain", m.properties().contentType());
            assertTrue(ms >= 1000, "the will came after " + ms + " ms; the delay was 2 s");

            // coming back within the delay cancels it
            MqttClient again = connect(cfg);
            proxy.sever();
            again.closeFuture().get(5, TimeUnit.SECONDS);
            Thread.sleep(500);
            connect(cfg).disconnect().get(5, TimeUnit.SECONDS);
            assertNull(q.poll(3, TimeUnit.SECONDS), "the client reconnected before the will was due");
            connect(cfg.withCleanSession(true).withV5(MqttConfig.V5.DEFAULTS).withWill(null)).disconnect().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void largeMessagesAndManyInFlightWorkWithTheBrokersReceiveMaximum() throws Exception {
        MqttClient pub = connect(Broker.config5().withLimits(500, 100_000));
        MqttClient sub = connect(Broker.config5().withMaxPacketBytes(8 << 20));
        String topic = Broker.id("many");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        byte[] big = new byte[1_500_000];
        new java.util.Random(5).nextBytes(big);
        pub.publish(topic, big, QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withUserProperty("size", "big")).get(20, TimeUnit.SECONDS);
        MqttMessage m = next(q);
        assertArrayEquals(big, m.payload());
        assertEquals("big", m.properties().userProperty("size"));
        int n = 3000;
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
    void qos2ExactlyOnceUnderMqtt5() throws Exception {
        MqttClient pub = connect(Broker.config5().withLimits(50, 100_000));
        MqttClient sub = connect();
        String topic = Broker.id("q2");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.EXACTLY_ONCE);
        int n = 1000;
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
        assertNull(q.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    void aCutConnectionRestoresSubscriptionsWithTheirOptionsAndIdentifiers() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(Broker.HOST, Broker.PORT)) {
            List<String> events = new CopyOnWriteArrayList<>();
            MqttClient sub = MqttClient.connect(group, MqttConfig.of("127.0.0.1", proxy.port()).withVersion(5).withClientId(Broker.id("rc"))
                    .withReconnect(io.hydrangea.Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(200)).withJitter(0)), new MqttListener() {
                @Override
                public void connected(boolean sp) {
                    events.add("up");
                }

                @Override
                public void connectionLost(Throwable cause) {
                    events.add("down");
                }
            }).get(10, TimeUnit.SECONDS);
            clients.add(sub);
            MqttClient pub = connect();
            String root = Broker.id("rc");
            AtomicInteger own = new AtomicInteger();
            BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
            sub.subscribe(root + "/in", SubscribeOptions.of(QoS.AT_LEAST_ONCE).withNoLocal(true), q::add).get(10, TimeUnit.SECONDS);
            for (int round = 1; round <= 2; round++) {
                int r = round;
                proxy.sever();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while ((events.stream().filter("up"::equals).count() != r + 1 || !sub.isConnected()) && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                // no-local must still be in force: our own publish is not echoed, the other client's is
                sub.publish(root + "/in", b("mine" + r), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
                pub.publish(root + "/in", b("theirs" + r), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
                assertEquals("theirs" + r, next(q).payloadAsString());
            }
            assertNull(q.poll(300, TimeUnit.MILLISECONDS));
        }
    }
}
