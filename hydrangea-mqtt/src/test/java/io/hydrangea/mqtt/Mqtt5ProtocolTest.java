package io.hydrangea.mqtt;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** MQTT 5 against a scripted peer: exact packets out, hostile and awkward packets in. No broker needed. */
class Mqtt5ProtocolTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        group.shutdown().get(5, TimeUnit.SECONDS);
    }

    private static MqttConfig cfg(FakeBroker b) {
        return MqttConfig.of("127.0.0.1", b.port()).withVersion(5).withKeepAlive(null).withReconnect(null).withClientId("t5");
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private record Online(MqttClient client, FakeBroker.Conn conn) {
    }

    private static Online online(FakeBroker broker, MqttConfig cfg, Props5 connack) throws Exception {
        CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
        FakeBroker.Conn c = broker.accept();
        c.expect(Packets.CONNECT);
        c.connack5(false, 0, connack);
        return new Online(f.get(5, TimeUnit.SECONDS), c);
    }

    private static Online online(FakeBroker broker) throws Exception {
        return online(broker, cfg(broker), null);
    }

    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return fail("expected a failure");
    }

    private static void assertDropped(Online o) throws Exception {
        assertTrue(o.conn.closedByClient(3000), "the client should have closed the connection");
        o.client.closeFuture().get(5, TimeUnit.SECONDS);
    }

    private static void await(java.util.function.BooleanSupplier c) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!c.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(c.getAsBoolean(), "timed out");
    }

    /** Reads the properties out of a client PUBLISH body (topic, optional id, properties, payload). */
    private static Props5 propsOfPublish(FakeBroker.Packet p, int qos) {
        Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(p.body()));
        r.str();
        if (qos > 0) r.u16();
        return Props5.read(r);
    }

    private static int idOfPublish(FakeBroker.Packet p) {
        int topicLen = (p.body()[0] & 0xff) << 8 | p.body()[1] & 0xff;
        return (p.body()[2 + topicLen] & 0xff) << 8 | p.body()[3 + topicLen] & 0xff;
    }

    // ---- CONNECT and CONNACK ----

    @Test
    void connectCarriesVersion5AndTheV5Properties() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withCleanSession(false).withKeepAlive(Duration.ofSeconds(20))
                    .withV5(MqttConfig.V5.DEFAULTS.withSessionExpiry(3600).withReceiveMaximum(50).withTopicAliasMaximum(4).withMaximumPacketSize(100_000)
                            .withUserProperty("k", "v"))
                    .withWill(new MqttConfig.Will("bye", b("gone"), QoS.AT_LEAST_ONCE, false,
                            MqttProperties.NONE.withContentType("text/plain").withMessageExpiry(60), 30));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            FakeBroker.Packet p = c.expect(Packets.CONNECT);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(p.body()));
            assertEquals("MQTT", r.str());
            assertEquals(5, r.u8());
            assertEquals(0x04 | 0x08, r.u8(), "will + will QoS 1, no clean start");
            assertEquals(20, r.u16());
            Props5 props = Props5.read(r);
            assertEquals(3600L, props.sessionExpiry);
            assertEquals(50, props.receiveMaximum);
            assertEquals(4, props.topicAliasMaximum);
            assertEquals(100_000L, props.maximumPacketSize);
            assertEquals(List.of(new UserProperty("k", "v")), props.user);
            assertEquals("t5", r.str());
            Props5 will = Props5.read(r);
            assertEquals(30L, will.willDelay);
            assertEquals("text/plain", will.contentType);
            assertEquals(60L, will.messageExpiry);
            assertEquals("bye", r.str());
            assertEquals(4, r.u16());
            c.connack5(false, 0, null);
            assertTrue(f.get(5, TimeUnit.SECONDS).isConnected());
        }
    }

    @Test
    void defaultsAreNotSentAndAPasswordAloneIsAllowed() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker).withCredentials(null, "token"));
            FakeBroker.Conn c = broker.accept();
            FakeBroker.Packet p = c.expect(Packets.CONNECT);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(p.body()));
            r.str();
            r.u8();
            assertEquals(0x40 | 0x02, r.u8(), "password flag, clean start");
            r.u16();
            Props5 props = Props5.read(r);
            assertNull(props.sessionExpiry);
            assertNull(props.receiveMaximum);
            assertNull(props.maximumPacketSize);
            assertNull(props.topicAliasMaximum);
            c.connack5(false, 0, null);
            f.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void connackPropertiesAreExposedAndTheBrokersLimitsApply() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Props5 ack = new Props5();
            ack.receiveMaximum = 2;
            ack.maximumQoS = 1;
            ack.retainAvailable = 0;
            ack.maximumPacketSize = 200L;
            ack.topicAliasMaximum = 7;
            ack.wildcardAvailable = 0;
            ack.sharedAvailable = 0;
            ack.subIdsAvailable = 0;
            Online o = online(broker, cfg(broker), ack);
            MqttServerInfo info = o.client.serverInfo();
            assertEquals(2, info.receiveMaximum());
            assertEquals(1, info.maximumQoS());
            assertFalse(info.retainAvailable());
            assertEquals(200, info.maximumPacketSize());
            assertEquals(7, info.topicAliasMaximum());
            assertFalse(info.wildcardSubscriptionAvailable());
            assertFalse(info.sharedSubscriptionAvailable());
            assertFalse(info.subscriptionIdentifiersAvailable());

            assertEquals(0x9B, ((MqttException) failure(o.client.publish("t", b("x"), QoS.EXACTLY_ONCE))).reasonCode(), "QoS 2 is over the broker's maximum");
            assertEquals(0x9A, ((MqttException) failure(o.client.publish("t", b("x"), QoS.AT_MOST_ONCE, true))).reasonCode(), "retain not available");
            MqttException big = (MqttException) failure(o.client.publish("t", new byte[500], QoS.AT_LEAST_ONCE));
            assertEquals(0x95, big.reasonCode());
            assertFalse(big.retriable());
            assertNull(o.conn.read(200), "nothing of that reached the wire");

            // receive maximum 2: the third unacknowledged QoS 1 message waits
            for (int i = 0; i < 3; i++) {
                o.client.publish("t", b("m" + i), QoS.AT_LEAST_ONCE);
            }
            int id0 = idOfPublish(o.conn.expect(Packets.PUBLISH));
            o.conn.expect(Packets.PUBLISH);
            assertNull(o.conn.read(300), "the broker allows two in flight");
            o.conn.ack5(Packets.PUBACK, id0, 0, null);
            o.conn.expect(Packets.PUBLISH);
        }
    }

    @Test
    void anAssignedClientIdentifierIsUsedAgainWhenReconnecting() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withClientId("").withCleanSession(false)
                    .withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(30), Duration.ofMillis(30)).withJitter(0));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c1 = broker.accept();
            FakeBroker.Packet first = c1.expect(Packets.CONNECT);
            Props5 ack = new Props5();
            ack.assignedClientId = "auto-123";
            c1.connack5(false, 0, ack);
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            assertEquals("auto-123", client.clientId());
            assertEquals("auto-123", client.serverInfo().assignedClientId());
            c1.close();
            FakeBroker.Conn c2 = broker.accept();
            FakeBroker.Packet second = c2.expect(Packets.CONNECT);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(second.body()));
            r.str();
            r.u8();
            r.u8();
            r.u16();
            Props5.read(r);
            assertEquals("auto-123", r.str(), "the session can only resume under the identifier the broker gave");
            c2.connack5(true, 0, null);
            await(client::sessionPresent);
        }
    }

    @Test
    void serverKeepAliveOverridesTheClients() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Props5 ack = new Props5();
            ack.serverKeepAlive = 1;
            Online o = online(broker, cfg(broker).withKeepAlive(Duration.ofSeconds(60)), ack);
            FakeBroker.Packet ping = o.conn.read(2500);
            assertNotNull(ping, "a PINGREQ within the broker's 1 s, not the client's 60 s");
            assertEquals(Packets.PINGREQ, ping.type());
        }
    }

    @Test
    void everyKindOfRefusedConnectIsReportedWithItsReasonCode() throws Exception {
        int[][] cases = {{0x80, 1}, {0x82, 0}, {0x84, 0}, {0x85, 0}, {0x86, 0}, {0x87, 0}, {0x88, 1}, {0x89, 1}, {0x8A, 0}, {0x8C, 0}, {0x95, 0}, {0x97, 1}, {0x9F, 1}};
        for (int[] c : cases) {
            try (FakeBroker broker = new FakeBroker()) {
                CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
                FakeBroker.Conn conn = broker.accept();
                conn.expect(Packets.CONNECT);
                Props5 p = new Props5();
                p.reasonString = "because";
                conn.connack5(false, c[0], p);
                MqttException e = assertInstanceOf(MqttException.class, failure(f));
                assertEquals(c[0], e.reasonCode());
                assertEquals(c[1] == 1, e.retriable(), "code 0x" + Integer.toHexString(c[0]));
                assertTrue(e.getMessage().contains("because"), e.getMessage());
            }
        }
    }

    @Test
    void aConnackReasonBelow0x80ThatIsNotZeroIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.connack5(false, 0x10, null);
            assertFalse(assertInstanceOf(MqttException.class, failure(f)).retriable());
        }
    }

    // ---- PUBLISH ----

    @Test
    void publishPropertiesGoOutAndComeBackIn() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            MqttProperties props = MqttProperties.NONE.withUtf8Payload().withContentType("application/json").withResponseTopic("reply/me")
                    .withCorrelationData(new byte[] {1, 2, 3}).withMessageExpiry(120).withUserProperty("a", "1").withUserProperty("a", "2");
            o.client.publish("t/x", b("{}"), QoS.AT_LEAST_ONCE, false, props);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            Props5 got = propsOfPublish(p, 1);
            assertEquals(1, got.payloadFormat);
            assertEquals("application/json", got.contentType);
            assertEquals("reply/me", got.responseTopic);
            assertArrayEquals(new byte[] {1, 2, 3}, got.correlationData);
            assertEquals(120L, got.messageExpiry);
            assertEquals(List.of(new UserProperty("a", "1"), new UserProperty("a", "2")), got.user, "repeatable, in order");

            // and the other way
            List<MqttMessage> in = new CopyOnWriteArrayList<>();
            o.client.subscribe("t/#", QoS.AT_LEAST_ONCE, in::add);
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.suback5(Packets.SUBACK, sub.id(), 1);
            Props5 inProps = new Props5();
            inProps.contentType = "text/plain";
            inProps.correlationData = new byte[] {9};
            inProps.user.add(new UserProperty("origin", "broker"));
            inProps.messageExpiry = 5L;
            o.conn.publish5("t/y", b("hi"), 1, 77, inProps);
            assertEquals(77, o.conn.expect(Packets.PUBACK).id());
            await(() -> in.size() == 1);
            MqttProperties m = in.get(0).properties();
            assertEquals("text/plain", m.contentType());
            assertArrayEquals(new byte[] {9}, m.correlationData());
            assertEquals("broker", m.userProperty("origin"));
            assertEquals(5L, m.messageExpirySeconds());
        }
    }

    @Test
    void anMqtt311ClientRefusesMessagePropertiesInsteadOfDroppingThem() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker).withVersion(4));
            FakeBroker.Conn c = broker.accept();
            c.handshake(false, 0);
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            assertInstanceOf(IllegalArgumentException.class, failure(client.publish("t", b("x"), QoS.AT_MOST_ONCE, false, MqttProperties.NONE.withContentType("x"))));
            assertInstanceOf(IllegalArgumentException.class, failure(client.subscribe("t", SubscribeOptions.of(QoS.AT_MOST_ONCE).withNoLocal(true), m -> { })));
            assertNull(c.read(200));
        }
    }

    @Test
    void publishReasonCodesDecideTheFutureAndAreNotRetriableUnlessTransient() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> ok = o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            o.conn.ack5(Packets.PUBACK, idOfPublish(o.conn.expect(Packets.PUBLISH)), 0x10, null);   // no matching subscribers: the publish still worked
            ok.get(5, TimeUnit.SECONDS);

            CompletableFuture<Void> denied = o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            Props5 why = new Props5();
            why.reasonString = "no write access";
            o.conn.ack5(Packets.PUBACK, idOfPublish(o.conn.expect(Packets.PUBLISH)), 0x87, why);
            MqttException e = assertInstanceOf(MqttException.class, failure(denied));
            assertEquals(0x87, e.reasonCode());
            assertFalse(e.retriable());
            assertTrue(e.getMessage().contains("no write access"), e.getMessage());

            CompletableFuture<Void> quota = o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            o.conn.ack5(Packets.PUBACK, idOfPublish(o.conn.expect(Packets.PUBLISH)), 0x97, null);
            assertTrue(assertInstanceOf(MqttException.class, failure(quota)).retriable(), "quota exceeded can pass");

            CompletableFuture<Void> q2 = o.client.publish("t", b("x"), QoS.EXACTLY_ONCE);
            o.conn.ack5(Packets.PUBREC, idOfPublish(o.conn.expect(Packets.PUBLISH)), 0x87, null);
            assertEquals(0x87, ((MqttException) failure(q2)).reasonCode());
            assertNull(o.conn.read(200), "a refused QoS 2 message gets no PUBREL");
            assertTrue(o.client.isConnected(), "a refused publish is not a connection error");
            assertEquals(0, o.client.pendingMessages().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void qos2HandshakeAcceptsReasonCodesAndPubcomp0x92() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> f = o.client.publish("t", b("x"), QoS.EXACTLY_ONCE);
            int id = idOfPublish(o.conn.expect(Packets.PUBLISH));
            o.conn.ack5(Packets.PUBREC, id, 0x10, null);
            assertEquals(id, o.conn.expect(Packets.PUBREL).id());
            o.conn.ack5(Packets.PUBCOMP, id, 0x92, null);   // the broker no longer knows it: it completed earlier
            f.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void inboundQos2AndPubrelWorkWithReasonCodes() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<MqttMessage> in = new CopyOnWriteArrayList<>();
            o.client.subscribe("t", QoS.EXACTLY_ONCE, in::add);
            o.conn.suback5(Packets.SUBACK, o.conn.expect(Packets.SUBSCRIBE).id(), 2);
            o.conn.publish5("t", b("v"), 2, 5, null);
            assertEquals(5, o.conn.expect(Packets.PUBREC).id());
            o.conn.ack5(Packets.PUBREL, 5, 0, null);
            assertEquals(5, o.conn.expect(Packets.PUBCOMP).id());
            o.conn.publish5("t", b("w"), 2, 6, null);
            o.conn.expect(Packets.PUBREC);
            o.conn.ack5(Packets.PUBREL, 6, 0x92, null);   // a PUBREL with a (non-failure-bearing) reason is still answered
            o.conn.expect(Packets.PUBCOMP);
            await(() -> in.size() == 2);
        }
    }

    // ---- subscriptions ----

    @Test
    void subscribeOptionsAndTheSubscriptionIdentifierAreSent() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.subscribe("a/+", SubscribeOptions.of(QoS.AT_LEAST_ONCE).withNoLocal(true).withRetainAsPublished(true)
                    .withRetainHandling(SubscribeOptions.RetainHandling.NEVER), m -> { });
            FakeBroker.Packet p = o.conn.expect(Packets.SUBSCRIBE);
            assertEquals(2, p.flags());
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(p.body()));
            r.u16();
            Props5 props = Props5.read(r);
            assertEquals(List.of(1), props.subscriptionIds);
            assertEquals("a/+", r.str());
            assertEquals(1 | 4 | 8 | 2 << 4, r.u8());
            r.end();
            o.client.subscribe("b", QoS.AT_MOST_ONCE, m -> { });
            Packets.Reader r2 = new Packets.Reader(ByteBuffer.wrap(o.conn.expect(Packets.SUBSCRIBE).body()));
            r2.u16();
            assertEquals(List.of(2), Props5.read(r2).subscriptionIds, "each subscription has its own identifier");
        }
    }

    @Test
    void messagesAreRoutedByTheirSubscriptionIdentifiersSoOverlappingFiltersDoNotDuplicate() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<String> got = new CopyOnWriteArrayList<>();
            o.client.subscribe("a/#", QoS.AT_MOST_ONCE, m -> got.add("hash:" + m.payloadAsString()));
            o.client.subscribe("a/+", QoS.AT_MOST_ONCE, m -> got.add("plus:" + m.payloadAsString()));
            FakeBroker.Packet s1 = o.conn.expect(Packets.SUBSCRIBE);
            FakeBroker.Packet s2 = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.suback5(Packets.SUBACK, s1.id(), 0);
            o.conn.suback5(Packets.SUBACK, s2.id(), 0);
            Props5 both = new Props5();
            both.subscriptionIds.add(1);
            both.subscriptionIds.add(2);
            o.conn.publish5("a/x", b("1"), 0, 0, both);      // one copy for both subscriptions
            Props5 only2 = new Props5();
            only2.subscriptionIds.add(2);
            o.conn.publish5("a/y", b("2"), 0, 0, only2);     // a copy for the second only
            Props5 none = new Props5();
            o.conn.publish5("a/z", b("3"), 0, 0, none);      // no identifiers: fall back to matching the filters
            await(() -> got.size() == 5);
            assertEquals(List.of("hash:1", "plus:1", "plus:2", "hash:3", "plus:3"), got);
        }
    }

    @Test
    void sharedSubscriptionsReceiveMessagesOfTheirFilter() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Props5 ack = new Props5();
            ack.subIdsAvailable = 0;   // no identifiers, so routing is by filter
            Online o = online(broker, cfg(broker), ack);
            List<String> got = new CopyOnWriteArrayList<>();
            o.client.subscribe("$share/workers/jobs/+", QoS.AT_LEAST_ONCE, m -> got.add(m.topic()));
            FakeBroker.Packet s = o.conn.expect(Packets.SUBSCRIBE);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(s.body()));
            r.u16();
            assertTrue(Props5.read(r).subscriptionIds.isEmpty(), "the broker said it does not support identifiers");
            assertEquals("$share/workers/jobs/+", r.str());
            o.conn.suback5(Packets.SUBACK, s.id(), 1);
            o.conn.publish5("jobs/1", b("x"), 0, 0, null);
            await(() -> got.size() == 1);
            assertEquals(List.of("jobs/1"), got);
        }
    }

    @Test
    void subackReasonCodesAreReportedAndUnsubackToo() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            for (int code : new int[] {0x87, 0x8F, 0x9E, 0xA1, 0xA2, 0x80, 0x91, 0x97}) {
                CompletableFuture<QoS> f = o.client.subscribe("x", QoS.AT_MOST_ONCE, m -> { });
                o.conn.suback5(Packets.SUBACK, o.conn.expect(Packets.SUBSCRIBE).id(), code);
                MqttException e = assertInstanceOf(MqttException.class, failure(f));
                assertEquals(code, e.reasonCode());
            }
            CompletableFuture<QoS> ok = o.client.subscribe("x", QoS.EXACTLY_ONCE, m -> { });
            o.conn.suback5(Packets.SUBACK, o.conn.expect(Packets.SUBSCRIBE).id(), 1);
            assertEquals(QoS.AT_LEAST_ONCE, ok.get(5, TimeUnit.SECONDS));

            CompletableFuture<Void> u = o.client.unsubscribe("x");
            FakeBroker.Packet un = o.conn.expect(Packets.UNSUBSCRIBE);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(un.body()));
            r.u16();
            Props5.read(r);
            assertEquals("x", r.str());
            o.conn.suback5(Packets.UNSUBACK, un.id(), 0x11);   // no subscription existed: fine, it is gone
            u.get(5, TimeUnit.SECONDS);

            CompletableFuture<Void> u2 = o.client.unsubscribe("y");
            o.conn.suback5(Packets.UNSUBACK, o.conn.expect(Packets.UNSUBSCRIBE).id(), 0x87);
            assertEquals(0x87, ((MqttException) failure(u2)).reasonCode());
        }
    }

    @Test
    void aSubackWithTheWrongNumberOfReasonCodesIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.subscribe("x", QoS.AT_MOST_ONCE, m -> { });
            o.conn.suback5(Packets.SUBACK, o.conn.expect(Packets.SUBSCRIBE).id(), 0, 0);
            assertDropped(o);
        }
    }

    // ---- topic aliases ----

    @Test
    void inboundTopicAliasesAreResolved() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withTopicAliasMaximum(3)), null);
            List<String> got = new CopyOnWriteArrayList<>();
            o.client.subscribe("#", QoS.AT_MOST_ONCE, m -> got.add(m.topic() + "=" + m.payloadAsString()));
            o.conn.suback5(Packets.SUBACK, o.conn.expect(Packets.SUBSCRIBE).id(), 0);
            Props5 a = new Props5();
            a.topicAlias = 2;
            o.conn.publish5("long/topic/name", b("1"), 0, 0, a);   // defines alias 2
            o.conn.publish5("", b("2"), 0, 0, a);                    // uses it
            await(() -> got.size() == 2);
            assertEquals(List.of("long/topic/name=1", "long/topic/name=2"), got);
        }
    }

    @Test
    void invalidTopicAliasesAreProtocolErrors() throws Exception {
        for (int alias : new int[] {4, 1}) {   // over the maximum; never defined
            try (FakeBroker broker = new FakeBroker()) {
                Online o = online(broker, cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withTopicAliasMaximum(3)), null);
                Props5 a = new Props5();
                a.topicAlias = alias;
                o.conn.publish5(alias == 4 ? "t" : "", b("x"), 0, 0, a);
                assertDropped(o);
            }
        }
    }

    @Test
    void aTopicAliasWhenNoneWereOfferedIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            Props5 a = new Props5();
            a.topicAlias = 1;
            o.conn.publish5("t", b("x"), 0, 0, a);
            assertDropped(o);
        }
    }

    // ---- DISCONNECT and AUTH from the broker ----

    @Test
    void aServerDisconnectIsReportedWithItsReason() throws Exception {
        int[][] cases = {{0x8B, 1}, {0x8E, 0}, {0x87, 0}, {0x8D, 1}, {0x98, 1}, {0x00, 1}};
        for (int[] c : cases) {
            try (FakeBroker broker = new FakeBroker()) {
                List<Throwable> lost = new CopyOnWriteArrayList<>();
                CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker), new MqttListener() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        lost.add(cause);
                    }

                    @Override
                    public void gaveUp(Throwable cause) {
                        lost.add(cause);
                    }
                });
                FakeBroker.Conn conn = broker.accept();
                conn.expect(Packets.CONNECT);
                conn.connack5(false, 0, null);
                f.get(5, TimeUnit.SECONDS);
                Props5 p = new Props5();
                p.reasonString = "bye now";
                Packets.Writer w = new Packets.Writer(8).u8(c[0]).props(p.encode());
                conn.packet(Packets.DISCONNECT, 0, w.toByteArray());
                await(() -> !lost.isEmpty());
                MqttException e = assertInstanceOf(MqttException.class, lost.get(0));
                assertEquals(c[0], e.reasonCode());
                assertEquals(c[1] == 1, e.retriable(), "reason 0x" + Integer.toHexString(c[0]));
                assertTrue(e.getMessage().contains("bye now"));
            }
        }
    }

    @Test
    void aBareServerDisconnectMeansNormalClosure() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.conn.packet(Packets.DISCONNECT, 0, new byte[0]);
            assertDropped(o);
        }
    }

    @Test
    void anAuthPacketWithoutAnAuthenticatorIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.conn.packet(Packets.AUTH, 0, new byte[] {0x18, 0});
            assertDropped(o);
        }
    }

    @Test
    void clientDisconnectIsTheShortFormInMqtt5() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.disconnect().get(5, TimeUnit.SECONDS);
            FakeBroker.Packet p = o.conn.expect(Packets.DISCONNECT);
            assertEquals(0, p.body().length);
        }
    }

    // ---- hostile property blocks ----

    @Test
    void malformedPropertiesCloseTheConnection() throws Exception {
        // PUBLISH QoS 0, topic "t", then a properties block
        byte[][] bad = {
                {0x01, 0x01, 0x02},                       // payload format 2? (only 0 and 1 are allowed): length 2: id 1 value 2 -> see below
                {0x01, 0x70},                             // an unknown property identifier
                {0x04, 0x01, 0x01, 0x01, 0x00},           // payload format twice
                {0x02, 0x02, 0x00},                       // message expiry cut short (needs 4 bytes)
                {0x03, 0x23, 0x00, 0x00},                 // topic alias 0
                {0x05, 0x0B, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80},   // subscription identifier of five bytes
                {0x03, 0x0B, 0x00, 0x00},                 // subscription identifier 0
                {0x08, 0x03, 0x00, 0x02, 'a', 'b', 0x03, 0x00, 0x05},   // a second property cut short
                {0x05, 0x26, 0x00, 0x01, 'k'},            // user property without a value
                {0x04, 0x03, 0x00, 0x01, 0},              // content type containing U+0000
                {0x7F, 0x01},                             // properties longer than the packet
        };
        // the first entry is deliberately wrong-sized; replace it with a well-sized invalid value
        bad[0] = new byte[] {0x02, 0x01, 0x02};
        for (int i = 0; i < bad.length; i++) {
            try (FakeBroker broker = new FakeBroker()) {
                Online o = online(broker);
                byte[] body = new byte[3 + bad[i].length];
                body[1] = 1;
                body[2] = 't';
                System.arraycopy(bad[i], 0, body, 3, bad[i].length);
                o.conn.packet(Packets.PUBLISH, 0, body);
                assertDropped(o);
            } catch (AssertionError e) {
                throw new AssertionError("case " + i + ": " + e.getMessage(), e);
            }
        }
    }

    @Test
    void invalidConnackAndAckPropertiesCloseTheConnection() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.packet(Packets.CONNACK, 0, new byte[] {0, 0, 3, 0x21, 0, 0});   // receive maximum 0
            assertInstanceOf(MqttException.class, failure(f));
        }
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            int id = idOfPublish(o.conn.expect(Packets.PUBLISH));
            o.conn.packet(Packets.PUBACK, 0, new byte[] {(byte) (id >> 8), (byte) id, 0, 2, 0x70, 0});   // unknown property
            assertDropped(o);
        }
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            int id = idOfPublish(o.conn.expect(Packets.PUBLISH));
            o.conn.packet(Packets.PUBACK, 0, new byte[] {(byte) (id >> 8), (byte) id, 0, 0, 9});   // trailing byte
            assertDropped(o);
        }
    }

    // ---- reconnect with a surviving session ----

    @Test
    void resentMessagesKeepTheirProperties() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withCleanSession(false)
                    .withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(30), Duration.ofMillis(30)).withJitter(0));
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, cfg);
            FakeBroker.Conn c1 = broker.accept();
            c1.expect(Packets.CONNECT);
            c1.connack5(false, 0, null);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> f = client.publish("t", b("x"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withContentType("text/plain"));
            FakeBroker.Packet p1 = c1.expect(Packets.PUBLISH);
            c1.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.expect(Packets.CONNECT);
            c2.connack5(true, 0, null);
            FakeBroker.Packet p2 = c2.expect(Packets.PUBLISH);
            assertEquals(2 | 8, p2.flags());
            assertEquals("text/plain", propsOfPublish(p2, 1).contentType);
            assertEquals(idOfPublish(p1), idOfPublish(p2));
            c2.ack5(Packets.PUBACK, idOfPublish(p2), 0, null);
            f.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void subscriptionsKeepTheirOptionsAndIdentifiersWhenRestoredAfterALostSession() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(30), Duration.ofMillis(30)).withJitter(0));
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, cfg);
            FakeBroker.Conn c1 = broker.accept();
            c1.expect(Packets.CONNECT);
            c1.connack5(false, 0, null);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<QoS> s = client.subscribe("x/+", SubscribeOptions.of(QoS.AT_LEAST_ONCE).withNoLocal(true), m -> { });
            c1.suback5(Packets.SUBACK, c1.expect(Packets.SUBSCRIBE).id(), 1);
            s.get(5, TimeUnit.SECONDS);
            c1.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.expect(Packets.CONNECT);
            c2.connack5(false, 0, null);
            FakeBroker.Packet again = c2.expect(Packets.SUBSCRIBE);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(again.body()));
            r.u16();
            assertEquals(List.of(1), Props5.read(r).subscriptionIds, "the same identifier, so messages still find their handler");
            assertEquals("x/+", r.str());
            assertEquals(1 | 4, r.u8());
        }
    }

    // ---- misc ----

    @Test
    void aLongFormAckIsNotAllowedInMqtt311() throws Exception {
        // a 4-byte PUBACK is the v5 long form; in v3 it would be too long
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker).withVersion(4));
            FakeBroker.Conn c = broker.accept();
            c.handshake(false, 0);
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            int id = idOfPublish(c.expect(Packets.PUBLISH));
            c.packet(Packets.PUBACK, 0, new byte[] {(byte) (id >> 8), (byte) id, 0, 0});
            assertTrue(c.closedByClient(3000));
        }
    }

    @Test
    void configValidation() {
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withVersion(3));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.V5.DEFAULTS.withReceiveMaximum(0));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.V5.DEFAULTS.withSessionExpiry(-1));
        assertThrows(IllegalArgumentException.class, () -> new MqttProperties(2, null, null, null, null, null, null));
        assertDoesNotThrow(() -> MqttConfig.of("h", 1).withVersion(5).withClientId("").withCleanSession(false));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withClientId("").withCleanSession(false));
    }
}
