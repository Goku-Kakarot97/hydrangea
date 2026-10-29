package io.hydrangea.mqtt;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The client against a scripted peer: exact packets out, hostile and awkward packets in. No broker needed. */
class MqttProtocolTest {

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
        return MqttConfig.of("127.0.0.1", b.port()).withKeepAlive(null).withReconnect(null).withClientId("t");
    }

    private static MqttConfig fast(FakeBroker b) {
        return cfg(b).withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(100)).withJitter(0));
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static int idOfPublish(FakeBroker.Packet p) {
        int topicLen = (p.body()[0] & 0xff) << 8 | p.body()[1] & 0xff;
        return (p.body()[2 + topicLen] & 0xff) << 8 | p.body()[3 + topicLen] & 0xff;
    }

    private record Online(MqttClient client, FakeBroker.Conn conn) {
    }

    private static Online online(FakeBroker broker, MqttConfig cfg, MqttListener l) throws Exception {
        CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg, l);
        FakeBroker.Conn c = broker.accept();
        c.handshake(false, 0);
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
        assertFalse(o.client.isConnected());
    }

    private static void await(java.util.function.BooleanSupplier c) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!c.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(c.getAsBoolean(), "timed out");
    }

    // ---- connecting ----

    @Test
    void sendsConnectWithTheConfiguredFields() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withClientId("id-1").withKeepAlive(Duration.ofSeconds(20)).withCredentials("bob", "pw")
                    .withWill(new MqttConfig.Will("bye", b("gone"), QoS.AT_LEAST_ONCE, false));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            FakeBroker.Packet p = c.handshake(false, 0);
            assertEquals(0, p.flags());
            assertArrayEquals(new byte[] {0, 4, 'M', 'Q', 'T', 'T', 4, (byte) (0x80 | 0x40 | 0x08 | 0x04 | 0x02), 0, 20, 0, 4, 'i', 'd', '-', '1',
                    0, 3, 'b', 'y', 'e', 0, 4, 'g', 'o', 'n', 'e', 0, 3, 'b', 'o', 'b', 0, 2, 'p', 'w'}, p.body());
            assertTrue(f.get(5, TimeUnit.SECONDS).isConnected());
        }
    }

    @Test
    void everyRefusalCodeIsReportedAndOnlyServerUnavailableIsRetriable() throws Exception {
        for (int code = 1; code <= 5; code++) {
            try (FakeBroker broker = new FakeBroker()) {
                CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
                FakeBroker.Conn c = broker.accept();
                c.handshake(false, code);
                MqttException e = assertInstanceOf(MqttException.class, failure(f));
                assertEquals(code, e.returnCode());
                assertEquals(code == 3, e.retriable(), "code " + code);
                assertTrue(c.closedByClient(2000));
            }
        }
    }

    @Test
    void anUnknownRefusalCodeAndReservedBitsAreProtocolErrors() throws Exception {
        for (int[] bad : new int[][] {{0, 6}, {0, 255}, {2, 0}, {0x80, 0}}) {
            try (FakeBroker broker = new FakeBroker()) {
                CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
                FakeBroker.Conn c = broker.accept();
                c.expect(Packets.CONNECT);
                c.write(0x20, 2, bad[0], bad[1]);
                assertFalse(assertInstanceOf(MqttException.class, failure(f)).retriable());
            }
        }
    }

    @Test
    void aBrokerThatReportsASessionToACleanClientIsWrong() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
            broker.accept().handshake(true, 0);
            assertInstanceOf(MqttException.class, failure(f));
        }
    }

    @Test
    void theFirstPacketMustBeConnack() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.write(0xD0, 0); // PINGRESP
            assertFalse(assertInstanceOf(MqttException.class, failure(f)).retriable());
        }
    }

    @Test
    void noConnackInTimeFailsTheConnect() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg(broker).withConnectTimeout(Duration.ofMillis(300)));
            broker.accept().expect(Packets.CONNECT);
            MqttException e = assertInstanceOf(MqttException.class, failure(f));
            assertTrue(e.getMessage().contains("CONNACK"), e.getMessage());
        }
    }

    @Test
    void connectionRefusedFailsTheFirstAttemptWithoutRetrying() throws Exception {
        int port;
        try (FakeBroker broker = new FakeBroker()) {
            port = broker.port();
        }
        CompletableFuture<MqttClient> f = MqttClient.connect(group, MqttConfig.of("127.0.0.1", port).withClientId("x")
                .withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(10), Duration.ofMillis(10))));
        assertInstanceOf(java.io.IOException.class, failure(f));
    }

    // ---- hostile input ----

    @Test
    void malformedPacketsCloseTheConnection() throws Exception {
        byte[][] bad = {
                {0x3F, 0},                                            // PUBLISH QoS 3
                {0x30 | 8, 3, 0, 1, 't'},                             // QoS 0 with DUP
                {0x32, 6, 0, 1, 't', 0, 0, 'x'},                      // QoS 1, packet id 0
                {0x30, 3, 0, 5, 't'},                                 // topic longer than the packet
                {0x30, 4, 0, 1, '+', 'x'},                            // wildcard in a topic name
                {0x30, 2, 0, 0},                                      // empty topic
                {0x30, 5, 0, 2, (byte) 0xC0, (byte) 0x80, 'x'},       // overlong UTF-8
                {0x30, 4, 0, 1, 0, 'x'},                              // U+0000 in the topic
                {0x41, 2, 0, 1},                                      // PUBACK with flags 1
                {0x40, 3, 0, 1, 9},                                   // PUBACK with a trailing byte
                {0x40, 1, 0},                                         // PUBACK too short
                {0x60, 2, 0, 1},                                      // PUBREL with flags 0 (must be 2)
                {(byte) 0xD1, 0},                                     // PINGRESP with flags
                {(byte) 0xD0, 1, 0},                                  // PINGRESP with a body
                {0x10, 0},                                            // CONNECT from a broker
                {(byte) 0x82, 2, 0, 1},                               // SUBSCRIBE from a broker
                {(byte) 0xC0, 0},                                     // PINGREQ from a broker
                {(byte) 0xE0, 0},                                     // DISCONNECT from a broker
                {0x00, 0},                                            // reserved type 0
                {(byte) 0xF0, 0},                                     // reserved type 15
                {0x30, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F}, // remaining length of five bytes
        };
        for (int i = 0; i < bad.length; i++) {
            try (FakeBroker broker = new FakeBroker()) {
                Online o = online(broker);
                o.conn.writeRaw(bad[i]);
                assertDropped(o);
            } catch (AssertionError e) {
                throw new AssertionError("case " + i + ": " + e.getMessage(), e);
            }
        }
    }

    @Test
    void aPacketOverMaxPacketBytesIsRefusedWithoutBufferingIt() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withMaxPacketBytes(1000), null);
            // 256 MB announced; the client must act on the header alone
            o.conn.writeRaw(new byte[] {0x30, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F});
            assertDropped(o);
        }
    }

    @Test
    void aPacketExactlyAtTheLimitIsAccepted() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withMaxPacketBytes(1000), null);
            CompletableFuture<MqttMessage> got = new CompletableFuture<>();
            o.client.subscribe("t", QoS.AT_MOST_ONCE, got::complete);
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.publish("t", new byte[1000 - 3], 0, 0, false, false);
            assertEquals(997, got.get(5, TimeUnit.SECONDS).payload().length);
        }
    }

    @Test
    void aSubackWithTheWrongNumberOfCodesIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.subscribe("t", QoS.AT_MOST_ONCE, m -> { });
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 0, 0});
            assertDropped(o);
        }
    }

    @Test
    void anUnknownSubackCodeIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.subscribe("t", QoS.AT_MOST_ONCE, m -> { });
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 3});
            assertDropped(o);
        }
    }

    @Test
    void acknowledgementsForUnknownIdentifiersAreIgnored() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.conn.ack(Packets.PUBACK, 99);
            o.conn.ack(Packets.PUBCOMP, 99);
            o.conn.packet(Packets.SUBACK, 0, new byte[] {0, 99, 0});
            o.conn.ack(Packets.UNSUBACK, 99);
            // a PUBREC we do not know is still answered: the broker is waiting for the PUBREL
            o.conn.ack(Packets.PUBREC, 99);
            FakeBroker.Packet rel = o.conn.expect(Packets.PUBREL);
            assertEquals(2, rel.flags());
            assertEquals(99, rel.id());
            assertTrue(o.client.isConnected());
        }
    }

    @Test
    void ackOfTheWrongKindForAMessageIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> f = o.client.publish("t", b("x"), QoS.EXACTLY_ONCE);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            o.conn.ack(Packets.PUBACK, idOfPublish(p));   // a QoS 2 message answered with PUBACK
            assertDropped(o);
            assertInstanceOf(MqttException.class, failure(f));
        }
    }

    @Test
    void pubcompBeforePubrecIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> f = o.client.publish("t", b("x"), QoS.EXACTLY_ONCE);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            o.conn.ack(Packets.PUBCOMP, idOfPublish(p));
            assertDropped(o);
            failure(f);
        }
    }

    // ---- stream handling ----

    @Test
    void packetsSplitIntoSingleBytesAndGluedTogetherAreBothParsed() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<String> got = new CopyOnWriteArrayList<>();
            o.client.subscribe("a/#", QoS.AT_MOST_ONCE, m -> got.add(m.payloadAsString()));
            o.conn.expect(Packets.SUBSCRIBE);
            java.io.ByteArrayOutputStream glued = new java.io.ByteArrayOutputStream();
            for (String p : new String[] {"one", "two", "three"}) {
                glued.write(new byte[] {0x30, (byte) (2 + 3 + p.length()), 0, 3, 'a', '/', 'x'}, 0, 7);
                glued.write(b(p));
            }
            o.conn.writeRaw(glued.toByteArray());      // three packets in one write
            for (byte x : glued.toByteArray()) {       // and the same three, one byte per write
                o.conn.writeRaw(new byte[] {x});
            }
            await(() -> got.size() == 6);
            assertEquals(List.of("one", "two", "three", "one", "two", "three"), got);
        }
    }

    @Test
    void aLargeInboundMessageIsReassembled() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withMaxPacketBytes(8 << 20), null);
            CompletableFuture<MqttMessage> got = new CompletableFuture<>();
            o.client.subscribe("big", QoS.AT_MOST_ONCE, got::complete);
            o.conn.expect(Packets.SUBSCRIBE);
            byte[] payload = new byte[3_000_000];
            new java.util.Random(1).nextBytes(payload);
            o.conn.publish("big", payload, 0, 0, false, true);
            MqttMessage m = got.get(10, TimeUnit.SECONDS);
            assertArrayEquals(payload, m.payload());
            assertTrue(m.retained());
        }
    }

    // ---- QoS state machines ----

    @Test
    void qos0PublishSendsOnePacketAndCompletesAtOnce() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.publish("a/b", b("hi"), QoS.AT_MOST_ONCE, true).get(5, TimeUnit.SECONDS);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            assertEquals(1, p.flags());
            assertArrayEquals(new byte[] {0, 3, 'a', '/', 'b', 'h', 'i'}, p.body());
        }
    }

    @Test
    void qos1PublishCompletesOnPubackAndUsesDistinctIdentifiers() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> f1 = o.client.publish("t", b("1"), QoS.AT_LEAST_ONCE);
            CompletableFuture<Void> f2 = o.client.publish("t", b("2"), QoS.AT_LEAST_ONCE);
            FakeBroker.Packet p1 = o.conn.expect(Packets.PUBLISH);
            FakeBroker.Packet p2 = o.conn.expect(Packets.PUBLISH);
            assertEquals(2, p1.flags());
            int id1 = idOfPublish(p1);
            int id2 = idOfPublish(p2);
            assertNotEquals(id1, id2);
            Thread.sleep(100);
            assertFalse(f1.isDone());
            o.conn.ack(Packets.PUBACK, id2);   // out of order
            f2.get(5, TimeUnit.SECONDS);
            assertFalse(f1.isDone());
            o.conn.ack(Packets.PUBACK, id1);
            f1.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void qos2PublishRunsTheFourWayHandshake() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> f = o.client.publish("t", b("x"), QoS.EXACTLY_ONCE);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            assertEquals(4, p.flags());
            int id = idOfPublish(p);
            o.conn.ack(Packets.PUBREC, id);
            FakeBroker.Packet rel = o.conn.expect(Packets.PUBREL);
            assertEquals(2, rel.flags());
            assertEquals(id, rel.id());
            o.conn.ack(Packets.PUBREC, id);                    // a repeated PUBREC is answered with a repeated PUBREL
            assertEquals(id, o.conn.expect(Packets.PUBREL).id());
            Thread.sleep(100);
            assertFalse(f.isDone(), "not complete before PUBCOMP");
            o.conn.ack(Packets.PUBCOMP, id);
            f.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void moreThanMaxInflightWaitInTheQueueAndTheTailGoesOutAsAcksArrive() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withLimits(3, 10), null);
            List<CompletableFuture<Void>> fs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                fs.add(o.client.publish("t", b("m" + i), QoS.AT_LEAST_ONCE));
            }
            int[] ids = new int[8];
            for (int i = 0; i < 3; i++) {
                ids[i] = idOfPublish(o.conn.expect(Packets.PUBLISH));
            }
            assertNull(o.conn.read(300), "a fourth must wait for an acknowledgement");
            assertEquals(8, o.client.pendingMessages().get(5, TimeUnit.SECONDS));
            for (int i = 0; i < 5; i++) {
                o.conn.ack(Packets.PUBACK, ids[i]);
                FakeBroker.Packet next = o.conn.expect(Packets.PUBLISH);
                ids[i + 3] = idOfPublish(next);
                assertEquals("m" + (i + 3), new String(next.body(), 5, next.body().length - 5, StandardCharsets.UTF_8), "sent in order");
            }
            for (int i = 5; i < 8; i++) {
                o.conn.ack(Packets.PUBACK, ids[i]);
            }
            for (CompletableFuture<Void> f : fs) {
                f.get(5, TimeUnit.SECONDS);
            }
            assertEquals(0, o.client.pendingMessages().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void beyondMaxQueuedPublishFailsAtOnce() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withLimits(2, 4), null);
            for (int i = 0; i < 4; i++) {
                o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            }
            assertTrue(assertInstanceOf(MqttException.class, failure(o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE))).retriable());
        }
    }

    @Test
    void inboundQos1IsAcknowledgedAfterTheHandlerRuns() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<String> order = new CopyOnWriteArrayList<>();
            o.client.subscribe("t", QoS.AT_LEAST_ONCE, m -> order.add("handler:" + m.payloadAsString() + ":" + m.qos() + ":" + m.duplicate()));
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.publish("t", b("v"), 1, 0x0A0B, true, false);
            assertEquals(0x0A0B, o.conn.expect(Packets.PUBACK).id());
            assertEquals(List.of("handler:v:AT_LEAST_ONCE:true"), order);
        }
    }

    @Test
    void aHandlerThatThrowsDoesNotStopTheConnectionOrTheAcknowledgement() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.subscribe("t", QoS.AT_LEAST_ONCE, m -> { throw new IllegalStateException("boom"); });
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.publish("t", b("v"), 1, 5, false, false);
            assertEquals(5, o.conn.expect(Packets.PUBACK).id());
            o.conn.publish("t", b("w"), 1, 6, false, false);
            assertEquals(6, o.conn.expect(Packets.PUBACK).id());
            assertTrue(o.client.isConnected());
        }
    }

    @Test
    void inboundQos2IsDeliveredOnceEvenWhenTheBrokerRepeatsIt() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            AtomicInteger n = new AtomicInteger();
            o.client.subscribe("t", QoS.EXACTLY_ONCE, m -> n.incrementAndGet());
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.publish("t", b("v"), 2, 7, false, false);
            assertEquals(7, o.conn.expect(Packets.PUBREC).id());
            o.conn.publish("t", b("v"), 2, 7, true, false);    // the PUBREC was "lost": the broker sends it again
            assertEquals(7, o.conn.expect(Packets.PUBREC).id());
            o.conn.ack(Packets.PUBREL, 7);
            assertEquals(7, o.conn.expect(Packets.PUBCOMP).id());
            assertEquals(1, n.get());
            // after PUBREL the identifier is free again: a new message with the same id is a new message
            o.conn.publish("t", b("v2"), 2, 7, false, false);
            o.conn.expect(Packets.PUBREC);
            await(() -> n.get() == 2);
        }
    }

    // ---- subscriptions ----

    @Test
    void subscribeCompletesWithTheGrantedQosAndAcceptsRetainedMessagesSentBeforeTheSuback() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<MqttMessage> got = new CopyOnWriteArrayList<>();
            CompletableFuture<QoS> f = o.client.subscribe("a/+", QoS.EXACTLY_ONCE, got::add);
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            assertEquals(2, sub.flags());
            o.conn.publish("a/x", b("old"), 0, 0, false, true);     // arrives ahead of the SUBACK
            await(() -> got.size() == 1);
            assertFalse(f.isDone());
            o.conn.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 1});
            assertEquals(QoS.AT_LEAST_ONCE, f.get(5, TimeUnit.SECONDS), "the broker granted less than asked");
            assertTrue(got.get(0).retained());
        }
    }

    @Test
    void aRefusedSubscriptionFailsAndLeavesNoHandlerBehind() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            List<MqttMessage> unhandled = new CopyOnWriteArrayList<>();
            Online o = online(broker, cfg(broker), new MqttListener() {
                @Override
                public void unhandledMessage(MqttMessage m) {
                    unhandled.add(m);
                }
            });
            AtomicInteger n = new AtomicInteger();
            CompletableFuture<QoS> f = o.client.subscribe("secret/#", QoS.AT_MOST_ONCE, m -> n.incrementAndGet());
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], (byte) 0x80});
            assertFalse(assertInstanceOf(MqttException.class, failure(f)).retriable());
            o.conn.publish("secret/x", b("v"), 0, 0, false, false);
            await(() -> unhandled.size() == 1);
            assertEquals(0, n.get());
        }
    }

    @Test
    void unsubscribeCompletesOnUnsuback() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<QoS> s = o.client.subscribe("t", QoS.AT_MOST_ONCE, m -> { });
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 0});
            s.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> u = o.client.unsubscribe("t");
            FakeBroker.Packet un = o.conn.expect(Packets.UNSUBSCRIBE);
            assertEquals(2, un.flags());
            assertFalse(u.isDone());
            o.conn.ack(Packets.UNSUBACK, un.id());
            u.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void overlappingFiltersEachSeeTheMessage() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            List<String> got = new CopyOnWriteArrayList<>();
            o.client.subscribe("a/#", QoS.AT_MOST_ONCE, m -> got.add("hash"));
            o.client.subscribe("a/+", QoS.AT_MOST_ONCE, m -> got.add("plus"));
            o.client.subscribe("b", QoS.AT_MOST_ONCE, m -> got.add("b"));
            for (int i = 0; i < 3; i++) {
                o.conn.expect(Packets.SUBSCRIBE);
            }
            o.conn.publish("a/x", b("v"), 0, 0, false, false);
            o.conn.publish("b", b("v"), 0, 0, false, false);
            await(() -> got.size() == 3);
            assertEquals(List.of("hash", "plus", "b"), got);
        }
    }

    @Test
    void aHandlerMayChangeSubscriptionsWhileMessagesAreBeingDelivered() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            AtomicInteger n = new AtomicInteger();
            o.client.subscribe("t", QoS.AT_MOST_ONCE, m -> {
                n.incrementAndGet();
                o.client.subscribe("other/" + n.get(), QoS.AT_MOST_ONCE, x -> { });
                o.client.unsubscribe("t");
            });
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.publish("t", b("v"), 0, 0, false, false);
            o.conn.expect(Packets.SUBSCRIBE);
            o.conn.expect(Packets.UNSUBSCRIBE);
            assertEquals(1, n.get());
        }
    }

    @Test
    void inputIsValidatedBeforeAnythingIsSent() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            for (String bad : new String[] {"", "a/+", "#", "a\0"}) {
                assertInstanceOf(IllegalArgumentException.class, failure(o.client.publish(bad, b("x"), QoS.AT_MOST_ONCE)), bad);
            }
            for (String bad : new String[] {"", "a#", "a/#/b", "a+"}) {
                assertInstanceOf(IllegalArgumentException.class, failure(o.client.subscribe(bad, QoS.AT_MOST_ONCE, m -> { })), bad);
                assertInstanceOf(IllegalArgumentException.class, failure(o.client.unsubscribe(bad)), bad);
            }
            assertNull(o.conn.read(300), "nothing reaches the wire");
        }
    }

    @Test
    void messagesTooLargeToSendAreRefused() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            assertInstanceOf(IllegalArgumentException.class, failure(o.client.publish("t", new byte[268_435_455], QoS.AT_MOST_ONCE)));
            assertTrue(o.client.isConnected());
        }
    }

    @Test
    void largeOutboundMessagesArriveIntact() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            byte[] payload = new byte[5_000_000];
            new java.util.Random(7).nextBytes(payload);
            CompletableFuture<Void> f = o.client.publish("big", payload, QoS.AT_LEAST_ONCE);
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            assertEquals(2 + 3 + 2 + payload.length, p.body().length);
            assertArrayEquals(payload, Arrays.copyOfRange(p.body(), 7, p.body().length));
            o.conn.ack(Packets.PUBACK, idOfPublish(p));
            f.get(5, TimeUnit.SECONDS);
        }
    }

    // ---- keep-alive ----

    @Test
    void anIdleClientPingsAndStaysConnectedWhileTheBrokerAnswers() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withKeepAlive(Duration.ofSeconds(1)), null);
            long t0 = System.nanoTime();
            for (int i = 0; i < 3; i++) {
                FakeBroker.Packet p = o.conn.read(2500);
                assertNotNull(p, "a PINGREQ should arrive within the keep-alive");
                assertEquals(Packets.PINGREQ, p.type());
                o.conn.write(0xD0, 0);
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 3000, "three pings in " + ms + " ms: at least one per keep-alive");
            assertTrue(o.client.isConnected());
        }
    }

    @Test
    void aSilentBrokerIsDroppedAfterOneAndAHalfKeepAlives() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            List<Throwable> lost = new CopyOnWriteArrayList<>();
            Online o = online(broker, cfg(broker).withKeepAlive(Duration.ofSeconds(1)), new MqttListener() {
                @Override
                public void gaveUp(Throwable cause) {
                    lost.add(cause);
                }
            });
            long t0 = System.nanoTime();
            assertDropped(o);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms >= 1300 && ms < 2800, "dropped after " + ms + " ms (expected about 1500)");
            assertEquals(1, lost.size());
            assertTrue(assertInstanceOf(MqttException.class, lost.get(0)).retriable());
        }
    }

    @Test
    void trafficFromTheClientCountsAsActivityForItsOwnPings() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker).withKeepAlive(Duration.ofSeconds(2)), null);
            // a publish every 300 ms for 1.5 s: no PINGREQ is needed (it goes out only after keepAlive/2 = 1 s of silence)
            for (int i = 0; i < 5; i++) {
                o.client.publish("t", b("x"), QoS.AT_MOST_ONCE);
                assertEquals(Packets.PUBLISH, o.conn.expect(Packets.PUBLISH).type());
                Thread.sleep(300);
            }
        }
    }

    // ---- reconnect semantics ----

    @Test
    void unacknowledgedMessagesAreResentWithDupWhenTheSessionSurvives() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            List<String> events = new CopyOnWriteArrayList<>();
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, fast(broker).withCleanSession(false), new MqttListener() {
                @Override
                public void connected(boolean sp) {
                    events.add("connected:" + sp);
                }

                @Override
                public void connectionLost(Throwable cause) {
                    events.add("lost");
                }

                @Override
                public void reconnecting(int attempt, Duration delay) {
                    events.add("reconnecting:" + attempt);
                }
            });
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> q1 = client.publish("t", b("one"), QoS.AT_LEAST_ONCE);
            CompletableFuture<Void> q2 = client.publish("t", b("two"), QoS.EXACTLY_ONCE);
            FakeBroker.Packet p1 = c1.expect(Packets.PUBLISH);
            FakeBroker.Packet p2 = c1.expect(Packets.PUBLISH);
            int id2 = idOfPublish(p2);
            c1.ack(Packets.PUBREC, id2);
            c1.expect(Packets.PUBREL);
            c1.close();                                   // lost with one message un-acked and one awaiting PUBCOMP
            await(() -> events.contains("lost"));         // otherwise "three" would still go out on the dying connection (and be resent with DUP)
            CompletableFuture<Void> queuedWhileDown = client.publish("t", b("three"), QoS.AT_LEAST_ONCE);

            FakeBroker.Conn c2 = broker.accept();
            c2.handshake(true, 0);
            FakeBroker.Packet r1 = c2.expect(Packets.PUBLISH);
            assertEquals(2 | 8, r1.flags(), "QoS 1 with DUP");
            assertArrayEquals(p1.body(), r1.body(), "the same identifier and payload");
            assertEquals(id2, c2.expect(Packets.PUBREL).id(), "the QoS 2 message resumes at PUBREL, it is not published again");
            FakeBroker.Packet r3 = c2.expect(Packets.PUBLISH);   // the one accepted while down
            assertEquals(0, r3.flags() & 8, "a first transmission has no DUP");
            c2.ack(Packets.PUBACK, idOfPublish(r1));
            c2.ack(Packets.PUBCOMP, id2);
            c2.ack(Packets.PUBACK, idOfPublish(r3));
            q1.get(5, TimeUnit.SECONDS);
            q2.get(5, TimeUnit.SECONDS);
            queuedWhileDown.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("connected:false", "lost", "reconnecting:1", "connected:true"), events);
            assertTrue(client.sessionPresent());
        }
    }

    @Test
    void whenTheBrokerLostTheSessionInFlightMessagesFailAndSubscriptionsAreRestored() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, fast(broker).withCleanSession(false));
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            List<String> got = new CopyOnWriteArrayList<>();
            CompletableFuture<QoS> s = client.subscribe("x/#", QoS.AT_LEAST_ONCE, m -> got.add(m.topic()));
            FakeBroker.Packet sub = c1.expect(Packets.SUBSCRIBE);
            c1.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 1});
            s.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> lost = client.publish("t", b("v"), QoS.AT_LEAST_ONCE);
            c1.expect(Packets.PUBLISH);
            c1.close();

            FakeBroker.Conn c2 = broker.accept();
            c2.handshake(false, 0);                        // "session present" is 0: everything is gone
            assertTrue(assertInstanceOf(MqttException.class, failure(lost)).retriable());
            FakeBroker.Packet again = c2.expect(Packets.SUBSCRIBE);
            assertArrayEquals(new byte[] {0, 3, 'x', '/', '#', 1}, Arrays.copyOfRange(again.body(), 2, again.body().length),
                    "the same filter at the QoS that was asked for");
            c2.packet(Packets.SUBACK, 0, new byte[] {again.body()[0], again.body()[1], 1});
            c2.publish("x/y", b("v"), 0, 0, false, false);
            await(() -> !got.isEmpty());
            assertEquals(List.of("x/y"), got);
        }
    }

    @Test
    void aSurvivingSessionIsNotResubscribed() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, fast(broker).withCleanSession(false));
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<QoS> s = client.subscribe("x", QoS.AT_MOST_ONCE, m -> { });
            FakeBroker.Packet sub = c1.expect(Packets.SUBSCRIBE);
            c1.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 0});
            s.get(5, TimeUnit.SECONDS);
            c1.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.handshake(true, 0);
            assertNull(c2.read(500), "the broker still has the subscription: sending it again would replay the retained messages");
        }
    }

    @Test
    void aFailedResubscribeIsReported() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            List<String> failed = new CopyOnWriteArrayList<>();
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, fast(broker), new MqttListener() {
                @Override
                public void resubscribeFailed(String filter) {
                    failed.add(filter);
                }
            });
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<QoS> s = client.subscribe("x", QoS.AT_MOST_ONCE, m -> { });
            FakeBroker.Packet sub = c1.expect(Packets.SUBSCRIBE);
            c1.packet(Packets.SUBACK, 0, new byte[] {sub.body()[0], sub.body()[1], 0});
            s.get(5, TimeUnit.SECONDS);
            c1.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.handshake(false, 0);
            FakeBroker.Packet again = c2.expect(Packets.SUBSCRIBE);
            c2.packet(Packets.SUBACK, 0, new byte[] {again.body()[0], again.body()[1], (byte) 0x80});
            await(() -> !failed.isEmpty());
            assertEquals(List.of("x"), failed);
        }
    }

    @Test
    void whileDownQos0AndSubscribeFailAtOnceQos1WaitsAndAnOpenSubscribeFails() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, cfg(broker).withReconnect(
                    Backoff.defaults().withDelays(Duration.ofSeconds(5), Duration.ofSeconds(5)).withJitter(0)));
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            CompletableFuture<QoS> pendingSub = client.subscribe("x", QoS.AT_MOST_ONCE, m -> { });
            c1.expect(Packets.SUBSCRIBE);
            c1.close();
            assertTrue(assertInstanceOf(MqttException.class, failure(pendingSub)).retriable(), "a subscribe in flight fails when the connection goes");
            assertTrue(assertInstanceOf(MqttException.class, failure(client.publish("t", b("x"), QoS.AT_MOST_ONCE))).retriable());
            assertTrue(assertInstanceOf(MqttException.class, failure(client.subscribe("y", QoS.AT_MOST_ONCE, m -> { }))).retriable());
            assertTrue(assertInstanceOf(MqttException.class, failure(client.whenWritable())).retriable());
            CompletableFuture<Void> q = client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            Thread.sleep(100);
            assertFalse(q.isDone(), "QoS 1 waits for the connection");
            client.disconnect().get(5, TimeUnit.SECONDS);
            assertInstanceOf(MqttException.class, failure(q));
        }
    }

    @Test
    void givingUpFailsEverythingAndSaysSo() throws Exception {
        FakeBroker broker = new FakeBroker();
        List<String> events = new CopyOnWriteArrayList<>();
        MqttConfig cfg = cfg(broker).withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(20), Duration.ofMillis(20)).withJitter(0).withMaxAttempts(3));
        CompletableFuture<MqttClient> cf = MqttClient.connect(group, cfg, new MqttListener() {
            @Override
            public void reconnecting(int attempt, Duration delay) {
                events.add("r" + attempt);
            }

            @Override
            public void gaveUp(Throwable cause) {
                events.add("gaveUp");
            }
        });
        FakeBroker.Conn c1 = broker.accept();
        c1.handshake(false, 0);
        MqttClient client = cf.get(5, TimeUnit.SECONDS);
        CompletableFuture<Void> q = client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
        c1.expect(Packets.PUBLISH);
        broker.close();     // nothing listens any more
        c1.close();
        assertFalse(assertInstanceOf(MqttException.class, failure(q)).retriable());
        client.closeFuture().get(5, TimeUnit.SECONDS);
        assertEquals(List.of("r1", "r2", "r3", "gaveUp"), events);
        assertFalse(assertInstanceOf(MqttException.class, failure(client.publish("t", b("x"), QoS.AT_LEAST_ONCE))).retriable());
    }

    @Test
    void aRefusalDuringReconnectRetriesOnlyWhenTheServerIsUnavailable() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> cf = MqttClient.connect(group, fast(broker));
            FakeBroker.Conn c1 = broker.accept();
            c1.handshake(false, 0);
            MqttClient client = cf.get(5, TimeUnit.SECONDS);
            c1.close();
            broker.accept().handshake(false, 3);          // server unavailable: try again
            broker.accept().handshake(false, 5);          // not authorized: give up
            client.closeFuture().get(5, TimeUnit.SECONDS);
            assertFalse(client.isConnected());
        }
    }

    @Test
    void disconnectSendsDisconnectAndDoesNotReconnect() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, fast(broker), null);
            o.client.disconnect().get(5, TimeUnit.SECONDS);
            assertEquals(0, o.conn.expect(Packets.DISCONNECT).body().length);
            assertNull(o.conn.read(), "closed after DISCONNECT");
            assertThrows(java.net.SocketTimeoutException.class, () -> {
                broker.acceptWithin(600);       // a reconnect would land here
            });
        }
    }

    @Test
    void disconnectIsIdempotentAndFailsTheOutstandingWork() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            CompletableFuture<Void> q = o.client.publish("t", b("x"), QoS.AT_LEAST_ONCE);
            o.conn.expect(Packets.PUBLISH);
            o.client.disconnect();
            o.client.disconnect().get(5, TimeUnit.SECONDS);
            o.client.close();
            assertInstanceOf(MqttException.class, failure(q));
        }
    }

    @Test
    void whenWritableCompletesWhileConnected() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker);
            o.client.whenWritable().get(5, TimeUnit.SECONDS);
            assertTrue(o.client.isWritable());
        }
    }

    @Test
    void aBrokerClosingTheConnectionEndsAClientWithoutReconnect() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            List<Throwable> lost = new CopyOnWriteArrayList<>();
            Online o = online(broker, cfg(broker), new MqttListener() {
                @Override
                public void connectionLost(Throwable cause) {
                    lost.add(cause);
                }
            });
            o.conn.close();
            o.client.closeFuture().get(5, TimeUnit.SECONDS);
            assertEquals(1, lost.size());
        }
    }
}
