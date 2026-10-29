package io.hydrangea.mqtt;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enhanced authentication, outgoing topic aliases, message expiry that counts down, and request/response, against a scripted peer
 * (no broker needed).
 */
class Mqtt5ExtrasTest {

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
        return MqttConfig.of("127.0.0.1", b.port()).withVersion(5).withKeepAlive(null).withReconnect(null).withClientId("x5");
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return fail("expected a failure");
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

    private static Props5 aliases(int max) {
        Props5 p = new Props5();
        p.topicAliasMaximum = max;
        return p;
    }

    private static Props5 connectProps(FakeBroker.Packet connect) {
        Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(connect.body()));
        r.str();
        r.u8();
        r.u8();
        r.u16();
        return Props5.read(r);
    }

    private static Props5 authProps(FakeBroker.Packet auth) {
        Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(auth.body()));
        r.u8();
        return Props5.read(r);
    }

    private static void auth(FakeBroker.Conn c, int reason, String method, byte[] data) throws Exception {
        Props5 p = new Props5();
        p.authMethod = method;
        p.authData = data;
        c.packet(Packets.AUTH, 0, new Packets.Writer(16).u8(reason).props(p.encode()).toByteArray());
    }

    /** A published message as the client sent it: topic, id, properties, payload. */
    private record Sent(String topic, int id, Props5 props, byte[] payload) {
    }

    private static Sent sent(FakeBroker.Packet p) {
        int qos = (p.flags() >> 1) & 3;
        Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(p.body()));
        String topic = r.str();
        int id = qos > 0 ? r.u16() : 0;
        Props5 props = Props5.read(r);
        return new Sent(topic, id, props, r.rest());
    }

    // ---- enhanced authentication ----

    /** Records what the client asks of it, and answers with fixed data. */
    private static final class Toy implements MqttAuthenticator {
        final List<String> events = new CopyOnWriteArrayList<>();
        volatile RuntimeException failOnRespond;
        volatile RuntimeException failOnFinish;

        @Override
        public String method() {
            return "TOY";
        }

        @Override
        public Exchange begin() {
            events.add("begin");
            return new Exchange() {
                @Override
                public byte[] initialData() {
                    return b("hello");
                }

                @Override
                public byte[] respond(byte[] challenge) {
                    events.add("respond:" + new String(challenge, StandardCharsets.UTF_8));
                    if (failOnRespond != null) throw failOnRespond;
                    return b("answer-to-" + new String(challenge, StandardCharsets.UTF_8));
                }

                @Override
                public void finish(byte[] data) {
                    events.add("finish:" + (data == null ? null : new String(data, StandardCharsets.UTF_8)));
                    if (failOnFinish != null) throw failOnFinish;
                }
            };
        }
    }

    private static MqttConfig authCfg(FakeBroker broker, Toy toy) {
        return cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withAuthenticator(toy));
    }

    @Test
    void connectCarriesTheMethodAndTheFirstDataAndTheExchangeRunsThroughConnack() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Toy toy = new Toy();
            CompletableFuture<MqttClient> f = MqttClient.connect(group, authCfg(broker, toy));
            FakeBroker.Conn c = broker.accept();
            Props5 cp = connectProps(c.expect(Packets.CONNECT));
            assertEquals("TOY", cp.authMethod);
            assertArrayEquals(b("hello"), cp.authData);
            auth(c, 0x18, "TOY", b("c1"));
            FakeBroker.Packet reply = c.expect(Packets.AUTH);
            assertEquals(0x18, reply.body()[0] & 0xff);
            assertEquals("TOY", authProps(reply).authMethod);
            assertArrayEquals(b("answer-to-c1"), authProps(reply).authData);
            auth(c, 0x18, "TOY", b("c2"));
            assertArrayEquals(b("answer-to-c2"), authProps(c.expect(Packets.AUTH)).authData);
            Props5 done = new Props5();
            done.authMethod = "TOY";
            done.authData = b("welcome");
            c.connack5(false, 0, done);
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            assertTrue(client.isConnected());
            assertEquals(List.of("begin", "respond:c1", "respond:c2", "finish:welcome"), toy.events);
        }
    }

    @Test
    void aBrokerThatAcceptsWithoutAChallengeStillGetsToCheckItsFinalData() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Toy toy = new Toy();
            toy.failOnFinish = new MqttException("the broker's proof is wrong", false);
            CompletableFuture<MqttClient> f = MqttClient.connect(group, authCfg(broker, toy));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.connack5(false, 0, null);
            Throwable e = failure(f);
            assertInstanceOf(MqttException.class, e);
            assertFalse(((MqttException) e).retriable());
            assertTrue(e.getMessage().contains("proof"));
            assertEquals(List.of("begin", "finish:null"), toy.events);
        }
    }

    @Test
    void anAuthenticatorThatThrowsEndsTheConnectionAndIsNotRetried() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Toy toy = new Toy();
            toy.failOnRespond = new IllegalStateException("no key");
            CompletableFuture<MqttClient> f = MqttClient.connect(group, authCfg(broker, toy));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            auth(c, 0x18, "TOY", b("c1"));
            Throwable e = failure(f);
            assertInstanceOf(MqttException.class, e);
            assertFalse(((MqttException) e).retriable());
            assertTrue(e.getMessage().contains("no key"), e.getMessage());
            assertTrue(c.closedByClient(3000));
        }
    }

    @Test
    void anAuthPacketWithAnotherMethodOrAnUnknownReasonIsAProtocolError() throws Exception {
        for (int variant = 0; variant < 3; variant++) {
            try (FakeBroker broker = new FakeBroker()) {
                CompletableFuture<MqttClient> f = MqttClient.connect(group, authCfg(broker, new Toy()));
                FakeBroker.Conn c = broker.accept();
                c.expect(Packets.CONNECT);
                switch (variant) {
                    case 0 -> auth(c, 0x18, "OTHER", b("x"));            // another method
                    case 1 -> auth(c, 0x00, "TOY", b("x"));              // success belongs in CONNACK while connecting
                    default -> c.packet(Packets.AUTH, 0, new byte[0]);   // no method at all
                }
                Throwable e = failure(f);
                assertInstanceOf(MqttException.class, e, "variant " + variant);
                assertFalse(((MqttException) e).retriable(), "variant " + variant);
                assertTrue(c.closedByClient(3000));
            }
        }
    }

    @Test
    void aConnackWithAnotherMethodIsRejected() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, authCfg(broker, new Toy()));
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            Props5 p = new Props5();
            p.authMethod = "SOMETHING-ELSE";
            c.connack5(false, 0, p);
            assertTrue(failure(f).getMessage().contains("SOMETHING-ELSE"));
        }
    }

    @Test
    void reauthenticationRunsAnExchangeOnTheOpenConnectionAndTrafficGoesOnMeanwhile() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Toy toy = new Toy();
            Online o = online(broker, authCfg(broker, toy), null);   // the broker accepted at once; finish(null) came from CONNACK
            assertEquals(List.of("begin", "finish:null"), toy.events);
            CompletableFuture<Void> re = o.client.reauthenticate();
            FakeBroker.Packet start = o.conn.expect(Packets.AUTH);
            assertEquals(0x19, start.body()[0] & 0xff);
            assertEquals("TOY", authProps(start).authMethod);
            assertArrayEquals(b("hello"), authProps(start).authData);
            CompletableFuture<Void> pub = o.client.publish("t/x", b("p"), QoS.AT_LEAST_ONCE);   // not blocked by the re-authentication
            FakeBroker.Packet p = o.conn.expect(Packets.PUBLISH);
            o.conn.ack5(Packets.PUBACK, sent(p).id(), 0, null);
            pub.get(5, TimeUnit.SECONDS);
            auth(o.conn, 0x18, "TOY", b("again"));
            assertArrayEquals(b("answer-to-again"), authProps(o.conn.expect(Packets.AUTH)).authData);
            assertFalse(re.isDone());
            auth(o.conn, 0x00, "TOY", b("ok"));
            re.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("begin", "finish:null", "begin", "respond:again", "finish:ok"), toy.events);
            assertTrue(o.client.isConnected());
        }
    }

    @Test
    void aBrokerThatRefusesReauthenticationClosesTheConnectionAndTheFutureFails() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, authCfg(broker, new Toy()), null);
            CompletableFuture<Void> re = o.client.reauthenticate();
            o.conn.expect(Packets.AUTH);
            o.conn.packet(Packets.DISCONNECT, 0, new byte[] {(byte) 0x8C});
            Throwable e = failure(re);
            assertInstanceOf(MqttException.class, e);
            assertEquals(0x8C, ((MqttException) e).reasonCode());
        }
    }

    @Test
    void anAuthPacketNobodyAskedForIsAProtocolError() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, authCfg(broker, new Toy()), null);
            auth(o.conn, 0x00, "TOY", null);
            assertTrue(o.conn.closedByClient(3000));
        }
    }

    @Test
    void reauthenticateNeedsAnAuthenticatorAndASecondOneIsRefusedWhileOneRuns() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online plain = online(broker, cfg(broker), null);
            assertInstanceOf(IllegalStateException.class, failure(plain.client.reauthenticate()));
        }
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, authCfg(broker, new Toy()), null);
            CompletableFuture<Void> first = o.client.reauthenticate();
            o.conn.expect(Packets.AUTH);
            Throwable e = failure(o.client.reauthenticate());
            assertInstanceOf(MqttException.class, e);
            assertTrue(e.getMessage().contains("already"));
            assertFalse(first.isDone());
        }
    }

    @Test
    void anAuthenticatorIsRefusedForMqtt311() {
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withV5(MqttConfig.V5.DEFAULTS.withAuthenticator(new Toy())));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withV5(MqttConfig.V5.DEFAULTS.withOutgoingTopicAliases(true)));
    }

    // ---- SCRAM-SHA-256: the example of RFC 7677 ----

    private static final String NONCE = "rOprNGfwEbeRWgbNEkqO";
    private static final String SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    private static final String CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    private static final String SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    @Test
    void scramSha256MatchesTheRfc7677Example() {
        MqttAuthenticator.Exchange ex = new ScramSha256("user", "pencil", NONCE).begin();
        assertEquals("n,,n=user,r=" + NONCE, new String(ex.initialData(), StandardCharsets.UTF_8));
        assertEquals(CLIENT_FINAL, new String(ex.respond(b(SERVER_FIRST)), StandardCharsets.UTF_8));
        ex.finish(b(SERVER_FINAL));   // the right proof is accepted
        MqttAuthenticator.Exchange bad = new ScramSha256("user", "pencil", NONCE).begin();
        bad.respond(b(SERVER_FIRST));
        MqttException e = assertThrows(MqttException.class, () -> bad.finish(b("v=" + "A".repeat(43) + "=")));
        assertTrue(e.getMessage().contains("proof"));
    }

    @Test
    void scramSha256RefusesBrokenServerMessages() {
        for (String bad : new String[] {"r=other,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",                   // nonce does not extend ours
                "r=" + NONCE + ",s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",                               // adds nothing
                "r=" + NONCE + "x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=abc",
                "r=" + NONCE + "x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=2000000000",                         // a broker must not make us hash for hours
                "r=" + NONCE + "x,i=4096",
                ""}) {
            MqttAuthenticator.Exchange ex = new ScramSha256("user", "pencil", NONCE).begin();
            assertThrows(MqttException.class, () -> ex.respond(b(bad)), bad);
        }
        MqttAuthenticator.Exchange early = new ScramSha256("user", "pencil", NONCE).begin();
        assertThrows(MqttException.class, () -> early.finish(b(SERVER_FINAL)), "a final message before the exchange ran");
        MqttAuthenticator.Exchange twice = new ScramSha256("user", "pencil", NONCE).begin();
        twice.respond(b(SERVER_FIRST));
        assertThrows(MqttException.class, () -> twice.respond(b(SERVER_FIRST)));
    }

    @Test
    void scramSha256ThroughAClientAgainstAScriptedBroker() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withAuthenticator(new ScramSha256("user", "pencil", NONCE)));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            Props5 cp = connectProps(c.expect(Packets.CONNECT));
            assertEquals("SCRAM-SHA-256", cp.authMethod);
            assertEquals("n,,n=user,r=" + NONCE, new String(cp.authData, StandardCharsets.UTF_8));
            auth(c, 0x18, "SCRAM-SHA-256", b(SERVER_FIRST));
            assertEquals(CLIENT_FINAL, new String(authProps(c.expect(Packets.AUTH)).authData, StandardCharsets.UTF_8));
            Props5 done = new Props5();
            done.authMethod = "SCRAM-SHA-256";
            done.authData = b(SERVER_FINAL);
            c.connack5(false, 0, done);
            assertTrue(f.get(5, TimeUnit.SECONDS).isConnected());
        }
    }

    @Test
    void scramSha256RejectsABrokerThatDoesNotKnowThePassword() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withAuthenticator(new ScramSha256("user", "pencil", NONCE)));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            auth(c, 0x18, "SCRAM-SHA-256", b(SERVER_FIRST));
            c.expect(Packets.AUTH);
            Props5 done = new Props5();
            done.authMethod = "SCRAM-SHA-256";
            done.authData = b("v=" + "A".repeat(43) + "=");
            c.connack5(false, 0, done);
            assertTrue(failure(f).getMessage().contains("does not know the password"));
        }
    }

    // ---- outgoing topic aliases ----

    private static MqttConfig aliasCfg(FakeBroker broker) {
        return cfg(broker).withV5(MqttConfig.V5.DEFAULTS.withOutgoingTopicAliases(true));
    }

    @Test
    void aTopicIsSentOnceAndThenAsAnAlias() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, aliasCfg(broker), aliases(2));
            for (int i = 0; i < 3; i++) {
                o.client.publish("sensors/room-1/temp", b("v" + i), QoS.AT_MOST_ONCE);
            }
            Sent first = sent(o.conn.expect(Packets.PUBLISH));
            assertEquals("sensors/room-1/temp", first.topic());
            assertEquals(1, first.props().topicAlias);
            for (int i = 1; i < 3; i++) {
                Sent next = sent(o.conn.expect(Packets.PUBLISH));
                assertEquals("", next.topic(), "known: only the alias");
                assertEquals(1, next.props().topicAlias);
                assertArrayEquals(b("v" + i), next.payload());
            }
        }
    }

    @Test
    void theLeastRecentlyUsedAliasIsReusedWhenTheBrokerAllowsOnlyAFew() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, aliasCfg(broker), aliases(2));
            for (String t : new String[] {"topic/aaaa", "topic/bbbb", "topic/aaaa", "topic/cccc", "topic/bbbb", "topic/cccc"}) {
                o.client.publish(t, b("x"), QoS.AT_MOST_ONCE);
            }
            record Seen(String topic, int alias) {
            }
            List<Seen> seen = new java.util.ArrayList<>();
            for (int i = 0; i < 6; i++) {
                Sent s = sent(o.conn.expect(Packets.PUBLISH));
                seen.add(new Seen(s.topic(), s.props().topicAlias));
            }
            assertEquals(List.of(
                    new Seen("topic/aaaa", 1),   // defines 1
                    new Seen("topic/bbbb", 2),   // defines 2
                    new Seen("", 1),             // aaaa again
                    new Seen("topic/cccc", 2),   // the table is full: bbbb is the least recently used, its alias goes to cccc
                    new Seen("topic/bbbb", 1),   // bbbb is unknown again: aaaa (older than cccc) gives up alias 1
                    new Seen("", 2)),            // cccc is still 2
                    seen);
        }
    }

    @Test
    void noAliasesWhenOffWhenTheBrokerAllowsNoneOrForShortTopics() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online off = online(broker, cfg(broker), aliases(5));   // the option is off
            off.client.publish("sensors/room-1/temp", b("x"), QoS.AT_MOST_ONCE);
            off.client.publish("sensors/room-1/temp", b("x"), QoS.AT_MOST_ONCE);
            for (int i = 0; i < 2; i++) {
                Sent s = sent(off.conn.expect(Packets.PUBLISH));
                assertEquals("sensors/room-1/temp", s.topic());
                assertNull(s.props().topicAlias);
            }
        }
        try (FakeBroker broker = new FakeBroker()) {
            Online none = online(broker, aliasCfg(broker), null);   // the broker said nothing: no aliases
            none.client.publish("sensors/room-1/temp", b("x"), QoS.AT_MOST_ONCE);
            assertNull(sent(none.conn.expect(Packets.PUBLISH)).props().topicAlias);
        }
        try (FakeBroker broker = new FakeBroker()) {
            Online shortTopic = online(broker, aliasCfg(broker), aliases(5));
            shortTopic.client.publish("a/b", b("x"), QoS.AT_MOST_ONCE);
            shortTopic.client.publish("a/b", b("x"), QoS.AT_MOST_ONCE);
            for (int i = 0; i < 2; i++) {
                Sent s = sent(shortTopic.conn.expect(Packets.PUBLISH));
                assertEquals("a/b", s.topic(), "an alias costs more than a topic of three bytes");
                assertNull(s.props().topicAlias);
            }
        }
    }

    @Test
    void aliasesStartOverOnANewConnectionAndAResentMessageDefinesItsAliasAgain() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = aliasCfg(broker).withCleanSession(false).withReconnect(new Backoff(Duration.ofMillis(20), Duration.ofMillis(50), 2.0, 0.0, 5));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.connack5(false, 0, aliases(3));
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            client.publish("sensors/room-1/temp", b("one"), QoS.AT_LEAST_ONCE).thenApply(v -> v);
            CompletableFuture<Void> two = client.publish("sensors/room-1/temp", b("two"), QoS.AT_LEAST_ONCE);
            Sent s1 = sent(c.expect(Packets.PUBLISH));
            Sent s2 = sent(c.expect(Packets.PUBLISH));
            assertEquals("sensors/room-1/temp", s1.topic());
            assertEquals("", s2.topic());
            c.ack5(Packets.PUBACK, s1.id(), 0, null);   // "one" is done; "two" is not acknowledged
            c.socket.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.expect(Packets.CONNECT);
            c2.connack5(true, 0, aliases(3));   // the session survived
            FakeBroker.Packet again = c2.expect(Packets.PUBLISH);
            assertTrue((again.flags() & 8) != 0, "DUP");
            Sent resent = sent(again);
            assertEquals("sensors/room-1/temp", resent.topic(), "this connection knows no alias yet: the topic must be spelled out");
            assertEquals(1, resent.props().topicAlias);
            c2.ack5(Packets.PUBACK, resent.id(), 0, null);
            two.get(5, TimeUnit.SECONDS);
        }
    }

    // ---- message expiry ----

    @Test
    void theMessageExpiryIntervalIsSentAsAskedWhenThereIsNoDelay() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            o.client.publish("t/e", b("x"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withMessageExpiry(30).withContentType("a/b"));
            Sent s = sent(o.conn.expect(Packets.PUBLISH));
            assertEquals(30L, s.props().messageExpiry);
            assertEquals("a/b", s.props().contentType, "the other properties are still there");
        }
    }

    @Test
    void aMessageThatWaitsInTheQueueSendsLessTimeAndOneThatRanOutIsDropped() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Props5 limit = new Props5();
            limit.receiveMaximum = 1;
            Online o = online(broker, cfg(broker), limit);
            o.client.publish("t/e", b("first"), QoS.AT_LEAST_ONCE);
            CompletableFuture<Void> waits = o.client.publish("t/e", b("second"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withMessageExpiry(10));
            CompletableFuture<Void> shortLived = o.client.publish("t/e", b("third"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withMessageExpiry(1));
            Sent first = sent(o.conn.expect(Packets.PUBLISH));
            Thread.sleep(1300);   // the other two wait for the window
            o.conn.ack5(Packets.PUBACK, first.id(), 0, null);
            Sent second = sent(o.conn.expect(Packets.PUBLISH));
            assertEquals("second", new String(second.payload(), StandardCharsets.UTF_8));
            assertEquals(9L, second.props().messageExpiry, "10 s minus the 1 s it waited");
            o.conn.ack5(Packets.PUBACK, second.id(), 0, null);
            waits.get(5, TimeUnit.SECONDS);
            Throwable e = failure(shortLived);
            assertInstanceOf(MqttException.class, e);
            assertFalse(((MqttException) e).retriable());
            assertTrue(e.getMessage().contains("expired"), e.getMessage());
            assertNull(o.conn.read(300), "the expired message was never sent");
        }
    }

    @Test
    void aResentMessageCarriesTheTimeThatIsLeft() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            MqttConfig cfg = cfg(broker).withCleanSession(false).withReconnect(new Backoff(Duration.ofMillis(20), Duration.ofMillis(50), 2.0, 0.0, 5));
            CompletableFuture<MqttClient> f = MqttClient.connect(group, cfg);
            FakeBroker.Conn c = broker.accept();
            c.expect(Packets.CONNECT);
            c.connack5(false, 0, null);
            MqttClient client = f.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> p = client.publish("t/e", b("x"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withMessageExpiry(10));
            CompletableFuture<Void> p2 = client.publish("t/e", b("y"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withMessageExpiry(1));
            assertEquals(10L, sent(c.expect(Packets.PUBLISH)).props().messageExpiry);
            c.expect(Packets.PUBLISH);
            Thread.sleep(1300);
            c.socket.close();
            FakeBroker.Conn c2 = broker.accept();
            c2.expect(Packets.CONNECT);
            c2.connack5(true, 0, null);
            Sent a = sent(c2.expect(Packets.PUBLISH));
            Sent b2 = sent(c2.expect(Packets.PUBLISH));
            assertEquals(9L, a.props().messageExpiry);
            assertEquals(1L, b2.props().messageExpiry, "ran out: goes again with the shortest interval, since 0 means no expiry to some brokers");
            c2.ack5(Packets.PUBACK, a.id(), 0, null);
            c2.ack5(Packets.PUBACK, b2.id(), 0, null);
            p.get(5, TimeUnit.SECONDS);
            p2.get(5, TimeUnit.SECONDS);
        }
    }

    // ---- request / response ----

    private static String correlationOf(Sent s) {
        assertEquals(8, s.props().correlationData.length);
        return java.util.HexFormat.of().formatHex(s.props().correlationData);
    }

    /** Answers the SUBSCRIBE the first request makes, and returns the request as published. */
    private static Sent acceptRequest(FakeBroker.Conn c) throws Exception {
        FakeBroker.Packet sub = c.expect(Packets.SUBSCRIBE);
        c.suback5(Packets.SUBACK, sub.id(), 1);
        Sent req = sent(c.expect(Packets.PUBLISH));
        if (((req.id()) > 0)) {
            c.ack5(Packets.PUBACK, req.id(), 0, null);
        }
        return req;
    }

    @Test
    void aRequestSubscribesToItsResponseTopicPublishesAndMatchesTheAnswerByCorrelation() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> answer = o.client.request("svc/echo", b("ping"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5));
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            Packets.Reader r = new Packets.Reader(ByteBuffer.wrap(sub.body()));
            r.u16();
            Props5.read(r);
            String filter = r.str();
            assertTrue(filter.startsWith("hydrangea/response/"), filter);
            o.conn.suback5(Packets.SUBACK, sub.id(), 1);
            Sent req = sent(o.conn.expect(Packets.PUBLISH));
            assertEquals("svc/echo", req.topic());
            assertEquals(filter, req.props().responseTopic, "it asks to be answered where it listens");
            assertArrayEquals(b("ping"), req.payload());
            o.conn.ack5(Packets.PUBACK, req.id(), 0, null);
            assertFalse(answer.isDone());
            Props5 reply = new Props5();
            reply.correlationData = req.props().correlationData;
            o.conn.publish5(filter, b("pong"), 0, 0, reply);
            MqttMessage m = answer.get(5, TimeUnit.SECONDS);
            assertEquals("pong", m.payloadAsString());
            assertEquals(filter, m.topic());
        }
    }

    @Test
    void severalRequestsShareOneSubscriptionAndAnswersMayComeInAnyOrder() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> a = o.client.request("svc/a", b("A"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5));
            Sent ra = acceptRequest(o.conn);
            CompletableFuture<MqttMessage> b2 = o.client.request("svc/b", b("B"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5));
            Sent rb = sent(o.conn.expect(Packets.PUBLISH));   // no second SUBSCRIBE
            o.conn.ack5(Packets.PUBACK, rb.id(), 0, null);
            assertNotEquals(correlationOf(ra), correlationOf(rb));
            for (Sent s : new Sent[] {rb, ra}) {   // answered in reverse
                Props5 reply = new Props5();
                reply.correlationData = s.props().correlationData;
                o.conn.publish5(s.props().responseTopic, b("re:" + new String(s.payload(), StandardCharsets.UTF_8)), 0, 0, reply);
            }
            assertEquals("re:A", a.get(5, TimeUnit.SECONDS).payloadAsString());
            assertEquals("re:B", b2.get(5, TimeUnit.SECONDS).payloadAsString());
        }
    }

    @Test
    void aRequestTimesOutAndALateOrUnknownAnswerIsIgnored() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> answer = o.client.request("svc/slow", b("x"), QoS.AT_LEAST_ONCE, Duration.ofMillis(300));
            Sent req = acceptRequest(o.conn);
            Throwable e = failure(answer);
            assertInstanceOf(MqttException.class, e);
            assertTrue(((MqttException) e).retriable());
            assertTrue(e.getMessage().contains("no response"), e.getMessage());
            Props5 late = new Props5();
            late.correlationData = req.props().correlationData;
            o.conn.publish5(req.props().responseTopic, b("late"), 0, 0, late);
            Props5 stranger = new Props5();
            stranger.correlationData = new byte[] {1, 2, 3};
            o.conn.publish5(req.props().responseTopic, b("stranger"), 0, 0, stranger);
            o.conn.publish5(req.props().responseTopic, b("none"), 0, 0, null);
            assertTrue(o.client.isConnected());
            assertEquals(0, o.client.pendingMessages().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aRequestFailsWhenTheSubscriptionOrThePublishIsRefusedAndTheNextOneTriesTheSubscriptionAgain() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> a = o.client.request("svc/a", b("x"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5));
            FakeBroker.Packet sub = o.conn.expect(Packets.SUBSCRIBE);
            o.conn.suback5(Packets.SUBACK, sub.id(), 0x87);
            Throwable e = failure(a);
            assertEquals(0x87, ((MqttException) e).reasonCode());
            CompletableFuture<MqttMessage> b2 = o.client.request("svc/a", b("x"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5));
            FakeBroker.Packet sub2 = o.conn.expect(Packets.SUBSCRIBE);   // asked again
            o.conn.suback5(Packets.SUBACK, sub2.id(), 1);
            Sent req = sent(o.conn.expect(Packets.PUBLISH));
            o.conn.ack5(Packets.PUBACK, req.id(), 0x87, null);   // the publish is refused
            assertEquals(0x87, ((MqttException) failure(b2)).reasonCode());
        }
    }

    @Test
    void aRequestWithItsOwnResponseTopicUsesIt() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> answer = o.client.request("svc/a", b("x"), QoS.AT_LEAST_ONCE,
                    MqttProperties.NONE.withResponseTopic("my/replies"), Duration.ofSeconds(5));
            Sent req = acceptRequest(o.conn);
            assertEquals("my/replies", req.props().responseTopic);
            Props5 reply = new Props5();
            reply.correlationData = req.props().correlationData;
            o.conn.publish5("my/replies", b("ok"), 0, 0, reply);
            assertEquals("ok", answer.get(5, TimeUnit.SECONDS).payloadAsString());
        }
    }

    @Test
    void requestsAreRefusedForBadArgumentsAndForMqtt311() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            assertInstanceOf(IllegalArgumentException.class, failure(o.client.request("a/b", b("x"), QoS.AT_MOST_ONCE, Duration.ZERO)));
            assertInstanceOf(IllegalArgumentException.class, failure(o.client.request("a/b", b("x"), QoS.AT_MOST_ONCE, null)));
            assertInstanceOf(IllegalArgumentException.class,
                    failure(o.client.request("a/b", b("x"), QoS.AT_MOST_ONCE, MqttProperties.NONE.withCorrelationData(new byte[1]), Duration.ofSeconds(1))));
            assertInstanceOf(IllegalArgumentException.class,
                    failure(o.client.request("a/b", b("x"), QoS.AT_MOST_ONCE, MqttProperties.NONE.withResponseTopic("a/#"), Duration.ofSeconds(1))));
            assertInstanceOf(IllegalArgumentException.class, failure(o.client.reply(new MqttMessage("a/b", b("x"), QoS.AT_MOST_ONCE, false, false), b("y"), QoS.AT_MOST_ONCE)));
        }
        try (FakeBroker broker = new FakeBroker()) {
            CompletableFuture<MqttClient> f = MqttClient.connect(group, MqttConfig.of("127.0.0.1", broker.port()).withKeepAlive(null).withReconnect(null).withClientId("v3"));
            FakeBroker.Conn c = broker.accept();
            c.handshake(false, 0);
            MqttClient v3 = f.get(5, TimeUnit.SECONDS);
            assertInstanceOf(IllegalArgumentException.class, failure(v3.request("a/b", b("x"), QoS.AT_MOST_ONCE, Duration.ofSeconds(1))));
        }
    }

    @Test
    void replyPublishesToTheResponseTopicWithTheCorrelationData() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            MqttProperties asked = MqttProperties.NONE.withResponseTopic("who/asked").withCorrelationData(new byte[] {9, 8, 7});
            MqttMessage request = new MqttMessage("svc/echo", b("ping"), QoS.AT_LEAST_ONCE, false, false, asked);
            CompletableFuture<Void> done = o.client.reply(request, b("pong"), QoS.AT_LEAST_ONCE);
            Sent s = sent(o.conn.expect(Packets.PUBLISH));
            assertEquals("who/asked", s.topic());
            assertArrayEquals(new byte[] {9, 8, 7}, s.props().correlationData);
            assertNull(s.props().responseTopic);
            o.conn.ack5(Packets.PUBACK, s.id(), 0, null);
            done.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void closingTheClientFailsTheRequestsThatWait() throws Exception {
        try (FakeBroker broker = new FakeBroker()) {
            Online o = online(broker, cfg(broker), null);
            CompletableFuture<MqttMessage> answer = o.client.request("svc/a", b("x"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(30));
            acceptRequest(o.conn);
            o.client.disconnect().get(5, TimeUnit.SECONDS);
            Throwable e = failure(answer);
            assertInstanceOf(MqttException.class, e);
            assertFalse(((MqttException) e).retriable());
        }
    }
}
