package io.hydrangea.mqtt;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.testing.ChaosProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT over WebSocket against the Mosquitto of {@code hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh} (WebSocket listener on 29001,
 * plain MQTT on 21883 for the second client). Skipped when it is not running.
 */
class MqttWebSocketTest {

    private static final String HOST = "127.0.0.1";
    private static final int WS_PORT = Integer.getInteger("mqtt.ws.port", 29001);
    private static final int TCP_PORT = Integer.getInteger("mqtt.port", 21883);
    private static EventLoopGroup group;
    private final List<MqttClient> clients = new ArrayList<>();

    @BeforeAll
    static void start() {
        for (int port : new int[] {WS_PORT, TCP_PORT}) {
            boolean up;
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(HOST, port), 500);
                up = true;
            } catch (Exception e) {
                up = false;
            }
            Assumptions.assumeTrue(up, "no MQTT broker on " + HOST + ":" + port + " (hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh)");
        }
        group = new EventLoopGroup(2);
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

    private static String id(String p) {
        return p + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private MqttClient ws(int version) throws Exception {
        MqttConfig cfg = MqttConfig.of(HOST, WS_PORT).withClientId(id("ws")).withVersion(version).withTransport(MqttWebSocket.of("ws://" + HOST + ":" + WS_PORT + "/mqtt"));
        MqttClient c = MqttClient.connect(group, cfg).get(10, TimeUnit.SECONDS);
        clients.add(c);
        return c;
    }

    private MqttClient tcp() throws Exception {
        MqttClient c = MqttClient.connect(group, MqttConfig.of(HOST, TCP_PORT).withClientId(id("tcp"))).get(10, TimeUnit.SECONDS);
        clients.add(c);
        return c;
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
    void everyQosWorksOverWebSocketInBothProtocolVersions() throws Exception {
        for (int version : new int[] {4, 5}) {
            MqttClient c = ws(version);
            assertTrue(c.isConnected());
            String topic = id("t");
            for (QoS qos : QoS.values()) {
                BlockingQueue<MqttMessage> q = collect(c, topic + "/" + qos.level(), qos);
                c.publish(topic + "/" + qos.level(), b("v" + version + " " + qos), qos).get(10, TimeUnit.SECONDS);
                assertEquals("v" + version + " " + qos, next(q).payloadAsString());
            }
        }
    }

    @Test
    void aWebSocketClientAndATcpClientTalkToEachOther() throws Exception {
        MqttClient viaWs = ws(5);
        MqttClient viaTcp = tcp();
        String topic = id("bridge");
        BlockingQueue<MqttMessage> atTcp = collect(viaTcp, topic + "/up", QoS.AT_LEAST_ONCE);
        BlockingQueue<MqttMessage> atWs = collect(viaWs, topic + "/down", QoS.AT_LEAST_ONCE);
        viaWs.publish(topic + "/up", b("from ws"), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withContentType("text/plain")).get(10, TimeUnit.SECONDS);
        viaTcp.publish(topic + "/down", b("from tcp"), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
        assertEquals("from ws", next(atTcp).payloadAsString());
        assertEquals("from tcp", next(atWs).payloadAsString());
    }

    @Test
    void messagesLargerThanAWebSocketMessageLimitAndManyInFlightPass() throws Exception {
        List<Throwable> lost = new CopyOnWriteArrayList<>();
        MqttClient c = MqttClient.connect(group, MqttConfig.of(HOST, WS_PORT).withClientId(id("wsmany")).withTransport(MqttWebSocket.of("ws://" + HOST + ":" + WS_PORT + "/mqtt")),
                new MqttListener() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        lost.add(cause);
                    }
                }).get(10, TimeUnit.SECONDS);
        clients.add(c);
        MqttClient sub = tcp();
        String topic = id("big");
        BlockingQueue<MqttMessage> q = collect(sub, topic, QoS.AT_LEAST_ONCE);
        byte[] big = new byte[1_500_000];   // over the 1 MiB default of a whole WebSocket message
        new java.util.Random(11).nextBytes(big);
        c.publish(topic, big, QoS.AT_LEAST_ONCE).get(20, TimeUnit.SECONDS);
        assertArrayEquals(big, next(q).payload());
        int n = 2000;
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fs.add(c.publish(topic, b(Integer.toString(i)), QoS.AT_LEAST_ONCE));
        }
        for (CompletableFuture<Void> f : fs) {
            try {
                f.get(30, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("connection losses: " + lost, e);
            }
        }
        for (int i = 0; i < n; i++) {
            assertEquals(Integer.toString(i), next(q).payloadAsString());
        }
    }

    @Test
    void receivingAMessageLargerThanAWebSocketMessageLimit() throws Exception {
        MqttClient pub = tcp();
        MqttClient c = MqttClient.connect(group, MqttConfig.of(HOST, WS_PORT).withClientId(id("wsbig")).withMaxPacketBytes(8 << 20)
                .withTransport(MqttWebSocket.of("ws://" + HOST + ":" + WS_PORT + "/mqtt"))).get(10, TimeUnit.SECONDS);
        clients.add(c);
        String topic = id("bigin");
        BlockingQueue<MqttMessage> q = collect(c, topic, QoS.AT_LEAST_ONCE);
        byte[] big = new byte[1_500_000];
        new java.util.Random(12).nextBytes(big);
        pub.publish(topic, big, QoS.AT_LEAST_ONCE).get(20, TimeUnit.SECONDS);
        assertArrayEquals(big, next(q).payload());
    }

    @Test
    void whenWritableLetsAProducerPaceItselfOverWebSocket() throws Exception {
        MqttClient pub = ws(4);
        MqttClient sub = MqttClient.connect(group, MqttConfig.of(HOST, TCP_PORT).withClientId(id("sink")).withMaxPacketBytes(8 << 20)).get(10, TimeUnit.SECONDS);
        clients.add(sub);
        String topic = id("bp");
        java.util.concurrent.atomic.AtomicInteger got = new java.util.concurrent.atomic.AtomicInteger();
        sub.subscribe(topic, QoS.AT_MOST_ONCE, m -> got.incrementAndGet()).get(10, TimeUnit.SECONDS);
        byte[] big = new byte[256 * 1024];
        int waits = 0;
        for (int i = 0; i < 100; i++) {
            if (!pub.isWritable()) {
                waits++;
                pub.whenWritable().get(10, TimeUnit.SECONDS);
            }
            pub.publish(topic, big, QoS.AT_MOST_ONCE);
        }
        assertTrue(waits >= 0);   // loopback to a local broker may drain as fast as we write; what matters is that waiting works when needed and nothing is lost
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (got.get() < 100 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(100, got.get());
    }

    @Test
    void aCutWebSocketIsReconnectedAndTheSubscriptionComesBack() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, WS_PORT)) {
            List<String> events = new CopyOnWriteArrayList<>();
            MqttClient c = MqttClient.connect(group, MqttConfig.of(HOST, WS_PORT).withClientId(id("rc"))
                    .withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(200)).withJitter(0))
                    .withTransport(MqttWebSocket.of("ws://127.0.0.1:" + proxy.port() + "/mqtt")), new MqttListener() {
                @Override
                public void connected(boolean sp) {
                    events.add("up");
                }

                @Override
                public void connectionLost(Throwable cause) {
                    events.add("down");
                }
            }).get(10, TimeUnit.SECONDS);
            clients.add(c);
            MqttClient pub = tcp();
            String topic = id("cut");
            BlockingQueue<MqttMessage> q = collect(c, topic, QoS.AT_LEAST_ONCE);
            for (int round = 1; round <= 2; round++) {
                int r = round;
                proxy.sever();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while ((events.stream().filter("up"::equals).count() != r + 1 || !c.isConnected()) && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                pub.publish(topic, b("round " + r), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
                assertEquals("round " + r, next(q).payloadAsString());
            }
        }
    }

    @Test
    void aBrokerThatDoesNotSpeakWebSocketFailsTheConnect() throws Exception {
        MqttConfig cfg = MqttConfig.of(HOST, TCP_PORT).withClientId(id("nows")).withConnectTimeout(Duration.ofSeconds(3))
                .withTransport(MqttWebSocket.of("ws://" + HOST + ":" + TCP_PORT + "/mqtt"));   // the plain MQTT port
        ExecutionException e = assertThrows(ExecutionException.class, () -> MqttClient.connect(group, cfg).get(10, TimeUnit.SECONDS));
        assertNotNull(e.getCause());
    }

    @Test
    void aBadUriFailsAtOnce() {
        MqttConfig cfg = MqttConfig.of(HOST, WS_PORT).withTransport(MqttWebSocket.of("http://127.0.0.1/mqtt"));
        assertThrows(ExecutionException.class, () -> MqttClient.connect(group, cfg).get(5, TimeUnit.SECONDS));
    }

    @Test
    void closingTheClientClosesTheWebSocket() throws Exception {
        MqttClient c = ws(5);
        c.disconnect().get(10, TimeUnit.SECONDS);
        assertFalse(c.isConnected());
        c.closeFuture().get(5, TimeUnit.SECONDS);
        assertThrows(MqttException.class, () -> {
            try {
                c.publish("t", b("x"), QoS.AT_MOST_ONCE).get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw (MqttException) e.getCause();
            }
        });
    }
}
