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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Topic aliases, message expiry and request/response against a real Mosquitto. Skipped when none listens. */
class Mqtt5ExtrasBrokerTest {

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

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void outgoingTopicAliasesDeliverEveryMessageToTheRightTopicEvenWhenTheTableOverflows() throws Exception {
        MqttClient sub = connect(Broker.config5());
        String root = Broker.id("alias");
        BlockingQueue<MqttMessage> got = new LinkedBlockingQueue<>();
        sub.subscribe(root + "/#", QoS.AT_LEAST_ONCE, got::add).get(10, TimeUnit.SECONDS);
        MqttClient pub = connect(Broker.config5().withV5(MqttConfig.V5.DEFAULTS.withOutgoingTopicAliases(true)));
        int max = pub.serverInfo().topicAliasMaximum();
        assertTrue(max > 0, "Mosquitto offers topic aliases");
        int topics = max + 5;   // more topics than aliases: the least recently used are reused
        List<CompletableFuture<Void>> done = new ArrayList<>();
        int n = topics * 6;
        for (int i = 0; i < n; i++) {
            done.add(pub.publish(root + "/room-" + (i * 7 % topics), b("m" + i), QoS.AT_LEAST_ONCE));
        }
        for (CompletableFuture<Void> f : done) {
            f.get(10, TimeUnit.SECONDS);
        }
        for (int i = 0; i < n; i++) {
            MqttMessage m = got.poll(10, TimeUnit.SECONDS);
            assertNotNull(m, "message " + i + " of " + n);
            assertEquals(root + "/room-" + (i * 7 % topics), m.topic(), "message " + i);
            assertEquals("m" + i, m.payloadAsString());
        }
    }

    @Test
    void messageExpiryCountsDownAtTheBrokerAndExpiredMessagesAreGone() throws Exception {
        MqttClient pub = connect(Broker.config5());
        String topic = Broker.id("exp");
        pub.publish(topic, b("short"), QoS.AT_LEAST_ONCE, true, MqttProperties.NONE.withMessageExpiry(3)).get(10, TimeUnit.SECONDS);
        Thread.sleep(1500);
        MqttClient early = connect(Broker.config5());
        BlockingQueue<MqttMessage> q = new LinkedBlockingQueue<>();
        early.subscribe(topic, QoS.AT_LEAST_ONCE, q::add).get(10, TimeUnit.SECONDS);
        MqttMessage m = q.poll(5, TimeUnit.SECONDS);
        assertNotNull(m, "still alive after 1.5 s");
        assertTrue(m.properties().messageExpirySeconds() <= 2 && m.properties().messageExpirySeconds() >= 1,
                "what is left of 3 s: " + m.properties().messageExpirySeconds());
        Thread.sleep(2500);
        MqttClient late = connect(Broker.config5());
        BlockingQueue<MqttMessage> q2 = new LinkedBlockingQueue<>();
        late.subscribe(topic, QoS.AT_LEAST_ONCE, q2::add).get(10, TimeUnit.SECONDS);
        assertNull(q2.poll(1, TimeUnit.SECONDS), "expired");
        pub.publish(topic, new byte[0], QoS.AT_LEAST_ONCE, true).get(10, TimeUnit.SECONDS);   // tidy: clears the retained message
    }

    @Test
    void requestAndReplyThroughTheBroker() throws Exception {
        MqttClient server = connect(Broker.config5());
        String service = Broker.id("svc") + "/upper";
        server.subscribe(service, QoS.AT_LEAST_ONCE, req -> server.reply(req, b(req.payloadAsString().toUpperCase()), QoS.AT_LEAST_ONCE)).get(10, TimeUnit.SECONDS);
        MqttClient client = connect(Broker.config5());
        MqttMessage a = client.request(service, b("hello"), QoS.AT_LEAST_ONCE, Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);
        assertEquals("HELLO", a.payloadAsString());
        List<CompletableFuture<MqttMessage>> many = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            many.add(client.request(service, b("n" + i), QoS.AT_LEAST_ONCE, Duration.ofSeconds(10)));
        }
        for (int i = 0; i < 200; i++) {
            assertEquals("N" + i, many.get(i).get(10, TimeUnit.SECONDS).payloadAsString());
        }
        MqttClient nobody = connect(Broker.config5());
        CompletableFuture<MqttMessage> none = nobody.request(Broker.id("nobody") + "/x", b("x"), QoS.AT_LEAST_ONCE, Duration.ofMillis(300));
        ExecutionExceptionCheck.expectRetriable(none);
    }

    /** The cause of a failed future. */
    private static final class ExecutionExceptionCheck {
        static void expectRetriable(CompletableFuture<?> f) throws Exception {
            try {
                f.get(5, TimeUnit.SECONDS);
                fail("expected a timeout");
            } catch (java.util.concurrent.ExecutionException e) {
                MqttException m = assertInstanceOf(MqttException.class, e.getCause());
                assertTrue(m.retriable());
            }
        }
    }
}
