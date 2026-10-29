package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Credentials, ACLs and TLS against the Mosquitto of {@code start-broker.sh}. Skipped when it is not running. */
class MqttAuthTlsTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        Broker.require(Broker.AUTH_PORT);
        Broker.require(Broker.TLS_PORT);
        group = Broker.newGroup();
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static MqttConfig auth() {
        return MqttConfig.of(Broker.HOST, Broker.AUTH_PORT).withClientId(Broker.id("a"));
    }

    private static Throwable connectFailure(MqttConfig cfg) throws Exception {
        try {
            MqttClient.connect(group, cfg).get(15, TimeUnit.SECONDS).close();
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return fail("expected the connect to fail");
    }

    @Test
    void correctCredentialsAreAccepted() throws Exception {
        MqttClient c = MqttClient.connect(group, auth().withCredentials("alice", "secret")).get(10, TimeUnit.SECONDS);
        assertTrue(c.isConnected());
        c.close();
    }

    @Test
    void wrongPasswordIsRefusedAndIsNotRetriable() throws Exception {
        MqttException e = assertInstanceOf(MqttException.class, connectFailure(auth().withCredentials("alice", "nope")));
        assertEquals(MqttException.REFUSED_NOT_AUTHORIZED, e.returnCode(), "Mosquitto answers 'not authorized' for bad credentials");
        assertFalse(e.retriable());
    }

    @Test
    void anonymousIsRefusedWhereCredentialsAreRequired() throws Exception {
        MqttException e = assertInstanceOf(MqttException.class, connectFailure(auth()));
        assertFalse(e.retriable());
    }

    @Test
    void aclDeniedTopicsAreSilentWhileAllowedOnesWork() throws Exception {
        // Mosquitto 2 accepts a subscription that the ACL denies (it grants the QoS and never delivers); a broker that answers 0x80 is
        // covered by MqttProtocolTest. What matters here: the connection survives and only the allowed tree carries messages.
        MqttClient c = MqttClient.connect(group, auth().withCredentials("alice", "secret")).get(10, TimeUnit.SECONDS);
        try {
            java.util.concurrent.BlockingQueue<MqttMessage> q = new java.util.concurrent.LinkedBlockingQueue<>();
            c.subscribe("allowed/x", QoS.AT_LEAST_ONCE, q::add).get(10, TimeUnit.SECONDS);
            c.subscribe("forbidden/x", QoS.AT_LEAST_ONCE, q::add).get(10, TimeUnit.SECONDS);
            c.publish("forbidden/x", "no".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
            c.publish("allowed/x", "yes".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
            assertEquals("yes", q.poll(10, TimeUnit.SECONDS).payloadAsString());
            assertNull(q.poll(500, TimeUnit.MILLISECONDS));
            assertTrue(c.isConnected());
        } finally {
            c.close();
        }
    }

    @Test
    void tlsWithTheRightCaWorksAndCarriesMessages() throws Exception {
        MqttConfig cfg = MqttConfig.of("localhost", Broker.TLS_PORT).withClientId(Broker.id("tls")).withTls(Broker.trusting("/mqtt/ca.pem"));
        MqttClient c = MqttClient.connect(group, cfg).get(10, TimeUnit.SECONDS);
        try {
            String topic = Broker.id("tls");
            java.util.concurrent.BlockingQueue<MqttMessage> q = new java.util.concurrent.LinkedBlockingQueue<>();
            c.subscribe(topic, QoS.EXACTLY_ONCE, q::add).get(10, TimeUnit.SECONDS);
            byte[] big = new byte[1_000_000];
            new java.util.Random(3).nextBytes(big);
            c.publish(topic, big, QoS.EXACTLY_ONCE).get(20, TimeUnit.SECONDS);
            assertArrayEquals(big, q.poll(10, TimeUnit.SECONDS).payload());
        } finally {
            c.close();
        }
    }

    @Test
    void tlsWithAnUntrustedCaIsRefused() throws Exception {
        MqttConfig cfg = MqttConfig.of("localhost", Broker.TLS_PORT).withClientId(Broker.id("tls")).withTls(Broker.trusting("/mqtt/other-ca.pem"));
        Throwable t = connectFailure(cfg);
        assertInstanceOf(javax.net.ssl.SSLException.class, t.getCause() != null && !(t instanceof javax.net.ssl.SSLException) ? t.getCause() : t);
    }

    @Test
    void plainClientOnTheTlsPortDoesNotConnect() throws Exception {
        MqttConfig cfg = MqttConfig.of(Broker.HOST, Broker.TLS_PORT).withClientId(Broker.id("x")).withConnectTimeout(java.time.Duration.ofSeconds(2));
        connectFailure(cfg);
    }

    @Test
    void tlsClientOnAPlainPortDoesNotConnect() throws Exception {
        MqttConfig cfg = MqttConfig.of(Broker.HOST, Broker.PORT).withClientId(Broker.id("x")).withTls(Broker.trusting("/mqtt/ca.pem"))
                .withConnectTimeout(java.time.Duration.ofSeconds(2));
        connectFailure(cfg);
    }
}
