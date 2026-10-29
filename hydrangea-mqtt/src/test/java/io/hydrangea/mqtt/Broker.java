package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.Assumptions;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.UUID;

/** Where the Mosquitto of {@code start-broker.sh} listens; tests that need it are skipped when it is not there. */
final class Broker {

    static final String HOST = System.getProperty("mqtt.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("mqtt.port", 21883);
    static final int AUTH_PORT = Integer.getInteger("mqtt.auth.port", 21884);
    static final int TLS_PORT = Integer.getInteger("mqtt.tls.port", 28883);

    private Broker() {
    }

    static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no MQTT broker on " + HOST + ":" + port + " (start one with hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh)");
    }

    static final int WS_PORT = Integer.getInteger("mqtt.ws.port", 29001);

    static MqttConfig config5() {
        return config().withVersion(5);
    }

    static MqttConfig config() {
        return MqttConfig.of(HOST, PORT).withClientId(id("c"));
    }

    /** A topic or client id no other test run shares. */
    static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    static SSLContext trusting(String resource) throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = Broker.class.getResourceAsStream(resource)) {
            ks.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    static EventLoopGroup newGroup() {
        return new EventLoopGroup(2);
    }
}
