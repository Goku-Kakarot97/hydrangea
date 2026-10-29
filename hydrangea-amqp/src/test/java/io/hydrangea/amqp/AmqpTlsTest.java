package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AMQP over TLS against a real RabbitMQ with a TLS listener; {@code hydrangea-amqp/src/test/resources/amqp/start-broker.sh} starts one
 * (plain 25672, TLS 25671, certificate for localhost / 127.0.0.1 signed by {@code ca.pem}). Skipped when nothing listens there.
 */
class AmqpTlsTest {

    private static final String HOST = "127.0.0.1";
    private static final int TLS_PORT = Integer.getInteger("amqp.tls.port", 25671);
    private static final int PLAIN_PORT = Integer.getInteger("amqp.port", 25672);
    private static EventLoopGroup group;

    /** A context that trusts exactly the CA in the given classpath resource. */
    static SSLContext trusting(String resource) throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = AmqpTlsTest.class.getResourceAsStream(resource)) {
            ks.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    @BeforeAll
    static void start() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, TLS_PORT), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no RabbitMQ with TLS on " + HOST + ":" + TLS_PORT + " (see hydrangea-amqp/src/test/resources/amqp/start-broker.sh)");
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static AmqpConfig tls(SSLContext ctx) {
        return AmqpConfig.of(HOST, TLS_PORT).withTls(ctx).withHeartbeat(Duration.ofSeconds(30));
    }

    @Test
    void connectsAndExchangesMessagesOverTls() throws Exception {
        AmqpConnection c = AmqpConnection.connect(group, tls(trusting("/amqp/ca.pem"))).get(10, TimeUnit.SECONDS);
        try {
            assertEquals("RabbitMQ", c.serverProperties().get("product"));
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            String q = ch.queueDeclare().get(5, TimeUnit.SECONDS).name();
            ch.confirmSelect().get(5, TimeUnit.SECONDS);
            byte[] small = "over tls".getBytes(StandardCharsets.UTF_8);
            byte[] big = new byte[3_000_000]; // many TLS records, several frames, partial writes
            new Random(5).nextBytes(big);
            ch.basicPublish("", q, false, null, small).get(10, TimeUnit.SECONDS);
            ch.basicPublish("", q, false, null, big).get(20, TimeUnit.SECONDS);
            assertArrayEquals(small, get(ch, q));
            assertArrayEquals(big, get(ch, q));
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    private static byte[] get(AmqpChannel ch, String q) throws Exception {
        for (int i = 0; i < 300; i++) {
            Delivery d = ch.basicGet(q, true).get(10, TimeUnit.SECONDS);
            if (d != null) {
                return d.body();
            }
            Thread.sleep(10);
        }
        return fail("no message");
    }

    @Test
    void consumersWorkOverTls() throws Exception {
        AmqpConnection c = AmqpConnection.connect(group, tls(trusting("/amqp/ca.pem"))).get(10, TimeUnit.SECONDS);
        try {
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            String q = ch.queueDeclare().get(5, TimeUnit.SECONDS).name();
            java.util.concurrent.CountDownLatch n = new java.util.concurrent.CountDownLatch(500);
            ch.basicConsume(q, true, d -> n.countDown()).get(5, TimeUnit.SECONDS);
            for (int i = 0; i < 500; i++) {
                ch.basicPublish("", q, false, null, new byte[200]);
            }
            assertTrue(n.await(10, TimeUnit.SECONDS));
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aServerCertificateFromAnUntrustedCaIsRefused() throws Exception {
        CompletionException e = assertThrows(CompletionException.class,
                () -> AmqpConnection.connect(group, tls(trusting("/amqp/other-ca.pem"))).join());
        Throwable t = e.getCause();
        boolean tlsFailure = false;
        for (Throwable x = t; x != null; x = x.getCause()) {
            if (x instanceof javax.net.ssl.SSLException || x instanceof java.security.cert.CertificateException) {
                tlsFailure = true;
            }
        }
        assertTrue(tlsFailure, "expected a TLS failure, got " + t);
    }

    @Test
    void bothNamesInTheCertificateAreAccepted() throws Exception {
        for (String name : new String[] {"127.0.0.1", "localhost"}) {
            AmqpConnection c = AmqpConnection.connect(group, new AmqpConfig(name, TLS_PORT, "guest", "guest", "/", null, 131072, 0, 1 << 20,
                    Duration.ofSeconds(5), trusting("/amqp/ca.pem"), null)).get(10, TimeUnit.SECONDS);
            c.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    /** A certificate signed by the CA we trust, but naming another host (wrong.example): possession of a valid chain is not enough. */
    @Test
    void aTrustedCertificateForAnotherHostIsRefused() throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        java.security.cert.Certificate leaf;
        java.security.cert.Certificate ca;
        try (InputStream in = AmqpTlsTest.class.getResourceAsStream("/amqp/wrongname.pem")) {
            leaf = cf.generateCertificate(in);
        }
        try (InputStream in = AmqpTlsTest.class.getResourceAsStream("/amqp/ca.pem")) {
            ca = cf.generateCertificate(in);
        }
        String pem;
        try (InputStream in = AmqpTlsTest.class.getResourceAsStream("/amqp/wrongname.key")) {
            pem = new String(in.readAllBytes(), StandardCharsets.US_ASCII);
        }
        byte[] der = java.util.Base64.getMimeDecoder().decode(pem.replaceAll("-----[A-Z ]+-----", ""));
        java.security.PrivateKey key = java.security.KeyFactory.getInstance("RSA").generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(der));
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setKeyEntry("server", key, new char[0], new java.security.cert.Certificate[] {leaf, ca});
        javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        try (javax.net.ssl.SSLServerSocket server = (javax.net.ssl.SSLServerSocket) serverCtx.getServerSocketFactory()
                .createServerSocket(0, 1, java.net.InetAddress.getByName(HOST))) {
            Thread t = new Thread(() -> {
                try (Socket s = server.accept()) {
                    ((javax.net.ssl.SSLSocket) s).startHandshake();
                    s.getInputStream().read(); // would be the AMQP protocol header, if the client had accepted the certificate
                } catch (Exception expected) {
                    // the client walks away during the handshake
                }
            });
            t.setDaemon(true);
            t.start();
            AmqpConfig cfg = new AmqpConfig(HOST, server.getLocalPort(), "guest", "guest", "/", null, 131072, 0, 1 << 20, Duration.ofSeconds(5),
                    trusting("/amqp/ca.pem"), null);
            CompletionException e = assertThrows(CompletionException.class, () -> AmqpConnection.connect(group, cfg).join());
            StringBuilder chain = new StringBuilder();
            for (Throwable x = e; x != null; x = x.getCause()) {
                chain.append(x).append(" | ");
            }
            assertTrue(chain.toString().contains("subject alternative") || chain.toString().contains("No name matching")
                    || chain.toString().contains("CertificateException"), "unexpected failure: " + chain);
        }
    }

    @Test
    void plainAmqpToATlsPortFailsPromptly() {
        AmqpConfig cfg = new AmqpConfig(HOST, TLS_PORT, "guest", "guest", "/", null, 131072, 0, 1 << 20, Duration.ofSeconds(3), null, null);
        long t0 = System.nanoTime();
        assertThrows(CompletionException.class, () -> AmqpConnection.connect(group, cfg).join());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 6000);
    }

    @Test
    void tlsToAPlainPortFailsPromptly() throws Exception {
        AmqpConfig cfg = new AmqpConfig(HOST, PLAIN_PORT, "guest", "guest", "/", null, 131072, 0, 1 << 20, Duration.ofSeconds(3),
                trusting("/amqp/ca.pem"), null);
        long t0 = System.nanoTime();
        assertThrows(CompletionException.class, () -> AmqpConnection.connect(group, cfg).join());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 6000);
    }
}
