package io.hydrangea.redis;

import org.junit.jupiter.api.Assumptions;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.UUID;

/** Where the Redis of {@code start-redis.sh} listens; tests that need it are skipped when it is not there. */
final class Redis {

    static final String HOST = System.getProperty("redis.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("redis.port", 26379);
    static final int TLS_PORT = Integer.getInteger("redis.tls.port", 26380);
    static final int AUTH_PORT = Integer.getInteger("redis.auth.port", 26381);

    private Redis() {
    }

    static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no Redis on " + HOST + ":" + port + " (start one with hydrangea-redis/src/test/resources/redis/start-redis.sh)");
    }

    static RedisConfig config() {
        return RedisConfig.of(HOST, PORT);
    }

    /** A key no other test run shares. */
    static String key(String prefix) {
        return prefix + ":" + UUID.randomUUID().toString().substring(0, 8);
    }

    static SSLContext trusting(String resource) throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream in = Redis.class.getResourceAsStream(resource)) {
            ks.setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }
}
