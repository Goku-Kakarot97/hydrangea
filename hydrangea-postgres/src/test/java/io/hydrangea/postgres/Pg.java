package io.hydrangea.postgres;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/** Where the PostgreSQL of {@code start-postgres.sh} listens; tests that need it are skipped when it is not there. */
final class Pg {

    static final String HOST = System.getProperty("pg.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("pg.port", 25432);          // scram-sha-256
    static final int MD5_PORT = Integer.getInteger("pg.md5.port", 25433);
    static final int PASSWORD_PORT = Integer.getInteger("pg.password.port", 25434);
    static final int TLS_PORT = Integer.getInteger("pg.tls.port", 25435);
    static final int TLS_WRONG_NAME_PORT = Integer.getInteger("pg.tls.wrongname.port", 25436);
    static final String USER = System.getProperty("pg.user", "hydra");
    static final String PASSWORD = System.getProperty("pg.password", "hydra");
    static final String DATABASE = System.getProperty("pg.db", "hydra");
    /** Set (-Dpg.remote=true) when pointing the tests at a database that is not ours: the tests that create tables or try wrong passwords are skipped. */
    static final boolean REMOTE = Boolean.getBoolean("pg.remote");

    private Pg() {
    }

    static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no PostgreSQL on " + HOST + ":" + port + " (start one with hydrangea-postgres/src/test/resources/postgres/start-postgres.sh)");
    }

    static PgConfig config() {
        return config(PORT);
    }

    static PgConfig config(int port) {
        return PgConfig.of(HOST, port, USER, DATABASE).withPassword(PASSWORD).withAllowClearTextPassword(port == PASSWORD_PORT);   // that container asks for it, and is ours
    }

    /** For a test that creates tables or provokes login failures. */
    static void notOnRemote() {
        Assumptions.assumeFalse(REMOTE, "skipped against a database that is not ours (pg.remote=true)");
    }

    /** An SSL context that trusts the test CA of {@code make-certs.sh}. */
    static javax.net.ssl.SSLContext trusting() throws Exception {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        ks.load(null, null);
        try (java.io.InputStream in = Pg.class.getResourceAsStream("/postgres/ca.pem")) {
            ks.setCertificateEntry("ca", java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    /** A table name no other test run shares. */
    static String table(String prefix) {
        return prefix + "_" + Long.toString(System.nanoTime(), 36);
    }

    static <T> T get(java.util.concurrent.CompletableFuture<T> f) throws Exception {
        return f.get(REMOTE ? 120 : 10, TimeUnit.SECONDS);     // a remote link can stall a big upload for seconds
    }
}
