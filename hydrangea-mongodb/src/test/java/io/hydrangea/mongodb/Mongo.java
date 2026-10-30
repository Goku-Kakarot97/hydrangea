package io.hydrangea.mongodb;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;

/** Where the MongoDB of {@code start-mongodb.sh} listens; tests that need it are skipped when it is not there. */
final class Mongo {

    static final String HOST = System.getProperty("mongo.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("mongo.port", 29017);
    static final String USER = System.getProperty("mongo.user", "hydra");
    static final String PASSWORD = System.getProperty("mongo.password", "hydra");
    static final String DATABASE = System.getProperty("mongo.db", "hydra");
    static final int TLS_PORT = Integer.getInteger("mongo.tls.port", 29021);
    static final int TLS_WRONG_NAME_PORT = Integer.getInteger("mongo.tls.wrongname.port", 29022);
    /** The members of the replica set of {@code start-mongodb-rs.sh}. */
    static final java.util.List<String> RS_HOSTS = java.util.List.of("127.0.0.1:29018", "127.0.0.1:29019", "127.0.0.1:29020");

    private Mongo() {
    }

    static void require() {
        require(PORT, "start-mongodb.sh");
    }

    static void require(int port, String script) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no MongoDB on " + HOST + ":" + port + " (start one with hydrangea-mongodb/src/test/resources/mongodb/" + script + ")");
    }

    /** An SSL context that trusts the test CA. */
    static javax.net.ssl.SSLContext trusting() throws Exception {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        ks.load(null, null);
        try (java.io.InputStream in = Mongo.class.getResourceAsStream("/mongodb/ca.pem")) {
            ks.setCertificateEntry("ca", java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    static MongoConfig config() {
        return MongoConfig.of(HOST, PORT, DATABASE).withCredentials(USER, PASSWORD);
    }

    /** A collection name nobody else uses. */
    static String collection(String prefix) {
        return prefix + "_" + Long.toHexString(System.nanoTime());
    }
}
