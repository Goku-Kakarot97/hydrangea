package io.hydrangea.mysql;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;

/** Where the MySQL servers of {@code start-mysql.sh} listen; tests that need one are skipped when it is not there. */
final class My {

    static final String HOST = System.getProperty("my.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("my.port", 23306);                 // caching_sha2_password
    static final int NATIVE_PORT = Integer.getInteger("my.native.port", 23307);   // mysql_native_password
    static final int TLS_PORT = Integer.getInteger("my.tls.port", 23308);
    static final int TLS_WRONG_NAME_PORT = Integer.getInteger("my.tls.wrongname.port", 23309);
    static final String USER = System.getProperty("my.user", "hydra");
    static final String PASSWORD = System.getProperty("my.password", "hydra");
    static final String DATABASE = System.getProperty("my.db", "hydra");

    private My() {
    }

    static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no MySQL on " + HOST + ":" + port + " (start one with hydrangea-mysql/src/test/resources/mysql/start-mysql.sh)");
    }

    static MyConfig config() {
        return config(PORT);
    }

    static MyConfig config(int port) {
        return MyConfig.of(HOST, port, USER, DATABASE).withPassword(PASSWORD).withAllowPublicKeyRetrieval(true);   // the test containers are ours and have no TLS: the first login of a user needs the key
    }

    /** An SSL context that trusts the test CA of {@code make-certs.sh}. */
    static javax.net.ssl.SSLContext trusting() throws Exception {
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        ks.load(null, null);
        try (java.io.InputStream in = My.class.getResourceAsStream("/mysql/ca.pem")) {
            ks.setCertificateEntry("ca", java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }
}
