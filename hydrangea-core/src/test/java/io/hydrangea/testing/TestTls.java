package io.hydrangea.testing;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/** TLS material for tests: a throw-away self-signed certificate for localhost / 127.0.0.1, made with the JDK's {@code keytool}. */
public final class TestTls {

    private TestTls() {
    }

    /** @return {server context, client context} sharing a throw-away self-signed certificate; {@code dir} holds the key store */
    public static SSLContext[] tls(Path dir) throws Exception {
        Path ks = dir.resolve("t.p12");
        Process pr = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "t", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=localhost",
                "-ext", "san=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit")
                .redirectErrorStream(true).start();
        pr.getInputStream().readAllBytes();
        int exit = pr.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("keytool failed with exit code " + exit);
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(store);
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        SSLContext clientCtx = SSLContext.getInstance("TLS");
        clientCtx.init(null, tmf.getTrustManagers(), null);
        return new SSLContext[]{serverCtx, clientCtx};
    }
}
