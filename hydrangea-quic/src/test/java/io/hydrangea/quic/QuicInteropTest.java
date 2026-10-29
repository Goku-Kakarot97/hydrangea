package io.hydrangea.quic;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Talks to the QUIC server with aioquic, an independent implementation (Python). Skipped when {@code python3} with aioquic is not installed
 * ({@code pip install aioquic}). Runs the same cases against an ECDSA P-256 and an RSA certificate.
 */
class QuicInteropTest {

    static final List<String> CASES = List.of("echo-small", "echo-large", "many-streams", "loss", "key-update", "server-close", "alpn-mismatch", "version-negotiation");
    static EventLoopGroup group;
    static QuicServer ecServer, rsaServer, retryServer;
    static Path script;

    private static Path keystore(Path dir, String name, String algorithm) throws Exception {
        Path ks = dir.resolve(name + ".p12");
        List<String> cmd = new java.util.ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(), "-genkeypair", "-alias", "k",
                "-validity", "2", "-dname", "CN=localhost", "-ext", "san=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", ks.toString(),
                "-storepass", "changeit", "-keypass", "changeit"));
        if (algorithm.equals("EC")) {
            cmd.addAll(List.of("-keyalg", "EC", "-groupname", "secp256r1", "-sigalg", "SHA256withECDSA"));
        } else {
            cmd.addAll(List.of("-keyalg", "RSA", "-keysize", "2048"));
        }
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertEquals(0, p.waitFor());
        return ks;
    }

    private static QuicServer serve(Path ks, boolean retry) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        return QuicServer.bind(group, new InetSocketAddress("127.0.0.1", 0), QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("echo")).withRetry(retry), new QuicHandler() {
            @Override
            public void onStreamData(QuicConnection c, long streamId, byte[] data, boolean fin) {
                if (new String(data, StandardCharsets.ISO_8859_1).equals("CLOSE")) {
                    c.close(42, "bye");
                    return;
                }
                c.send(streamId, data, fin);
            }
        });
    }

    @BeforeAll
    static void start(@TempDir Path dir) throws Exception {
        Process probe;
        try {
            probe = new ProcessBuilder("python3", "-c", "import aioquic").redirectErrorStream(true).start();
            probe.getInputStream().readAllBytes();
            Assumptions.assumeTrue(probe.waitFor() == 0, "python3 with aioquic is not available");
        } catch (java.io.IOException e) {
            Assumptions.abort("python3 is not available");
        }
        script = Path.of(QuicInteropTest.class.getResource("/quic/quic_interop.py").toURI());
        group = new EventLoopGroup(2);
        Path ec = keystore(dir, "ec", "EC");
        ecServer = serve(ec, false);
        rsaServer = serve(keystore(dir, "rsa", "RSA"), false);
        retryServer = serve(ec, true);
    }

    @AfterAll
    static void stop() {
        if (ecServer != null) ecServer.close();
        if (rsaServer != null) rsaServer.close();
        if (retryServer != null) retryServer.close();
        if (group != null) group.close();
    }

    static Stream<Arguments> cases() {
        return Stream.concat(Stream.of("ec", "rsa").flatMap(k -> CASES.stream().map(c -> Arguments.of(k, c))),
                Stream.of("echo-small", "echo-large", "many-streams", "loss", "key-update", "server-close", "retry-raw").map(c -> Arguments.of("retry", c)));
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("cases")
    void interop(String key, String testCase) throws Exception {
        QuicServer server = switch (key) {
            case "ec" -> ecServer;
            case "rsa" -> rsaServer;
            default -> retryServer;
        };
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        Process p = new ProcessBuilder("python3", script.toString(), Integer.toString(port), testCase).redirectErrorStream(true).start();
        boolean finished = p.waitFor(90, TimeUnit.SECONDS);
        if (!finished) {
            p.destroyForcibly();
        }
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(finished, "client timed out: " + output);
        assertEquals(0, p.exitValue(), output);
        assertTrue(output.contains("OK " + testCase), output);
    }
}
