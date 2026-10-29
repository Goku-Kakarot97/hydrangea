package io.hydrangea.http;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.quic.QuicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Talks HTTP/3 to {@link Http3Server} with aioquic's HTTP/3 client, an independent implementation. Skipped when {@code python3} with aioquic is absent.
 */
class Http3InteropTest {

    static EventLoopGroup group;
    static Http3Server server;
    static Path script;

    static HttpRouter router() {
        HttpRouter r = new HttpRouter();
        r.get("/plain", req -> HttpResponse.ok("hello"));
        r.post("/echo", req -> HttpResponse.status(200).body(req.body()));
        byte[] big = new byte[3_000_000];
        Arrays.fill(big, (byte) 'x');
        r.get("/big", req -> HttpResponse.status(200).body(big));
        r.get("/boom", req -> {
            throw new IllegalStateException("boom");
        });
        r.getAsync("/slow", req -> CompletableFuture.supplyAsync(() -> HttpResponse.ok("later"), CompletableFuture.delayedExecutor(150, TimeUnit.MILLISECONDS)));
        r.get("/headers", req -> {
            StringBuilder sb = new StringBuilder();
            req.request().headers().forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
            // repeated headers arrive as separate fields; show them joined
            sb.append("x-a=").append(String.join(",", req.request().headers().getAll("x-a"))).append('\n');
            return HttpResponse.ok(sb.toString());
        });
        return r;
    }

    @BeforeAll
    static void start(@TempDir Path dir) throws Exception {
        try {
            Process probe = new ProcessBuilder("python3", "-c", "import aioquic.h3").redirectErrorStream(true).start();
            probe.getInputStream().readAllBytes();
            Assumptions.assumeTrue(probe.waitFor() == 0, "python3 with aioquic is not available");
        } catch (java.io.IOException e) {
            Assumptions.abort("python3 is not available");
        }
        script = Path.of(Http3InteropTest.class.getResource("/quic/h3_interop.py").toURI());
        Path ks = dir.resolve("k.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(), "-genkeypair", "-alias", "k", "-keyalg", "EC",
                "-groupname", "secp256r1", "-validity", "2", "-dname", "CN=localhost", "-ext", "san=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12",
                "-keystore", ks.toString(), "-storepass", "changeit", "-keypass", "changeit").redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertEquals(0, p.waitFor());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        group = new EventLoopGroup(2);
        server = Http3Server.bind(group, new InetSocketAddress("127.0.0.1", 0), QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("ignored")),
                new HttpConfig(3000, 2_000_000), router());
    }

    @AfterAll
    static void stop() {
        if (server != null) server.close();
        if (group != null) group.close();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"plain", "echo-small", "echo-large", "big-response", "concurrent", "sequential", "not-found", "headers", "huge-headers",
            "too-large-body", "handler-error", "async"})
    void interop(String testCase) throws Exception {
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
