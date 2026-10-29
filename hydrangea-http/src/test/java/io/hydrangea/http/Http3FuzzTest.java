package io.hydrangea.http;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.quic.QuicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Hostile HTTP/3: after a real QUIC handshake (ALPN h3), aioquic writes random and mutated frames, QPACK blocks and unidirectional stream types. The
 * server may close the connection or answer with an error status; it must not log an internal error, block its event loop, or stop serving a normal
 * client. Skipped without python3 + aioquic.
 */
class Http3FuzzTest {

    static EventLoopGroup group;
    static Http3Server server;
    static final List<LogRecord> LOGGED = new CopyOnWriteArrayList<>();
    static Handler handler;
    static java.util.logging.Logger h3Logger;
    static final java.util.Map<String, Integer> ERRORS = new java.util.concurrent.ConcurrentHashMap<>();
    static Path script;

    @BeforeAll
    static void start(@TempDir Path dir) throws Exception {
        try {
            Process probe = new ProcessBuilder("python3", "-c", "import aioquic").redirectErrorStream(true).start();
            probe.getInputStream().readAllBytes();
            Assumptions.assumeTrue(probe.waitFor() == 0, "python3 with aioquic is not available");
        } catch (java.io.IOException e) {
            Assumptions.abort("python3 is not available");
        }
        script = Path.of(Http3FuzzTest.class.getResource("/quic/h3_fuzz.py").toURI());
        handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getMessage() != null && r.getMessage().startsWith("http3 connection error")) {
                    ERRORS.merge(String.valueOf(r.getParameters()[0]), 1, Integer::sum);
                }
                if (r.getLevel().intValue() >= Level.WARNING.intValue() && r.getLoggerName() != null && r.getLoggerName().startsWith("io.hydrangea")) {
                    LOGGED.add(r);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger.getLogger("").addHandler(handler);
        handler.setLevel(Level.ALL);
        h3Logger = java.util.logging.Logger.getLogger(Http3Server.class.getName());   // strong reference: JUL keeps loggers weakly
        h3Logger.setLevel(Level.FINE);
        Path ks = dir.resolve("k.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(), "-genkeypair", "-alias", "k", "-keyalg", "EC",
                "-groupname", "secp256r1", "-validity", "2", "-dname", "CN=localhost", "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                "-keypass", "changeit").redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertEquals(0, p.waitFor());
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        group = new EventLoopGroup(2);
        HttpRouter router = new HttpRouter();
        router.get("/plain", req -> HttpResponse.ok("hello"));
        router.post("/echo", req -> HttpResponse.status(200).body(req.body()));
        server = Http3Server.bind(group, new InetSocketAddress("127.0.0.1", 0),
                QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("h3")).withIdleTimeout(Duration.ofSeconds(2)), new HttpConfig(4096, 100_000), router);
    }

    @AfterAll
    static void stop() {
        java.util.logging.Logger.getLogger("").removeHandler(handler);
        if (server != null) server.close();
        if (group != null) group.close();
    }

    @Test
    void hostileHttp3() throws Exception {
        int rounds = Integer.getInteger("fuzz.h3.rounds", 40);
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        group.metrics().resetMaxBlockNanos();
        for (int seed = 1; seed <= 3; seed++) {
            Process p = new ProcessBuilder("python3", script.toString(), Integer.toString(port), Integer.toString(seed), Integer.toString(rounds))
                    .redirectErrorStream(true).start();
            boolean finished = p.waitFor(180, TimeUnit.SECONDS);
            if (!finished) {
                p.destroyForcibly();
            }
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(finished, "fuzz client timed out: " + out);
            assertEquals(0, p.exitValue(), "seed " + seed + ": " + out);
            assertTrue(out.contains("OK"), out);
            assertTrue(LOGGED.isEmpty(), () -> "the server logged: " + LOGGED.get(0).getMessage() + " " + LOGGED.get(0).getThrown());
        }
        System.out.println("http3 fuzz connection errors by code: " + ERRORS);
        long blockedMs = group.metrics().resetMaxBlockNanos() / 1_000_000;
        assertTrue(blockedMs < 250, "an event loop was blocked for " + blockedMs + " ms");
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (server.activeConnections() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(0, server.activeConnections(), "connections leaked");
    }
}
