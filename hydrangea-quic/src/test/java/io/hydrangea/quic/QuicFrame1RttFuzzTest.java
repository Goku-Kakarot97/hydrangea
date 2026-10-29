package io.hydrangea.quic;

import io.hydrangea.EventLoopGroup;
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
 * Hostile frames after the handshake. aioquic completes a real handshake, then sends 1-RTT packets protected with the connection's own keys whose
 * frames are random or mutated (every frame type, huge and degenerate varints, offsets and limits). The server may close the connection; it must
 * not log an internal error, block its event loop, leak connections, or stop serving a clean client. Skipped without python3 + aioquic.
 */
class QuicFrame1RttFuzzTest {

    static EventLoopGroup group;
    static QuicServer server;
    static final List<LogRecord> LOGGED = new CopyOnWriteArrayList<>();
    static Handler handler;
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
        script = Path.of(QuicFrame1RttFuzzTest.class.getResource("/quic/quic_fuzz_1rtt.py").toURI());
        handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
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
        server = QuicServer.bind(group, new InetSocketAddress("127.0.0.1", 0),
                QuicConfig.fromKeyStore(store, "k", "changeit".toCharArray(), List.of("echo")).withIdleTimeout(Duration.ofSeconds(2)), new QuicHandler() {
                    @Override
                    public void onStreamData(QuicConnection c, long streamId, byte[] data, boolean fin) {
                        c.send(streamId, data, fin);
                    }
                });
    }

    @AfterAll
    static void stop() {
        java.util.logging.Logger.getLogger("").removeHandler(handler);
        if (server != null) server.close();
        if (group != null) group.close();
    }

    @Test
    void hostileFrames() throws Exception {
        int rounds = Integer.getInteger("fuzz.quic.rounds", 12);
        int packets = Integer.getInteger("fuzz.quic.packets", 60);
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        group.metrics().resetMaxBlockNanos();
        for (int seed = 1; seed <= 3; seed++) {
            Process p = new ProcessBuilder("python3", script.toString(), Integer.toString(port), Integer.toString(seed), Integer.toString(rounds), Integer.toString(packets))
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
        long blockedMs = group.metrics().resetMaxBlockNanos() / 1_000_000;
        assertTrue(blockedMs < 250, "an event loop was blocked for " + blockedMs + " ms");
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (server.activeConnections() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertEquals(0, server.activeConnections(), "connections leaked");
    }
}
