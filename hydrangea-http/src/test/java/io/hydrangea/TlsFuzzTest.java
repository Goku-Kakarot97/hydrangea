package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.fuzz.Fuzz;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.*;

/** Garbage, truncated and mutated handshakes sent to a TLS listener (HTTP/1.1 and h2 by ALPN). */
class TlsFuzzTest {

    private static byte[] text(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
    }

    /** The first flight of a real JDK client (its ClientHello with ALPN), captured from a socket that never answers. */
    private static byte[] captureClientHello(SSLContext client) throws Exception {
        try (ServerSocket cap = new ServerSocket(0, 5, InetAddress.getLoopbackAddress())) {
            Thread t = new Thread(() -> {
                try (SSLSocket s = (SSLSocket) client.getSocketFactory().createSocket("localhost", cap.getLocalPort())) {
                    javax.net.ssl.SSLParameters p = s.getSSLParameters();
                    p.setApplicationProtocols(new String[]{"h2", "http/1.1"});
                    s.setSSLParameters(p);
                    s.setSoTimeout(500);
                    s.startHandshake();
                } catch (Exception expected) {
                    // nobody answers the handshake
                }
            });
            t.setDaemon(true);
            t.start();
            try (Socket c = cap.accept()) {
                c.setSoTimeout(2000);
                byte[] buf = new byte[4096];
                int n = c.getInputStream().read(buf);
                return java.util.Arrays.copyOf(buf, n);
            }
        }
    }

    @Test
    void handshakesAndRecords(@TempDir Path dir) throws Exception {
        SSLContext[] ctx = TestTls.tls(dir);
        List<LogRecord> logged = new CopyOnWriteArrayList<>();
        Handler h = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue() && r.getLoggerName() != null && r.getLoggerName().startsWith("io.hydrangea")) {
                    logged.add(r);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger.getLogger("").addHandler(h);
        EventLoopGroup group = new EventLoopGroup(2);
        HttpConfig config = HttpConfig.defaults().withHttp2(Http2Config.defaults()).withHeaderTimeout(java.time.Duration.ofSeconds(2));
        Server server = HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), ctx[0], config,
                new HttpRouter().get("/plain", r -> HttpResponse.ok("Hello")));
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        try {
            byte[] hello = captureClientHello(ctx[1]);
            assertTrue(hello.length > 100 && hello[0] == 0x16, "captured a TLS handshake record");
            List<byte[]> corpus = new ArrayList<>(List.of(
                    hello,
                    text("GET /plain HTTP/1.1\r\nHost: x\r\n\r\n"),                       // cleartext on the TLS port
                    new byte[]{0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28},               // a fatal alert
                    new byte[]{0x17, 0x03, 0x03, 0x00, 0x10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},   // application data before any handshake
                    new byte[]{0x16, 0x03, 0x01, (byte) 0xff, (byte) 0xff},             // a record claiming 65535 bytes
                    new byte[]{(byte) 0x80, 0x2e, 0x01, 0x03, 0x01, 0x00, 0x15, 0x00, 0x00, 0x00, 0x10}));   // SSLv2-style hello
            List<Fuzz.Finding> findings = Fuzz.run("tls listener", corpus, in -> {
                int before = logged.size();
                group.metrics().resetMaxBlockNanos();
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 2000);
                    s.setSoTimeout(25);
                    try {
                        s.getOutputStream().write(in);
                        long end = System.nanoTime() + 60_000_000L;
                        byte[] sink = new byte[8192];
                        while (System.nanoTime() < end) {
                            try {
                                if (s.getInputStream().read(sink) < 0) {
                                    break;
                                }
                            } catch (SocketTimeoutException quiet) {
                                break;
                            }
                        }
                    } catch (IOException closedByServer) {
                        // fine
                    }
                }
                if (logged.size() > before) {
                    LogRecord r = logged.get(before);
                    throw new AssertionError("the server logged: " + r.getMessage() + " " + r.getThrown());
                }
                long ms = group.metrics().resetMaxBlockNanos() / 1_000_000;
                if (ms > 250) {
                    throw new AssertionError("an event loop was blocked for " + ms + " ms");
                }
            }, Set.of(), 3000, Math.min(Fuzz.ITERATIONS, Integer.getInteger("fuzz.protocol.iterations", 500)));
            assertTrue(findings.isEmpty(), () -> findings.size() + " finding(s):\n" + String.join("\n", findings.stream().map(Fuzz.Finding::toString).toList()));

            long deadline = System.nanoTime() + 15_000_000_000L;   // half-open handshakes are cut by the handshake timeout
            while (server.activeConnections() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertEquals(0, server.activeConnections(), "connections leaked");
            HttpClient client = new HttpClient(group, HttpClientConfig.defaults().withSslContext(ctx[1]).withHttp2(Http2ClientConfig.defaults()));
            try {
                assertEquals("Hello", client.send(ClientRequest.get("https://localhost:" + port + "/plain")).get(5, TimeUnit.SECONDS).bodyAsString());
            } finally {
                client.close();
            }
        } finally {
            java.util.logging.Logger.getLogger("").removeHandler(h);
            server.close();
            group.close();
        }
    }
}
