package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** "Upgrade: h2c" (RFC 7540 section 3.2): how curl and the JDK client start HTTP/2 on a cleartext port. */
class Http2UpgradeTest {

    private static final int DATA = 0, HEADERS = 1, SETTINGS = 4, WINDOW_UPDATE = 8, END_STREAM = 1;

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private Server server(boolean http2, SSLContext tls) throws Exception {
        return server(http2, tls, null);
    }

    private Server server(boolean http2, SSLContext tls, HttpRouter only) throws Exception {
        HttpRouter router = only != null ? only : new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version()))
                .get("/big/{n}", r -> HttpResponse.status(200).body(new byte[r.pathInt("n")]))
                .post("/echo", r -> HttpResponse.status(200).body(r.body()))
                .get("/headers", r -> HttpResponse.ok(r.request().headers().toString()));
        HttpConfig cfg = new HttpConfig(16 * 1024, 1024 * 1024);
        return HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), tls, http2 ? cfg.withHttp2(Http2Config.defaults()) : cfg, router);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    /** The HTTP2-Settings value for the given (id, value) pairs. */
    private static String settings(int... idValue) {
        byte[] b = new byte[idValue.length * 3];
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(b);
        for (int i = 0; i < idValue.length; i += 2) {
            bb.putShort((short) idValue[i]).putInt(idValue[i + 1]);
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private static String upgradeRequest(String method, String path, String http2Settings, String extraHeaders, String body) {
        return method + " " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\n"
                + (http2Settings == null ? "" : "HTTP2-Settings: " + http2Settings + "\r\n") + extraHeaders
                + (body == null ? "" : "Content-Length: " + body.length() + "\r\n") + "\r\n" + (body == null ? "" : body);
    }

    /** Reads an HTTP/1.1 response head (up to the blank line), one byte at a time so the frames behind it stay in the socket. */
    private static String readHead(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int state = 0;
        int b;
        while ((b = in.read()) >= 0) {
            buf.write(b);
            state = (b == '\r' && (state == 0 || state == 2)) ? state + 1 : (b == '\n' && (state == 1 || state == 3)) ? state + 1 : 0;
            if (state == 4) {
                break;
            }
        }
        return buf.toString(StandardCharsets.ISO_8859_1);
    }

    // ---- the happy path ----

    @Test
    void http2StreamsBeingHandledAreCounted() throws Exception {
        CaptureSink sink = new CaptureSink();
        group.metrics().bindTo(sink);
        java.util.concurrent.CompletableFuture<HttpResponse> gate = new java.util.concurrent.CompletableFuture<>();
        HttpRouter held = new HttpRouter();
        held.getAsync("/hold", req -> gate);
        try (Server s = server(true, null, held)) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
            var pending = client.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(s) + "/hold")).build(), BodyHandlers.ofString());
            CaptureSink.eventually(() -> sink.get("http.server.active") == 1, "the stream to be counted");
            gate.complete(HttpResponse.ok("late"));
            assertEquals("late", pending.get(5, java.util.concurrent.TimeUnit.SECONDS).body());
            CaptureSink.eventually(() -> sink.get("http.server.active") == 0, "the answered stream to leave");
        }
    }

    @Test
    void theJdkClientUpgradesACleartextConnection() throws Exception {
        try (Server s = server(true, null)) {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
            var r = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(s) + "/hello")).build(), BodyHandlers.ofString());
            assertEquals(HttpClient.Version.HTTP_2, r.version());
            assertEquals("hello over HTTP/2", r.body());
            var post = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(s) + "/echo")).POST(HttpRequest.BodyPublishers.ofString("a body")).build(),
                    BodyHandlers.ofString());
            assertEquals("a body", post.body());
            assertEquals(HttpClient.Version.HTTP_2, post.version());
        }
    }

    @Test
    void theRequestBecomesStreamOneAndTheClientsSettingsApply() throws Exception {
        try (Server s = server(true, null); Socket sock = new Socket("127.0.0.1", port(s))) {
            sock.setSoTimeout(5000);
            // HTTP2-Settings: SETTINGS_INITIAL_WINDOW_SIZE = 1000, so the 5000-byte answer must stop after 1000 bytes
            sock.getOutputStream().write(upgradeRequest("GET", "/big/5000", settings(4, 1000), "", null).getBytes(StandardCharsets.ISO_8859_1));
            String head = readHead(sock.getInputStream());
            assertTrue(head.startsWith("HTTP/1.1 101"), head);
            assertTrue(head.toLowerCase().contains("upgrade: h2c"), head);
            try (Peer p = new Peer(sock, true)) {   // the client's preface follows the 101
                p.settings();
                List<Frame> seen = new ArrayList<>();
                Frame f;
                while ((f = p.pollOrNull(600)) != null) {
                    seen.add(f);
                }
                assertEquals(SETTINGS, seen.get(0).type(), "the server's preface comes first");
                long data = seen.stream().filter(x -> x.type() == DATA && x.sid() == 1).mapToLong(x -> x.payload().length).sum();
                assertTrue(seen.stream().anyMatch(x -> x.type() == HEADERS && x.sid() == 1), "the answer arrives on stream 1");
                assertEquals(1000, data, "the window from HTTP2-Settings is honoured");
                p.windowUpdate(1, 10_000);
                p.windowUpdate(0, 10_000);
                long more = 0;
                boolean end = false;
                while (!end && (f = p.pollOrNull(2000)) != null) {
                    if (f.type() == DATA && f.sid() == 1) {
                        more += f.payload().length;
                        end = (f.flags() & END_STREAM) != 0;
                    }
                }
                assertEquals(4000, more);
                assertTrue(end);
                p.get(3, "/hello");     // and the connection carries on as HTTP/2
                assertEquals("hello over HTTP/2", p.response(3).text());
            }
        }
    }

    @Test
    void aRequestBodyIsDeliveredBeforeTheSwitch() throws Exception {
        try (Server s = server(true, null); Socket sock = new Socket("127.0.0.1", port(s))) {
            sock.setSoTimeout(5000);
            sock.getOutputStream().write(upgradeRequest("POST", "/echo", settings(), "", "payload that came with the upgrade").getBytes(StandardCharsets.ISO_8859_1));
            assertTrue(readHead(sock.getInputStream()).startsWith("HTTP/1.1 101"));
            try (Peer p = new Peer(sock, true)) {
                p.settings();
                assertEquals("payload that came with the upgrade", p.response(1).text());
            }
        }
    }

    @Test
    void connectionSpecificHeadersAreNotPassedOn() throws Exception {
        try (Server s = server(true, null); Socket sock = new Socket("127.0.0.1", port(s))) {
            sock.setSoTimeout(5000);
            sock.getOutputStream().write(upgradeRequest("GET", "/headers", settings(), "X-Mine: yes\r\n", null).getBytes(StandardCharsets.ISO_8859_1));
            readHead(sock.getInputStream());
            try (Peer p = new Peer(sock, true)) {
                p.settings();
                String seen = p.response(1).text().toLowerCase();
                assertTrue(seen.contains("x-mine"), seen);
                assertFalse(seen.contains("http2-settings") || seen.contains("upgrade") || seen.contains("connection"), seen);
            }
        }
    }

    // ---- offers that are not accepted: served as the HTTP/1.1 requests they are ----

    private String plainExchange(Server s, String request) throws Exception {
        try (Socket sock = new Socket("127.0.0.1", port(s))) {
            sock.setSoTimeout(5000);
            sock.getOutputStream().write(request.getBytes(StandardCharsets.ISO_8859_1));
            String head = readHead(sock.getInputStream());
            return head;
        }
    }

    @Test
    void invalidHttp2SettingsMeanNoUpgrade() throws Exception {
        try (Server s = server(true, null)) {
            for (String bad : new String[]{"%%%not-base64", settings(2, 7), settings(4, -1), settings(5, 100), "AAQAAA"}) {
                String head = plainExchange(s, upgradeRequest("GET", "/hello", bad, "", null));
                assertTrue(head.startsWith("HTTP/1.1 200"), bad + " -> " + head);
            }
            assertTrue(plainExchange(s, upgradeRequest("GET", "/hello", null, "", null)).startsWith("HTTP/1.1 200"), "no HTTP2-Settings at all");
            assertTrue(plainExchange(s, upgradeRequest("GET", "/hello", settings(), "HTTP2-Settings: " + settings() + "\r\n", null)).startsWith("HTTP/1.1 200"),
                    "two HTTP2-Settings headers");
            String noConnectionToken = "GET /hello HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: h2c\r\nHTTP2-Settings: " + settings() + "\r\n\r\n";
            assertTrue(plainExchange(s, noConnectionToken).startsWith("HTTP/1.1 200"), "Connection must name HTTP2-Settings too");
            String otherProtocol = "GET /hello HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: websocket\r\nHTTP2-Settings: " + settings() + "\r\n\r\n";
            assertTrue(plainExchange(s, otherProtocol).startsWith("HTTP/1.1 200"));
        }
    }

    @Test
    void nothingHappensWhenHttp2IsNotEnabled() throws Exception {
        try (Server s = server(false, null)) {
            assertTrue(plainExchange(s, upgradeRequest("GET", "/hello", settings(), "", null)).startsWith("HTTP/1.1 200"));
            var r = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build()
                    .send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(s) + "/hello")).build(), BodyHandlers.ofString());
            assertEquals(HttpClient.Version.HTTP_1_1, r.version());
            assertEquals("hello over HTTP/1.1", r.body());
        }
    }

    @Test
    void anIgnoredOfferLeavesTheConnectionUsableAsHttp11() throws Exception {
        try (Server s = server(true, null); Socket sock = new Socket("127.0.0.1", port(s))) {
            sock.setSoTimeout(5000);
            var out = sock.getOutputStream();
            out.write(upgradeRequest("GET", "/hello", "garbage!", "", null).getBytes(StandardCharsets.ISO_8859_1));
            assertTrue(readHead(sock.getInputStream()).startsWith("HTTP/1.1 200"));
            sock.getInputStream().readNBytes("hello over HTTP/1.1".length());
            out.write("GET /hello HTTP/1.1\r\nHost: x\r\n\r\n".getBytes());
            assertTrue(readHead(sock.getInputStream()).startsWith("HTTP/1.1 200"), "keep-alive carries on");
        }
    }

    @Test
    void neverUpgradesOverTls(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server s = server(true, tls[0])) {
            SSLSocket sock = (SSLSocket) tls[1].getSocketFactory().createSocket("localhost", port(s));
            SSLParameters p = sock.getSSLParameters();
            p.setApplicationProtocols(new String[]{"http/1.1"});
            sock.setSSLParameters(p);
            sock.setSoTimeout(5000);
            sock.startHandshake();
            sock.getOutputStream().write(upgradeRequest("GET", "/hello", settings(), "", null).getBytes(StandardCharsets.ISO_8859_1));
            String head = readHead(sock.getInputStream());
            assertTrue(head.startsWith("HTTP/1.1 200"), "RFC 7540 section 3.2: no h2c upgrade on a TLS connection: " + head);
            sock.close();
        }
    }
}
