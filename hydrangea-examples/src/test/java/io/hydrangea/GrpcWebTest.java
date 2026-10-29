package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.GrpcServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** gRPC-Web on a plain HTTP/1.1 connection, byte by byte as a browser's fetch would see it. */
class GrpcWebTest {

    private EventLoopGroup group;
    private Server server;

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        server = GrpcEchoExample.builder().allowCors("https://app.example", "x-custom").build()
                .bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private int port() {
        return ((InetSocketAddress) server.localAddress()).getPort();
    }

    private static byte[] framed(Msg m) throws Exception {
        byte[] body = GrpcEchoExample.MSG.encode(m);
        return ByteBuffer.allocate(5 + body.length).put((byte) 0).putInt(body.length).put(body).array();
    }

    private record Reply(int status, Map<String, String> headers, byte[] body) {
        List<Msg> messages() throws Exception {
            List<Msg> out = new ArrayList<>();
            ByteBuffer b = ByteBuffer.wrap(body);
            while (b.remaining() >= 5) {
                int flag = b.get() & 0xff;
                byte[] p = new byte[b.getInt()];
                b.get(p);
                if ((flag & 0x80) == 0) {
                    out.add(GrpcEchoExample.MSG.decode(p));
                }
            }
            return out;
        }

        /** The trailer frame's lines. */
        Map<String, String> trailers() {
            ByteBuffer b = ByteBuffer.wrap(body);
            Map<String, String> t = new LinkedHashMap<>();
            while (b.remaining() >= 5) {
                int flag = b.get() & 0xff;
                byte[] p = new byte[b.getInt()];
                b.get(p);
                if ((flag & 0x80) != 0) {
                    for (String line : new String(p, StandardCharsets.ISO_8859_1).split("\r\n")) {
                        int i = line.indexOf(": ");
                        t.put(line.substring(0, i), line.substring(i + 2));
                    }
                }
            }
            return t;
        }
    }

    private Reply http(String method, String path, Map<String, String> headers, byte[] body) throws Exception {
        try (Socket s = new Socket("127.0.0.1", port())) {
            s.setSoTimeout(10000);
            StringBuilder req = new StringBuilder(method + " " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
            headers.forEach((k, v) -> req.append(k).append(": ").append(v).append("\r\n"));
            if (body != null) {
                req.append("Content-Length: ").append(body.length).append("\r\n");
            }
            s.getOutputStream().write((req + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            if (body != null) {
                s.getOutputStream().write(body);
            }
            s.getOutputStream().flush();
            InputStream in = s.getInputStream();
            ByteArrayOutputStream raw = new ByteArrayOutputStream();
            in.transferTo(raw);
            byte[] all = raw.toByteArray();
            int end = indexOf(all, "\r\n\r\n".getBytes());
            String[] lines = new String(all, 0, end, StandardCharsets.ISO_8859_1).split("\r\n");
            Map<String, String> h = new LinkedHashMap<>();
            for (int i = 1; i < lines.length; i++) {
                int c = lines[i].indexOf(':');
                h.put(lines[i].substring(0, c).toLowerCase(), lines[i].substring(c + 1).trim());
            }
            byte[] payload = java.util.Arrays.copyOfRange(all, end + 4, all.length);
            if ("chunked".equalsIgnoreCase(h.get("transfer-encoding"))) {
                payload = dechunk(payload);
            }
            return new Reply(Integer.parseInt(lines[0].split(" ")[1]), h, payload);
        }
    }

    private static byte[] dechunk(byte[] in) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int pos = 0;
        for (;;) {
            int eol = indexOf(in, pos, "\r\n".getBytes());
            int size = Integer.parseInt(new String(in, pos, eol - pos, StandardCharsets.US_ASCII).trim(), 16);
            if (size == 0) {
                return out.toByteArray();
            }
            out.write(in, eol + 2, size);
            pos = eol + 2 + size + 2;
        }
    }

    private static int indexOf(byte[] a, byte[] pat) {
        return indexOf(a, 0, pat);
    }

    private static int indexOf(byte[] a, int from, byte[] pat) {
        outer:
        for (int i = from; i <= a.length - pat.length; i++) {
            for (int j = 0; j < pat.length; j++) {
                if (a[i + j] != pat[j]) continue outer;
            }
            return i;
        }
        throw new AssertionError("pattern not found");
    }

    private static final Map<String, String> WEB = Map.of("Content-Type", "application/grpc-web+proto", "X-Grpc-Web", "1", "Origin", "https://app.example");

    @Test
    void aUnaryCallHasOneMessageThenATrailerFrame() throws Exception {
        Reply r = http("POST", "/hydrangea.test.Echo/Unary", WEB, framed(new Msg("hello", 4)));
        assertEquals(200, r.status());
        assertEquals("application/grpc-web+proto", r.headers().get("content-type"));
        assertEquals("chunked", r.headers().get("transfer-encoding"));
        assertEquals("echo:hello", r.messages().get(0).text());
        assertEquals("0", r.trailers().get("grpc-status"));
    }

    @Test
    void errorsTravelInTheTrailerFrame() throws Exception {
        Reply r = http("POST", "/hydrangea.test.Echo/Unary", WEB, framed(new Msg("FAIL:5:gone café")));
        assertEquals(0, r.messages().size());
        assertEquals("5", r.trailers().get("grpc-status"));
        assertEquals("gone caf%C3%A9", r.trailers().get("grpc-message"));
    }

    @Test
    void serverStreamingSendsEveryMessageBeforeTheTrailers() throws Exception {
        Reply r = http("POST", "/hydrangea.test.Echo/ServerStream", WEB, framed(new Msg("row", 5)));
        assertEquals(5, r.messages().size());
        assertEquals("row-4", r.messages().get(4).text());
        assertEquals("0", r.trailers().get("grpc-status"));
    }

    @Test
    void trailingMetadataJoinsTheTrailerFrame() throws Exception {
        Reply r = http("POST", "/hydrangea.test.Echo/Unary", Map.of("Content-Type", "application/grpc-web", "x-test", "v1"), framed(new Msg("META")));
        assertEquals("v1", r.messages().get(0).text());
        assertEquals("yes", r.trailers().get("x-trailer"));
    }

    @Test
    void corsPreflightIsAnsweredForTheAllowedOrigin() throws Exception {
        Reply r = http("OPTIONS", "/hydrangea.test.Echo/Unary", Map.of("Origin", "https://app.example", "Access-Control-Request-Method", "POST",
                "Access-Control-Request-Headers", "content-type,x-grpc-web"), null);
        assertEquals(204, r.status());
        assertEquals("https://app.example", r.headers().get("access-control-allow-origin"));
        assertTrue(r.headers().get("access-control-allow-headers").contains("x-grpc-web"));
        assertTrue(r.headers().get("access-control-allow-headers").contains("x-custom"));
        Reply other = http("OPTIONS", "/hydrangea.test.Echo/Unary", Map.of("Origin", "https://evil.example", "Access-Control-Request-Method", "POST"), null);
        assertNull(other.headers().get("access-control-allow-origin"), "other origins get no permission");
    }

    @Test
    void responsesCarryTheCorsHeaders() throws Exception {
        Reply r = http("POST", "/hydrangea.test.Echo/Unary", WEB, framed(new Msg("x")));
        assertEquals("https://app.example", r.headers().get("access-control-allow-origin"));
        assertTrue(r.headers().get("access-control-expose-headers").contains("grpc-status"));
    }

    @Test
    void unknownMethodsAndClientStreamingAreRefused() throws Exception {
        Reply nope = http("POST", "/hydrangea.test.Echo/Nope", WEB, framed(new Msg("x")));
        assertEquals(200, nope.status());
        assertEquals("12", nope.headers().get("grpc-status"), "Trailers-Only: the status is in the headers");
        Reply cs = http("POST", "/hydrangea.test.Echo/ClientStream", WEB, framed(new Msg("x")));
        assertEquals("12", cs.headers().get("grpc-status"));
        Reply get = http("GET", "/hydrangea.test.Echo/Unary", Map.of("Content-Type", "application/grpc-web"), null);
        assertEquals(405, get.status());
    }

    private Reply textCall(String path, byte[] requestBody) throws Exception {
        Reply raw = http("POST", path, Map.of("Content-Type", "application/grpc-web-text+proto"), requestBody);
        // the whole response body is ONE valid base64 string, however many writes it took
        byte[] decoded = java.util.Base64.getDecoder().decode(new String(raw.body(), StandardCharsets.US_ASCII));
        return new Reply(raw.status(), raw.headers(), decoded);
    }

    @Test
    void textModeRoundTripsAsOneBase64Stream() throws Exception {
        byte[] request = java.util.Base64.getEncoder().encode(framed(new Msg("text", 2)));
        Reply r = textCall("/hydrangea.test.Echo/Unary", request);
        assertEquals("application/grpc-web-text+proto", r.headers().get("content-type"));
        assertEquals("echo:text", r.messages().get(0).text());
        assertEquals("0", r.trailers().get("grpc-status"));
    }

    @Test
    void textModeStreamsManyMessages() throws Exception {
        Reply r = textCall("/hydrangea.test.Echo/ServerStream", java.util.Base64.getEncoder().encode(framed(new Msg("row", 25))));
        assertEquals(25, r.messages().size());
        assertEquals("row-24", r.messages().get(24).text());
        assertEquals("0", r.trailers().get("grpc-status"));
    }

    @Test
    void textModeAcceptsRequestPiecesEncodedSeparately() throws Exception {
        byte[] whole = framed(new Msg("pieces", 1));
        int cut = 7;   // not a multiple of 3, so both pieces end with padding
        String body = java.util.Base64.getEncoder().encodeToString(java.util.Arrays.copyOfRange(whole, 0, cut))
                + java.util.Base64.getEncoder().encodeToString(java.util.Arrays.copyOfRange(whole, cut, whole.length));
        Reply r = textCall("/hydrangea.test.Echo/Unary", body.getBytes(StandardCharsets.US_ASCII));
        assertEquals("echo:pieces", r.messages().get(0).text());
    }

    @Test
    void textModeRejectsMalformedBase64() throws Exception {
        Reply raw = http("POST", "/hydrangea.test.Echo/Unary", Map.of("Content-Type", "application/grpc-web-text"), "!!not base64!!".getBytes());
        assertEquals("13", raw.headers().get("grpc-status"));
    }

    @Test
    void aCallPastItsDeadlineEndsWithDeadlineExceeded() throws Exception {
        Map<String, String> h = new LinkedHashMap<>(WEB);
        h.put("grpc-timeout", "150m");
        Reply r = http("POST", "/hydrangea.test.Echo/Unary", h, framed(new Msg("SLOW:5000")));
        assertEquals("4", r.trailers().get("grpc-status"));
    }

    @Test
    void theSameCallWorksOverHttp2() throws Exception {
        try (Http2Test.Peer p = Http2Test.Peer.connect(port()).handshake()) {
            p.headers(1, false, true, ":method", "POST", ":scheme", "http", ":authority", "x", ":path", "/hydrangea.test.Echo/Unary",
                    "content-type", "application/grpc-web+proto");
            p.data(1, true, framed(new Msg("h2")));
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            for (;;) {
                Http2Test.Frame f = p.nextInteresting();
                if (f.sid() != 1) continue;
                if (f.type() == 0) {
                    body.write(f.payload());
                }
                if ((f.flags() & 1) != 0) break;
            }
            Reply r = new Reply(200, Map.of(), body.toByteArray());
            assertEquals("echo:h2", r.messages().get(0).text());
            assertEquals("0", r.trailers().get("grpc-status"));
        }
    }
}
