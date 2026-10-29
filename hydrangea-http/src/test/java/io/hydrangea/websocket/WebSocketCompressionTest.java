package io.hydrangea.websocket;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.websocket.WebSocketTest.Frame;
import io.hydrangea.websocket.WebSocketTest.Raw;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.junit.jupiter.api.Assertions.*;

class WebSocketCompressionTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private static final String OFFER = "Sec-WebSocket-Extensions: permessage-deflate\r\n";
    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @BeforeEach
    void setUp() {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : cleanup) c.close();
        clientGroup.close();
        serverGroup.close();
    }

    private static final PerMessageDeflate DEFLATE = PerMessageDeflate.defaults().withThresholdBytes(64);

    private Server start(EventLoopGroup group, WebSocketConfig cfg, Supplier<? extends WebSocketListener> listener) throws IOException {
        HttpRouter router = new HttpRouter().get("/ws", r -> WebSocketUpgrade.accept(r.request(), cfg, listener));
        Server s = HttpServer.bind(group, ANY, Options.defaults(), null, HttpConfig.defaults(), router);
        cleanup.add(s);
        return s;
    }

    private Server start(WebSocketConfig cfg, Supplier<? extends WebSocketListener> listener) throws IOException {
        return start(serverGroup, cfg, listener);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String sha(byte[] b) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Echoes whole messages. */
    static class Echo implements WebSocketListener {
        final CompletableFuture<WebSocket> opened = new CompletableFuture<>();
        final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<int[]> closed = new CompletableFuture<>();
        final AtomicInteger received = new AtomicInteger();

        @Override
        public void onOpen(WebSocket ws) {
            opened.complete(ws);
        }

        @Override
        public void onText(WebSocket ws, String text) {
            received.incrementAndGet();
            ws.sendText(text);
        }

        @Override
        public void onBinary(WebSocket ws, ByteBuffer data) {
            received.incrementAndGet();
            ws.sendBinary(data);
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            errors.add(error);
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            closed.complete(new int[]{code});
        }
    }

    // ---- an independent implementation of the extension's wire format (java.util.zip, as the RFC describes it) ----

    /** What a peer does: raw deflate, sync flush, drop the trailing 00 00 ff ff; with or without keeping its window. */
    static final class PeerCodec {
        private final Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        private final Inflater inflater = new Inflater(true);
        private final boolean resetSend;
        private final boolean resetReceive;

        PeerCodec(boolean resetSend, boolean resetReceive) {
            this.resetSend = resetSend;
            this.resetReceive = resetReceive;
        }

        byte[] compress(byte[] data) {
            deflater.setInput(data);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            do {
                n = deflater.deflate(buf, 0, buf.length, Deflater.SYNC_FLUSH);
                out.write(buf, 0, n);
            } while (n == buf.length);
            byte[] r = out.toByteArray();
            assertTrue(r.length >= 4 && r[r.length - 4] == 0 && r[r.length - 3] == 0 && (r[r.length - 2] & 0xff) == 0xff && (r[r.length - 1] & 0xff) == 0xff,
                    "a sync-flushed message ends with 00 00 ff ff");
            if (resetSend) deflater.reset();
            return java.util.Arrays.copyOf(r, r.length - 4);
        }

        byte[] decompress(byte[] payload) throws DataFormatException {
            byte[] withTail = java.util.Arrays.copyOf(payload, payload.length + 4);
            withTail[payload.length + 2] = (byte) 0xff;
            withTail[payload.length + 3] = (byte) 0xff;
            inflater.setInput(withTail);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[8192];
            while (true) {
                int n = inflater.inflate(buf);
                if (n > 0) out.write(buf, 0, n);
                else if (inflater.needsInput() || inflater.finished()) break;
            }
            if (resetReceive) inflater.reset();
            return out.toByteArray();
        }
    }

    private static String repetitiveJson(int approxBytes) {
        StringBuilder sb = new StringBuilder("[");
        int i = 0;
        while (sb.length() < approxBytes) {
            sb.append("{\"id\":").append(i++).append(",\"name\":\"item\",\"active\":true,\"tags\":[\"alpha\",\"beta\",\"gamma\"]},");
        }
        return sb.append("{}]").toString();
    }

    // ---- header parsing and negotiation ----

    @Test
    void offersAreParsedIncludingParametersQuotingAndDuplicates() {
        var one = Extensions.parse(List.of("permessage-deflate"));
        assertEquals(1, one.size());
        assertEquals("permessage-deflate", one.get(0).name());
        assertTrue(one.get(0).wellFormed() && one.get(0).params().isEmpty());

        var many = Extensions.parse(List.of("permessage-deflate; client_max_window_bits; server_max_window_bits=10, permessage-deflate", "foo; a=b"));
        assertEquals(3, many.size());
        assertEquals("10", many.get(0).params().get("server_max_window_bits"));
        assertTrue(many.get(0).params().containsKey("client_max_window_bits"));
        assertNull(many.get(0).params().get("client_max_window_bits"));
        assertEquals("foo", many.get(2).name());

        assertEquals("10", Extensions.parse(List.of("permessage-deflate; server_max_window_bits=\"10\"")).get(0).params().get("server_max_window_bits"));
        assertEquals("permessage-deflate", Extensions.parse(List.of("Permessage-DEFLATE; Server_No_Context_Takeover")).get(0).name());
        assertTrue(Extensions.parse(List.of("Permessage-DEFLATE; Server_No_Context_Takeover")).get(0).params().containsKey("server_no_context_takeover"));

        assertFalse(Extensions.parse(List.of("permessage-deflate; client_no_context_takeover; client_no_context_takeover")).get(0).wellFormed(), "a repeated parameter");
        assertFalse(Extensions.parse(List.of("permessage-deflate;;")).get(0).wellFormed());
        assertFalse(Extensions.parse(List.of("permessage-deflate; a b=c")).get(0).wellFormed());
        assertTrue(Extensions.parse(List.of("", "  ")).isEmpty());
    }

    @Test
    void theServerOnlyAgreesToOffersItCanHonour() {
        PerMessageDeflate cfg = PerMessageDeflate.defaults();
        assertNull(Extensions.negotiateServer(List.of("permessage-deflate"), null), "compression not configured");
        assertEquals("permessage-deflate", Extensions.negotiateServer(List.of("permessage-deflate"), cfg).responseHeader());
        assertEquals("permessage-deflate; server_no_context_takeover; client_no_context_takeover",
                Extensions.negotiateServer(List.of("permessage-deflate; client_no_context_takeover; server_no_context_takeover"), cfg).responseHeader());
        assertEquals("permessage-deflate; server_max_window_bits=15",
                Extensions.negotiateServer(List.of("permessage-deflate; server_max_window_bits=15"), cfg).responseHeader());
        // the client may announce what it will use itself; no echo is needed
        assertEquals("permessage-deflate", Extensions.negotiateServer(List.of("permessage-deflate; client_max_window_bits"), cfg).responseHeader());
        assertEquals("permessage-deflate", Extensions.negotiateServer(List.of("permessage-deflate; client_max_window_bits=9"), cfg).responseHeader());
        // the JDK deflater cannot shrink its window: such offers are declined, and the next acceptable one is used
        assertNull(Extensions.negotiateServer(List.of("permessage-deflate; server_max_window_bits=10"), cfg));
        assertEquals("permessage-deflate", Extensions.negotiateServer(List.of("permessage-deflate; server_max_window_bits=9, permessage-deflate"), cfg).responseHeader());
        // malformed or unknown: declined
        for (String bad : new String[]{"permessage-deflate; unknown_param", "permessage-deflate; server_max_window_bits=99", "permessage-deflate; server_max_window_bits=abc",
                "permessage-deflate; server_max_window_bits", "permessage-deflate; server_no_context_takeover=1", "permessage-deflate; client_max_window_bits=7",
                "permessage-deflate; client_max_window_bits=015", "permessage-deflate; client_no_context_takeover; client_no_context_takeover", "x-webkit-deflate-frame"}) {
            assertNull(Extensions.negotiateServer(List.of(bad), cfg), bad);
        }
        // the server's own configuration may force no-context-takeover even if the client did not ask
        var forced = PerMessageDeflate.defaults().withNoContextTakeover(true, true);
        assertEquals("permessage-deflate; server_no_context_takeover; client_no_context_takeover",
                Extensions.negotiateServer(List.of("permessage-deflate"), forced).responseHeader());
    }

    @Test
    void theClientChecksWhatTheServerSelected() throws Exception {
        PerMessageDeflate offered = PerMessageDeflate.defaults();
        assertNull(Extensions.acceptServerResponse(List.of(), offered));
        var plain = Extensions.acceptServerResponse(List.of("permessage-deflate"), offered);
        assertFalse(plain.compressorResets() || plain.decompressorResets());
        var flags = Extensions.acceptServerResponse(List.of("permessage-deflate; server_no_context_takeover; client_no_context_takeover"), offered);
        assertTrue(flags.compressorResets() && flags.decompressorResets());
        assertNotNull(Extensions.acceptServerResponse(List.of("permessage-deflate; server_max_window_bits=10"), offered), "a smaller server window only helps us");
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("permessage-deflate"), null), "we did not offer it");
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("foo"), offered));
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("permessage-deflate; client_max_window_bits=10"), offered));
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("permessage-deflate", "permessage-deflate"), offered));
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("permessage-deflate; surprise"), offered));
        assertThrows(WebSocketHandshakeException.class, () -> Extensions.acceptServerResponse(List.of("permessage-deflate; server_max_window_bits=3"), offered));
        // a client that asked for client_no_context_takeover resets even if the server did not echo it
        var asked = Extensions.acceptServerResponse(List.of("permessage-deflate"), PerMessageDeflate.defaults().withNoContextTakeover(false, true));
        assertTrue(asked.compressorResets());
    }

    // ---- on the wire ----

    @Test
    void theExtensionIsNegotiatedAndAnEchoComesBackCompressed() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
        PeerCodec peer = new PeerCodec(false, false);
        try (Raw raw = new Raw(port(server), OFFER)) {
            assertTrue(raw.head.contains("Sec-WebSocket-Extensions: permessage-deflate\r\n"), raw.head);
            String text = repetitiveJson(5000);
            byte[] compressed = peer.compress(utf8(text));
            assertTrue(compressed.length < 600, "the test payload does compress: " + compressed.length);
            raw.send(Raw.frame(0xC1, compressed)); // FIN + RSV1 + text
            Frame reply = raw.read();
            assertEquals(1, reply.op());
            assertTrue(reply.rsv1(), "the reply is compressed too");
            assertTrue(reply.payload().length < 600);
            byte[] p = reply.payload();
            assertFalse(p.length >= 4 && p[p.length - 4] == 0 && p[p.length - 3] == 0 && (p[p.length - 2] & 0xff) == 0xff && (p[p.length - 1] & 0xff) == 0xff,
                    "RFC 7692 §7.2.1: the sender removes the trailing 00 00 ff ff of the last block");
            assertEquals(text, new String(peer.decompress(reply.payload()), StandardCharsets.UTF_8));
        }
    }

    @Test
    void messagesBelowTheThresholdAreSentPlainButStillAccepted() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
        PeerCodec peer = new PeerCodec(false, false);
        try (Raw raw = new Raw(port(server), OFFER)) {
            raw.send(Raw.frame(0xC1, peer.compress(utf8("tiny")))); // the client may compress whatever it likes
            Frame reply = raw.read();
            assertFalse(reply.rsv1(), "4 bytes are below the 64-byte threshold: not worth compressing");
            assertEquals("tiny", new String(reply.payload(), StandardCharsets.UTF_8));
            raw.send(Raw.frame(0x81, utf8("uncompressed, RSV1 clear")));   // and mixing is allowed
            assertEquals("uncompressed, RSV1 clear", new String(raw.read().payload(), StandardCharsets.UTF_8));
            // the plain message was never fed to the peer's deflater, so its window stays in step with the server's
            String second = repetitiveJson(3000);
            raw.send(Raw.frame(0xC1, peer.compress(utf8(second))));
            Frame r2 = raw.read();
            assertEquals(second, new String(peer.decompress(r2.payload()), StandardCharsets.UTF_8));
        }
    }

    @Test
    void contextTakeoverMakesRepeatsCheapAndNoContextTakeoverDoesNot() throws Exception {
        // random hex does not compress within itself, so a repeat is only cheap if the window remembers the first copy
        Random rnd = new Random(77);
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < 4000; i++) hex.append("0123456789abcdef".charAt(rnd.nextInt(16)));
        String text = hex.toString();
        for (boolean noContext : new boolean[]{false, true}) {
            Echo echo = new Echo();
            Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
            String offer = "Sec-WebSocket-Extensions: permessage-deflate" + (noContext ? "; server_no_context_takeover; client_no_context_takeover" : "") + "\r\n";
            PeerCodec peer = new PeerCodec(noContext, noContext);
            try (Raw raw = new Raw(port(server), offer)) {
                int[] sizes = new int[3];
                for (int i = 0; i < 3; i++) {
                    raw.send(Raw.frame(0xC1, peer.compress(utf8(text))));
                    Frame reply = raw.read();
                    sizes[i] = reply.payload().length;
                    assertEquals(text, new String(peer.decompress(reply.payload()), StandardCharsets.UTF_8), "round " + i + " noContext=" + noContext);
                }
                if (noContext) {
                    assertTrue(Math.abs(sizes[1] - sizes[0]) < 16 && Math.abs(sizes[2] - sizes[0]) < 16, "every message starts from scratch: " + java.util.Arrays.toString(sizes));
                } else {
                    assertTrue(sizes[1] < sizes[0] / 10, "the window remembers the first message: " + java.util.Arrays.toString(sizes));
                }
            }
        }
    }

    @Test
    void aCompressedMessageMaySpanSeveralFramesAndPingsMayInterrupt() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
        PeerCodec peer = new PeerCodec(false, false);
        try (Raw raw = new Raw(port(server), OFFER)) {
            String text = repetitiveJson(30_000);
            byte[] c = peer.compress(utf8(text));
            int a = c.length / 3, b = 2 * c.length / 3;
            raw.send(Raw.frame(0x41, java.util.Arrays.copyOfRange(c, 0, a)));       // text, RSV1, not final
            raw.send(Raw.frame(0x89, "mid".getBytes()));                             // a ping in the middle
            assertEquals("mid", new String(raw.read().payload()), "pong first");
            raw.send(Raw.frame(0x00, java.util.Arrays.copyOfRange(c, a, b)));       // continuation: RSV1 clear
            raw.send(Raw.frame(0x80, java.util.Arrays.copyOfRange(c, b, c.length))); // final
            Frame reply = raw.read();
            assertEquals(text, new String(peer.decompress(reply.payload()), StandardCharsets.UTF_8));
        }
    }

    @Test
    void emptyCompressedMessagesWork() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
        try (Raw raw = new Raw(port(server), OFFER)) {
            raw.send(Raw.frame(0xC1, new byte[]{0x00}));  // RFC 7692 §7.2.3.6: the compressed form of an empty message
            Frame r1 = raw.read();
            assertEquals(0, r1.payload().length == 0 && !r1.rsv1() ? 0 : (r1.rsv1() ? 0 : 1));
            raw.send(Raw.frame(0xC2, new byte[]{0x00}));
            assertEquals(2, raw.read().op());
            raw.send(Raw.frame(0x81, utf8("still in sync")));
            assertEquals("still in sync", new String(raw.read().payload(), StandardCharsets.UTF_8));
        }
    }

    // ---- violations ----

    @Test
    void misplacedAndCorruptCompressionIsRejected() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE), () -> echo);
        PeerCodec peer = new PeerCodec(false, false);
        byte[] ok = peer.compress(utf8(repetitiveJson(1000)));
        record Case(String name, byte[][] frames, int status) {
        }
        List<Case> cases = List.of(
                new Case("RSV1 on a continuation frame", new byte[][]{Raw.frame(0x01, utf8("a")), Raw.frame(0x40, utf8("b"))}, 1002),
                new Case("RSV1 on a ping", new byte[][]{Raw.frame(0xC9, new byte[0])}, 1002),
                new Case("RSV1 on a close frame", new byte[][]{Raw.frame(0xC8, new byte[]{0x03, (byte) 0xe8})}, 1002),
                new Case("RSV2 is never valid", new byte[][]{Raw.frame(0xA1, utf8("x"))}, 1002),
                new Case("RSV3 is never valid", new byte[][]{Raw.frame(0x91, utf8("x"))}, 1002),
                new Case("garbage instead of deflate data", new byte[][]{Raw.frame(0xC1, new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x12, 0x34})}, 1007),
                new Case("valid deflate holding invalid UTF-8", new byte[][]{Raw.frame(0xC1, peer.compress(new byte[]{(byte) 0xff, (byte) 0xfe, 'x'}))}, 1007));
        for (Case c : cases) {
            try (Raw raw = new Raw(port(server), OFFER)) {
                for (byte[] f : c.frames()) raw.send(f);
                assertEquals(c.status(), raw.closeStatusThenEof(), c.name());
            }
            peer = new PeerCodec(false, false); // the failed case may have advanced our window; the next connection starts fresh
            ok = peer.compress(utf8(repetitiveJson(1000)));
        }
    }

    @Test
    void rsv1WithoutANegotiatedExtensionIsStillAProtocolError() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults(), () -> echo); // compression not configured
        try (Raw raw = new Raw(port(server), OFFER)) {
            assertFalse(raw.head.toLowerCase().contains("sec-websocket-extensions"), "the offer was ignored: " + raw.head);
            raw.send(Raw.frame(0xC1, utf8("x")));
            assertEquals(1002, raw.closeStatusThenEof());
        }
    }

    // ---- decompression bombs ----

    /** A message that is {@code decompressedBytes} zeros once inflated and a tiny fraction of that on the wire. */
    private static byte[] bomb(long decompressedBytes) {
        Deflater d = new Deflater(9, true);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[65536];
        byte[] zeros = new byte[1 << 20];
        long fed = 0;
        while (fed < decompressedBytes) {
            int n = (int) Math.min(zeros.length, decompressedBytes - fed);
            d.setInput(zeros, 0, n);
            fed += n;
            while (!d.needsInput()) {
                out.write(buf, 0, d.deflate(buf, 0, buf.length, Deflater.NO_FLUSH));
            }
        }
        int r;
        do {
            r = d.deflate(buf, 0, buf.length, Deflater.SYNC_FLUSH);
            out.write(buf, 0, r);
        } while (r == buf.length);
        d.end();
        byte[] all = out.toByteArray();
        assertTrue(all.length > 4 && all[all.length - 4] == 0 && all[all.length - 3] == 0 && (all[all.length - 2] & 0xff) == 0xff && (all[all.length - 1] & 0xff) == 0xff);
        return java.util.Arrays.copyOf(all, all.length - 4);
    }

    /** The bomb may be larger than the socket buffers; the server stops reading it half way, so the write can fail. */
    private static void sendInBackground(Raw raw, byte[] bytes) {
        Thread.startVirtualThread(() -> {
            try {
                raw.send(bytes);
            } catch (IOException serverAlreadyHungUp) {
                // expected once the limit was crossed
            }
        });
    }

    @Test
    void aDecompressionBombIsStoppedByTheMessageLimitAfterInflating() throws Exception {
        Echo echo = new Echo();
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(1024 * 1024).withCompression(DEFLATE), () -> echo);
        byte[] bomb = bomb(400L * 1024 * 1024);
        assertTrue(bomb.length < 2_000_000, "400 MB of zeros squeeze into " + bomb.length + " bytes");
        long t0 = System.nanoTime();
        try (Raw raw = new Raw(port(server), OFFER)) {
            sendInBackground(raw, Raw.frame(0xC2, bomb));
            assertEquals(1009, raw.closeStatusThenEof());
        }
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 5000, "the connection is cut as soon as the limit is crossed, not after inflating everything");
        assertEquals(0, echo.received.get());
    }

    @Test
    void aStreamedBombIsStoppedByTheStreamedLimitToo() throws Exception {
        WebSocketStreamingTest.Streamer rec = new WebSocketStreamingTest.Streamer();
        Server server = start(WebSocketConfig.defaults().withMaxStreamedMessageBytes(2L * 1024 * 1024).withCompression(DEFLATE), () -> rec);
        try (Raw raw = new Raw(port(server), OFFER)) {
            sendInBackground(raw, Raw.frame(0xC2, bomb(300L * 1024 * 1024)));
            assertEquals(1009, raw.closeStatusThenEof());
        }
        assertTrue(rec.bytes().length <= 2L * 1024 * 1024 + 16 * 1024, "the listener never saw much more than the limit: " + rec.bytes().length);
    }

    // ---- streaming and flow control with compression ----

    @Test
    void streamedMessagesAreInflatedProgressivelyAndPauseStopsTheExpansion() throws Exception {
        WebSocketStreamingTest.Streamer rec = new WebSocketStreamingTest.Streamer();
        Server server = start(WebSocketConfig.defaults().withCompression(DEFLATE).withMaxMessageBytes(1000), () -> rec);
        long total = 120L * 1024 * 1024;
        byte[] bomb = bomb(total); // a few hundred KB on the wire, 120 MB once inflated
        rec.afterPart = ws -> {
            if (rec.binaryParts.get() == 1) ws.setAutoRead(false);
        };
        try (Raw raw = new Raw(port(server), OFFER)) {
            sendInBackground(raw, Raw.frame(0xC2, bomb));
            for (int i = 0; i < 100 && rec.binaryParts.get() < 1; i++) Thread.sleep(20);
            Thread.sleep(700);
            assertEquals(1, rec.binaryParts.get(), "paused after the first piece: the inflater must not keep expanding the rest");
            rec.afterPart = ws -> { };
            rec.opened.get().setAutoRead(true);
            rec.messageEnded.get(60, TimeUnit.SECONDS);
        }
        assertEquals(total, rec.bytes().length);
        assertTrue(rec.binaryParts.get() > 100, "delivered in many small pieces: " + rec.binaryParts.get());
        assertEquals(1, rec.lastFlags.get());
    }

    // ---- both ends of Hydrangea ----

    @Test
    void ourClientAndServerCompressEndToEndAndTheWireGetsMuchSmaller() throws Exception {
        String json = repetitiveJson(4 * 1024 * 1024);
        long[] wire = new long[2];
        for (int mode = 0; mode < 2; mode++) {
            boolean compress = mode == 1;
            EventLoopGroup sg = new EventLoopGroup(1);
            cleanup.add(sg::close);
            WebSocketConfig cfg = compress ? WebSocketConfig.defaults().withMaxMessageBytes(16 << 20).withCompression(DEFLATE)
                    : WebSocketConfig.defaults().withMaxMessageBytes(16 << 20);
            Echo echo = new Echo();
            Server server = start(sg, cfg, () -> echo);
            CompletableFuture<String> back = new CompletableFuture<>();
            WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() {
                @Override
                public void onText(WebSocket w, String t) {
                    back.complete(t);
                }
            }, cfg).get(10, TimeUnit.SECONDS);
            ws.sendText(json).get(10, TimeUnit.SECONDS);
            assertEquals(json, back.get(30, TimeUnit.SECONDS));
            wire[mode] = sg.metrics().snapshot().bytesRead();
        }
        assertTrue(wire[1] < wire[0] / 5, "compressed upload used " + wire[1] + " bytes against " + wire[0] + " uncompressed");
    }

    @Test
    void textBinaryPartsAndStreamsAllSurviveCompressionInBothDirections() throws Exception {
        WebSocketStreamingTest.Streamer serverSide = new WebSocketStreamingTest.Streamer();
        WebSocketConfig cfg = WebSocketConfig.defaults().withCompression(PerMessageDeflate.defaults().withThresholdBytes(0)).withMaxMessageBytes(1 << 20);
        Server server = start(cfg, () -> serverSide);
        WebSocketStreamingTest.Streamer clientSide = new WebSocketStreamingTest.Streamer();
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", clientSide, cfg).get(10, TimeUnit.SECONDS);

        // text sent in parts (one cut inside a surrogate pair), received in parts
        String text = "héllo 🎉 " + repetitiveJson(20_000);
        int cut = text.indexOf("🎉") + 1;
        ws.sendTextPart(text.substring(0, cut), false).get(5, TimeUnit.SECONDS);
        ws.sendTextPart(text.substring(cut, cut + 5000), false).get(5, TimeUnit.SECONDS);
        ws.sendTextPart(text.substring(cut + 5000), true).get(5, TimeUnit.SECONDS);
        serverSide.messageEnded.get(10, TimeUnit.SECONDS);
        assertEquals(text, serverSide.texts());

        // a big random (incompressible) binary stream, then the server answers with a compressible one
        byte[] random = new byte[6 * 1024 * 1024];
        new Random(5).nextBytes(random);
        serverSide.messageEnded.getNow(null);
        WebSocketStreamingTest.Streamer s2 = new WebSocketStreamingTest.Streamer();
        ws.sendBinaryStream(new ByteArrayInputStream(random)).get(60, TimeUnit.SECONDS);
        for (int i = 0; i < 300 && serverSide.bytes().length < random.length; i++) Thread.sleep(50);
        assertEquals(sha(random), sha(serverSide.bytes()), "incompressible data goes through intact");

        byte[] zeros = new byte[10 * 1024 * 1024];
        serverSide.opened.get().sendBinaryStream(new ByteArrayInputStream(zeros)).get(60, TimeUnit.SECONDS);
        for (int i = 0; i < 300 && clientSide.bytes().length < zeros.length; i++) Thread.sleep(50);
        assertEquals(zeros.length, clientSide.bytes().length);
        assertEquals(sha(zeros), sha(clientSide.bytes()));
    }

    @Test
    void manyThreadsSendingCompressedMessagesKeepContentAndPerThreadOrder() throws Exception {
        Echo echo = new Echo();
        WebSocketConfig cfg = WebSocketConfig.defaults().withCompression(PerMessageDeflate.defaults().withThresholdBytes(0));
        Server server = start(cfg, () -> echo);
        List<String> got = Collections.synchronizedList(new ArrayList<>());
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() {
            @Override
            public void onText(WebSocket w, String t) {
                got.add(t);
            }
        }, cfg).get(10, TimeUnit.SECONDS);
        int threads = 4, per = 1500;
        CountDownLatch go = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int id = t;
            Thread.startVirtualThread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < per; i++) ws.sendText(id + ":" + i + ":" + "payload-".repeat(1 + (i % 9)));
            });
        }
        go.countDown();
        for (int i = 0; i < 400 && got.size() < threads * per; i++) Thread.sleep(50);
        assertEquals(threads * per, got.size());
        int[] next = new int[threads];
        for (String s : new ArrayList<>(got)) {
            String[] p = s.split(":");
            int id = Integer.parseInt(p[0]);
            assertEquals(next[id]++, Integer.parseInt(p[1]));
            assertEquals("payload-".repeat(1 + (Integer.parseInt(p[1]) % 9)), p[2], "content survived the shared compression window");
        }
        assertTrue(echo.errors.isEmpty());
    }

    @Test
    void closingConnectionsWhileCompressedMessagesAreInFlightLeavesTheServerHealthy() throws Exception {
        Echo echo = new Echo();
        WebSocketConfig cfg = WebSocketConfig.defaults().withCompression(PerMessageDeflate.defaults().withThresholdBytes(0)).withMaxMessageBytes(8 << 20);
        Server server = start(cfg, () -> echo);
        String payload = repetitiveJson(200_000);
        for (int i = 0; i < 60; i++) {
            WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() { }, cfg).get(10, TimeUnit.SECONDS);
            for (int m = 0; m < 5; m++) ws.sendText(payload);
            if (i % 2 == 0) ws.close(); // orderly
            else clientGroup.next().execute(() -> { }); // abandon: the group closes the socket at teardown anyway
        }
        // the server still serves a fresh, normal conversation
        CompletableFuture<String> back = new CompletableFuture<>();
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", new WebSocketListener() {
            @Override
            public void onText(WebSocket w, String t) {
                back.complete(t);
            }
        }, cfg).get(10, TimeUnit.SECONDS);
        ws.sendText("still alive, still alive, still alive, still alive, still alive, still alive!");
        assertEquals("still alive, still alive, still alive, still alive, still alive, still alive!", back.get(10, TimeUnit.SECONDS));
    }

    // ---- the client's view of a negotiation gone wrong ----

    private int scriptedServer(String extensionsHeaderLine) throws IOException {
        ServerSocket ss = new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        var pool = Executors.newVirtualThreadPerTaskExecutor();
        cleanup.add(pool::shutdownNow);
        pool.execute(() -> {
            while (!ss.isClosed()) {
                try (Socket s = ss.accept()) {
                    var in = s.getInputStream();
                    ByteArrayOutputStream head = new ByteArrayOutputStream();
                    while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) head.write(in.read());
                    String key = head.toString(StandardCharsets.ISO_8859_1).lines().filter(x -> x.startsWith("Sec-WebSocket-Key")).findFirst().get().split(": ")[1];
                    OutputStream out = s.getOutputStream();
                    out.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                            + WebSocketUpgrade.acceptKey(key) + "\r\n" + extensionsHeaderLine + "\r\n").getBytes());
                    out.flush();
                    while (in.read() >= 0) { /* idle */ }
                } catch (Exception ignored) { }
            }
        });
        return ss.getLocalPort();
    }

    @Test
    void theClientRefusesAnIllegalExtensionResponse() throws Exception {
        WebSocketConfig withDeflate = WebSocketConfig.defaults().withCompression(DEFLATE);
        for (String bad : new String[]{"Sec-WebSocket-Extensions: foo\r\n", "Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits=10\r\n",
                "Sec-WebSocket-Extensions: permessage-deflate\r\nSec-WebSocket-Extensions: permessage-deflate\r\n", "Sec-WebSocket-Extensions: permessage-deflate; x=1\r\n"}) {
            int port = scriptedServer(bad);
            var f = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port + "/", new WebSocketListener() { }, withDeflate);
            assertInstanceOf(WebSocketHandshakeException.class, failure(f), bad);
        }
        // an extension the client never offered is refused even if it is a legal one
        int port = scriptedServer("Sec-WebSocket-Extensions: permessage-deflate\r\n");
        assertInstanceOf(WebSocketHandshakeException.class,
                failure(WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port + "/", new WebSocketListener() { }, WebSocketConfig.defaults())));
        // a legal, smaller server window is accepted
        int ok = scriptedServer("Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=10\r\n");
        assertNotNull(WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + ok + "/", new WebSocketListener() { }, withDeflate).get(10, TimeUnit.SECONDS));
    }

    @Test
    void theClientOffersWhatItsConfigurationSays() throws Exception {
        CompletableFuture<String> offered = new CompletableFuture<>();
        ServerSocket ss = new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
        cleanup.add(ss);
        Thread.startVirtualThread(() -> {
            try (Socket s = ss.accept()) {
                ByteArrayOutputStream head = new ByteArrayOutputStream();
                var in = s.getInputStream();
                while (!head.toString(StandardCharsets.ISO_8859_1).endsWith("\r\n\r\n")) head.write(in.read());
                offered.complete(head.toString(StandardCharsets.ISO_8859_1).lines().filter(x -> x.startsWith("Sec-WebSocket-Extensions")).findFirst().orElse("none"));
            } catch (Exception e) {
                offered.completeExceptionally(e);
            }
        });
        WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + ss.getLocalPort() + "/", new WebSocketListener() { },
                WebSocketConfig.defaults().withCompression(PerMessageDeflate.defaults().withNoContextTakeover(true, true)).withHandshakeTimeout(java.time.Duration.ofMillis(500)));
        assertEquals("Sec-WebSocket-Extensions: permessage-deflate; client_no_context_takeover; server_no_context_takeover", offered.get(5, TimeUnit.SECONDS));
    }

    private static Throwable failure(CompletableFuture<?> f) {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        }
        throw new AssertionError("expected failure");
    }
}
