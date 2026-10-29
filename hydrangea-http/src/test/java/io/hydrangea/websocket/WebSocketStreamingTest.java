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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class WebSocketStreamingTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
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

    private Server start(WebSocketConfig cfg, Supplier<? extends WebSocketListener> listener) throws IOException {
        HttpRouter router = new HttpRouter().get("/ws", r -> WebSocketUpgrade.accept(r.request(), cfg, listener));
        Server s = HttpServer.bind(serverGroup, ANY, Options.defaults(), null, HttpConfig.defaults(), router);
        cleanup.add(s);
        return s;
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

    /** Takes messages in parts and records what it sees. */
    static class Streamer implements WebSocketListener {
        volatile boolean streams = true;
        final ByteArrayOutputStream binary = new ByteArrayOutputStream();
        final StringBuilder text = new StringBuilder();
        final AtomicInteger binaryParts = new AtomicInteger();
        final AtomicInteger textParts = new AtomicInteger();
        final AtomicInteger lastFlags = new AtomicInteger();
        final List<String> wholeTexts = Collections.synchronizedList(new ArrayList<>());
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<Void> messageEnded = new CompletableFuture<>();
        final CompletableFuture<int[]> closed = new CompletableFuture<>();
        final CompletableFuture<WebSocket> opened = new CompletableFuture<>();
        final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        volatile Consumer<WebSocket> afterPart = ws -> { };
        volatile Consumer<WebSocket> afterText = ws -> { };

        @Override
        public boolean streaming() {
            return streams;
        }

        @Override
        public void onOpen(WebSocket ws) {
            opened.complete(ws);
        }

        @Override
        public void onTextPart(WebSocket ws, String part, boolean last) {
            synchronized (text) {
                text.append(part);
            }
            textParts.incrementAndGet();
            events.add("textPart");
            if (last) {
                lastFlags.incrementAndGet();
                messageEnded.complete(null);
            }
            afterPart.accept(ws);
        }

        @Override
        public void onBinaryPart(WebSocket ws, ByteBuffer part, boolean last) {
            byte[] b = new byte[part.remaining()];
            part.get(b);
            synchronized (binary) {
                binary.writeBytes(b);
            }
            binaryParts.incrementAndGet();
            events.add("binPart");
            if (last) {
                lastFlags.incrementAndGet();
                messageEnded.complete(null);
            }
            afterPart.accept(ws);
        }

        @Override
        public void onText(WebSocket ws, String text) {
            wholeTexts.add(text);
            events.add("text");
            afterText.accept(ws);
        }

        @Override
        public void onPing(WebSocket ws, ByteBuffer data) {
            events.add("ping");
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            errors.add(error);
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            closed.complete(new int[]{code});
        }

        byte[] bytes() {
            synchronized (binary) {
                return binary.toByteArray();
            }
        }

        String texts() {
            synchronized (text) {
                return text.toString();
            }
        }
    }

    // ---- receiving in parts ----

    @Test
    void aHugeFrameIsDeliveredInPiecesNotBufferedAndIgnoresTheMessageLimit() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(1000), () -> rec); // 1 KB buffered limit
        byte[] data = new byte[24 * 1024 * 1024];
        new Random(1).nextBytes(data);
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x82, data)); // ONE frame, 24000x the limit
            rec.messageEnded.get(30, TimeUnit.SECONDS);
        }
        assertEquals(sha(data), sha(rec.bytes()));
        assertTrue(rec.binaryParts.get() > 10, "delivered progressively, not as one blob: " + rec.binaryParts.get() + " parts");
        assertEquals(1, rec.lastFlags.get(), "the end of the message is flagged exactly once");
        assertTrue(rec.errors.isEmpty());
    }

    @Test
    void textSplitAnywhereArrivesAsWholeCharacters() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(100), () -> rec);
        String original = "Việt Nam ✓ 🎉 xin chào, ñandú! ".repeat(40);
        byte[] bytes = utf8(original);
        try (Raw raw = new Raw(port(server), "")) {
            // fragments cut at arbitrary byte offsets (inside multi-byte characters), delivered a few bytes at a time
            Random rnd = new Random(2);
            int pos = 0;
            boolean first = true;
            while (pos < bytes.length) {
                int n = Math.min(1 + rnd.nextInt(97), bytes.length - pos);
                boolean last = pos + n == bytes.length;
                byte[] frame = Raw.frame((last ? 0x80 : 0x00) | (first ? 0x01 : 0x00), java.util.Arrays.copyOfRange(bytes, pos, pos + n));
                for (int i = 0; i < frame.length; i += 3) {
                    raw.send(java.util.Arrays.copyOfRange(frame, i, Math.min(frame.length, i + 3)));
                }
                first = false;
                pos += n;
            }
            rec.messageEnded.get(20, TimeUnit.SECONDS);
        }
        assertEquals(original, rec.texts());
        assertEquals(1, rec.lastFlags.get());
        assertTrue(rec.textParts.get() > 5);
    }

    @Test
    void invalidUtf8EndsAStreamedMessageAtOnce() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x01, utf8("good start ")));
            raw.send(Raw.frame(0x00, new byte[]{(byte) 0xc0, (byte) 0xaf})); // overlong encoding, and the message is not finished
            assertEquals(1007, raw.closeStatusThenEof());
        }
        assertTrue(rec.texts().startsWith("good start"), "what was valid was delivered before the failure");
        assertEquals(0, rec.lastFlags.get());
    }

    @Test
    void theStreamedMessageLimitIsEnforcedWhenConfigured() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults().withMaxStreamedMessageBytes(100_000), () -> rec);
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x02, new byte[60_000]));
            raw.send(Raw.frame(0x00, new byte[60_000]));
            assertEquals(1009, raw.closeStatusThenEof());
        }
        // and the same message is fine without the limit (the default)
        Streamer unlimited = new Streamer();
        Server server2 = start(WebSocketConfig.defaults(), () -> unlimited);
        try (Raw raw = new Raw(port(server2), "")) {
            raw.send(Raw.frame(0x02, new byte[60_000]));
            raw.send(Raw.frame(0x80, new byte[60_000]));
            unlimited.messageEnded.get(10, TimeUnit.SECONDS);
        }
        assertEquals(120_000, unlimited.bytes().length);
    }

    @Test
    void controlFramesInTheMiddleOfAStreamedMessageAreStillAnswered() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        try (Raw raw = new Raw(port(server), "")) {
            raw.send(Raw.frame(0x02, "first-".getBytes()));
            raw.send(Raw.frame(0x89, "ping-me".getBytes()));
            Frame pong = raw.read();
            assertEquals(10, pong.op());
            assertEquals("ping-me", new String(pong.payload()));
            raw.send(Raw.frame(0x80, "second".getBytes()));
            rec.messageEnded.get(5, TimeUnit.SECONDS);
        }
        assertEquals("first-second", new String(rec.bytes()));
    }

    @Test
    void theListenerChoosesPerMessageBetweenWholeAndParts() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        try (Raw raw = new Raw(port(server), "")) {
            rec.streams = false;
            raw.send(Raw.frame(0x81, utf8("whole message")));
            for (int i = 0; i < 100 && rec.wholeTexts.isEmpty(); i++) Thread.sleep(20);
            rec.streams = true;
            raw.send(Raw.frame(0x81, utf8("streamed message")));
            rec.messageEnded.get(5, TimeUnit.SECONDS);
        }
        assertEquals(List.of("whole message"), rec.wholeTexts);
        assertEquals("streamed message", rec.texts());
    }

    // ---- flow control ----

    @Test
    void pausingHoldsBackDeliveryAndSlowsTheSender() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        int total = 40 * 1024 * 1024;
        rec.afterPart = ws -> {
            if (rec.binaryParts.get() == 1) ws.setAutoRead(false); // pause after the first piece
        };
        try (Raw raw = new Raw(port(server), "")) {
            byte[] payload = new byte[total];
            new Random(3).nextBytes(payload);
            byte[] frame = Raw.frame(0x82, payload);
            AtomicLong sent = new AtomicLong();
            Thread writer = Thread.startVirtualThread(() -> {
                try {
                    for (int off = 0; off < frame.length; off += 64 * 1024) {
                        int n = Math.min(64 * 1024, frame.length - off);
                        raw.s.getOutputStream().write(frame, off, n);
                        sent.addAndGet(n);
                    }
                } catch (IOException ignored) { }
            });
            for (int i = 0; i < 100 && rec.binaryParts.get() < 1; i++) Thread.sleep(20);
            Thread.sleep(800);
            int partsWhilePaused = rec.binaryParts.get();
            assertEquals(1, partsWhilePaused, "the pause took effect inside the first piece: no second piece may follow");
            long sentWhilePaused = sent.get();
            Thread.sleep(500);
            assertEquals(partsWhilePaused, rec.binaryParts.get(), "nothing is delivered while paused, though bytes were already read");
            assertTrue(sentWhilePaused < total / 2, "TCP backpressure throttled the sender: " + sentWhilePaused + " of " + total);
            rec.afterPart = ws -> { };
            WebSocket ws = rec.opened.get();
            ws.setAutoRead(true);
            rec.messageEnded.get(30, TimeUnit.SECONDS);
            writer.join(10_000);
            assertEquals(sha(payload), sha(rec.bytes()), "resuming drains the backlog that was buffered during the pause, in order");
        }
    }

    @Test
    void messagesAlreadyReadWhenPausedWaitForTheResume() throws Exception {
        Streamer rec = new Streamer();
        rec.streams = false;
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        // the listener pauses while handling the FIRST message; the other two are then already sitting in the read buffer
        rec.afterText = ws -> {
            if (rec.wholeTexts.size() == 1) ws.setAutoRead(false);
        };
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = rec.opened.get(5, TimeUnit.SECONDS);
            raw.send(concat(Raw.frame(0x81, utf8("one")), Raw.frame(0x81, utf8("two")), Raw.frame(0x81, utf8("three"))));  // one TCP segment
            for (int i = 0; i < 100 && rec.wholeTexts.isEmpty(); i++) Thread.sleep(20);
            Thread.sleep(400);
            assertEquals(List.of("one"), rec.wholeTexts, "bytes of the next messages are already here, but nothing may be delivered while paused");
            ws.setAutoRead(true);
            for (int i = 0; i < 100 && rec.wholeTexts.size() < 3; i++) Thread.sleep(20);
            assertEquals(List.of("one", "two", "three"), rec.wholeTexts, "the backlog is delivered, in order, on resume");
        }
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) o.writeBytes(p);
        return o.toByteArray();
    }

    // ---- sending in parts ----

    private static final class Sender implements WebSocketListener {
        final CompletableFuture<WebSocket> opened = new CompletableFuture<>();

        @Override
        public void onOpen(WebSocket ws) {
            opened.complete(ws);
        }
    }

    @Test
    void partsGoOutAsRealFragmentsAndASurrogatePairMayBeSplit() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults(), () -> sender);
        String full = "héllo 🎉 wörld ✓";
        int cut = full.indexOf("🎉") + 1; // between the two halves of the surrogate pair
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            ws.sendTextPart(full.substring(0, cut), false).get(5, TimeUnit.SECONDS);
            ws.sendTextPart(full.substring(cut, cut + 5), false).get(5, TimeUnit.SECONDS);
            ws.sendTextPart(full.substring(cut + 5), true).get(5, TimeUnit.SECONDS);
            Frame f1 = raw.read(), f2 = raw.read(), f3 = raw.read();
            assertEquals(1, f1.op());
            assertFalse(f1.fin());
            assertEquals(0, f2.op());
            assertFalse(f2.fin());
            assertEquals(0, f3.op());
            assertTrue(f3.fin());
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            all.writeBytes(f1.payload());
            all.writeBytes(f2.payload());
            all.writeBytes(f3.payload());
            assertEquals(full, new String(all.toByteArray(), StandardCharsets.UTF_8), "no replacement characters: the pair was kept together");
            // frame 1 must itself be valid UTF-8 (the dangling high surrogate was held back, not encoded as '?')
            assertEquals(full.substring(0, cut - 1), new String(f1.payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void controlFramesMayBeInterleavedButOtherDataMessagesMayNot() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults(), () -> sender);
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            ws.sendBinaryPart(ByteBuffer.wrap(new byte[]{1, 2}), false).get(5, TimeUnit.SECONDS);
            ws.sendPing(ByteBuffer.wrap(new byte[]{9})).get(5, TimeUnit.SECONDS);
            assertInstanceOf(IllegalStateException.class, failure(ws.sendText("not now")), "a whole message inside a fragmented one");
            assertInstanceOf(IllegalStateException.class, failure(ws.sendBinary(ByteBuffer.wrap(new byte[1]))));
            assertInstanceOf(IllegalStateException.class, failure(ws.sendTextPart("wrong type", false)), "text part inside a binary message");
            ws.sendBinaryPart(ByteBuffer.wrap(new byte[]{3}), true).get(5, TimeUnit.SECONDS);
            ws.sendText("after").get(5, TimeUnit.SECONDS);   // fine once the message is finished
            assertEquals(List.of("2:0:2", "9:1:1", "0:1:1", "1:1:5"), List.of(desc(raw.read()), desc(raw.read()), desc(raw.read()), desc(raw.read())));
        }
    }

    private static String desc(Frame f) {
        return f.op() + ":" + (f.fin() ? 1 : 0) + ":" + f.payload().length;
    }

    @Test
    void aPartThatWouldBreakTheTextIsRejected() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults(), () -> sender);
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            ws.sendTextPart("ok", false).get(5, TimeUnit.SECONDS);
            assertInstanceOf(IllegalArgumentException.class, failure(ws.sendTextPart("\uD83C", true)),
                    "the message cannot end in the middle of a surrogate pair");
        }
    }

    @Test
    void partsAreNotLimitedByTheMessageLimitWhichProtectsTheReceivingSide() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(1000), () -> sender);
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            ws.sendBinaryPart(ByteBuffer.wrap(new byte[50_000]), true).get(5, TimeUnit.SECONDS);
            assertEquals("2:1:50000", desc(raw.read()));
            assertThrows(IllegalArgumentException.class, () -> ws.sendBinary(ByteBuffer.wrap(new byte[50_000])),
                    "a whole message is still checked against the limit");
        }
    }

    @Test
    void anEmptyStreamIsAnEmptyBinaryMessage() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults(), () -> sender);
        try (Raw raw = new Raw(port(server), "")) {
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            ws.sendBinaryStream(new ByteArrayInputStream(new byte[0])).get(5, TimeUnit.SECONDS);
            assertEquals("2:1:0", desc(raw.read()));
            // exact multiples of the block size end with the right "last" flag as well
            ws.sendBinaryStream(new ByteArrayInputStream(new byte[64 * 1024])).get(5, TimeUnit.SECONDS);
            assertEquals("2:1:" + 64 * 1024, desc(raw.read()));
            ws.sendBinaryStream(new ByteArrayInputStream(new byte[64 * 1024 + 1])).get(5, TimeUnit.SECONDS);
            assertEquals("2:0:" + 64 * 1024, desc(raw.read()));
            assertEquals("0:1:1", desc(raw.read()));
        }
    }

    @Test
    void sendBinaryStreamStopsReadingWhenThePeerStopsReading() throws Exception {
        Sender sender = new Sender();
        Server server = start(WebSocketConfig.defaults(), () -> sender);
        AtomicLong produced = new AtomicLong();
        long total = 400L * 1024 * 1024;
        try (Raw raw = new Raw(port(server), "")) { // after the handshake this client reads nothing
            WebSocket ws = sender.opened.get(5, TimeUnit.SECONDS);
            InputStream endless = new InputStream() {
                @Override
                public int read() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    if (produced.get() >= total) return -1;
                    int n = (int) Math.min(len, total - produced.get());
                    produced.addAndGet(n);
                    return n;
                }
            };
            CompletableFuture<Void> done = ws.sendBinaryStream(endless);
            Thread.sleep(1500);
            long stalled = produced.get();
            Thread.sleep(500);
            assertEquals(stalled, produced.get(), "the source is no longer being read: the socket is full");
            assertTrue(stalled < 64L * 1024 * 1024, "memory stays bounded; read " + stalled / 1024 / 1024 + " MB of " + total / 1024 / 1024);
            assertFalse(done.isDone());
        }
    }

    // ---- both ends, and an independent implementation ----

    @Test
    void ourClientAndServerStreamLargeMessagesBothWays() throws Exception {
        Streamer serverSide = new Streamer();
        Server server = start(WebSocketConfig.defaults().withMaxMessageBytes(4096), () -> serverSide);
        Streamer clientSide = new Streamer();
        WebSocket ws = WebSocketClient.connect(clientGroup, "ws://127.0.0.1:" + port(server) + "/ws", clientSide,
                WebSocketConfig.defaults().withMaxMessageBytes(4096)).get(10, TimeUnit.SECONDS);
        byte[] data = new byte[30 * 1024 * 1024];
        new Random(4).nextBytes(data);
        ws.sendBinaryStream(new ByteArrayInputStream(data)).get(60, TimeUnit.SECONDS);
        serverSide.messageEnded.get(60, TimeUnit.SECONDS);
        assertEquals(sha(data), sha(serverSide.bytes()), "client -> server, masked frames, byte exact");
        // and back
        WebSocket back = serverSide.opened.get();
        back.sendBinaryStream(new ByteArrayInputStream(data)).get(60, TimeUnit.SECONDS);
        clientSide.messageEnded.get(60, TimeUnit.SECONDS);
        assertEquals(sha(data), sha(clientSide.bytes()), "server -> client");
        assertTrue(serverSide.binaryParts.get() > 5 && clientSide.binaryParts.get() > 5);
    }

    @Test
    void theJdkClientInteroperatesInBothDirections() throws Exception {
        Streamer rec = new Streamer();
        Server server = start(WebSocketConfig.defaults(), () -> rec);
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<Void> gotAll = new CompletableFuture<>();
        var jdk = java.net.http.HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(
                URI.create("ws://127.0.0.1:" + port(server) + "/ws"), new java.net.http.WebSocket.Listener() {
                    private final StringBuilder sb = new StringBuilder();

                    @Override
                    public CompletionStage<?> onText(java.net.http.WebSocket w, CharSequence data, boolean last) {
                        sb.append(data);
                        if (last) {
                            received.add(sb.toString());
                            gotAll.complete(null);
                        }
                        w.request(1);
                        return null;
                    }
                }).get(10, TimeUnit.SECONDS);
        // JDK -> us: three fragments, one split inside a character
        byte[] euro = utf8("€");
        jdk.sendText("A€", false).get(5, TimeUnit.SECONDS);
        jdk.sendText("B", false).get(5, TimeUnit.SECONDS);
        jdk.sendText("C🎉", true).get(5, TimeUnit.SECONDS);
        rec.messageEnded.get(10, TimeUnit.SECONDS);
        assertEquals("A€BC🎉", rec.texts());
        // us -> JDK: parts become fragments that the JDK reassembles
        WebSocket ours = rec.opened.get();
        ours.sendTextPart("héllo ", false).get(5, TimeUnit.SECONDS);
        ours.sendTextPart("🎉 ", false).get(5, TimeUnit.SECONDS);
        ours.sendTextPart("wörld", true).get(5, TimeUnit.SECONDS);
        gotAll.get(10, TimeUnit.SECONDS);
        assertEquals(List.of("héllo 🎉 wörld"), received);
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
