package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import io.hydrangea.grpc.proto.ProtoReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

class GrpcBinaryLogTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    /** One decoded GrpcLogEntry. */
    private record Entry(int logger, long callId, long seq, int type, byte[] payload, boolean truncated, byte[] peer) {
        static Entry parse(byte[] b) {
            ProtoReader r = new ProtoReader(b);
            long call = 0, seq = 0;
            int type = 0, logger = 0;
            byte[] payload = null, peer = null;
            boolean truncated = false;
            while (r.next()) {
                switch (r.field()) {
                    case 2 -> call = r.varint();
                    case 3 -> seq = r.varint();
                    case 4 -> type = (int) r.varint();
                    case 5 -> logger = (int) r.varint();
                    case 6, 7, 8, 9 -> payload = r.bytes();
                    case 10 -> truncated = r.bool();
                    case 11 -> peer = r.bytes();
                    default -> r.skip();
                }
            }
            return new Entry(logger, call, seq, type, payload, truncated, peer);
        }
    }

    private static final int CLIENT_HEADER = 1, SERVER_HEADER = 2, CLIENT_MESSAGE = 3, SERVER_MESSAGE = 4, HALF_CLOSE = 5, SERVER_TRAILER = 6, CANCEL = 7;
    private static final int CLIENT = 1, SERVER = 2;

    private static List<Entry> decode(List<byte[]> raw) {
        List<Entry> out = new ArrayList<>();
        for (byte[] r : raw) {
            int i = 0;
            while ((r[i] & 0x80) != 0) i++;   // skip the varint length prefix
            out.add(Entry.parse(java.util.Arrays.copyOfRange(r, i + 1, r.length)));
        }
        return out;
    }

    private BinaryLog log(List<byte[]> into, String config) {
        return BinaryLog.to(into::add, config);
    }

    private static List<Entry> waitFor(List<byte[]> raw, int count) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (raw.size() < count && System.nanoTime() < end) Thread.sleep(10);
        Thread.sleep(50);
        return decode(new ArrayList<>(raw));
    }

    private Server serve(BinaryLog log) throws Exception {
        return GrpcEchoExample.builder().binaryLog(log).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    private static String url(Server s) {
        return "http://127.0.0.1:" + ((InetSocketAddress) s.localAddress()).getPort();
    }

    @Test
    void aUnaryCallProducesTheStandardEventSequenceOnBothSides() throws Exception {
        List<byte[]> raw = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(raw, "*"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).binaryLog(bl).build()) {
            ch.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            List<Entry> all = waitFor(raw, 12);
            for (int logger : new int[]{CLIENT, SERVER}) {
                List<Entry> es = all.stream().filter(e -> e.logger() == logger).toList();
                assertEquals(1, es.stream().map(Entry::callId).distinct().count(), "one call per side");
                List<Integer> types = es.stream().map(Entry::type).toList();
                // client: header, message, half close, then what came back; server: the same from its point of view
                assertEquals(CLIENT_HEADER, types.get(0));
                assertTrue(types.indexOf(CLIENT_MESSAGE) > 0 && types.indexOf(HALF_CLOSE) > types.indexOf(CLIENT_MESSAGE), types.toString());
                assertTrue(types.indexOf(SERVER_HEADER) >= 0 && types.indexOf(SERVER_MESSAGE) > types.indexOf(SERVER_HEADER), types.toString());
                assertEquals(SERVER_TRAILER, types.get(types.size() - 1));
                for (int i = 0; i < es.size(); i++) assertEquals(i + 1, es.get(i).seq(), "sequence ids count up from 1");
            }
            assertEquals(2, all.stream().map(Entry::callId).distinct().count(), "the two sides have different call ids");
        }
    }

    @Test
    void messageAndHeaderLimitsTruncateAndFlagTheEntry() throws Exception {
        List<byte[]> raw = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(raw, "*{h:5;m:10}"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            ch.unary(UNARY, new Msg("a message longer than ten bytes"), CallOptions.DEFAULT.withMetadata(new Metadata().put("x-long-header", "abcdefghijklmnop"))).get(10, TimeUnit.SECONDS);
            List<Entry> es = waitFor(raw, 6);
            Entry msg = es.stream().filter(e -> e.type() == CLIENT_MESSAGE).findFirst().orElseThrow();
            assertTrue(msg.truncated());
            ProtoReader r = new ProtoReader(msg.payload());
            long length = 0;
            int kept = -1;
            while (r.next()) {
                if (r.field() == 1) length = r.varint(); else if (r.field() == 2) kept = r.bytes().length; else r.skip();
            }
            assertEquals(10, kept);
            assertTrue(length > 10, "the real length is still reported: " + length);
            Entry hdr = es.stream().filter(e -> e.type() == CLIENT_HEADER).findFirst().orElseThrow();
            assertTrue(hdr.truncated(), "the long header did not fit in 5 bytes");
        }
    }

    @Test
    void headersOnlyAndMessagesOnlyModes() throws Exception {
        List<byte[]> headersOnly = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(headersOnly, "*{h}"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            ch.unary(UNARY, new Msg("secret"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            for (Entry e : waitFor(headersOnly, 6)) {
                if (e.type() == CLIENT_MESSAGE || e.type() == SERVER_MESSAGE) {
                    assertNull(e.payload(), "the event is recorded, its content is not");
                }
            }
        }
        List<byte[]> messagesOnly = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(messagesOnly, "*{m}"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMetadata(new Metadata().put("x-hidden", "yes"))).get(10, TimeUnit.SECONDS);
            Entry hdr = waitFor(messagesOnly, 6).stream().filter(e -> e.type() == CLIENT_HEADER).findFirst().orElseThrow();
            assertFalse(new String(hdr.payload()).contains("x-hidden"), "metadata is not logged");
        }
    }

    @Test
    void rulesChooseWhichMethodsAreLoggedMostSpecificFirst() throws Exception {
        List<byte[]> raw = new CopyOnWriteArrayList<>();
        // everything except Unary, but ServerStream only with headers
        try (BinaryLog bl = log(raw, "*, -hydrangea.test.Echo/Unary, hydrangea.test.Echo/ServerStream{h}"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            Thread.sleep(200);
            assertEquals(0, raw.size(), "Unary is excluded");
            CompletableFuture<Void> done = new CompletableFuture<>();
            ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("r", 2), CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            done.get(10, TimeUnit.SECONDS);
            List<Entry> es = waitFor(raw, 7);
            assertFalse(es.isEmpty());
            for (Entry e : es) {
                if (e.type() == CLIENT_MESSAGE || e.type() == SERVER_MESSAGE) assertNull(e.payload());
            }
        }
        List<byte[]> none = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(none, "some.other.Service/*"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            Thread.sleep(200);
            assertEquals(0, none.size(), "a method no rule matches is not logged");
        }
    }

    @Test
    void aCancelledCallEndsWithCancelOnTheClientAndTheServer() throws Exception {
        List<byte[]> raw = new CopyOnWriteArrayList<>();
        try (BinaryLog bl = log(raw, "*"); Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).binaryLog(bl).build()) {
            ClientCall<Msg, Msg> call = ch.newCall(UNARY, CallOptions.DEFAULT, new ClientCall.Listener<>() { });
            call.send(new Msg("SLOW:5000"));
            call.halfClose();
            Thread.sleep(300);
            call.cancel("enough");
            List<Entry> es = waitFor(raw, 8);
            for (int logger : new int[]{CLIENT, SERVER}) {
                List<Entry> side = es.stream().filter(e -> e.logger() == logger).toList();
                assertEquals(CANCEL, side.get(side.size() - 1).type(), "logger " + logger + ": " + side.stream().map(Entry::type).toList());
            }
        }
    }

    @Test
    void writesALengthDelimitedFileAndCountsNothingDropped(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("calls.binlog");
        BinaryLog bl = BinaryLog.toFile(file, "*");
        try (Server s = serve(bl); GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            for (int i = 0; i < 20; i++) ch.unary(UNARY, new Msg("n" + i), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
        }
        Thread.sleep(300);
        bl.close();
        byte[] all = Files.readAllBytes(file);
        int pos = 0, entries = 0;
        while (pos < all.length) {
            int len = 0, shift = 0;
            while ((all[pos] & 0x80) != 0) { len |= (all[pos++] & 0x7f) << shift; shift += 7; }
            len |= all[pos++] << shift;
            pos += len;
            entries++;
        }
        assertEquals(all.length, pos, "the file is exactly a sequence of delimited entries");
        assertEquals(20 * 6, entries, "per call: client header, client message, half close, server header, server message, trailer");
        assertEquals(0, bl.dropped());
    }

    @Test
    void badConfigurationsAreRejected() {
        for (String bad : new String[]{"foo", "svc/", "/m", "*{x}", "*{h:abc}", "*{h", "svc/m*", "*{h:-1}"}) {
            assertThrows(IllegalArgumentException.class, () -> BinaryLog.to(b -> { }, bad), bad);
        }
        assertDoesNotThrow(() -> BinaryLog.to(b -> { }, "*, -a.B/c, a.B/*{h:10;m:20}").close());
    }
}
