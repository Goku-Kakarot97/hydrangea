package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;
import io.hydrangea.http.Headers;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class GrpcCoreTest {

    private static byte[] hex(String s) {
        return HexFormat.of().parseHex(s.replace(" ", ""));
    }

    // ---- framing ----

    private static List<byte[]> decode(byte[] wire, int chunk, int max, boolean gzipOk) throws Exception {
        GrpcFraming.Decoder d = new GrpcFraming.Decoder(max, gzipOk);
        List<byte[]> out = new ArrayList<>();
        for (int i = 0; i < wire.length; i += chunk) {
            d.feed(ByteBuffer.wrap(wire, i, Math.min(chunk, wire.length - i)), out::add);
        }
        d.finish();
        return out;
    }

    @Test
    void messagesSurviveAnyWayTheTransportCutsThem() throws Exception {
        Random r = new Random(1);
        byte[] a = new byte[0], b = new byte[1], c = new byte[3000];
        r.nextBytes(c);
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write(GrpcFraming.frame(a, false));
        wire.write(GrpcFraming.frame(b, false));
        wire.write(GrpcFraming.frame(c, false));
        wire.write(GrpcFraming.frame(c, true));
        for (int chunk : new int[]{1, 2, 4, 5, 6, 7, 100, 4096, wire.size()}) {
            List<byte[]> got = decode(wire.toByteArray(), chunk, 1 << 20, true);
            assertEquals(4, got.size(), "chunk " + chunk);
            assertArrayEquals(a, got.get(0));
            assertArrayEquals(b, got.get(1));
            assertArrayEquals(c, got.get(2));
            assertArrayEquals(c, got.get(3), "the compressed one comes out decompressed");
        }
    }

    @Test
    void theFramingBytesAreTheDocumentedOnes() throws Exception {
        assertArrayEquals(hex("00 00 00 00 03 61 62 63"), GrpcFraming.frame("abc".getBytes(), false));
        byte[] gz = GrpcFraming.frame("abc".getBytes(), true);
        assertEquals(1, gz[0], "compressed flag");
        assertEquals(gz.length - 5, ByteBuffer.wrap(gz, 1, 4).getInt());
    }

    @Test
    void anOversizedMessageIsRefusedFromItsPrefixAlone() {
        GrpcFraming.Decoder d = new GrpcFraming.Decoder(1000, true);
        StatusException e = assertThrows(StatusException.class,
                () -> d.feed(ByteBuffer.wrap(hex("00 7f ff ff ff")), m -> fail("no message yet")));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, e.status().code());
    }

    @Test
    void protocolViolationsAreInternalErrors() throws Exception {
        assertEquals(Status.Code.INTERNAL, assertThrows(StatusException.class, () -> decode(hex("02 00 00 00 00"), 5, 100, true)).status().code(), "flag 2");
        assertEquals(Status.Code.INTERNAL, assertThrows(StatusException.class, () -> decode(GrpcFraming.frame("x".getBytes(), true), 5, 100, false)).status().code(),
                "compressed without a negotiated encoding");
        assertEquals(Status.Code.INTERNAL, assertThrows(StatusException.class, () -> decode(hex("00 00 00 00 05 61"), 5, 100, true)).status().code(), "cut in the payload");
        assertEquals(Status.Code.INTERNAL, assertThrows(StatusException.class, () -> decode(hex("00 00 00"), 5, 100, true)).status().code(), "cut in the prefix");
        byte[] corrupt = GrpcFraming.frame("hello hello hello".getBytes(), true);
        corrupt[corrupt.length - 12] ^= 0x55;
        assertEquals(Status.Code.INTERNAL, assertThrows(StatusException.class, () -> decode(corrupt, 5, 100, true)).status().code(), "corrupt gzip");
    }

    @Test
    void aCompressionBombIsStoppedAtTheMessageLimit() throws Exception {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(raw)) {
            g.write(new byte[50 * 1024 * 1024]);
        }
        ByteBuffer b = ByteBuffer.allocate(5 + raw.size());
        b.put((byte) 1).putInt(raw.size()).put(raw.toByteArray());
        StatusException e = assertThrows(StatusException.class, () -> decode(b.array(), 65536, 1 << 20, true));
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, e.status().code());
    }

    // ---- grpc-timeout / grpc-message ----

    @Test
    void timeoutsRoundTripInTheFinestUnitThatFitsEightDigits() {
        assertEquals("100000u", GrpcFraming.encodeTimeout(Duration.ofMillis(100)), "the finest unit that fits eight digits");
        for (Duration d : new Duration[]{Duration.ofNanos(1), Duration.ofMillis(100), Duration.ofSeconds(30), Duration.ofMinutes(5), Duration.ofHours(2), Duration.ofDays(400)}) {
            String enc = GrpcFraming.encodeTimeout(d);
            assertTrue(enc.matches("[0-9]{1,8}[nuSMHm]"), enc);
            Duration back = GrpcFraming.decodeTimeout(enc);
            assertNotNull(back, enc);
            assertTrue(back.compareTo(d) >= 0, "rounded up, never down: " + d + " -> " + enc);
        }
        assertEquals("99999999n", GrpcFraming.encodeTimeout(Duration.ofNanos(99_999_999)));
        assertTrue(GrpcFraming.encodeTimeout(Duration.ofNanos(100_000_000)).endsWith("u"), "nine digits do not fit: the next unit");
        assertEquals(Duration.ofSeconds(5), GrpcFraming.decodeTimeout("5S"));
        assertEquals(Duration.ofMinutes(3), GrpcFraming.decodeTimeout("3M"));
        assertEquals(Duration.ofMillis(250), GrpcFraming.decodeTimeout("250m"));
        for (String bad : new String[]{null, "", "5", "S", "5x", "-5S", "123456789S", "1.5S", "5 S"}) {
            assertNull(GrpcFraming.decodeTimeout(bad), String.valueOf(bad));
        }
    }

    @Test
    void messagesArePercentEncoded() {
        assertEquals("plain text", GrpcFraming.encodeMessage("plain text"));
        assertEquals("100%25 sure", GrpcFraming.encodeMessage("100% sure"));
        assertEquals("caf%C3%A9 %0A line", GrpcFraming.encodeMessage("café \n line"));
        for (String s : new String[]{"", "a", "100% sure", "café", "日本語 text", "line\nbreak", "%41 literal"}) {
            assertEquals(s, GrpcFraming.decodeMessage(GrpcFraming.encodeMessage(s)), s);
        }
        assertEquals("%zz and %4", GrpcFraming.decodeMessage("%zz and %4"), "a malformed escape stays");
        assertEquals("A", GrpcFraming.decodeMessage("%41"));
    }

    // ---- protobuf wire format (examples from the protobuf encoding guide) ----

    @Test
    void protobufExamplesFromTheEncodingGuide() {
        assertArrayEquals(hex("08 96 01"), new ProtoWriter().varint(1, 150).toByteArray());
        assertArrayEquals(hex("12 07 74 65 73 74 69 6e 67"), new ProtoWriter().string(2, "testing").toByteArray());
        assertArrayEquals(hex("1a 03 08 96 01"), new ProtoWriter().message(3, new ProtoWriter().varint(1, 150).toByteArray()).toByteArray());
        assertArrayEquals(hex("22 06 03 8e 02 9e a7 05"), new ProtoWriter().packedVarints(4, 3, 270, 86942).toByteArray());
        assertArrayEquals(hex("08 ff ff ff ff ff ff ff ff ff 01"), new ProtoWriter().varint(1, -1).toByteArray(), "negative int32/int64 take ten bytes");
        assertArrayEquals(hex("08 01"), new ProtoWriter().sint(1, -1).toByteArray(), "zigzag");
        assertArrayEquals(hex("0d 78 56 34 12"), new ProtoWriter().fixed32(1, 0x12345678).toByteArray());
        assertArrayEquals(hex("09 08 07 06 05 04 03 02 01"), new ProtoWriter().fixed64(1, 0x0102030405060708L).toByteArray());
    }

    @Test
    void protobufReadsWhatItWritesAndSkipsWhatItDoesNotKnow() {
        byte[] msg = new ProtoWriter().string(1, "näme").varint(2, 42).doubleValue(3, 2.5).floatValue(4, 1.5f).bytes(5, new byte[]{1, 2, 3})
                .sint(6, -7).bool(7, true).fixed32(8, -1).fixed64(9, Long.MIN_VALUE).message(10, new ProtoWriter().string(1, "inner").toByteArray())
                .packedVarints(11, 1, 300, 70000).varint(99, 5).string(100, "from the future").toByteArray();
        ProtoReader r = new ProtoReader(msg);
        String name = null, inner = null;
        long count = 0, sint = 0, f64 = 0;
        double d = 0;
        float f = 0;
        byte[] blob = null;
        boolean flag = false;
        int f32 = 0;
        List<Long> packed = null;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> count = r.varint();
                case 3 -> d = r.doubleValue();
                case 4 -> f = r.floatValue();
                case 5 -> blob = r.bytes();
                case 6 -> sint = r.sint();
                case 7 -> flag = r.bool();
                case 8 -> f32 = r.fixed32();
                case 9 -> f64 = r.fixed64();
                case 10 -> {
                    ProtoReader m = r.message();
                    while (m.next()) {
                        if (m.field() == 1) inner = m.string(); else m.skip();
                    }
                }
                case 11 -> packed = r.varints();
                default -> r.skip();
            }
        }
        assertEquals("näme", name);
        assertEquals(42, count);
        assertEquals(2.5, d);
        assertEquals(1.5f, f);
        assertArrayEquals(new byte[]{1, 2, 3}, blob);
        assertEquals(-7, sint);
        assertTrue(flag);
        assertEquals(-1, f32);
        assertEquals(Long.MIN_VALUE, f64);
        assertEquals("inner", inner);
        assertEquals(List.of(1L, 300L, 70000L), packed);
    }

    @Test
    void protobufRejectsMalformedInput() {
        for (String bad : new String[]{"08", "0a 05 01", "0a ff ff ff ff 0f 01", "08 ff ff ff ff ff ff ff ff ff ff 01", "0b", "0c", "0e", "00 00", "0d 01 02"}) {
            ProtoReader r = new ProtoReader(hex(bad));
            assertThrows(IllegalArgumentException.class, () -> {
                while (r.next()) {
                    r.skip();
                }
            }, bad);
        }
        ProtoReader wrongType = new ProtoReader(hex("08 01"));
        assertTrue(wrongType.next());
        assertThrows(IllegalArgumentException.class, wrongType::string);
    }

    // ---- metadata ----

    @Test
    void metadataKeysValuesAndBinary() {
        Metadata m = new Metadata().put("X-Request-Id", "abc").put("x-request-id", "def").putBinary("trace-bin", new byte[]{0, 1, (byte) 0xff});
        assertEquals("abc", m.get("x-request-id"));
        assertEquals(List.of("abc", "def"), m.getAll("X-REQUEST-ID"));
        assertArrayEquals(new byte[]{0, 1, (byte) 0xff}, m.getBinary("trace-bin"));
        assertThrows(IllegalArgumentException.class, () -> m.put("bad key", "v"));
        assertThrows(IllegalArgumentException.class, () -> m.put("blob-bin", "text"));
        assertThrows(IllegalArgumentException.class, () -> m.putBinary("blob", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> m.put("grpc-status", "0"), "reserved");
        assertThrows(IllegalArgumentException.class, () -> m.put("content-type", "x"), "reserved");
        assertThrows(IllegalArgumentException.class, () -> m.put("k", "line\nbreak"));
        assertThrows(IllegalArgumentException.class, () -> m.put("k", "café"));
    }

    @Test
    void metadataToAndFromHeaders() {
        Metadata m = new Metadata().put("k", "v1").put("k", "v2").putBinary("b-bin", "hello".getBytes());
        Headers h = new Headers();
        m.toHeaders(h);
        assertEquals("aGVsbG8", h.get("b-bin"), "base64 without padding");
        h.add(":path", "/x");
        h.add("content-type", "application/grpc");
        h.add("te", "trailers");
        h.add("grpc-timeout", "1S");
        h.add("user-agent", "ua");
        h.add("other-bin", "AAEC,AwQF");     // two values joined by an intermediary
        h.add("broken-bin", "!!!");
        Metadata back = Metadata.fromHeaders(h);
        assertEquals(List.of("v1", "v2"), back.getAll("k"));
        assertArrayEquals("hello".getBytes(), back.getBinary("b-bin"));
        assertEquals(2, back.getAllBinary("other-bin").size());
        assertNull(back.getBinary("broken-bin"));
        for (String reserved : new String[]{":path", "content-type", "te", "grpc-timeout", "user-agent"}) {
            assertFalse(back.containsKey(reserved), reserved);
        }
    }

    // ---- status ----

    @Test
    void statusCodes() {
        assertEquals(17, Status.Code.values().length);
        for (Status.Code c : Status.Code.values()) {
            assertEquals(c, Status.Code.fromValue(c.value()));
        }
        assertEquals(Status.Code.UNKNOWN, Status.Code.fromValue(99));
        assertEquals(Status.Code.UNKNOWN, Status.Code.fromValue(-1));
        assertEquals(Status.Code.NOT_FOUND, Status.fromThrowable(new RuntimeException("wrapper", Status.of(Status.Code.NOT_FOUND).asException())).code());
        assertEquals(Status.Code.UNKNOWN, Status.fromThrowable(new IllegalStateException("secret internals")).code());
        assertTrue(Status.OK.isOk());
        assertFalse(Status.of(Status.Code.INTERNAL).isOk());
    }
}
