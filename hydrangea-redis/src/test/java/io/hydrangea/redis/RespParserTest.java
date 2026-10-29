package io.hydrangea.redis;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class RespParserTest {

    private static final int BIG = 512 * 1024 * 1024;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.ISO_8859_1);
    }

    /** All the replies in the bytes, fed in one piece. */
    private static List<Object> parseAll(String wire) {
        return parseAll(b(wire));
    }

    private static List<Object> parseAll(byte[] wire) {
        RespParser p = new RespParser(BIG, 1_000_000);
        ByteBuffer in = ByteBuffer.wrap(wire);
        List<Object> out = new ArrayList<>();
        for (Object v; (v = p.next(in)) != RespParser.INCOMPLETE; ) {
            out.add(v);
        }
        assertEquals(0, in.remaining(), "everything was consumed");
        return out;
    }

    /** The same bytes, delivered in pieces of the given size (through a buffer that keeps the unread tail, as the client does). */
    private static List<Object> parseInPieces(byte[] wire, int piece) {
        RespParser p = new RespParser(BIG, 1_000_000);
        List<Object> out = new ArrayList<>();
        ByteBuffer tail = ByteBuffer.allocate(wire.length + 16).limit(0);
        for (int i = 0; i < wire.length; i += piece) {
            ByteBuffer more = ByteBuffer.allocate(tail.remaining() + Math.min(piece, wire.length - i));
            more.put(tail).put(wire, i, Math.min(piece, wire.length - i)).flip();
            for (Object v; (v = p.next(more)) != RespParser.INCOMPLETE; ) {
                out.add(v);
            }
            tail = more;
        }
        assertEquals(0, tail.remaining());
        return out;
    }

    private static Object one(String wire) {
        List<Object> l = parseAll(wire);
        assertEquals(1, l.size());
        return l.get(0);
    }

    @Test
    void theSimpleTypes() {
        assertEquals("OK", one("+OK\r\n"));
        assertEquals("PONG", one("+PONG\r\n"));
        assertEquals("héllo", one("+h\u00c3\u00a9llo\r\n"));
        assertEquals(new RedisError("ERR unknown command"), one("-ERR unknown command\r\n"));
        assertEquals("ERR", new RedisError("ERR unknown command").code());
        assertEquals(0L, one(":0\r\n"));
        assertEquals(-42L, one(":-42\r\n"));
        assertEquals(Long.MAX_VALUE, one(":9223372036854775807\r\n"));
        assertEquals(Long.MIN_VALUE, one(":-9223372036854775808\r\n"));
        assertNull(one("$-1\r\n"));
        assertNull(one("*-1\r\n"));
        assertNull(one("_\r\n"));
        assertEquals(Boolean.TRUE, one("#t\r\n"));
        assertEquals(Boolean.FALSE, one("#f\r\n"));
        assertEquals(1.5, one(",1.5\r\n"));
        assertEquals(Double.POSITIVE_INFINITY, one(",inf\r\n"));
        assertEquals(Double.NEGATIVE_INFINITY, one(",-inf\r\n"));
        assertTrue(Double.isNaN((Double) one(",nan\r\n")));
        assertEquals(new BigInteger("3492890328409238509324850943850943825024385"), one("(3492890328409238509324850943850943825024385\r\n"));
    }

    @Test
    void bulkStringsAreBytes() {
        assertArrayEquals(b("hello"), (byte[]) one("$5\r\nhello\r\n"));
        assertArrayEquals(new byte[0], (byte[]) one("$0\r\n\r\n"));
        assertArrayEquals(b("a\r\nb"), (byte[]) one("$4\r\na\r\nb\r\n"), "a CRLF inside is data");
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) {
            all[i] = (byte) i;
        }
        byte[] wire = new byte[all.length + 12];
        byte[] head = b("$256\r\n");
        System.arraycopy(head, 0, wire, 0, head.length);
        System.arraycopy(all, 0, wire, head.length, 256);
        wire[head.length + 256] = '\r';
        wire[head.length + 257] = '\n';
        List<Object> l = parseAll(java.util.Arrays.copyOf(wire, head.length + 258));
        assertArrayEquals(all, (byte[]) l.get(0));
    }

    @Test
    void aggregates() {
        assertEquals(List.of(), one("*0\r\n"));
        @SuppressWarnings("unchecked")
        List<Object> l = (List<Object>) one("*3\r\n:1\r\n$3\r\nabc\r\n+OK\r\n");
        assertEquals(1L, l.get(0));
        assertArrayEquals(b("abc"), (byte[]) l.get(1));
        assertEquals("OK", l.get(2));
        @SuppressWarnings("unchecked")
        List<Object> nested = (List<Object>) one("*2\r\n*2\r\n:1\r\n:2\r\n*0\r\n");
        assertEquals(List.of(List.of(1L, 2L), List.of()), nested);
        @SuppressWarnings("unchecked")
        List<Object> withNulls = (List<Object>) one("*3\r\n$-1\r\n_\r\n*-1\r\n");
        assertEquals(java.util.Arrays.asList(null, null, null), withNulls);
        @SuppressWarnings("unchecked")
        List<Object> withError = (List<Object>) one("*2\r\n+OK\r\n-WRONGTYPE bad\r\n");
        assertEquals(new RedisError("WRONGTYPE bad"), withError.get(1));
    }

    @Test
    void resp3MapsSetsAndPushes() {
        @SuppressWarnings("unchecked")
        Map<Object, Object> m = (Map<Object, Object>) one("%2\r\n$1\r\na\r\n:1\r\n+b\r\n*1\r\n$1\r\nx\r\n");
        assertEquals(List.of("a", "b"), List.copyOf(m.keySet()), "a bulk string key becomes a String, so that it can be a key");
        assertEquals(1L, m.get("a"));
        assertEquals(1, ((List<?>) m.get("b")).size());
        assertEquals(Map.of(), one("%0\r\n"));
        assertEquals(2, ((List<?>) one("~2\r\n:1\r\n:2\r\n")).size());
        RespParser.Push push = (RespParser.Push) one(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n");
        assertEquals(3, push.items().size());
        assertArrayEquals(b("message"), (byte[]) push.items().get(0));
        assertEquals(List.of(), ((RespParser.Push) one(">0\r\n")).items());
        assertEquals(2L, ((Map<?, ?>) one("%1\r\n:1\r\n:2\r\n")).get(1L), "other keys stay as they are");
    }

    @Test
    void anAttributeIsDroppedAndTheReplyItDescribesIsDelivered() {
        assertEquals(42L, one("|1\r\n+ttl\r\n:3600\r\n:42\r\n"));
        assertEquals(42L, one("|0\r\n:42\r\n"));
        @SuppressWarnings("unchecked")
        List<Object> l = (List<Object>) one("*2\r\n|1\r\n+k\r\n+v\r\n:1\r\n:2\r\n");
        assertEquals(List.of(1L, 2L), l);
    }

    @Test
    void errorsAndVerbatimStringsInBulkForm() {
        assertEquals(new RedisError("SYNTAX invalid syntax"), one("!21\r\nSYNTAX invalid syntax\r\n"));
        assertArrayEquals(b("Some string"), (byte[]) one("=15\r\ntxt:Some string\r\n"), "the format prefix is removed");
    }

    @Test
    void severalRepliesInARow() {
        List<Object> l = parseAll("+OK\r\n:1\r\n$2\r\nhi\r\n*1\r\n:9\r\n-ERR x\r\n");
        assertEquals(5, l.size());
        assertEquals("OK", l.get(0));
        assertEquals(1L, l.get(1));
    }

    @Test
    void theSameRepliesWhateverThePiecesTheyArriveIn() {
        StringBuilder wire = new StringBuilder("+OK\r\n:12345\r\n$5\r\nhello\r\n*3\r\n$3\r\nabc\r\n*2\r\n:1\r\n_\r\n%1\r\n+k\r\n,2.5\r\n>2\r\n$4\r\nPING\r\n:1\r\n-ERR e\r\n$-1\r\n#t\r\n");
        byte[] bytes = b(wire.toString());
        List<Object> whole = parseAll(bytes);
        for (int piece : new int[] {1, 2, 3, 5, 7, 16, 64}) {
            List<Object> parts = parseInPieces(bytes, piece);
            assertEquals(whole.size(), parts.size(), "pieces of " + piece);
            for (int i = 0; i < whole.size(); i++) {
                assertEquals(show(whole.get(i)), show(parts.get(i)), "reply " + i + " in pieces of " + piece);
            }
        }
        Random rnd = new Random(7);
        for (int round = 0; round < 200; round++) {
            RespParser p = new RespParser(BIG, 1000);
            List<Object> out = new ArrayList<>();
            ByteBuffer tail = ByteBuffer.allocate(0);
            int i = 0;
            while (i < bytes.length) {
                int n = 1 + rnd.nextInt(30);
                n = Math.min(n, bytes.length - i);
                ByteBuffer more = ByteBuffer.allocate(tail.remaining() + n);
                more.put(tail).put(bytes, i, n).flip();
                i += n;
                for (Object v; (v = p.next(more)) != RespParser.INCOMPLETE; ) {
                    out.add(v);
                }
                tail = more;
            }
            assertEquals(whole.size(), out.size());
        }
    }

    private static String show(Object v) {
        if (v instanceof byte[] bytes) return "bytes:" + new String(bytes, StandardCharsets.ISO_8859_1);
        if (v instanceof List<?> l) return l.stream().map(RespParserTest::show).toList().toString();
        if (v instanceof Map<?, ?> m) return m.toString();
        if (v instanceof RespParser.Push push) return "push" + show(push.items());
        return String.valueOf(v);
    }

    @Test
    void aLargeBulkIsFilledInPlaceAsItArrives() {
        byte[] data = new byte[300_000];
        new Random(3).nextBytes(data);
        byte[] header = b("$" + data.length + "\r\n");
        byte[] wire = new byte[header.length + data.length + 2];
        System.arraycopy(header, 0, wire, 0, header.length);
        System.arraycopy(data, 0, wire, header.length, data.length);
        wire[wire.length - 2] = '\r';
        wire[wire.length - 1] = '\n';
        for (int piece : new int[] {1000, 16384, 65536, 299_999, 400_000}) {
            RespParser p = new RespParser(BIG, 10);
            Object got = null;
            for (int i = 0; i < wire.length; i += piece) {
                ByteBuffer in = ByteBuffer.wrap(wire, i, Math.min(piece, wire.length - i));
                Object v = p.next(in);
                if (v != RespParser.INCOMPLETE) {
                    got = v;
                    assertEquals(0, in.remaining());
                } else {
                    assertEquals(0, in.remaining() > 0 && data.length >= 65536 && i > header.length ? in.remaining() : 0, "a large value is consumed as it comes");
                }
            }
            assertArrayEquals(data, (byte[]) got, "pieces of " + piece);
        }
        // the CRLF after a large value may arrive separately: what is not consumed stays in the caller's buffer
        RespParser p = new RespParser(BIG, 10);
        ByteBuffer first = ByteBuffer.wrap(wire, 0, wire.length - 1);
        assertSame(RespParser.INCOMPLETE, p.next(first));
        assertEquals(1, first.remaining(), "the CR waits for its LF");
        ByteBuffer rest = ByteBuffer.allocate(2).put(wire, wire.length - 2, 2).flip();
        assertArrayEquals(data, (byte[]) p.next(rest));
    }

    @Test
    void aBigArrayThatArrivesInManyReadsIsParsedOnce() {
        StringBuilder sb = new StringBuilder("*50000\r\n");
        for (int i = 0; i < 50_000; i++) {
            sb.append("$").append(Integer.toString(i).length()).append("\r\n").append(i).append("\r\n");
        }
        byte[] wire = b(sb.toString());
        long t0 = System.nanoTime();
        List<Object> got = parseInPieces(wire, 1000);
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(1, got.size());
        assertEquals(50_000, ((List<?>) got.get(0)).size());
        assertTrue(ms < 5000, "50 000 elements in 1 KB reads took " + ms + " ms: the parser must not start over at every read");
    }

    private static void assertProtocolError(String wire) {
        RedisException e = assertThrows(RedisException.class, () -> {
            RespParser p = new RespParser(1 << 20, 1000);
            ByteBuffer in = ByteBuffer.wrap(b(wire));
            while (p.next(in) != RespParser.INCOMPLETE) {
                // read on
            }
        }, wire.replace("\r", "\\r").replace("\n", "\\n"));
        assertFalse(e.retriable());
        assertTrue(e.getMessage().startsWith("protocol error"), e.getMessage());
    }

    @Test
    void hostileInputIsAProtocolErrorNotACrashOrAHang() {
        assertProtocolError("?x\r\n");
        assertProtocolError("\r\n");
        assertProtocolError(":abc\r\n");
        assertProtocolError(":\r\n");
        assertProtocolError(":9223372036854775808\r\n");
        assertProtocolError(":-9223372036854775809\r\n");
        assertProtocolError(":123456789012345678901234567890\r\n");
        assertProtocolError("$-2\r\n");
        assertProtocolError("$2097153\r\nx\r\n");           // over the limit of 1 MiB set above
        assertProtocolError("$1\r\nab\r\n");                // not followed by CRLF
        assertProtocolError("$2\r\nab\r\r");
        assertProtocolError("*-5\r\n");
        assertProtocolError("*1001\r\n");                   // over maxElements
        assertProtocolError("%501\r\n");                    // 1002 elements
        assertProtocolError("+OK\rX\n");
        assertProtocolError("#x\r\n");
        assertProtocolError("#tt\r\n");
        assertProtocolError("_x\r\n");
        assertProtocolError(",abc\r\n");
        assertProtocolError("(12x\r\n");
        assertProtocolError("*1\r\n".repeat(40) + ":1\r\n");   // nested 40 deep
    }

    @Test
    void aHeaderThatNeverEndsIsRefusedBeforeItFillsTheMemory() {
        RespParser p = new RespParser(1 << 20, 10);
        ByteBuffer in = ByteBuffer.wrap(b("+" + "x".repeat(5000)));
        assertThrows(RedisException.class, () -> p.next(in));
    }

    @Test
    void aBulkLengthIsNotTrustedForAllocationOfSmallValues() {
        RespParser p = new RespParser(BIG, 10);
        ByteBuffer in = ByteBuffer.wrap(b("$100000000\r\nonly a little"));
        assertSame(RespParser.INCOMPLETE, p.next(in), "waits (a large bulk is allocated as claimed, bounded by maxBulk: the limit is the protection)");
        RespParser small = new RespParser(1000, 10);
        assertThrows(RedisException.class, () -> small.next(ByteBuffer.wrap(b("$100000000\r\nx"))));
    }
}
