package io.hydrangea.amqp;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** The AMQP primitive types, without a broker. */
class WireTest {

    private static Wire.Reader reader(Wire.Writer w) {
        return new Wire.Reader(ByteBuffer.wrap(w.toByteArray()));
    }

    @Test
    void primitivesRoundTrip() {
        Wire.Writer w = new Wire.Writer(8).octet(255).shortInt(65535).longInt(0xFFFFFFFFL).longLong(Long.MIN_VALUE).shortStr("héllo").longStr("long");
        Wire.Reader r = reader(w);
        assertEquals(255, r.octet());
        assertEquals(65535, r.shortInt());
        assertEquals(0xFFFFFFFFL, r.longInt());
        assertEquals(Long.MIN_VALUE, r.longLong());
        assertEquals("héllo", r.shortStr());
        assertEquals("long", r.longStr());
        assertEquals(0, r.remaining());
    }

    @Test
    void bitsArePackedFromBitZero() {
        Wire.Writer w = new Wire.Writer(4).bits(true, false, true, true);
        assertArrayEquals(new byte[] {0b1101}, w.toByteArray());
        boolean[] b = reader(w).bits(4);
        assertTrue(b[0]);
        assertFalse(b[1]);
        assertTrue(b[2]);
        assertTrue(b[3]);
    }

    @Test
    void tableWithEveryType() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("x", 1);
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("bool", true);
        t.put("byte", (byte) -5);
        t.put("short", (short) -300);
        t.put("int", 123456);
        t.put("long", 1L << 40);
        t.put("float", 1.5f);
        t.put("double", -2.25);
        t.put("string", "text");
        t.put("bytes", new byte[] {1, 2, 3});
        t.put("time", Instant.ofEpochSecond(1_700_000_000L));
        t.put("decimal", new BigDecimal("12.34"));
        t.put("nested", inner);
        t.put("list", List.of(1, "a", true));
        t.put("void", null);
        Map<String, Object> back = reader(new Wire.Writer(8).table(t)).table();
        assertEquals(t.keySet(), back.keySet());
        assertEquals(true, back.get("bool"));
        assertEquals((byte) -5, back.get("byte"));
        assertEquals((short) -300, back.get("short"));
        assertEquals(123456, back.get("int"));
        assertEquals(1L << 40, back.get("long"));
        assertEquals(1.5f, back.get("float"));
        assertEquals(-2.25, back.get("double"));
        assertEquals("text", back.get("string"));
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) back.get("bytes"));
        assertEquals(Instant.ofEpochSecond(1_700_000_000L), back.get("time"));
        assertEquals(new BigDecimal("12.34"), back.get("decimal"));
        assertEquals(inner, back.get("nested"));
        assertEquals(List.of(1, "a", true), back.get("list"));
        assertTrue(back.containsKey("void"));
        assertNull(back.get("void"));
    }

    @Test
    void shortStringTooLongIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Wire.Writer(8).shortStr("x".repeat(256)));
    }

    @Test
    void truncatedInputIsAProtocolErrorNotAnIndexError() {
        Wire.Writer w = new Wire.Writer(8).table(Map.of("key", "a longer value"));
        byte[] full = w.toByteArray();
        for (int cut = 0; cut < full.length; cut++) {
            byte[] part = java.util.Arrays.copyOf(full, cut);
            assertThrows(AmqpException.class, () -> new Wire.Reader(ByteBuffer.wrap(part)).table(), "cut at " + cut);
        }
    }

    @Test
    void lyingLengthsAreRejected() {
        // a table claiming 2 GB inside a 10-byte frame
        byte[] evil = {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1, 'a', 'S', 0, 0, 0};
        assertThrows(AmqpException.class, () -> new Wire.Reader(ByteBuffer.wrap(evil)).table());
        byte[] evilString = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        assertThrows(AmqpException.class, () -> new Wire.Reader(ByteBuffer.wrap(evilString)).longStr());
    }

    @Test
    void deeplyNestedTableIsRejected() {
        Map<String, Object> m = new LinkedHashMap<>();
        Map<String, Object> cur = m;
        for (int i = 0; i < 40; i++) {
            Map<String, Object> next = new LinkedHashMap<>();
            cur.put("n", next);
            cur = next;
        }
        byte[] bytes = new Wire.Writer(8).table(m).toByteArray();
        assertThrows(AmqpException.class, () -> new Wire.Reader(ByteBuffer.wrap(bytes)).table());
    }

    @Test
    void randomGarbageOnlyEverThrowsAmqpException() {
        Random rnd = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            byte[] junk = new byte[rnd.nextInt(64)];
            rnd.nextBytes(junk);
            try {
                new Wire.Reader(ByteBuffer.wrap(junk)).table();
            } catch (AmqpException expected) {
                // fine
            }
        }
    }
}
