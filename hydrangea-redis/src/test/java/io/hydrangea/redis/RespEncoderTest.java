package io.hydrangea.redis;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RespEncoderTest {

    private static String wire(Object... args) {
        return new String(Resp.encode(args), StandardCharsets.ISO_8859_1);
    }

    @Test
    void aCommandIsAnArrayOfBulkStrings() {
        assertEquals("*1\r\n$4\r\nPING\r\n", wire("PING"));
        assertEquals("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", wire("SET", "k", "v"));
        assertEquals("*2\r\n$3\r\nGET\r\n$0\r\n\r\n", wire("GET", ""));
    }

    @Test
    void argumentsOfEveryAcceptedType() {
        assertEquals("*5\r\n$1\r\nx\r\n$2\r\n42\r\n$3\r\n-17\r\n$19\r\n9223372036854775807\r\n$3\r\n1.5\r\n", wire("x", 42, -17L, Long.MAX_VALUE, 1.5));
        assertEquals("*2\r\n$1\r\nx\r\n$1\r\n1\r\n", wire("x", true));
        assertEquals("*2\r\n$1\r\nx\r\n$3\r\n1.5\r\n", wire("x", 1.5f));
        assertEquals("*2\r\n$1\r\nx\r\n$3\r\ninf\r\n", wire("x", Double.POSITIVE_INFINITY));
        assertEquals("*2\r\n$1\r\nx\r\n$4\r\n-inf\r\n", wire("x", Double.NEGATIVE_INFINITY));
        assertEquals("*2\r\n$1\r\nx\r\n$4\r\n1000\r\n", wire("x", new BigDecimal("1E+3")));
        assertEquals("*2\r\n$1\r\nx\r\n$3\r\nNEW\r\n", wire("x", Thread.State.NEW));
        assertEquals("*2\r\n$1\r\nx\r\n$3\r\nabc\r\n", wire("x", new StringBuilder("abc")));
        assertEquals("*2\r\n$1\r\nx\r\n$2\r\n12\r\n", wire("x", (short) 12));
    }

    @Test
    void textIsUtf8AndBytesAreSentAsTheyAre() {
        byte[] encoded = Resp.encode(new Object[] {"SET", "k", "héllo ✓"});
        assertEquals("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$10\r\nh\u00c3\u00a9llo \u00e2\u009c\u0093\r\n", new String(encoded, StandardCharsets.ISO_8859_1));
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) all[i] = (byte) i;
        byte[] e = Resp.encode(new Object[] {"SET", "k", all});
        int tail = e.length - 2;
        for (int i = 0; i < 256; i++) {
            assertEquals((byte) i, e[tail - 256 + i]);
        }
    }

    @Test
    void lengthsWithManyDigits() {
        byte[] big = new byte[123_456];
        String w = new String(Resp.encode(new Object[] {"SET", "k", big}), StandardCharsets.ISO_8859_1);
        assertTrue(w.contains("$123456\r\n"));
        Object[] many = new Object[1234];
        java.util.Arrays.fill(many, "a");
        many[0] = "MSET";
        assertTrue(wire(many).startsWith("*1234\r\n$4\r\nMSET\r\n$1\r\na\r\n"));
    }

    @Test
    void whatCannotBeSentIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> Resp.encode(new Object[0]));
        assertThrows(IllegalArgumentException.class, () -> Resp.encode(new Object[] {"SET", null}));
        assertThrows(IllegalArgumentException.class, () -> Resp.encode(new Object[] {"SET", "k", new Object()}));
        assertThrows(IllegalArgumentException.class, () -> Resp.encode(new Object[] {"SET", "k", Double.NaN}));
        assertThrows(IllegalArgumentException.class, () -> Resp.encode(new Object[] {"SET", "k", java.util.List.of(1)}));
    }
}
