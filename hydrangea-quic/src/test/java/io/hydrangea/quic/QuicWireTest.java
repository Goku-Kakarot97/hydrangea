package io.hydrangea.quic;

import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class QuicWireTest {

    private static byte[] hex(String s) {
        return HexFormat.of().parseHex(s);
    }

    @Test
    void variableLengthIntegersMatchTheRfcExamples() {
        // RFC 9000 appendix A.1
        assertEquals(151_288_809_941_952_652L, new QuicWire.Reader(hex("c2197c5eff14e88c"), 0, 8).varint());
        assertEquals(494_878_333L, new QuicWire.Reader(hex("9d7f3e7d"), 0, 4).varint());
        assertEquals(15_293L, new QuicWire.Reader(hex("7bbd"), 0, 2).varint());
        assertEquals(37L, new QuicWire.Reader(hex("25"), 0, 1).varint());
        assertEquals(37L, new QuicWire.Reader(hex("4025"), 0, 2).varint(), "a longer encoding of the same value is legal");
        assertArrayEquals(hex("c2197c5eff14e88c"), new QuicWire.Writer(8).varint(151_288_809_941_952_652L).toByteArray());
        assertArrayEquals(hex("7bbd"), new QuicWire.Writer(8).varint(15_293L).toByteArray());
    }

    @Test
    void varintsRoundTripAtTheBoundaries() {
        long[] values = {0, 1, 63, 64, 16_383, 16_384, 1_073_741_823L, 1_073_741_824L, (1L << 62) - 1};
        for (long v : values) {
            QuicWire.Writer w = new QuicWire.Writer(8).varint(v);
            assertEquals(QuicWire.varintLength(v), w.n, "length of " + v);
            assertEquals(v, new QuicWire.Reader(w.b, 0, w.n).varint());
        }
        assertThrows(IllegalArgumentException.class, () -> new QuicWire.Writer(8).varint(1L << 62));
        assertThrows(IllegalArgumentException.class, () -> new QuicWire.Writer(8).varint(-1));
    }

    @Test
    void readersRefuseTruncatedInput() {
        assertThrows(QuicWire.Malformed.class, () -> new QuicWire.Reader(hex("40"), 0, 1).varint());
        assertThrows(QuicWire.Malformed.class, () -> new QuicWire.Reader(hex("0102"), 0, 2).bytes(3));
        assertThrows(QuicWire.Malformed.class, () -> new QuicWire.Reader(new byte[0], 0, 0).u8());
        assertThrows(QuicWire.Malformed.class, () -> new QuicWire.Reader(hex("00"), 0, 1).u16());
    }

    @Test
    void packetNumbersAreRecoveredFromTheirTruncatedForm() {
        // RFC 9000 appendix A.3
        assertEquals(0xa82f9b32L, QuicConnection.decodePacketNumber(0xa82f30eaL, 0x9b32, 16));
        assertEquals(5, QuicConnection.decodePacketNumber(-1, 5, 32));
        assertEquals(0x100, QuicConnection.decodePacketNumber(0xff, 0x00, 8), "wraps up");
        assertEquals(0xfe, QuicConnection.decodePacketNumber(0x100, 0xfe, 8), "an older packet after the wrap");
    }

    @Test
    void rangesMergeAndAnswerQuestionsAboutCoverage() {
        Ranges r = new Ranges();
        r.add(10, 20);
        r.add(30, 40);
        r.add(20, 30);                 // joins the two
        assertEquals(1, r.size());
        assertTrue(r.covers(10, 40));
        assertFalse(r.covers(9, 40));
        assertEquals(40, r.contiguousEnd(10));
        assertEquals(5, r.contiguousEnd(5), "not covered: itself");
        assertArrayEquals(new long[]{0, 10}, r.firstGap(0, 50));
        assertArrayEquals(new long[]{40, 50}, r.firstGap(15, 50));
        assertNull(r.firstGap(12, 38));
        r.add(100, 110);
        assertEquals(2, r.size());
        assertArrayEquals(new long[]{40, 100}, r.firstGap(40, 120));
        assertEquals(110, r.max());
        r.trimBelow(40);
        assertFalse(r.contains(20));
        assertTrue(r.contains(105));
    }

    @Test
    void rangesBehaveLikeASetOfIntegersUnderRandomUse() {
        Random rnd = new Random(3);
        boolean[] model = new boolean[500];
        Ranges r = new Ranges();
        for (int i = 0; i < 2000; i++) {
            int a = rnd.nextInt(480), len = 1 + rnd.nextInt(20);
            r.add(a, a + len);
            for (int k = a; k < a + len; k++) model[k] = true;
            int q = rnd.nextInt(500);
            assertEquals(model[q], r.contains(q), "contains " + q);
            int from = rnd.nextInt(480), to = from + rnd.nextInt(20);
            boolean all = true;
            for (int k = from; k < to; k++) all &= model[k];
            assertEquals(all, r.covers(from, to), "covers " + from + ".." + to);
        }
    }
}
