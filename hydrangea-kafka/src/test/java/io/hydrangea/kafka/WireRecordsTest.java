package io.hydrangea.kafka;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class WireRecordsTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void varintsRoundTrip() {
        long[] values = {0, 1, -1, 63, 64, -64, -65, 300, -300, Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
        for (long v : values) {
            Wire.Writer w = new Wire.Writer(4);
            w.varlong(v);
            assertEquals(Wire.varlongSize(v), w.len, "size of " + v);
            assertEquals(v, new Wire.Reader(w.toArray()).varlong());
        }
    }

    @Test
    void primitivesAndStrings() {
        Wire.Writer w = new Wire.Writer(4);
        w.i8(-5).i16(-300).i32(0x01020304).i64(-1L).str("héllo").str(null).bytes(new byte[] {1, 2, 3}).bytes(null);
        Wire.Reader r = new Wire.Reader(w.toArray());
        assertEquals(-5, r.i8());
        assertEquals(-300, r.i16());
        assertEquals(0x01020304, r.i32());
        assertEquals(-1L, r.i64());
        assertEquals("héllo", r.str());
        assertNull(r.str());
        assertArrayEquals(new byte[] {1, 2, 3}, r.bytes());
        assertNull(r.bytes());
        assertEquals(0, r.remaining());
    }

    @Test
    void aTruncatedOrLyingMessageIsAProtocolError() {
        assertThrows(KafkaException.class, () -> new Wire.Reader(new byte[] {0, 0, 0}).i32());
        assertThrows(KafkaException.class, () -> new Wire.Reader(new byte[] {0, 10, 'a'}).str());
        assertThrows(KafkaException.class, () -> new Wire.Reader(new byte[] {0, 0, 0, 100, 1}).bytes());
        assertThrows(KafkaException.class, () -> new Wire.Reader(new byte[] {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1}).arrayLength(), "2 billion elements in 5 bytes");
        byte[] endless = new byte[12];
        java.util.Arrays.fill(endless, (byte) 0x80);
        assertThrows(KafkaException.class, () -> new Wire.Reader(endless).varlong());
    }

    private static Records.Out rec(String k, String v, long ts, Header... h) {
        return new Records.Out(k == null ? null : b(k), v == null ? null : b(v), List.of(h), ts);
    }

    @Test
    void aBatchRoundTrips() {
        for (int codec : new int[] {Records.NONE, Records.GZIP}) {
            List<Records.Out> in = List.of(rec("k1", "v1", 1000), rec(null, "no key", 1005, new Header("h", b("x")), new Header("empty", null)), rec("k3", null, 990), rec("", "", 1000));
            byte[] batch = Records.encode(in, codec);
            List<ConsumerRecord> out = Records.decode("t", 2, batch, 0, 1 << 20);
            assertEquals(4, out.size());
            for (int i = 0; i < 4; i++) {
                assertEquals(i, out.get(i).offset());
                assertEquals(in.get(i).timestamp(), out.get(i).timestamp());
                assertArrayEquals(in.get(i).key(), out.get(i).key());
                assertArrayEquals(in.get(i).value(), out.get(i).value());
                assertEquals("t", out.get(i).topic());
                assertEquals(2, out.get(i).partition());
            }
            assertEquals("h", out.get(1).headers().get(0).key());
            assertArrayEquals(b("x"), out.get(1).headers().get(0).value());
            assertNull(out.get(1).headers().get(1).value());
        }
    }

    @Test
    void severalBatchesAndPartialOnesAndStartOffset() {
        byte[] one = Records.encode(List.of(rec("a", "1", 1), rec("b", "2", 2)), Records.NONE);
        // the broker numbers the offsets: patch the base offset of the second batch to 2 and the first to 10
        java.nio.ByteBuffer.wrap(one).putLong(0, 10);
        byte[] two = Records.encode(List.of(rec("c", "3", 3)), Records.NONE);
        java.nio.ByteBuffer.wrap(two).putLong(0, 12);
        byte[] all = new byte[one.length + two.length];
        System.arraycopy(one, 0, all, 0, one.length);
        System.arraycopy(two, 0, all, one.length, two.length);
        assertEquals(List.of(10L, 11L, 12L), Records.decode("t", 0, all, 0, 1 << 20).stream().map(ConsumerRecord::offset).toList());
        assertEquals(List.of(11L, 12L), Records.decode("t", 0, all, 11, 1 << 20).stream().map(ConsumerRecord::offset).toList(), "records before the requested offset are skipped");
        byte[] cut = java.util.Arrays.copyOf(all, all.length - 5);
        assertEquals(List.of(10L, 11L), Records.decode("t", 0, cut, 0, 1 << 20).stream().map(ConsumerRecord::offset).toList(), "a batch cut off at the end is ignored");
        assertEquals(List.of(), Records.decode("t", 0, new byte[0], 0, 100));
    }

    @Test
    void corruptionIsDetected() {
        byte[] good = Records.encode(List.of(rec("k", "value", 5)), Records.NONE);
        for (int pos : new int[] {25, 40, good.length - 1}) {
            byte[] bad = good.clone();
            bad[pos] ^= 0x55;
            KafkaException e = assertThrows(KafkaException.class, () -> Records.decode("t", 0, bad, 0, 1 << 20), "byte " + pos);
            assertEquals(KafkaException.CORRUPT_MESSAGE, e.code());
        }
        byte[] magic = good.clone();
        magic[16] = 1;
        assertThrows(KafkaException.class, () -> Records.decode("t", 0, magic, 0, 1 << 20));
        byte[] length = good.clone();
        length[11] = 5;   // batch length 5
        assertThrows(KafkaException.class, () -> Records.decode("t", 0, length, 0, 1 << 20));
    }

    @Test
    void aGzipBombIsRefused() {
        byte[] zeros = new byte[4 * 1024 * 1024];
        byte[] batch = Records.encode(List.of(new Records.Out(null, zeros, List.of(), 1)), Records.GZIP);
        assertTrue(batch.length < 10_000, "it compresses well: " + batch.length);
        assertThrows(KafkaException.class, () -> Records.decode("t", 0, batch, 0, 1024 * 1024));
        assertEquals(1, Records.decode("t", 0, batch, 0, 8 * 1024 * 1024).size());
    }

    @Test
    void randomRecordsRoundTrip() {
        Random rnd = new Random(4);
        for (int round = 0; round < 50; round++) {
            List<Records.Out> in = new ArrayList<>();
            int n = 1 + rnd.nextInt(30);
            for (int i = 0; i < n; i++) {
                byte[] k = rnd.nextBoolean() ? null : new byte[rnd.nextInt(40)];
                byte[] v = rnd.nextBoolean() ? null : new byte[rnd.nextInt(3000)];
                if (k != null) rnd.nextBytes(k);
                if (v != null) rnd.nextBytes(v);
                in.add(new Records.Out(k, v, List.of(), 1_700_000_000_000L + rnd.nextInt(100000)));
            }
            List<ConsumerRecord> out = Records.decode("t", 0, Records.encode(in, round % 2), 0, 1 << 24);
            assertEquals(n, out.size());
            for (int i = 0; i < n; i++) {
                assertArrayEquals(in.get(i).key(), out.get(i).key());
                assertArrayEquals(in.get(i).value(), out.get(i).value());
            }
        }
    }

    @Test
    void thePartitionerAgreesWithTheJavaClient() {
        // values computed with org.apache.kafka.common.utils.Utils.murmur2
        assertEquals(-973932308, KafkaProducer.murmur2("21".getBytes()));
        assertEquals(-790332482, KafkaProducer.murmur2("foobar".getBytes()));
        assertEquals(-985981536, KafkaProducer.murmur2("a-little-bit-long-string".getBytes()));
        assertEquals(275646681, KafkaProducer.murmur2(new byte[0]));
    }
}
