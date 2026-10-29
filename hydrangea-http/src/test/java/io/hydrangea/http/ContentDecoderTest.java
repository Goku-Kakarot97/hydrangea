package io.hydrangea.http;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class ContentDecoderTest {

    private static byte[] sample(int n) {
        byte[] b = new byte[n];
        Random r = new Random(n);
        for (int i = 0; i < n; i++) {
            b[i] = (byte) (r.nextInt(8) == 0 ? r.nextInt(256) : 'a' + i % 7);   // compressible, not trivially so
        }
        return b;
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(out)) {
            g.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] zlib(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DeflaterOutputStream d = new DeflaterOutputStream(out)) {
            d.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] rawDeflate(byte[] data) {
        Deflater d = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        d.setInput(data);
        d.finish();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        while (!d.finished()) {
            out.write(buf, 0, d.deflate(buf));
        }
        d.end();
        return out.toByteArray();
    }

    /** Feeds {@code encoded} in pieces of {@code chunk} bytes and returns what comes out. */
    private static byte[] decode(String encoding, byte[] encoded, int chunk, long limit) throws IOException {
        ContentDecoder d = ContentDecoder.forEncoding(encoding, limit);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < encoded.length; i += chunk) {
            d.write(encoded, i, Math.min(chunk, encoded.length - i), out::write);
        }
        d.finish();
        return out.toByteArray();
    }

    @Test
    void gzipInAnyPieceSize() throws Exception {
        byte[] data = sample(50_000);
        byte[] enc = gzip(data);
        for (int chunk : new int[]{1, 2, 3, 7, 100, 4096, enc.length}) {
            assertArrayEquals(data, decode("gzip", enc, chunk, Long.MAX_VALUE), "chunk " + chunk);
        }
    }

    @Test
    void zlibAndRawDeflate() throws Exception {
        byte[] data = sample(30_000);
        for (int chunk : new int[]{1, 5, 1000, 100_000}) {
            assertArrayEquals(data, decode("deflate", zlib(data), chunk, Long.MAX_VALUE), "zlib, chunk " + chunk);
            assertArrayEquals(data, decode("deflate", rawDeflate(data), chunk, Long.MAX_VALUE), "raw, chunk " + chunk);
        }
    }

    @Test
    void gzipHeaderWithOptionalFields() throws Exception {
        byte[] data = sample(1000);
        byte[] body = rawDeflate(data);
        CRC32 crc = new CRC32();
        crc.update(data);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] head = {0x1f, (byte) 0x8b, 8, 2 | 4 | 8 | 16, 0, 0, 0, 0, 0, 3};   // FHCRC | FEXTRA | FNAME | FCOMMENT
        out.write(head);
        out.write(new byte[]{5, 0, 'e', 'x', 't', 'r', 'a'});
        out.write("file.txt\0".getBytes());
        out.write("a comment\0".getBytes());
        out.write(new byte[]{0x12, 0x34});                                        // header CRC16 (not checked)
        out.write(body);
        long c = crc.getValue();
        out.write(new byte[]{(byte) c, (byte) (c >> 8), (byte) (c >> 16), (byte) (c >> 24), (byte) data.length, (byte) (data.length >> 8), 0, 0});
        for (int chunk : new int[]{1, 3, 1_000_000}) {
            assertArrayEquals(data, decode("gzip", out.toByteArray(), chunk, Long.MAX_VALUE), "chunk " + chunk);
        }
    }

    @Test
    void concatenatedMembersAreOneBody() throws Exception {
        byte[] a = sample(700), b = sample(900);
        ByteArrayOutputStream enc = new ByteArrayOutputStream();
        enc.write(gzip(a));
        enc.write(gzip(b));
        ByteArrayOutputStream both = new ByteArrayOutputStream();
        both.write(a);
        both.write(b);
        assertArrayEquals(both.toByteArray(), decode("gzip", enc.toByteArray(), 13, Long.MAX_VALUE));
    }

    @Test
    void bytesAfterTheLastMemberAreIgnored() throws Exception {
        byte[] a = sample(500);
        ByteArrayOutputStream enc = new ByteArrayOutputStream();
        enc.write(gzip(a));
        enc.write(new byte[]{0, 0, 0, 0, 0});
        assertArrayEquals(a, decode("gzip", enc.toByteArray(), 4, Long.MAX_VALUE));
    }

    @Test
    void anEmptyBodyIsFine() throws Exception {
        assertEquals(0, decode("gzip", new byte[0], 1, 100).length);
        assertEquals(0, decode("deflate", new byte[0], 1, 100).length);
        assertEquals(0, decode("gzip", gzip(new byte[0]), 1, 100).length);
    }

    @Test
    void corruptTruncatedAndMismatchedDataIsAnError() throws Exception {
        byte[] enc = gzip(sample(5000));
        byte[] flipped = enc.clone();
        flipped[flipped.length - 8] ^= 0x55;                                       // CRC32
        assertThrows(IOException.class, () -> decode("gzip", flipped, 100, Long.MAX_VALUE));
        byte[] badSize = enc.clone();
        badSize[badSize.length - 1] ^= 0x01;                                       // ISIZE
        assertThrows(IOException.class, () -> decode("gzip", badSize, 100, Long.MAX_VALUE));
        byte[] cut = java.util.Arrays.copyOf(enc, enc.length - 20);
        assertThrows(IOException.class, () -> decode("gzip", cut, 100, Long.MAX_VALUE));
        byte[] middle = enc.clone();
        middle[middle.length / 2] ^= (byte) 0xff;
        assertThrows(IOException.class, () -> decode("gzip", middle, 100, Long.MAX_VALUE));
        assertThrows(IOException.class, () -> decode("gzip", "not gzip at all".getBytes(), 3, Long.MAX_VALUE));
        byte[] z = zlib(sample(3000));
        assertThrows(IOException.class, () -> decode("deflate", java.util.Arrays.copyOf(z, z.length - 10), 50, Long.MAX_VALUE));
    }

    @Test
    void aDecompressionBombStopsAtTheLimit() throws Exception {
        byte[] bomb = gzip(new byte[200 * 1024 * 1024]);           // ~200 KB of ones and zeros that expand 1000-fold
        assertTrue(bomb.length < 400_000, "compressed size " + bomb.length);
        ContentDecoder d = ContentDecoder.forEncoding("gzip", 1024 * 1024);
        long[] seen = {0};
        IOException e = assertThrows(IOException.class, () -> d.write(bomb, 0, bomb.length, (b, off, len) -> seen[0] += len));
        assertTrue(e.getMessage().contains("exceeds the limit"), e.getMessage());
        assertTrue(seen[0] <= 1024 * 1024, "no more than the limit was ever handed out: " + seen[0]);
    }

    @Test
    void unknownOrCombinedCodingsAreLeftAlone() {
        assertNull(ContentDecoder.forEncoding(null, 1));
        assertNull(ContentDecoder.forEncoding("identity", 1));
        assertNull(ContentDecoder.forEncoding("br", 1));
        assertNull(ContentDecoder.forEncoding("gzip, br", 1));
        assertNotNull(ContentDecoder.forEncoding(" GZip ", 1));
        assertNotNull(ContentDecoder.forEncoding("x-gzip", 1));
    }
}
