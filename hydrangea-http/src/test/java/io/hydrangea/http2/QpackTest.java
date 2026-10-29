package io.hydrangea.http2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class QpackTest {

    private static byte[] b(int... v) {
        byte[] r = new byte[v.length];
        for (int i = 0; i < v.length; i++) r[i] = (byte) v[i];
        return r;
    }

    private static List<Qpack.Field> decode(byte[] block) throws QpackException {
        return Qpack.decode(block, 0, block.length, 64 * 1024);
    }

    @Test
    void staticTableMatchesTheRfcAtTheEnds() {
        assertEquals(99, Qpack.STATIC.length);
        assertArrayEquals(new String[]{":authority", ""}, Qpack.STATIC[0]);
        assertArrayEquals(new String[]{":method", "GET"}, Qpack.STATIC[17]);
        assertArrayEquals(new String[]{":status", "200"}, Qpack.STATIC[25]);
        assertArrayEquals(new String[]{"content-type", "text/plain;charset=utf-8"}, Qpack.STATIC[54]);
        assertArrayEquals(new String[]{"x-frame-options", "sameorigin"}, Qpack.STATIC[98]);
    }

    @Test
    void roundTrip() throws Exception {
        List<Qpack.Field> in = List.of(new Qpack.Field(":status", "200"), new Qpack.Field(":status", "418"), new Qpack.Field("content-type", "text/html; charset=utf-8"),
                new Qpack.Field("x-custom", "value"), new Qpack.Field("x-empty", ""), new Qpack.Field("x-long", "v".repeat(300)));
        assertEquals(in, decode(Qpack.encode(in)));
    }

    @Test
    void indexedStaticFieldIsOneByteAfterThePrefix() {
        byte[] enc = Qpack.encode(List.of(new Qpack.Field(":status", "200")));
        assertArrayEquals(b(0, 0, 0xc0 | 25), enc);
    }

    @Test
    void rfc9204AppendixBStaticOnlyExample() throws Exception {
        // :path = /index.html with a static name reference (index 1), no dynamic table: 0000 51 0b "/index.html"
        byte[] path = "/index.html".getBytes(StandardCharsets.US_ASCII);
        byte[] block = new byte[2 + 2 + path.length];
        block[2] = 0x51;
        block[3] = (byte) path.length;
        System.arraycopy(path, 0, block, 4, path.length);
        assertEquals(List.of(new Qpack.Field(":path", "/index.html")), decode(block));
    }

    @Test
    void huffmanCodedValue() throws Exception {
        // RFC 7541 C.4.1: "www.example.com" Huffman coded; QPACK uses the same code. Literal with static name :authority (0), H=1.
        byte[] huff = b(0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff);
        byte[] block = new byte[2 + 1 + 1 + huff.length];
        block[2] = 0x50;
        block[3] = (byte) (0x80 | huff.length);
        System.arraycopy(huff, 0, block, 4, huff.length);
        assertEquals(List.of(new Qpack.Field(":authority", "www.example.com")), decode(block));
    }

    @Test
    void multiByteIntegers() throws Exception {
        // static index 60 in a 6-bit prefix needs a continuation byte? 60 < 63, so use a long value length instead
        String v = "z".repeat(200);
        List<Qpack.Field> in = List.of(new Qpack.Field("x", v));
        assertEquals(in, decode(Qpack.encode(in)));
        byte[] enc = Qpack.encode(List.of(new Qpack.Field("name", "n".repeat(20))));
        assertEquals(List.of(new Qpack.Field("name", "n".repeat(20))), decode(enc));
    }

    @Test
    void dynamicTableReferencesAreRefused() {
        assertThrows(QpackException.class, () -> decode(b(1, 0, 0x80)));         // required insert count 1
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x80)));         // indexed, dynamic
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x10)));         // post-base index
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x40, 1, 'a')));  // literal, dynamic name reference
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x00)));         // post-base literal
    }

    @Test
    void malformedInputIsRefusedNotCrashed() {
        assertThrows(QpackException.class, () -> decode(b()));                                   // empty
        assertThrows(QpackException.class, () -> decode(b(0)));                                  // no base
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0xc0 | 63, 100)));               // static index 163
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x50, 5, 'a')));                 // value shorter than declared
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x20, 0x7f)));                   // name length integer truncated
        assertThrows(QpackException.class, () -> decode(b(0, 0, 0x50, 0x81, 0xff)));             // Huffman: EOS padding
        assertThrows(QpackException.class, () -> decode(b(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 0)));   // huge integer
    }

    @Test
    void sizeLimit() {
        byte[] enc = Qpack.encode(List.of(new Qpack.Field("x", "v".repeat(1000))));
        assertThrows(QpackException.class, () -> Qpack.decode(enc, 0, enc.length, 500));
    }

    @Test
    void randomBytesNeverEscapeAsAnythingButQpackException() {
        Random r = new Random(7);
        for (int i = 0; i < 20000; i++) {
            byte[] block = new byte[r.nextInt(40)];
            r.nextBytes(block);
            if (block.length > 1 && r.nextBoolean()) {
                block[0] = 0;
                block[1] = 0;
            }
            try {
                Qpack.decode(block, 0, block.length, 4096);
            } catch (QpackException expected) {
                // fine
            }
        }
    }
}
