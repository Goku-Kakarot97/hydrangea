package io.hydrangea.http2;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** The table-driven decoder against the encoder, and the strictness rules of RFC 7541 section 5.2 at the edges. */
class HuffmanTest {

    private static byte[] encode(byte[] s) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Huffman.encode(s, out);
        return out.toByteArray();
    }

    @Test
    void everyByteValueAloneAndInRandomStringsRoundTrips() throws Exception {
        for (int b = 0; b < 256; b++) {
            byte[] s = {(byte) b};
            assertArrayEquals(s, Huffman.decode(encode(s), 0, encode(s).length, 100), "byte " + b);
        }
        Random r = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            byte[] s = new byte[r.nextInt(80)];
            if (r.nextBoolean()) {
                r.nextBytes(s);
            } else {
                for (int k = 0; k < s.length; k++) {
                    s[k] = (byte) "abcdefghij-/:=0123456789 "
                            .charAt(r.nextInt(25));
                }
            }
            byte[] enc = encode(s);
            byte[] padded = new byte[enc.length + 6];   // decoding honours offset and length
            System.arraycopy(enc, 0, padded, 3, enc.length);
            assertArrayEquals(s, Huffman.decode(padded, 3, enc.length, s.length), "length " + s.length);
        }
    }

    @Test
    void theResultLengthLimitIsExact() throws Exception {
        byte[] s = "www.example.com".getBytes();
        byte[] enc = encode(s);
        assertArrayEquals(s, Huffman.decode(enc, 0, enc.length, s.length));
        assertThrows(HpackException.class, () -> Huffman.decode(enc, 0, enc.length, s.length - 1));
        assertThrows(HpackException.class, () -> Huffman.decode(enc, 0, enc.length, 0));
        assertEquals(0, Huffman.decode(new byte[0], 0, 0, 0).length);
    }

    @Test
    void paddingMustBeAtMostSevenOnes() {
        byte[] ok = encode("a".getBytes());                 // 'a' is 5 bits + 3 bits of ones
        assertDoesNotThrow(() -> Huffman.decode(ok, 0, 1, 10));
        assertThrows(HpackException.class, () -> Huffman.decode(new byte[]{(byte) (ok[0] & 0xfe)}, 0, 1, 10), "padding with a zero bit");
        byte[] eight = {ok[0], (byte) 0xff};                // a whole extra byte of ones is 8 bits of padding
        assertThrows(HpackException.class, () -> Huffman.decode(eight, 0, 2, 10));
        assertThrows(HpackException.class, () -> Huffman.decode(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, 0, 4, 10), "EOS");
    }
}
