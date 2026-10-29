package io.hydrangea.http2;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class HpackTest {

    private static byte[] hex(String s) {
        return HexFormat.of().parseHex(s.replace(" ", ""));
    }

    private static List<String> decode(HpackDecoder d, String hex) throws HpackException {
        List<String> out = new ArrayList<>();
        d.decode(hex(hex), (n, v) -> out.add(n + ": " + v));
        return out;
    }

    /** RFC 7541 C.3: three requests without Huffman coding, the dynamic table carrying state between them. */
    @Test
    void rfcRequestsWithoutHuffman() throws Exception {
        HpackDecoder d = new HpackDecoder(4096, 1 << 16);
        assertEquals(List.of(":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com"),
                decode(d, "8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d"));
        assertEquals(List.of(":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com", "cache-control: no-cache"),
                decode(d, "8286 84be 5808 6e6f 2d63 6163 6865"));
        assertEquals(List.of(":method: GET", ":scheme: https", ":path: /index.html", ":authority: www.example.com", "custom-key: custom-value"),
                decode(d, "8287 85bf 400a 6375 7374 6f6d 2d6b 6579 0c63 7573 746f 6d2d 7661 6c75 65"));
    }

    /** RFC 7541 C.4: the same requests with Huffman coded strings. */
    @Test
    void rfcRequestsWithHuffman() throws Exception {
        HpackDecoder d = new HpackDecoder(4096, 1 << 16);
        assertEquals(List.of(":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com"),
                decode(d, "8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff"));
        assertEquals(List.of(":method: GET", ":scheme: http", ":path: /", ":authority: www.example.com", "cache-control: no-cache"),
                decode(d, "8286 84be 5886 a8eb 1064 9cbf"));
        assertEquals(List.of(":method: GET", ":scheme: https", ":path: /index.html", ":authority: www.example.com", "custom-key: custom-value"),
                decode(d, "8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf"));
    }

    @Test
    void encoderOutputIsDecodedBack() throws Exception {
        HpackEncoder e = new HpackEncoder();
        HpackDecoder d = new HpackDecoder(4096, 1 << 16);
        Random rnd = new Random(1);
        for (int round = 0; round < 500; round++) {
            e.reset();
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 1 + rnd.nextInt(6); i++) {
                String name = new String[]{":status", "content-type", "x-id", "set-cookie", "server", "etag"}[rnd.nextInt(6)];
                String value = new String[]{"200", "text/plain", "", "abc", "x".repeat(200), "é"}[rnd.nextInt(6)] + rnd.nextInt(1000);
                e.header(name, value);
                expected.add(name + ": " + value);
            }
            List<String> got = new ArrayList<>();
            d.decode(e.toByteArray(), (n, v) -> got.add(n + ": " + v));
            assertEquals(expected, got);
        }
    }

    @Test
    void sensitiveHeadersAreNeverIndexed() {
        HpackEncoder e = new HpackEncoder();
        e.reset();
        e.header("authorization", "secret");
        assertEquals(0x10, e.toByteArray()[0] & 0xf0, "must use the never-indexed representation");
    }

    @Test
    void malformedBlocksAreRejected() {
        HpackDecoder d = new HpackDecoder(4096, 1000);
        assertThrows(HpackException.class, () -> d.decode(hex("80"), (n, v) -> { }), "index 0");
        assertThrows(HpackException.class, () -> d.decode(hex("ff ff ff ff ff 7f"), (n, v) -> { }), "integer overflow");
        assertThrows(HpackException.class, () -> d.decode(hex("40 05 61"), (n, v) -> { }), "string longer than the block");
        assertThrows(HpackException.class, () -> d.decode(hex("be"), (n, v) -> { }), "index beyond the (empty) dynamic table");
        assertThrows(HpackException.class, () -> d.decode(hex("3f e2 1f"), (n, v) -> { }), "table size update above the allowed size");
        assertThrows(HpackException.class, () -> d.decode(hex("82 20"), (n, v) -> { }), "table size update after a header");
        // Huffman: padding longer than 7 bits, padding that is not all ones, and the EOS symbol itself
        assertThrows(HpackException.class, () -> d.decode(hex("40 81 ff 00"), (n, v) -> { }), "8 bits of padding");
        assertThrows(HpackException.class, () -> d.decode(hex("40 81 00 00"), (n, v) -> { }), "padding that is not ones");
        assertThrows(HpackException.class, () -> d.decode(hex("40 84 ffff ffff 00"), (n, v) -> { }), "EOS inside a string");
    }

    @Test
    void hugeStringsAreRefused() {
        HpackDecoder d = new HpackDecoder(4096, 100);
        byte[] block = new byte[2 + 2 + 200];
        block[0] = 0x00;          // literal without indexing, new name
        block[1] = 0x00;          // empty name
        block[2] = (byte) 0x7f;   // value length: 127 + 73 = 200 (above the 100 allowed)
        block[3] = 73;
        assertThrows(HpackException.class, () -> d.decode(block, (n, v) -> { }));
    }

    @Test
    void dynamicTableIsBoundedByTheAnnouncedSize() throws Exception {
        HpackDecoder d = new HpackDecoder(100, 1 << 16);
        // 'a: b' (36 bytes each) inserted three times: only two fit into 100 bytes, the oldest is evicted
        assertEquals(1, decode(d, "40 01 61 01 62").size());
        decode(d, "40 01 63 01 64");
        decode(d, "40 01 65 01 66");
        assertThrows(HpackException.class, () -> decode(d, "c0"), "index 64 is beyond a two-entry table");
        assertEquals(List.of("e: f"), decode(d, "be"));
        assertEquals(List.of("c: d"), decode(d, "bf"));
    }
}
