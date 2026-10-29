package io.hydrangea.http2;

import io.hydrangea.fuzz.Fuzz;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hostile header blocks: the decoder rejects them with HpackException, within its size limits, and stays usable only as long as the protocol allows. */
class HpackFuzzTest {

    private static byte[] block(String... nv) {
        HpackEncoder e = new HpackEncoder();
        for (int i = 0; i < nv.length; i += 2) {
            e.header(nv[i], nv[i + 1]);
        }
        return e.toByteArray();
    }

    @Test
    void headerBlocks() {
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(block(":method", "GET", ":scheme", "https", ":path", "/", ":authority", "example.com"));
        corpus.add(block(":method", "POST", ":path", "/x?q=1", "content-type", "application/grpc", "te", "trailers", "user-agent", "fuzz/1"));
        corpus.add(block("x-long", "a".repeat(3000), "cookie", "a=b; c=d"));
        corpus.add(new byte[]{0x3f, (byte) 0xe1, 0x1f});                 // dynamic table size update
        corpus.add(new byte[]{0x40, 0x03, 'a', 'b', 'c', 0x03, 'd', 'e', 'f'});
        corpus.add(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01});   // huge integer
        long[] decoded = {0};
        HpackDecoder shared = new HpackDecoder(4096, 8192, 64 * 1024);
        List<Fuzz.Finding> f = new ArrayList<>(Fuzz.run("HpackDecoder fresh", corpus, in -> {
            new HpackDecoder(4096, 8192, 64 * 1024).decode(in, (n, v) -> decoded[0] += n.length() + v.length());
        }, Set.of(HpackException.class), 1_000));
        // one decoder for the whole campaign: the dynamic table is state that earlier hostile blocks may have left in an odd shape
        f.addAll(Fuzz.run("HpackDecoder shared", corpus, in -> shared.decode(in, (n, v) -> decoded[0] += n.length() + v.length()),
                Set.of(HpackException.class), 1_000));
        assertTrue(f.isEmpty(), () -> String.join("\n", f.stream().map(Fuzz.Finding::toString).toList()));
    }

    @Test
    void huffmanStrings() {
        List<byte[]> corpus = List.of(new byte[]{(byte) 0xf1, (byte) 0xe3, (byte) 0xc2, (byte) 0xe5, (byte) 0xf2, 0x3a, 0x6b, (byte) 0xa0, (byte) 0xab, (byte) 0x90, (byte) 0xf4, (byte) 0xff});
        var f = Fuzz.run("Huffman.decode", corpus, in -> Huffman.decode(in, 0, in.length, 4096), Set.of(HpackException.class), 1_000);
        assertTrue(f.isEmpty(), () -> String.join("\n", f.stream().map(Fuzz.Finding::toString).toList()));
    }
}
