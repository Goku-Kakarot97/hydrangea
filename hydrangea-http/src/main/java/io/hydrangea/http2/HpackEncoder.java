package io.hydrangea.http2;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * HPACK encoder (RFC 7541). It uses the static table and literal representations only: it never adds to the dynamic table, so
 * it needs no state, cannot be confused by the peer's table size, and a header block can be encoded in any order. Strings are
 * Huffman coded when that is shorter. Credentials are sent as "never indexed" so an intermediary will not index them either.
 */
public final class HpackEncoder {

    private static final Set<String> SENSITIVE = Set.of("authorization", "proxy-authorization", "cookie", "set-cookie");

    private final ByteArrayOutputStream out = new ByteArrayOutputStream(256);

    /** Starts a header block. */
    public void reset() {
        out.reset();
    }

    public void header(String name, String value) {
        boolean sensitive = SENSITIVE.contains(name);
        int full = sensitive ? 0 : HpackStatic.indexOf(name, value);
        if (full != 0) {
            writeInt(0x80, 7, full);
            return;
        }
        int nameIndex = HpackStatic.nameIndex(name);
        int prefix = sensitive ? 0x10 : 0x00; // never indexed / without indexing, both with a 4-bit prefix
        if (nameIndex != 0) {
            writeInt(prefix, 4, nameIndex);
        } else {
            writeInt(prefix, 4, 0);
            writeString(name);
        }
        writeString(value);
    }

    /** The header block built since {@link #reset()}. */
    public byte[] toByteArray() {
        return out.toByteArray();
    }

    /** Encoded string literals (length prefix included) by string: header names and the values that repeat (content types, statuses). Bounded; stateless, so shared. */
    private static final java.util.concurrent.ConcurrentHashMap<String, byte[]> LITERALS = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_CACHED = 2048;
    private static final int MAX_CACHED_LENGTH = 96;

    private void writeString(String s) {
        boolean cacheable = s.length() <= MAX_CACHED_LENGTH;
        if (cacheable) {
            byte[] cached = LITERALS.get(s);
            if (cached != null) {
                out.write(cached, 0, cached.length);
                return;
            }
        }
        int start = out.size();
        encodeString(s);
        if (cacheable && LITERALS.size() < MAX_CACHED) {
            byte[] all = out.toByteArray();
            LITERALS.putIfAbsent(s, java.util.Arrays.copyOfRange(all, start, all.length));
        }
    }

    private void encodeString(String s) {
        byte[] raw = s.getBytes(StandardCharsets.ISO_8859_1);
        int huff = Huffman.encodedLength(raw);
        if (huff < raw.length) {
            writeInt(0x80, 7, huff);
            Huffman.encode(raw, out);
        } else {
            writeInt(0x00, 7, raw.length);
            out.write(raw, 0, raw.length);
        }
    }

    private void writeInt(int flags, int prefixBits, int value) {
        int mask = (1 << prefixBits) - 1;
        if (value < mask) {
            out.write(flags | value);
            return;
        }
        out.write(flags | mask);
        value -= mask;
        while (value >= 0x80) {
            out.write((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write(value);
    }
}
