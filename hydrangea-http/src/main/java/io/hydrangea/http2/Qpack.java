package io.hydrangea.http2;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * QPACK (RFC 9204) with the dynamic table switched off: capacity 0 is advertised, so a peer can only use the static table and literals. That is a
 * complete and interoperable subset (every field line representation that does not name a dynamic entry); a peer that references the dynamic table
 * is refused with {@link QpackException}. Huffman-coded strings are decoded; encoding writes plain literals.
 */
public final class Qpack {

    private Qpack() {
    }

    /** A decoded header field. */
    public record Field(String name, String value) {
    }

    /** The static table (RFC 9204 Appendix A), 99 entries. */
    static final String[][] STATIC = {
        {":authority", ""},
        {":path", "/"},
        {"age", "0"},
        {"content-disposition", ""},
        {"content-length", "0"},
        {"cookie", ""},
        {"date", ""},
        {"etag", ""},
        {"if-modified-since", ""},
        {"if-none-match", ""},
        {"last-modified", ""},
        {"link", ""},
        {"location", ""},
        {"referer", ""},
        {"set-cookie", ""},
        {":method", "CONNECT"},
        {":method", "DELETE"},
        {":method", "GET"},
        {":method", "HEAD"},
        {":method", "OPTIONS"},
        {":method", "POST"},
        {":method", "PUT"},
        {":scheme", "http"},
        {":scheme", "https"},
        {":status", "103"},
        {":status", "200"},
        {":status", "304"},
        {":status", "404"},
        {":status", "503"},
        {"accept", "*/*"},
        {"accept", "application/dns-message"},
        {"accept-encoding", "gzip, deflate, br"},
        {"accept-ranges", "bytes"},
        {"access-control-allow-headers", "cache-control"},
        {"access-control-allow-headers", "content-type"},
        {"access-control-allow-origin", "*"},
        {"cache-control", "max-age=0"},
        {"cache-control", "max-age=2592000"},
        {"cache-control", "max-age=604800"},
        {"cache-control", "no-cache"},
        {"cache-control", "no-store"},
        {"cache-control", "public, max-age=31536000"},
        {"content-encoding", "br"},
        {"content-encoding", "gzip"},
        {"content-type", "application/dns-message"},
        {"content-type", "application/javascript"},
        {"content-type", "application/json"},
        {"content-type", "application/x-www-form-urlencoded"},
        {"content-type", "image/gif"},
        {"content-type", "image/jpeg"},
        {"content-type", "image/png"},
        {"content-type", "text/css"},
        {"content-type", "text/html; charset=utf-8"},
        {"content-type", "text/plain"},
        {"content-type", "text/plain;charset=utf-8"},
        {"range", "bytes=0-"},
        {"strict-transport-security", "max-age=31536000"},
        {"strict-transport-security", "max-age=31536000; includesubdomains"},
        {"strict-transport-security", "max-age=31536000; includesubdomains; preload"},
        {"vary", "accept-encoding"},
        {"vary", "origin"},
        {"x-content-type-options", "nosniff"},
        {"x-xss-protection", "1; mode=block"},
        {":status", "100"},
        {":status", "204"},
        {":status", "206"},
        {":status", "302"},
        {":status", "400"},
        {":status", "403"},
        {":status", "421"},
        {":status", "425"},
        {":status", "500"},
        {"accept-language", ""},
        {"access-control-allow-credentials", "FALSE"},
        {"access-control-allow-credentials", "TRUE"},
        {"access-control-allow-headers", "*"},
        {"access-control-allow-methods", "get"},
        {"access-control-allow-methods", "get, post, options"},
        {"access-control-allow-methods", "options"},
        {"access-control-expose-headers", "content-length"},
        {"access-control-request-headers", "content-type"},
        {"access-control-request-method", "get"},
        {"access-control-request-method", "post"},
        {"alt-svc", "clear"},
        {"authorization", ""},
        {"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"},
        {"early-data", "1"},
        {"expect-ct", ""},
        {"forwarded", ""},
        {"if-range", ""},
        {"origin", ""},
        {"purpose", "prefetch"},
        {"server", ""},
        {"timing-allow-origin", "*"},
        {"upgrade-insecure-requests", "1"},
        {"user-agent", ""},
        {"x-forwarded-for", ""},
        {"x-frame-options", "deny"},
        {"x-frame-options", "sameorigin"}
    };

    private static final Map<String, Integer> FIRST_NAME = new HashMap<>();
    private static final Map<String, Integer> EXACT = new HashMap<>();

    static {
        for (int i = 0; i < STATIC.length; i++) {
            FIRST_NAME.putIfAbsent(STATIC[i][0], i);
            EXACT.putIfAbsent(STATIC[i][0] + "\0" + STATIC[i][1], i);
        }
    }

    // ---- encoding ----

    /** Writes an integer with an N-bit prefix (RFC 7541 section 5.1, shared by QPACK); {@code flags} are the high bits of the first byte. */
    static void writeInt(ByteArrayOutputStream out, int flags, int prefixBits, long value) {
        int max = (1 << prefixBits) - 1;
        if (value < max) {
            out.write(flags | (int) value);
            return;
        }
        out.write(flags | max);
        value -= max;
        while (value >= 128) {
            out.write((int) (value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write((int) value);
    }

    /** An encoded field section (RFC 9204 section 4.5) for these fields; names must already be lower case. */
    public static byte[] encode(List<Field> fields) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(64 + fields.size() * 24);
        out.write(0);   // required insert count 0
        out.write(0);   // base 0
        for (Field f : fields) {
            Integer exact = EXACT.get(f.name() + "\0" + f.value());
            if (exact != null) {
                writeInt(out, 0xc0, 6, exact);                      // indexed field line, static
                continue;
            }
            byte[] value = f.value().getBytes(StandardCharsets.ISO_8859_1);
            Integer name = FIRST_NAME.get(f.name());
            if (name != null) {
                writeInt(out, 0x50, 4, name);                       // literal with static name reference, N=0
            } else {
                byte[] n = f.name().getBytes(StandardCharsets.ISO_8859_1);
                writeInt(out, 0x20, 3, n.length);                   // literal with literal name
                out.write(n, 0, n.length);
            }
            writeInt(out, 0, 7, value.length);
            out.write(value, 0, value.length);
        }
        return out.toByteArray();
    }

    // ---- decoding ----

    /**
     * Decodes one field section.
     *
     * @param maxBytes limit on the decoded size (names + values + 32 per field, as in HTTP/2); a larger section is refused
     */
    public static List<Field> decode(byte[] b, int off, int len, long maxBytes) throws QpackException {
        Reader r = new Reader(b, off, off + len);
        long requiredInsertCount = r.integer(8);
        if (requiredInsertCount != 0) {
            throw new QpackException("field section needs the dynamic table, which is disabled");
        }
        r.integer(7);   // sign and delta base: meaningless with no dynamic entries
        List<Field> fields = new ArrayList<>();
        long size = 0;
        while (r.hasMore()) {
            int first = r.peek();
            String name, value;
            if ((first & 0x80) != 0) {                                  // indexed field line
                if ((first & 0x40) == 0) {
                    throw new QpackException("reference to the dynamic table");
                }
                int index = r.intIndex(6);
                name = STATIC[index][0];
                value = STATIC[index][1];
            } else if ((first & 0x40) != 0) {                           // literal with name reference
                if ((first & 0x10) == 0) {
                    throw new QpackException("reference to the dynamic table");
                }
                int index = r.intIndex(4);
                name = STATIC[index][0];
                value = r.string(7, maxBytes - size);
            } else if ((first & 0x20) != 0) {                           // literal with literal name
                name = r.string(3, maxBytes - size);
                value = r.string(7, maxBytes - size);
            } else {                                                    // indexed or literal with a post-base reference
                throw new QpackException("reference to the dynamic table");
            }
            size += name.length() + value.length() + 32;
            if (size > maxBytes) {
                throw new QpackException("header section too large");
            }
            fields.add(new Field(name, value));
        }
        return fields;
    }

    private static final class Reader {
        final byte[] b;
        int pos;
        final int end;

        Reader(byte[] b, int pos, int end) {
            this.b = b;
            this.pos = pos;
            this.end = end;
        }

        boolean hasMore() {
            return pos < end;
        }

        int peek() throws QpackException {
            if (pos >= end) throw new QpackException("truncated field section");
            return b[pos] & 0xff;
        }

        long integer(int prefixBits) throws QpackException {
            int max = (1 << prefixBits) - 1;
            long v = peek() & max;
            pos++;
            if (v < max) {
                return v;
            }
            int shift = 0;
            while (true) {
                int x = peek();
                pos++;
                if (shift > 56) throw new QpackException("integer too large");
                v += (long) (x & 0x7f) << shift;
                if ((x & 0x80) == 0) {
                    return v;
                }
                shift += 7;
            }
        }

        int intIndex(int prefixBits) throws QpackException {
            long v = integer(prefixBits);
            if (v >= STATIC.length) throw new QpackException("static index " + v + " out of range");
            return (int) v;
        }

        /** A string literal whose length uses a {@code prefixBits}-bit prefix; the Huffman flag is the bit just above the prefix. */
        String string(int prefixBits, long limit) throws QpackException {
            boolean huffman = (peek() & (1 << prefixBits)) != 0;
            long n = integer(prefixBits);
            if (n > end - pos) throw new QpackException("truncated string");
            if (limit < 0 || (!huffman && n > limit)) throw new QpackException("header section too large");
            byte[] raw;
            if (huffman) {
                try {
                    raw = Huffman.decode(b, pos, (int) n, (int) Math.min(limit, Integer.MAX_VALUE));
                } catch (HpackException e) {
                    throw new QpackException(e.getMessage());
                }
            } else {
                raw = java.util.Arrays.copyOfRange(b, pos, pos + (int) n);
            }
            pos += (int) n;
            return new String(raw, StandardCharsets.ISO_8859_1);
        }
    }
}
