package io.hydrangea.mongodb;

import io.hydrangea.codec.Records;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * BSON to and from plain Java values, and documents to and from records.
 * <table border="1">
 *   <tr><th>BSON</th><th>Java (read)</th><th>Java (written as)</th></tr>
 *   <tr><td>document, array</td><td>{@code Map<String,Object>} (order kept), {@code List<Object>}</td><td>{@code Map}, any {@code Collection} or array, a record ({@link Name} renames a component)</td></tr>
 *   <tr><td>string</td><td>{@code String}</td><td>{@code String}, {@code Character}, enum (its name)</td></tr>
 *   <tr><td>int32, int64, double</td><td>{@code Integer}, {@code Long}, {@code Double}</td><td>{@code Integer}/{@code Short}/{@code Byte}; {@code Long} ({@code BigInteger} that fits); {@code Double}/{@code Float}</td></tr>
 *   <tr><td>decimal128</td><td>{@code BigDecimal} (NaN and infinities as {@code Double})</td><td>{@code BigDecimal}, a {@code BigInteger} past 64 bits</td></tr>
 *   <tr><td>boolean, null, undefined</td><td>{@code Boolean}, null, null</td><td>{@code Boolean}, null, an empty {@code Optional}</td></tr>
 *   <tr><td>ObjectId, UTC datetime, timestamp, regex</td><td>{@link ObjectId}, {@code Instant}, {@link BsonTimestamp}, {@link BsonRegex}</td><td>the same; {@code java.util.Date}, {@code LocalDate}/{@code LocalDateTime} (as UTC) and other {@code java.time} instants; {@code Pattern}</td></tr>
 *   <tr><td>binary</td><td>{@code byte[]} (subtype 0), {@code UUID} (subtype 4), else {@link BsonBinary}</td><td>the same</td></tr>
 *   <tr><td>MinKey, MaxKey</td><td>{@link #MIN_KEY}, {@link #MAX_KEY}</td><td>the same</td></tr>
 *   <tr><td>JavaScript, symbol, code with scope</td><td>{@code String} (the code)</td><td>–</td></tr>
 * </table>
 * Reading is bounded: every length is checked against the bytes present, nesting is limited to 200. Keys may not contain NUL. {@link #toRecord} and {@link #fromRecord}
 * map records with the shared record mapper ({@code Instant} and {@code ObjectId} components take the decoded values as they are).
 */
public final class Bson {

    /** The BSON field name of a record component when it differs from the component's name: {@code @Bson.Name("_id") ObjectId id}. */
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.RECORD_COMPONENT)
    public @interface Name {
        String value();
    }

    /** The MinKey and MaxKey types: less and greater than every other value. */
    public enum Key { MIN, MAX }

    public static final Key MIN_KEY = Key.MIN;
    public static final Key MAX_KEY = Key.MAX;

    private static final int MAX_DEPTH = 200;
    private static final Records RECORDS = new Records(Name.class);

    private Bson() {
    }

    // ---- records ----

    /** A document as a record ({@link Records}); null stays null. */
    public static <T> T toRecord(Class<T> type, Map<String, Object> document) {
        return RECORDS.to(type, document);
    }

    /** A record as a document (its components, nested records converted too). */
    public static Map<String, Object> fromRecord(Record record) {
        return decode(encode(record));
    }

    /** A document from alternating keys and values: {@code Bson.doc("name", "lamp", "price", 20)}. */
    public static Map<String, Object> doc(Object... keysAndValues) {
        if ((keysAndValues.length & 1) != 0) {
            throw new IllegalArgumentException("keys and values must alternate");
        }
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            m.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return m;
    }

    // ---- writing ----

    /** A document (a {@code Map} or a record) as BSON bytes. */
    public static byte[] encode(Object document) {
        Writer w = new Writer(256);
        w.document(document, 0);
        return w.toBytes();
    }

    static final class Writer {
        private byte[] buf;
        private int pos;

        Writer(int size) {
            buf = new byte[size];
        }

        byte[] toBytes() {
            return java.util.Arrays.copyOf(buf, pos);
        }

        int position() {
            return pos;
        }

        private void ensure(int n) {
            if (pos + n > buf.length) {
                buf = java.util.Arrays.copyOf(buf, Math.max(buf.length * 2, pos + n));
            }
        }

        void int32(int v) {
            ensure(4);
            buf[pos++] = (byte) v;
            buf[pos++] = (byte) (v >> 8);
            buf[pos++] = (byte) (v >> 16);
            buf[pos++] = (byte) (v >> 24);
        }

        void int32At(int at, int v) {
            buf[at] = (byte) v;
            buf[at + 1] = (byte) (v >> 8);
            buf[at + 2] = (byte) (v >> 16);
            buf[at + 3] = (byte) (v >> 24);
        }

        void int64(long v) {
            int32((int) v);
            int32((int) (v >> 32));
        }

        void bytes(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, buf, pos, b.length);
            pos += b.length;
        }

        void byte1(int b) {
            ensure(1);
            buf[pos++] = (byte) b;
        }

        void cstring(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            for (byte x : b) {
                if (x == 0) {
                    throw new IllegalArgumentException("a BSON key may not contain NUL: " + s);
                }
            }
            bytes(b);
            byte1(0);
        }

        void string(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            int32(b.length + 1);
            bytes(b);
            byte1(0);
        }

        /** Writes a document or array body: int32 length, elements, 0. */
        @SuppressWarnings("unchecked")
        void document(Object doc, int depth) {
            if (depth > MAX_DEPTH) {
                throw new IllegalArgumentException("nested too deeply (a cycle?)");
            }
            int start = pos;
            int32(0);
            if (doc instanceof Map<?, ?> m) {
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    element(String.valueOf(e.getKey()), e.getValue(), depth);
                }
            } else if (doc instanceof Record r) {
                for (Map.Entry<String, Object> e : RECORDS.fields(r).entrySet()) {
                    element(e.getKey(), e.getValue(), depth);
                }
            } else if (doc instanceof Collection<?> c) {
                int i = 0;
                for (Object o : c) {
                    element(Integer.toString(i++), o, depth);
                }
            } else if (doc != null && doc.getClass().isArray()) {
                int n = java.lang.reflect.Array.getLength(doc);
                for (int i = 0; i < n; i++) {
                    element(Integer.toString(i), java.lang.reflect.Array.get(doc, i), depth);
                }
            } else {
                throw new IllegalArgumentException("a document is a Map or a record, not " + (doc == null ? "null" : doc.getClass().getName()));
            }
            byte1(0);
            int32At(start, pos - start);
        }

        void element(String key, Object v, int depth) {
            int typeAt = pos;
            byte1(0);                                              // the type, filled in below
            cstring(key);
            int type = value(v, depth);                            // may grow (replace) buf: index the array only afterwards
            buf[typeAt] = (byte) type;
        }

        /** Writes a value's bytes and returns its BSON type. */
        int value(Object v, int depth) {
            switch (v) {
                case null -> {
                    return 0x0A;
                }
                case Double d -> {
                    int64(Double.doubleToRawLongBits(d));
                    return 0x01;
                }
                case Float f -> {
                    int64(Double.doubleToRawLongBits(f));
                    return 0x01;
                }
                case String s -> {
                    string(s);
                    return 0x02;
                }
                case Integer i -> {
                    int32(i);
                    return 0x10;
                }
                case Long l -> {
                    int64(l);
                    return 0x12;
                }
                case Short s -> {
                    int32(s);
                    return 0x10;
                }
                case Byte b -> {
                    int32(b);
                    return 0x10;
                }
                case Boolean b -> {
                    byte1(b ? 1 : 0);
                    return 0x08;
                }
                case Map<?, ?> m -> {
                    document(m, depth + 1);
                    return 0x03;
                }
                case Collection<?> c -> {
                    document(c, depth + 1);
                    return 0x04;
                }
                case ObjectId id -> {
                    bytes(id.bytes());
                    return 0x07;
                }
                case Instant t -> {
                    int64(t.toEpochMilli());
                    return 0x09;
                }
                case java.util.Date d -> {
                    int64(d.getTime());
                    return 0x09;
                }
                case java.time.LocalDate d -> {
                    int64(d.atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli());
                    return 0x09;
                }
                case java.time.LocalDateTime d -> {
                    int64(d.toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
                    return 0x09;
                }
                case java.time.OffsetDateTime d -> {
                    int64(d.toInstant().toEpochMilli());
                    return 0x09;
                }
                case java.time.ZonedDateTime d -> {
                    int64(d.toInstant().toEpochMilli());
                    return 0x09;
                }
                case byte[] b -> {
                    int32(b.length);
                    byte1(0);
                    bytes(b);
                    return 0x05;
                }
                case UUID u -> {
                    int32(16);
                    byte1(4);
                    int64(Long.reverseBytes(u.getMostSignificantBits()));
                    int64(Long.reverseBytes(u.getLeastSignificantBits()));
                    return 0x05;
                }
                case BsonBinary b -> {
                    int32(b.data().length);
                    byte1(b.subtype());
                    bytes(b.data());
                    return 0x05;
                }
                case BsonTimestamp t -> {
                    int32(t.increment());
                    int32(t.seconds());
                    return 0x11;
                }
                case BsonRegex r -> {
                    cstring(r.pattern());
                    cstring(r.options());
                    return 0x0B;
                }
                case java.util.regex.Pattern p -> {
                    cstring(p.pattern());
                    cstring(regexOptions(p.flags()));
                    return 0x0B;
                }
                case BigDecimal d -> {
                    decimal128(d);
                    return 0x13;
                }
                case BigInteger b -> {
                    if (b.bitLength() < 64) {
                        int64(b.longValue());
                        return 0x12;
                    }
                    decimal128(new BigDecimal(b));
                    return 0x13;
                }
                case Key k -> {
                    return k == Key.MIN ? 0xFF : 0x7F;
                }
                case Enum<?> e -> {
                    string(e.name());
                    return 0x02;
                }
                case Character c -> {
                    string(c.toString());
                    return 0x02;
                }
                case Optional<?> o -> {
                    return value(o.orElse(null), depth);
                }
                case Record r -> {
                    document(r, depth + 1);
                    return 0x03;
                }
                default -> {
                    if (v.getClass().isArray()) {
                        document(v, depth + 1);
                        return 0x04;
                    }
                    throw new IllegalArgumentException("cannot write a " + v.getClass().getName() + " as BSON");
                }
            }
        }

        private static String regexOptions(int flags) {
            StringBuilder sb = new StringBuilder();
            if ((flags & java.util.regex.Pattern.CASE_INSENSITIVE) != 0) sb.append('i');
            if ((flags & java.util.regex.Pattern.MULTILINE) != 0) sb.append('m');
            if ((flags & java.util.regex.Pattern.DOTALL) != 0) sb.append('s');
            if ((flags & java.util.regex.Pattern.COMMENTS) != 0) sb.append('x');
            return sb.toString();
        }

        private static final BigInteger MAX_COEFFICIENT = BigInteger.TEN.pow(34).subtract(BigInteger.ONE);

        /** IEEE 754-2008 decimal128, binary integer decimal encoding, little-endian low then high word. */
        private void decimal128(BigDecimal d) {
            BigInteger coefficient = d.unscaledValue();
            int exponent = -d.scale();
            boolean negative = coefficient.signum() < 0;
            coefficient = coefficient.abs();
            while (coefficient.compareTo(MAX_COEFFICIENT) > 0) {
                BigInteger[] qr = coefficient.divideAndRemainder(BigInteger.TEN);
                if (qr[1].signum() != 0) {
                    throw new IllegalArgumentException("decimal128 holds 34 significant digits: " + d);
                }
                coefficient = qr[0];
                exponent++;
            }
            while (exponent < -6176 && coefficient.signum() != 0) {           // too small: fewer digits
                BigInteger[] qr = coefficient.divideAndRemainder(BigInteger.TEN);
                if (qr[1].signum() != 0) {
                    throw new IllegalArgumentException("decimal128 exponent is at least -6176: " + d);
                }
                coefficient = qr[0];
                exponent++;
            }
            while (exponent > 6111 && coefficient.compareTo(MAX_COEFFICIENT.divide(BigInteger.TEN)) <= 0) {   // too big an exponent: more digits
                coefficient = coefficient.multiply(BigInteger.TEN);
                exponent--;
            }
            if (exponent < -6176 || exponent > 6111) {
                throw new IllegalArgumentException("decimal128 exponent is -6176..6111: " + d);
            }
            long lo = coefficient.longValue();
            long hi = coefficient.shiftRight(64).longValue() & 0x1FFFFFFFFFFFFL;
            hi |= ((long) (exponent + 6176)) << 49;
            if (negative) {
                hi |= 0x8000000000000000L;
            }
            int64(lo);
            int64(hi);
        }
    }

    // ---- reading ----

    /** The document at the buffer's position (little-endian, advanced past it). @throws IllegalArgumentException for malformed bytes */
    public static Map<String, Object> decode(ByteBuffer in) {
        return new Reader(in).document(0);
    }

    public static Map<String, Object> decode(byte[] bytes) {
        return decode(ByteBuffer.wrap(bytes));
    }

    static final class Reader {
        private final ByteBuffer in;

        Reader(ByteBuffer in) {
            this.in = in.order(ByteOrder.LITTLE_ENDIAN);
        }

        private IllegalArgumentException error(String what) {
            return new IllegalArgumentException("malformed BSON at " + in.position() + ": " + what);
        }

        private void need(int n) {
            if (n < 0 || in.remaining() < n) {
                throw error("needs " + n + " more bytes, " + in.remaining() + " left");
            }
        }

        Map<String, Object> document(int depth) {
            if (depth > MAX_DEPTH) {
                throw error("nested too deeply");
            }
            need(5);
            int start = in.position();
            int length = in.getInt();
            if (length < 5 || length > in.remaining() + 4) {
                throw error("bad document length " + length);
            }
            int end = start + length;
            Map<String, Object> m = new LinkedHashMap<>();
            for (;;) {
                need(1);
                int type = in.get() & 0xFF;
                if (type == 0) {
                    break;
                }
                String key = cstring();
                m.put(key, value(type, depth));
            }
            if (in.position() != end) {
                throw error("document length does not match its content");
            }
            return m;
        }

        private List<Object> array(int depth) {
            if (depth > MAX_DEPTH) {
                throw error("nested too deeply");
            }
            need(5);
            int start = in.position();
            int length = in.getInt();
            if (length < 5 || length > in.remaining() + 4) {
                throw error("bad array length " + length);
            }
            int end = start + length;
            List<Object> l = new ArrayList<>();
            for (;;) {
                need(1);
                int type = in.get() & 0xFF;
                if (type == 0) {
                    break;
                }
                cstring();                                         // the index: trusted to be in order
                l.add(value(type, depth));
            }
            if (in.position() != end) {
                throw error("array length does not match its content");
            }
            return l;
        }

        private String cstring() {
            int start = in.position();
            int end = start;
            int limit = in.limit();
            while (end < limit && in.get(end) != 0) {
                end++;
            }
            if (end == limit) {
                throw error("unterminated key");
            }
            String s = utf8(start, end - start);
            in.position(end + 1);
            return s;
        }

        private String string() {
            need(4);
            int length = in.getInt();
            if (length < 1) {
                throw error("bad string length " + length);
            }
            need(length);
            if (in.get(in.position() + length - 1) != 0) {
                throw error("string not NUL-terminated");
            }
            String s = utf8(in.position(), length - 1);
            in.position(in.position() + length);
            return s;
        }

        private String utf8(int at, int length) {
            if (in.hasArray()) {
                return new String(in.array(), in.arrayOffset() + at, length, StandardCharsets.UTF_8);
            }
            byte[] b = new byte[length];
            in.get(at, b);
            return new String(b, StandardCharsets.UTF_8);
        }

        private Object value(int type, int depth) {
            switch (type) {
                case 0x01 -> {
                    need(8);
                    return in.getDouble();
                }
                case 0x02, 0x0E -> {
                    return string();
                }
                case 0x03 -> {
                    return document(depth + 1);
                }
                case 0x04 -> {
                    return array(depth + 1);
                }
                case 0x05 -> {
                    need(5);
                    int length = in.getInt();
                    int subtype = in.get() & 0xFF;
                    need(length);
                    if (subtype == 2) {                            // old binary: the data has its own length in front
                        if (length < 4 || in.getInt() != length - 4) {
                            throw error("bad old-binary length");
                        }
                        length -= 4;
                        subtype = 0;
                    }
                    byte[] b = new byte[length];
                    in.get(b);
                    if (subtype == 0) {
                        return b;
                    }
                    if (subtype == 4 && length == 16) {
                        ByteBuffer u = ByteBuffer.wrap(b);
                        return new UUID(u.getLong(), u.getLong());
                    }
                    return new BsonBinary(subtype, b);
                }
                case 0x06 -> {
                    return null;                                   // undefined
                }
                case 0x07 -> {
                    need(12);
                    byte[] b = new byte[12];
                    in.get(b);
                    return ObjectId.of(b);
                }
                case 0x08 -> {
                    need(1);
                    return in.get() != 0;
                }
                case 0x09 -> {
                    need(8);
                    return Instant.ofEpochMilli(in.getLong());
                }
                case 0x0A -> {
                    return null;
                }
                case 0x0B -> {
                    return new BsonRegex(cstring(), cstring());
                }
                case 0x0C -> {                                     // DBPointer (deprecated): a string and an ObjectId
                    string();
                    need(12);
                    byte[] b = new byte[12];
                    in.get(b);
                    return ObjectId.of(b);
                }
                case 0x0D -> {
                    return string();                               // JavaScript code
                }
                case 0x0F -> {                                     // code with scope: int32 total, string code, document scope
                    need(4);
                    in.getInt();
                    String code = string();
                    document(depth + 1);
                    return code;
                }
                case 0x10 -> {
                    need(4);
                    return in.getInt();
                }
                case 0x11 -> {
                    need(8);
                    int increment = in.getInt();
                    int seconds = in.getInt();
                    return new BsonTimestamp(seconds, increment);
                }
                case 0x12 -> {
                    need(8);
                    return in.getLong();
                }
                case 0x13 -> {
                    need(16);
                    long lo = in.getLong();
                    long hi = in.getLong();
                    return decimal128(lo, hi);
                }
                case 0xFF -> {
                    return Key.MIN;
                }
                case 0x7F -> {
                    return Key.MAX;
                }
                default -> throw error("unknown type 0x" + Integer.toHexString(type));
            }
        }

        static Object decimal128(long lo, long hi) {
            boolean negative = hi < 0;
            if ((hi & 0x7800000000000000L) == 0x7800000000000000L) {
                return (hi & 0x7C00000000000000L) == 0x7C00000000000000L ? Double.NaN : negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            }
            int exponent;
            BigInteger coefficient;
            if ((hi & 0x6000000000000000L) == 0x6000000000000000L) {
                // the large-coefficient form: more than 34 digits, which MongoDB treats as zero
                exponent = (int) ((hi >>> 47) & 0x3FFF);
                coefficient = BigInteger.ZERO;
            } else {
                exponent = (int) ((hi >>> 49) & 0x3FFF);
                coefficient = new BigInteger(Long.toUnsignedString(hi & 0x1FFFFFFFFFFFFL)).shiftLeft(64).add(new BigInteger(Long.toUnsignedString(lo)));
                if (coefficient.compareTo(Writer.MAX_COEFFICIENT) > 0) {
                    coefficient = BigInteger.ZERO;
                }
            }
            if (negative) {
                coefficient = coefficient.negate();
            }
            return new BigDecimal(coefficient, -(exponent - 6176));
        }
    }
}
