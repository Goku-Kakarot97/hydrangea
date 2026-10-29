package io.hydrangea.amqp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AMQP 0-9-1 primitive types (spec section 4.2): encoding into a growing byte array, decoding from a bounded buffer. */
final class Wire {

    private Wire() {
    }

    /** Appends big-endian values. Bit fields are packed with {@link #bits}. */
    static final class Writer {
        private byte[] buf;
        private int len;

        Writer(int capacity) {
            buf = new byte[Math.max(16, capacity)];
        }

        int size() {
            return len;
        }

        private void ensure(int more) {
            if (len + more > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + more));
            }
        }

        Writer octet(int v) {
            ensure(1);
            buf[len++] = (byte) v;
            return this;
        }

        Writer shortInt(int v) {
            ensure(2);
            buf[len++] = (byte) (v >>> 8);
            buf[len++] = (byte) v;
            return this;
        }

        Writer longInt(long v) {
            ensure(4);
            buf[len++] = (byte) (v >>> 24);
            buf[len++] = (byte) (v >>> 16);
            buf[len++] = (byte) (v >>> 8);
            buf[len++] = (byte) v;
            return this;
        }

        Writer longLong(long v) {
            ensure(8);
            for (int s = 56; s >= 0; s -= 8) {
                buf[len++] = (byte) (v >>> s);
            }
            return this;
        }

        /** Up to eight bit fields in one octet, the first argument in bit 0. */
        Writer bits(boolean... flags) {
            int b = 0;
            for (int i = 0; i < flags.length; i++) {
                if (flags[i]) {
                    b |= 1 << i;
                }
            }
            return octet(b);
        }

        Writer shortStr(String s) {
            byte[] b = s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
            if (b.length > 255) {
                throw new IllegalArgumentException("a short string holds at most 255 bytes (UTF-8), got " + b.length);
            }
            octet(b.length);
            return raw(b, 0, b.length);
        }

        Writer longStr(byte[] b) {
            longInt(b.length);
            return raw(b, 0, b.length);
        }

        Writer longStr(String s) {
            return longStr(s.getBytes(StandardCharsets.UTF_8));
        }

        Writer raw(byte[] b, int off, int n) {
            ensure(n);
            System.arraycopy(b, off, buf, len, n);
            len += n;
            return this;
        }

        /** Overwrites four bytes already written (for length prefixes). */
        void putInt(int at, int v) {
            buf[at] = (byte) (v >>> 24);
            buf[at + 1] = (byte) (v >>> 16);
            buf[at + 2] = (byte) (v >>> 8);
            buf[at + 3] = (byte) v;
        }

        Writer table(Map<String, ?> t) {
            if (t == null || t.isEmpty()) {
                return longInt(0);
            }
            int at = len;
            longInt(0);
            for (Map.Entry<String, ?> e : t.entrySet()) {
                shortStr(e.getKey());
                value(e.getValue());
            }
            putInt(at, len - at - 4);
            return this;
        }

        private void value(Object v) {
            switch (v) {
                case null -> octet('V');
                case Boolean b -> octet('t').octet(b ? 1 : 0);
                case Byte b -> octet('b').octet(b);
                case Short s -> octet('s').shortInt(s);
                case Integer i -> octet('I').longInt(i);
                case Long l -> octet('l').longLong(l);
                case Float f -> octet('f').longInt(Float.floatToIntBits(f));
                case Double d -> octet('d').longLong(Double.doubleToLongBits(d));
                case String s -> octet('S').longStr(s);
                case byte[] b -> octet('x').longStr(b);
                case Instant t -> octet('T').longLong(t.getEpochSecond());
                case BigDecimal d -> {
                    BigInteger unscaled = d.unscaledValue();
                    if (d.scale() < 0 || d.scale() > 255 || unscaled.bitLength() > 31) {
                        throw new IllegalArgumentException("decimal out of range for AMQP: " + d);
                    }
                    octet('D').octet(d.scale()).longInt(unscaled.intValue());
                }
                case Map<?, ?> m -> {
                    octet('F');
                    @SuppressWarnings("unchecked")
                    Map<String, ?> typed = (Map<String, ?>) m;
                    table(typed);
                }
                case List<?> l -> {
                    octet('A');
                    int at = len;
                    longInt(0);
                    for (Object o : l) {
                        value(o);
                    }
                    putInt(at, len - at - 4);
                }
                default -> throw new IllegalArgumentException("cannot encode " + v.getClass().getName() + " as an AMQP field value");
            }
        }

        byte[] toByteArray() {
            return Arrays.copyOf(buf, len);
        }

        byte[] array() {
            return buf;
        }
    }

    /** Reads big-endian values from a buffer; reading past the end is a protocol error, never an {@link IndexOutOfBoundsException}. */
    static final class Reader {
        private final ByteBuffer b;

        Reader(ByteBuffer b) {
            this.b = b;
        }

        int remaining() {
            return b.remaining();
        }

        private void need(int n) {
            if (n < 0 || b.remaining() < n) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "frame too short", false);
            }
        }

        int octet() {
            need(1);
            return b.get() & 0xff;
        }

        int shortInt() {
            need(2);
            return b.getShort() & 0xffff;
        }

        long longInt() {
            need(4);
            return b.getInt() & 0xffffffffL;
        }

        long longLong() {
            need(8);
            return b.getLong();
        }

        /** Reads one octet of packed bit fields. */
        boolean[] bits(int n) {
            int o = octet();
            boolean[] r = new boolean[n];
            for (int i = 0; i < n; i++) {
                r[i] = (o & (1 << i)) != 0;
            }
            return r;
        }

        String shortStr() {
            int n = octet();
            need(n);
            String s = new String(b.array(), b.arrayOffset() + b.position(), n, StandardCharsets.UTF_8);
            b.position(b.position() + n);
            return s;
        }

        byte[] longStrBytes() {
            long n = longInt();
            if (n > b.remaining()) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "string longer than its frame", false);
            }
            byte[] r = new byte[(int) n];
            b.get(r);
            return r;
        }

        String longStr() {
            return new String(longStrBytes(), StandardCharsets.UTF_8);
        }

        private int depth;

        Map<String, Object> table() {
            if (++depth > 16) {
                throw new AmqpException(AmqpException.SYNTAX_ERROR, "field table nested too deeply", false);
            }
            try {
                return tableBody();
            } finally {
                depth--;
            }
        }

        private Map<String, Object> tableBody() {
            long n = longInt();
            if (n > b.remaining()) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "table longer than its frame", false);
            }
            Map<String, Object> m = new LinkedHashMap<>();
            int end = b.position() + (int) n;
            while (b.position() < end) {
                String name = shortStr();
                m.put(name, value(end));
            }
            if (b.position() != end) {
                throw new AmqpException(AmqpException.FRAME_ERROR, "table entry crosses the table end", false);
            }
            return m;
        }

        private Object value(int limit) {
            int type = octet();
            return switch (type) {
                case 't' -> octet() != 0;
                case 'b' -> { need(1); yield b.get(); }
                case 'B' -> (short) octet();
                case 's' -> { need(2); yield b.getShort(); }
                case 'u' -> shortInt();
                case 'I' -> { need(4); yield b.getInt(); }
                case 'i' -> longInt();
                case 'l' -> longLong();
                case 'f' -> { need(4); yield b.getFloat(); }
                case 'd' -> { need(8); yield b.getDouble(); }
                case 'D' -> {
                    int scale = octet();
                    need(4);
                    yield new BigDecimal(BigInteger.valueOf(b.getInt()), scale);
                }
                case 'S' -> longStr();
                case 'x' -> longStrBytes();
                case 'T' -> Instant.ofEpochSecond(longLong());
                case 'F' -> table();
                case 'A' -> {
                    if (++depth > 16) {
                        throw new AmqpException(AmqpException.SYNTAX_ERROR, "field array nested too deeply", false);
                    }
                    long n = longInt();
                    if (n > b.remaining()) {
                        throw new AmqpException(AmqpException.FRAME_ERROR, "array longer than its frame", false);
                    }
                    int end = b.position() + (int) n;
                    List<Object> l = new ArrayList<>();
                    while (b.position() < end) {
                        l.add(value(end));
                    }
                    depth--;
                    yield l;
                }
                case 'V' -> null;
                default -> throw new AmqpException(AmqpException.SYNTAX_ERROR, "unknown field type " + (char) type, false);
            };
        }

        /** Skips n bytes. */
        void skip(int n) {
            need(n);
            b.position(b.position() + n);
        }
    }
}
