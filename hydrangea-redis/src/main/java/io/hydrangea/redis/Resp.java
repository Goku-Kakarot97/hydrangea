package io.hydrangea.redis;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Encoding of commands: every command is an array of bulk strings (RESP2 and RESP3 agree on this). */
final class Resp {

    private Resp() {
    }

    /**
     * The bytes of a command. An argument is a {@code byte[]} (sent as is), a {@link CharSequence} (UTF-8), an integer type or a {@link Double} / {@link Float}
     * (decimal text), a {@link BigDecimal}, an {@link Enum} (its name) or a {@link Boolean} (1 / 0). Anything else, and null, is an {@link IllegalArgumentException}.
     */
    static byte[] encode(Object[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("a command needs at least its name");
        }
        byte[][] parts = new byte[args.length][];
        long total = 1 + digits(args.length) + 2;
        for (int i = 0; i < args.length; i++) {
            byte[] b = bytesOf(args[i]);
            parts[i] = b;
            total += 1 + digits(b.length) + 2 + b.length + 2;
        }
        if (total > Integer.MAX_VALUE - 16) {
            throw new IllegalArgumentException("command of " + total + " bytes is too large");
        }
        byte[] out = new byte[(int) total];
        int p = 0;
        out[p++] = '*';
        p = number(out, p, args.length);
        for (byte[] b : parts) {
            out[p++] = '$';
            p = number(out, p, b.length);
            System.arraycopy(b, 0, out, p, b.length);
            p += b.length;
            out[p++] = '\r';
            out[p++] = '\n';
        }
        return out;
    }

    static byte[] bytesOf(Object a) {
        if (a instanceof byte[] b) {
            return b;
        }
        if (a instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        if (a instanceof CharSequence s) {
            return s.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (a instanceof Long || a instanceof Integer || a instanceof Short || a instanceof Byte) {
            return Long.toString(((Number) a).longValue()).getBytes(StandardCharsets.US_ASCII);
        }
        if (a instanceof Double d) {
            return decimal(d).getBytes(StandardCharsets.US_ASCII);
        }
        if (a instanceof Float f) {
            return decimal(f.doubleValue()).getBytes(StandardCharsets.US_ASCII);
        }
        if (a instanceof BigDecimal d) {
            return d.toPlainString().getBytes(StandardCharsets.US_ASCII);
        }
        if (a instanceof Boolean b) {
            return new byte[] {(byte) (b ? '1' : '0')};
        }
        if (a instanceof Enum<?> e) {
            return e.name().getBytes(StandardCharsets.UTF_8);
        }
        if (a == null) {
            throw new IllegalArgumentException("a command argument may not be null");
        }
        throw new IllegalArgumentException("cannot send " + a.getClass().getName() + " as a command argument (use byte[], String or a number)");
    }

    /** Redis reads {@code inf}, {@code -inf} for infinities and plain decimal text for the rest. */
    private static String decimal(double d) {
        if (Double.isNaN(d)) {
            throw new IllegalArgumentException("NaN is not a Redis number");
        }
        return Double.isInfinite(d) ? (d > 0 ? "inf" : "-inf") : Double.toString(d);
    }

    private static int digits(int n) {
        return n < 10 ? 1 : n < 100 ? 2 : n < 1000 ? 3 : n < 10_000 ? 4 : n < 100_000 ? 5 : n < 1_000_000 ? 6 : n < 10_000_000 ? 7 : n < 100_000_000 ? 8 : n < 1_000_000_000 ? 9 : 10;
    }

    private static int number(byte[] out, int p, int n) {
        int d = digits(n);
        int end = p + d;
        for (int i = end - 1; i >= p; i--) {
            out[i] = (byte) ('0' + n % 10);
            n /= 10;
        }
        out[end] = '\r';
        out[end + 1] = '\n';
        return end + 2;
    }

    static ByteBuffer buffer(byte[] encoded) {
        return ByteBuffer.wrap(encoded);
    }
}
