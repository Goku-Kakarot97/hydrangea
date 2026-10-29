package io.hydrangea.redis;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads replies, RESP2 and RESP3, from bytes that arrive in pieces. It keeps its place: an array of a million elements that arrives in a hundred reads is parsed
 * once, not a hundred times, and a large bulk string is copied straight into its final array instead of being gathered in a buffer first.
 * <p>
 * Java values: simple string → {@code String}; bulk string, verbatim string → {@code byte[]}; integer → {@code Long}; null (either kind) → {@code null};
 * array and set → {@code List<Object>}; map → {@code LinkedHashMap<Object, Object>} (a key that is a bulk string becomes a {@code String}, byte[] having no equals);
 * boolean → {@code Boolean}; double → {@code Double}; big number → {@code BigInteger}; error → {@link RedisError}; push → {@link Push}. An attribute (RESP3 metadata
 * that precedes a reply) is read and dropped.
 */
final class RespParser {

    /** A RESP3 push message: out of band, not the answer to a command (pub/sub messages, cache invalidations). */
    record Push(List<Object> items) {
    }

    static final Object INCOMPLETE = new Object();
    private static final Object PUSHED = new Object();     // an aggregate was opened: its elements follow
    private static final int LARGE = 64 * 1024;           // bulk strings from here on are copied into place as they arrive
    private static final int MAX_DEPTH = 32;
    private static final String OK = "OK";

    private static final class Frame {
        final byte kind;       // * ~ % > |
        final int count;       // elements expected (for a map, twice the pairs)
        final ArrayList<Object> items;

        Frame(byte kind, int count) {
            this.kind = kind;
            this.count = count;
            this.items = new ArrayList<>(Math.min(count, 1024));
        }
    }

    private final int maxBulk;
    private final int maxElements;
    private Frame[] stack = new Frame[8];
    private int depth;
    private byte[] bulk;       // a large bulk string being filled
    private int bulkFilled;
    private byte bulkKind;

    RespParser(int maxBulk, int maxElements) {
        this.maxBulk = maxBulk;
        this.maxElements = maxElements;
    }

    /** The next complete reply, or {@link #INCOMPLETE}; everything consumed from {@code in} belongs to replies already returned or to the one being built. */
    Object next(ByteBuffer in) {
        outer:
        for (;;) {
            Object v = bulk != null ? continueBulk(in) : item(in);
            if (v == INCOMPLETE) {
                return INCOMPLETE;
            }
            if (v == PUSHED) {
                continue;
            }
            for (;;) {
                if (depth == 0) {
                    return v;
                }
                Frame f = stack[depth - 1];
                f.items.add(v);
                if (f.items.size() < f.count) {
                    continue outer;
                }
                stack[--depth] = null;
                if (f.kind == '|') {
                    continue outer;   // an attribute is dropped: what follows is the reply it described
                }
                v = finish(f);
            }
        }
    }

    private static Object finish(Frame f) {
        switch (f.kind) {
            case '%' -> {
                Map<Object, Object> m = new LinkedHashMap<>(Math.max(4, f.items.size()));
                for (int i = 0; i + 1 < f.items.size(); i += 2) {
                    Object k = f.items.get(i);
                    m.put(k instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : k, f.items.get(i + 1));
                }
                return m;
            }
            case '>' -> {
                return new Push(f.items);
            }
            default -> {
                return f.items;
            }
        }
    }

    // ---- one item ----

    private Object item(ByteBuffer in) {
        if (!in.hasRemaining()) {
            return INCOMPLETE;
        }
        int p = in.position();
        byte t = in.get(p);
        if ("+-:_#,($!=*~%>|".indexOf(t) < 0) {
            throw bad("unexpected byte 0x" + Integer.toHexString(t & 0xff));   // refused at once: waiting for a line end that may never come helps nobody
        }
        int cr = lineEnd(in, p + 1);
        if (cr < 0) {
            if (in.limit() - p > 1024 + 64) {
                throw new RedisException("a header line of more than 1 KiB: not a Redis server?", false);
            }
            return INCOMPLETE;
        }
        switch (t) {
            case '+' -> {
                String s = simple(in, p + 1, cr);
                in.position(cr + 2);
                return s;
            }
            case '-' -> {
                RedisError e = new RedisError(simple(in, p + 1, cr));
                in.position(cr + 2);
                return e;
            }
            case ':' -> {
                long n = parseLong(in, p + 1, cr);
                in.position(cr + 2);
                return n;
            }
            case '_' -> {
                if (cr != p + 1) {
                    throw bad("null with content");
                }
                in.position(cr + 2);
                return null;
            }
            case '#' -> {
                if (cr != p + 2 || in.get(p + 1) != 't' && in.get(p + 1) != 'f') {
                    throw bad("boolean");
                }
                boolean b = in.get(p + 1) == 't';
                in.position(cr + 2);
                return b;
            }
            case ',' -> {
                double d = parseDouble(simple(in, p + 1, cr));
                in.position(cr + 2);
                return d;
            }
            case '(' -> {
                BigInteger b;
                try {
                    b = new BigInteger(simple(in, p + 1, cr));
                } catch (NumberFormatException e) {
                    throw bad("big number");
                }
                in.position(cr + 2);
                return b;
            }
            case '$', '!', '=' -> {
                return bulkItem(in, t, p, cr);
            }
            case '*', '~', '%', '>', '|' -> {
                long n = parseLong(in, p + 1, cr);
                in.position(cr + 2);
                if (n == -1 && t == '*') {
                    return null;   // RESP2's null array
                }
                if (n < 0 || n > maxElements) {
                    throw bad("an aggregate of " + n + " elements (limit " + maxElements + ")");
                }
                long elements = t == '%' || t == '|' ? n * 2 : n;
                if (elements > maxElements) {
                    throw bad("an aggregate of " + elements + " elements (limit " + maxElements + ")");
                }
                if (elements == 0) {
                    return t == '%' ? new LinkedHashMap<>() : t == '>' ? new Push(new ArrayList<>()) : t == '|' ? PUSHED_EMPTY_ATTRIBUTE : new ArrayList<>(0);
                }
                if (depth >= MAX_DEPTH) {
                    throw bad("nesting deeper than " + MAX_DEPTH);
                }
                if (depth == stack.length) {
                    stack = Arrays.copyOf(stack, depth * 2);
                }
                stack[depth++] = new Frame((byte) t, (int) elements);
                return PUSHED;
            }
            default -> throw bad("unexpected byte 0x" + Integer.toHexString(t & 0xff));
        }
    }

    /** An attribute with no entries: nothing to drop, and the reply follows. */
    private static final Object PUSHED_EMPTY_ATTRIBUTE = PUSHED;

    private Object bulkItem(ByteBuffer in, byte t, int p, int cr) {
        long len = parseLong(in, p + 1, cr);
        if (len == -1 && t == '$') {
            in.position(cr + 2);
            return null;   // RESP2's null bulk string
        }
        if (len < 0 || len > maxBulk) {
            throw bad("a bulk string of " + len + " bytes (limit " + maxBulk + ")");
        }
        int n = (int) len;
        int start = cr + 2;
        int available = in.limit() - start;
        if (available >= n + 2) {
            if (in.get(start + n) != '\r' || in.get(start + n + 1) != '\n') {
                throw bad("a bulk string not followed by CRLF");
            }
            byte[] b = new byte[n];
            in.get(start, b);
            in.position(start + n + 2);
            return wrap(t, b);
        }
        if (n < LARGE) {
            return INCOMPLETE;
        }
        // a large value: allocate it now and fill it as the bytes come
        bulk = new byte[n];
        bulkKind = t;
        bulkFilled = Math.min(available, n);
        in.get(start, bulk, 0, bulkFilled);
        in.position(start + bulkFilled);
        return continueBulk(in);
    }

    private Object continueBulk(ByteBuffer in) {
        int take = Math.min(in.remaining(), bulk.length - bulkFilled);
        if (take > 0) {
            in.get(bulk, bulkFilled, take);
            bulkFilled += take;
        }
        if (bulkFilled < bulk.length || in.remaining() < 2) {
            return INCOMPLETE;
        }
        int p = in.position();
        if (in.get(p) != '\r' || in.get(p + 1) != '\n') {
            throw bad("a bulk string not followed by CRLF");
        }
        in.position(p + 2);
        byte[] b = bulk;
        byte kind = bulkKind;
        bulk = null;
        return wrap(kind, b);
    }

    private static Object wrap(byte kind, byte[] b) {
        return switch (kind) {
            case '!' -> new RedisError(new String(b, StandardCharsets.UTF_8));
            case '=' -> b.length >= 4 ? Arrays.copyOfRange(b, 4, b.length) : b;   // "txt:" or "mkd:" first
            default -> b;
        };
    }

    // ---- lines and numbers ----

    /** The index of the CR of the first CRLF at or after {@code from}, or -1 if there is none yet. */
    private static int lineEnd(ByteBuffer in, int from) {
        int limit = in.limit();
        for (int i = from; i < limit; i++) {
            if (in.get(i) == '\r') {
                if (i + 1 >= limit) {
                    return -1;
                }
                if (in.get(i + 1) != '\n') {
                    throw bad("a CR without its LF");
                }
                return i;
            }
        }
        return -1;
    }

    private static String simple(ByteBuffer in, int from, int to) {
        int n = to - from;
        if (n == 2 && in.get(from) == 'O' && in.get(from + 1) == 'K') {
            return OK;
        }
        byte[] b = new byte[n];
        in.get(from, b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static long parseLong(ByteBuffer in, int from, int to) {
        if (from >= to) {
            throw bad("an empty number");
        }
        int i = from;
        boolean negative = false;
        if (in.get(i) == '-') {
            negative = true;
            i++;
        } else if (in.get(i) == '+') {
            i++;
        }
        if (i >= to || to - i > 19) {
            throw bad("a number of the wrong length");
        }
        long v = 0;
        for (; i < to; i++) {
            int d = in.get(i) - '0';
            if (d < 0 || d > 9) {
                throw bad("a number with a non-digit");
            }
            if (v > (Long.MAX_VALUE - d) / 10) {
                if (negative && v == Long.MAX_VALUE / 10 && d == 8 && i == to - 1) {
                    return Long.MIN_VALUE;
                }
                throw bad("a number out of range");
            }
            v = v * 10 + d;
        }
        return negative ? -v : v;
    }

    private static double parseDouble(String s) {
        return switch (s) {
            case "inf", "+inf" -> Double.POSITIVE_INFINITY;
            case "-inf" -> Double.NEGATIVE_INFINITY;
            case "nan", "-nan" -> Double.NaN;
            default -> {
                try {
                    yield Double.parseDouble(s);
                } catch (NumberFormatException e) {
                    throw bad("double");
                }
            }
        };
    }

    private static RedisException bad(String what) {
        return new RedisException("protocol error: " + what, false);
    }
}
