package io.hydrangea.elasticsearch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small strict JSON reader and writer: the client has no dependency to do it with. Values are plain JDK objects:
 * <ul>
 *   <li>object → {@link Map} (insertion order kept), array → {@link List}, string → {@link String}, {@code true/false} → {@link Boolean}, {@code null} → null</li>
 *   <li>number → {@link Long} when it is written as an integer that fits, else {@link Double} ({@code _seq_no}, {@code _version}, {@code took} stay exact)</li>
 * </ul>
 * Writing accepts those, every {@link Number}, {@link Enum} (its name), {@link java.time.temporal.Temporal} (ISO text), arrays, and {@link Iterable}; anything else is an
 * error rather than a silent {@code toString()}. {@link #obj} builds an object from alternating keys and values.
 * <p>
 * The reader refuses nesting deeper than 256, control characters in strings, trailing text and leading zeros; a {@link #at path lookup} returns null for a missing
 * step instead of throwing, so reading a response is {@code Json.lng(Json.at(root, "hits", "total", "value"))}.
 * <p>
 * <b>Records</b>: {@link #to} turns a parsed value into a record (and {@link #write} turns a record into JSON), by its components, with no library and no
 * annotations beyond an optional {@link Name} for a JSON field named differently from the component ({@code @Json.Name("created_at") Instant createdAt}).
 * Components may be any of: the primitives and their boxes, {@code String}, enums, other records, {@code List}/{@code Set}/{@code Collection}/{@code Map} with
 * their type arguments, arrays, {@code Optional}, {@code Object} (left as parsed), {@code BigDecimal}/{@code BigInteger}, {@code UUID}, {@code byte[]} (base64),
 * and the {@code java.time} types ({@code Instant} also from epoch milliseconds, as Elasticsearch stores dates). A field missing from the JSON is null (0 or false for
 * a primitive); a JSON field the record has no component for is ignored, so a document may carry more than the record reads.
 */
public final class Json {

    private static final int MAX_DEPTH = 256;

    /** The JSON name of a record component when it differs from the component's name. */
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.RECORD_COMPONENT)
    public @interface Name {
        String value();
    }

    private final String s;
    private int pos;

    private Json(String s) {
        this.s = s;
    }

    /** Parses one JSON text. @throws IllegalArgumentException when it is not JSON */
    public static Object parse(String text) {
        Json j = new Json(text);
        j.ws();
        Object v = j.value(0);
        j.ws();
        if (j.pos != text.length()) {
            throw j.error("unexpected trailing characters");
        }
        return v;
    }

    public static Object parse(byte[] utf8) {
        return parse(new String(utf8, StandardCharsets.UTF_8));
    }

    /** An object from alternating keys and values: {@code Json.obj("query", Json.obj("match_all", Json.obj()))}. Null values are kept (written as {@code null}). */
    public static Map<String, Object> obj(Object... keysAndValues) {
        if ((keysAndValues.length & 1) != 0) {
            throw new IllegalArgumentException("keys and values must alternate");
        }
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            m.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return m;
    }

    /** An array. */
    public static List<Object> arr(Object... values) {
        return new ArrayList<>(java.util.Arrays.asList(values));
    }

    // ---- navigation ----

    /** Walks {@code path} (String keys into objects, Integer indexes into arrays) from {@code root}; null if any step is missing or of the wrong kind. */
    public static Object at(Object root, Object... path) {
        Object cur = root;
        for (Object step : path) {
            if (step instanceof String k && cur instanceof Map<?, ?> m) {
                cur = m.get(k);
            } else if (step instanceof Integer i && cur instanceof List<?> l) {
                cur = i >= 0 && i < l.size() ? l.get(i) : null;
            } else {
                return null;
            }
            if (cur == null) {
                return null;
            }
        }
        return cur;
    }

    /** The value as text: a string as is, a number or boolean written; null for null. */
    public static String str(Object v) {
        return v == null ? null : v instanceof String s ? s : v instanceof Map || v instanceof List ? write(v) : v.toString();
    }

    /** The value as a long (a double is truncated, a numeric string parsed); {@code 0} for null. */
    public static long lng(Object v) {
        return v == null ? 0 : v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
    }

    public static double dbl(Object v) {
        return v == null ? 0 : v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
    }

    public static boolean bool(Object v) {
        return v instanceof Boolean b ? b : v != null && Boolean.parseBoolean(v.toString());
    }

    /** The value as an object, or an empty one when it is null or not an object. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> map(Object v) {
        return v instanceof Map ? (Map<String, Object>) v : Map.of();
    }

    /** The value as an array, or an empty one when it is null or not an array. */
    @SuppressWarnings("unchecked")
    public static List<Object> list(Object v) {
        return v instanceof List ? (List<Object>) v : List.of();
    }

    // ---- writing ----

    public static String write(Object v) {
        StringBuilder sb = new StringBuilder(64);
        write(sb, v, 0);
        return sb.toString();
    }

    public static byte[] writeBytes(Object v) {
        return write(v).getBytes(StandardCharsets.UTF_8);
    }

    public static void write(StringBuilder sb, Object v, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("nested too deeply (a cycle?)");
        }
        switch (v) {
            case null -> sb.append("null");
            case String s -> writeString(sb, s);
            case Boolean b -> sb.append(b ? "true" : "false");
            case Integer i -> sb.append((int) i);
            case Long l -> sb.append((long) l);
            case Short i -> sb.append((short) i);
            case Byte i -> sb.append((byte) i);
            case Double d -> writeDouble(sb, d);
            case Float f -> writeDouble(sb, f);
            case Number n -> sb.append(n.toString());             // BigDecimal, BigInteger
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    writeString(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    write(sb, e.getValue(), depth + 1);
                }
                sb.append('}');
            }
            case Iterable<?> it -> {
                sb.append('[');
                boolean first = true;
                for (Object o : it) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    write(sb, o, depth + 1);
                }
                sb.append(']');
            }
            case Object[] a -> write(sb, java.util.Arrays.asList(a), depth);
            case int[] a -> {
                sb.append('[');
                for (int i = 0; i < a.length; i++) {
                    sb.append(i == 0 ? "" : ",").append(a[i]);
                }
                sb.append(']');
            }
            case long[] a -> {
                sb.append('[');
                for (int i = 0; i < a.length; i++) {
                    sb.append(i == 0 ? "" : ",").append(a[i]);
                }
                sb.append(']');
            }
            case double[] a -> {
                sb.append('[');
                for (int i = 0; i < a.length; i++) {
                    sb.append(i == 0 ? "" : ",");
                    writeDouble(sb, a[i]);
                }
                sb.append(']');
            }
            case Enum<?> e -> writeString(sb, e.name());
            case java.time.temporal.Temporal t -> writeString(sb, t.toString());
            case java.time.Duration d -> writeString(sb, d.toString());
            case Character c -> writeString(sb, c.toString());
            case java.util.UUID u -> writeString(sb, u.toString());
            case byte[] b -> writeString(sb, java.util.Base64.getEncoder().encodeToString(b));
            case java.util.Optional<?> o -> write(sb, o.orElse(null), depth);
            case Record r -> write(sb, RECORDS.fields(r), depth);
            default -> throw new IllegalArgumentException("cannot write a " + v.getClass().getName() + " as JSON");
        }
    }

    // ---- records ----

    private static final io.hydrangea.codec.Records RECORDS = new io.hydrangea.codec.Records(Name.class);

    /** A parsed value as a {@code type}: a record, a {@code List<Record>} is {@link #toList}, a scalar or anything else the class javadoc lists. */
    public static <T> T to(Class<T> type, Object json) {
        return RECORDS.to(type, json);
    }

    /** A parsed array as a list of {@code element}. */
    public static <T> List<T> toList(Class<T> element, Object jsonArray) {
        return RECORDS.toList(element, jsonArray);
    }

    /** A record (or any value {@link #write} accepts) as the parsed form: a {@code Map} for a record, with nested records, lists and maps converted the same way. */
    public static Object from(Object value) {
        return value == null ? null : parse(write(value));
    }

    private static void writeDouble(StringBuilder sb, double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new IllegalArgumentException("NaN and infinity are not JSON");
        }
        if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            sb.append((long) d);                                   // 3.0 → 3: what ES does with its own integers
        } else {
            sb.append(d);
        }
    }

    public static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            String esc;
            switch (c) {
                case '"' -> esc = "\\\"";
                case '\\' -> esc = "\\\\";
                case '\n' -> esc = "\\n";
                case '\r' -> esc = "\\r";
                case '\t' -> esc = "\\t";
                case '\b' -> esc = "\\b";
                case '\f' -> esc = "\\f";
                default -> {
                    if (c < 0x20 || c == 0x2028 || c == 0x2029) {
                        esc = String.format("\\u%04x", (int) c);
                    } else {
                        continue;
                    }
                }
            }
            sb.append(s, start, i).append(esc);
            start = i + 1;
        }
        sb.append(s, start, s.length()).append('"');
    }

    // ---- reading ----

    private IllegalArgumentException error(String what) {
        return new IllegalArgumentException("invalid JSON at " + pos + ": " + what);
    }

    private void ws() {
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                return;
            }
            pos++;
        }
    }

    private Object value(int depth) {
        if (depth > MAX_DEPTH) {
            throw error("nested too deeply");
        }
        if (pos >= s.length()) {
            throw error("unexpected end");
        }
        char c = s.charAt(pos);
        switch (c) {
            case '{': {
                pos++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (peek('}')) {
                    pos++;
                    return m;
                }
                for (;;) {
                    ws();
                    if (!peek('"')) {
                        throw error("object key expected");
                    }
                    String k = string();
                    ws();
                    expect(':');
                    ws();
                    m.put(k, value(depth + 1));
                    ws();
                    if (peek(',')) {
                        pos++;
                    } else {
                        expect('}');
                        return m;
                    }
                }
            }
            case '[': {
                pos++;
                List<Object> l = new ArrayList<>();
                ws();
                if (peek(']')) {
                    pos++;
                    return l;
                }
                for (;;) {
                    ws();
                    l.add(value(depth + 1));
                    ws();
                    if (peek(',')) {
                        pos++;
                    } else {
                        expect(']');
                        return l;
                    }
                }
            }
            case '"':
                return string();
            case 't':
                return literal("true", Boolean.TRUE);
            case 'f':
                return literal("false", Boolean.FALSE);
            case 'n':
                return literal("null", null);
            default:
                return number();
        }
    }

    private boolean peek(char c) {
        return pos < s.length() && s.charAt(pos) == c;
    }

    private void expect(char c) {
        if (!peek(c)) {
            throw error("'" + c + "' expected");
        }
        pos++;
    }

    private Object literal(String word, Object v) {
        if (!s.startsWith(word, pos)) {
            throw error("unknown literal");
        }
        pos += word.length();
        return v;
    }

    private Number number() {
        int start = pos;
        if (peek('-')) {
            pos++;
        }
        if (peek('0')) {
            pos++;
        } else if (pos < s.length() && s.charAt(pos) >= '1' && s.charAt(pos) <= '9') {
            digits();
        } else {
            throw error("value expected");
        }
        boolean integral = true;
        if (peek('.')) {
            pos++;
            integral = false;
            if (!digits()) {
                throw error("digits expected after '.'");
            }
        }
        if (peek('e') || peek('E')) {
            pos++;
            integral = false;
            if (peek('+') || peek('-')) {
                pos++;
            }
            if (!digits()) {
                throw error("digits expected in exponent");
            }
        }
        String text = s.substring(start, pos);
        if (integral && text.length() <= 19) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException overflow) {
                // 19 digits past Long.MAX_VALUE: a double then
            }
        }
        return Double.parseDouble(text);
    }

    private boolean digits() {
        int start = pos;
        while (pos < s.length() && s.charAt(pos) >= '0' && s.charAt(pos) <= '9') {
            pos++;
        }
        return pos > start;
    }

    private String string() {
        expect('"');
        int plain = pos;
        while (plain < s.length()) {                                // the common case: no escape, one substring
            char c = s.charAt(plain);
            if (c == '"') {
                String r = s.substring(pos, plain);
                pos = plain + 1;
                return r;
            }
            if (c == '\\' || c < 0x20) {
                break;
            }
            plain++;
        }
        StringBuilder sb = new StringBuilder(plain - pos + 16);
        sb.append(s, pos, plain);
        pos = plain;
        while (pos < s.length()) {
            char c = s.charAt(pos++);
            if (c == '"') {
                return sb.toString();
            }
            if (c < 0x20) {
                throw error("control character in string");
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (pos >= s.length()) {
                break;
            }
            char e = s.charAt(pos++);
            switch (e) {
                case '"', '\\', '/' -> sb.append(e);
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (pos + 4 > s.length()) {
                        throw error("bad \\u escape");
                    }
                    int cp = 0;
                    for (int i = 0; i < 4; i++) {
                        int d = Character.digit(s.charAt(pos + i), 16);
                        if (d < 0) {
                            throw error("bad \\u escape");
                        }
                        cp = cp << 4 | d;
                    }
                    sb.append((char) cp);
                    pos += 4;
                }
                default -> throw error("bad escape");
            }
        }
        throw error("unterminated string");
    }
}
