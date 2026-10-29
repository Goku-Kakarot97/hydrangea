package io.hydrangea.postgres;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Conversions between Java values and PostgreSQL's text format. */
final class PgTypes {

    static final int BOOL = 16, BYTEA = 17, INT8 = 20, INT2 = 21, INT4 = 23, FLOAT4 = 700, FLOAT8 = 701, NUMERIC = 1700,
            DATE = 1082, TIME = 1083, TIMESTAMP = 1114, TIMESTAMPTZ = 1184, UUID_OID = 2950;

    /** The array types (their OIDs are fixed in {@code pg_type}) and the type of their elements. */
    private static final Map<Integer, Integer> ARRAY_OF = Map.ofEntries(
            Map.entry(1000, 16), Map.entry(1001, 17), Map.entry(1003, 19), Map.entry(1005, 21), Map.entry(1007, 23), Map.entry(1009, 25), Map.entry(1014, 1042),
            Map.entry(1015, 1043), Map.entry(1016, 20), Map.entry(1021, 700), Map.entry(1022, 701), Map.entry(1028, 26), Map.entry(1115, 1114), Map.entry(1182, 1082),
            Map.entry(1183, 1083), Map.entry(1185, 1184), Map.entry(1187, 1186), Map.entry(1231, 1700), Map.entry(1270, 1266), Map.entry(199, 114), Map.entry(2951, 2950),
            Map.entry(3807, 3802));

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private PgTypes() {
    }

    /** A parameter as it goes on the wire: its type OID (0 = let the server infer it) and its text, or null for SQL NULL. */
    record Param(int oid, byte[] text) {
    }

    static final Param NULL = new Param(0, null);

    static Param encode(Object o) {
        if (o == null) {
            return NULL;
        }
        if (o instanceof String s) {
            return new Param(0, s.getBytes(StandardCharsets.UTF_8));
        }
        if (o instanceof Integer || o instanceof Long || o instanceof Short || o instanceof Byte) {
            int oid = o instanceof Long ? INT8 : o instanceof Integer ? INT4 : INT2;
            return new Param(oid, o.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof Boolean b) {
            return new Param(BOOL, new byte[]{(byte) (b ? 't' : 'f')});
        }
        if (o instanceof Double d) {
            return new Param(FLOAT8, d.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof Float f) {
            return new Param(FLOAT4, f.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof BigDecimal d) {
            return new Param(NUMERIC, d.toPlainString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof byte[] b) {
            byte[] out = new byte[2 + 2 * b.length];
            out[0] = '\\';
            out[1] = 'x';
            for (int i = 0; i < b.length; i++) {
                out[2 + 2 * i] = (byte) HEX[(b[i] >> 4) & 0xf];
                out[3 + 2 * i] = (byte) HEX[b[i] & 0xf];
            }
            return new Param(BYTEA, out);
        }
        if (o instanceof UUID u) {
            return new Param(UUID_OID, u.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof LocalDate d) {
            return new Param(DATE, d.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof LocalTime t) {
            return new Param(TIME, t.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof LocalDateTime t) {
            return new Param(TIMESTAMP, t.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof OffsetDateTime t) {
            return new Param(TIMESTAMPTZ, t.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof Instant t) {
            return new Param(TIMESTAMPTZ, t.toString().getBytes(StandardCharsets.US_ASCII));
        }
        if (o instanceof Collection<?> || (o.getClass().isArray() && !(o instanceof byte[]))) {
            return encodeArray(o);
        }
        if (o instanceof Enum<?> e) {
            return new Param(0, e.name().getBytes(StandardCharsets.UTF_8));
        }
        throw new IllegalArgumentException("cannot send a " + o.getClass().getName() + " as a parameter (use String and cast in SQL)");
    }

    /** A Java collection or array (not {@code byte[]}, which is bytea) as the text of a PostgreSQL array and the array type of its elements (0 when they carry no type: strings, empty). */
    /** The text of this parameter has not been built: the caller thread skipped it because the statement may go in binary; {@link PgClient} builds it if it turns out to be needed. */
    static final byte[] LAZY = new byte[0];

    /**
     * What a query keeps of a parameter until the event loop gets to it: parameters that the caller may change afterwards (a list or array that is reused for the next call, a
     * {@code byte[]}) are copied now, as the text form always was; the rest are immutable and kept as they are. Nested lists are copied all the way down.
     */
    static Object snapshot(Object o) {
        if (o instanceof byte[] b) {
            return b.clone();
        }
        List<Object> items = isArrayValue(o) ? itemsOf(o) : null;
        if (items == null) {
            return o;
        }
        List<Object> copy = new ArrayList<>(items.size());
        for (Object item : items) {
            copy.add(snapshot(item));
        }
        return java.util.Collections.unmodifiableList(copy);
    }

    /** True for the parameters whose text form is worth not building ahead of time: collections and arrays (not {@code byte[]}). */
    static boolean isArrayValue(Object o) {
        return o instanceof Collection<?> || (o != null && o.getClass().isArray() && !(o instanceof byte[]));
    }

    /** What {@link #encode} gives as the type of an array value, without building its text: the array type of the first typed element, 0 if there is none (strings, empty). */
    static int arrayOidOf(Object array) {
        return arrayOidOfElement(firstElementOid(array));
    }

    private static int firstElementOid(Object array) {
        List<Object> items = itemsOf(array);
        for (Object item : items) {
            if (item == null || item instanceof String || item instanceof Enum<?>) {
                continue;
            }
            if (itemsOf(item) != null) {
                int e = firstElementOid(item);
                if (e != 0) {
                    return e;
                }
                continue;
            }
            if (item instanceof Integer) {
                return INT4;
            }
            if (item instanceof Long) {
                return INT8;
            }
            if (item instanceof Short || item instanceof Byte) {
                return INT2;
            }
            return encode(item).oid();
        }
        return 0;
    }

    private static final Map<Integer, Integer> ARRAY_TYPE_OF_ELEMENT = reverse();

    private static Map<Integer, Integer> reverse() {
        Map<Integer, Integer> m = new java.util.HashMap<>();
        for (Map.Entry<Integer, Integer> e : ARRAY_OF.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    private static int arrayOidOfElement(int elementOid) {
        Integer a = ARRAY_TYPE_OF_ELEMENT.get(elementOid);
        return a == null ? 0 : a;
    }

    private static Param encodeArray(Object o) {
        StringBuilder sb = new StringBuilder();
        int[] elementOid = {0};
        appendArray(sb, o, elementOid);
        return new Param(arrayOidOfElement(elementOid[0]), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void appendArray(StringBuilder sb, Object array, int[] elementOid) {
        List<Object> items = itemsOf(array);
        sb.append('{');
        boolean first = true;
        for (Object item : items) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            if (item == null) {
                sb.append("NULL");
            } else if (item instanceof Collection<?> || (item.getClass().isArray() && !(item instanceof byte[]))) {
                appendArray(sb, item, elementOid);
            } else if (item instanceof String str) {                  // the common elements without the detour through a Param and a byte[]
                appendElement(sb, str);
            } else if (item instanceof Integer || item instanceof Long || item instanceof Short) {
                if (elementOid[0] == 0) {
                    elementOid[0] = item instanceof Long ? INT8 : item instanceof Integer ? INT4 : INT2;
                }
                sb.append(item.toString());
            } else {
                Param p = encode(item);
                if (elementOid[0] == 0 && !(item instanceof Enum<?>)) {
                    elementOid[0] = p.oid();
                }
                appendElement(sb, new String(p.text(), StandardCharsets.UTF_8));
            }
        }
        sb.append('}');
    }

    /** Quotes an element when the array syntax needs it: empty, with braces, commas, quotes, backslashes or white space, or reading as NULL. */
    private static void appendElement(StringBuilder sb, String text) {
        boolean plain = !text.isEmpty() && !text.equalsIgnoreCase("NULL");
        for (int i = 0; plain && i < text.length(); i++) {
            char c = text.charAt(i);
            plain = !(c == '{' || c == '}' || c == ',' || c == '"' || c == '\\' || Character.isWhitespace(c));
        }
        if (plain) {
            sb.append(text);
            return;
        }
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        sb.append('"');
    }


    // ---- the binary format ----

    private static final long PG_EPOCH_DAY = 10957;          // 2000-01-01 as a java.time epoch day
    private static final long PG_EPOCH_SECOND = 946_684_800L; // the same, in seconds

    /** Element types of arrays that are sent in binary (as parameters, when the server says which array type a parameter is). */
    private static boolean binaryParamElement(int e) {
        switch (e) {
            case BOOL, INT2, INT4, INT8, FLOAT4, FLOAT8, BYTEA, UUID_OID, NUMERIC, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, 25, 1043, 1042, 19:
                return true;
            default:
                return false;
        }
    }

    /**
     * Types whose results are requested in binary: those for which the text form can be rebuilt exactly from the binary one ({@link #textOfBinary}), so nothing the caller can see
     * depends on the format. Floats, dates and times, json and the rest stay text (a Java double prints differently from PostgreSQL's, a timestamptz depends on the session's zone...).
     */
    static boolean binaryResultOk(int oid) {
        switch (oid) {
            case BOOL, INT2, INT4, INT8, BYTEA, UUID_OID, NUMERIC:
                return true;
            default: {
                Integer e = ARRAY_OF.get(oid);
                if (e == null) {
                    return false;
                }
                switch (e) {
                    case BOOL, INT2, INT4, INT8, NUMERIC, UUID_OID, 25, 1043, 1042, 19:
                        return true;
                    default:
                        return false;
                }
            }
        }
    }

    /** {@code value} in binary for a parameter that the server declared as {@code serverOid}, or null if this class does not do it (the caller then sends the text form). */
    static byte[] encodeBinary(Object value, int serverOid) {
        if (value == null) {
            return null;
        }
        try {
            switch (serverOid) {
                case BOOL:
                    return value instanceof Boolean b ? new byte[]{(byte) (b ? 1 : 0)} : null;
                case INT2: {
                    if (!(value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Byte)) {
                        return null;
                    }
                    long n = ((Number) value).longValue();
                    return n >= Short.MIN_VALUE && n <= Short.MAX_VALUE ? ByteBuffer.allocate(2).putShort((short) n).array() : null;
                }
                case INT4: {
                    if (!(value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Byte)) {
                        return null;
                    }
                    long n = ((Number) value).longValue();
                    return n >= Integer.MIN_VALUE && n <= Integer.MAX_VALUE ? ByteBuffer.allocate(4).putInt((int) n).array() : null;
                }
                case INT8:
                    return value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Byte ? ByteBuffer.allocate(8).putLong(((Number) value).longValue()).array() : null;
                case FLOAT4:
                    return value instanceof Float || value instanceof Double ? ByteBuffer.allocate(4).putFloat(((Number) value).floatValue()).array() : null;
                case FLOAT8:
                    return value instanceof Float || value instanceof Double ? ByteBuffer.allocate(8).putDouble(((Number) value).doubleValue()).array() : null;
                case BYTEA:
                    return value instanceof byte[] b ? b : null;
                case UUID_OID:
                    return value instanceof UUID u ? ByteBuffer.allocate(16).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).array() : null;
                case DATE:
                    return value instanceof LocalDate d ? ByteBuffer.allocate(4).putInt(Math.toIntExact(d.toEpochDay() - PG_EPOCH_DAY)).array() : null;
                case TIME:
                    return value instanceof LocalTime t ? ByteBuffer.allocate(8).putLong((t.toNanoOfDay() + 500) / 1000).array() : null;
                case TIMESTAMP:
                    return value instanceof LocalDateTime t ? ByteBuffer.allocate(8).putLong(micros(t.toEpochSecond(ZoneOffset.UTC) - PG_EPOCH_SECOND, t.getNano())).array() : null;
                case TIMESTAMPTZ: {
                    Instant i = value instanceof Instant x ? x : value instanceof OffsetDateTime o ? o.toInstant() : null;
                    return i == null ? null : ByteBuffer.allocate(8).putLong(micros(i.getEpochSecond() - PG_EPOCH_SECOND, i.getNano())).array();
                }
                case NUMERIC:
                    return value instanceof BigDecimal d ? encodeNumeric(d) : null;
                default: {
                    Integer element = ARRAY_OF.get(serverOid);
                    if (element != null && binaryParamElement(element) && (value instanceof Collection<?> || (value.getClass().isArray() && !(value instanceof byte[])))) {
                        return encodeBinaryArray(value, element);
                    }
                    return null;
                }
            }
        } catch (ArithmeticException e) {
            return null;                        // out of range for the binary form: the text form lets the server say so
        }
    }

    private static long micros(long seconds, int nanos) {
        return Math.addExact(Math.multiplyExact(seconds, 1_000_000L), (nanos + 500) / 1000);      // to the nearest microsecond, as the server does for the text form
    }

    /** The rectangular shape of nested lists/arrays ({@code [2, 3]} for two lists of three), or null if the sublists differ; {@code []} is not an array here (a leaf). */
    private static int[] shapeOf(Object value) {
        List<Object> items = itemsOf(value);
        if (items == null) {
            return new int[0];
        }
        if (items.isEmpty()) {
            return new int[]{0};
        }
        int[] inner = null;
        for (Object item : items) {
            int[] sub = item == null ? new int[0] : shapeOf(item);
            if (sub == null || (inner != null && !java.util.Arrays.equals(inner, sub))) {
                return null;
            }
            inner = sub;
        }
        int[] shape = new int[1 + inner.length];
        shape[0] = items.size();
        System.arraycopy(inner, 0, shape, 1, inner.length);
        return shape;
    }

    /** The elements of a collection or array as a list, without copying when it already is one (null if {@code value} is neither). */
    @SuppressWarnings("unchecked")
    private static List<Object> itemsOf(Object value) {
        if (value instanceof List<?> l) {
            return (List<Object>) l;
        }
        if (value instanceof Collection<?> c) {
            return new ArrayList<>(c);
        }
        if (value instanceof Object[] oa) {
            return java.util.Arrays.asList(oa);
        }
        if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
            int n = java.lang.reflect.Array.getLength(value);
            List<Object> items = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                items.add(java.lang.reflect.Array.get(value, i));
            }
            return items;
        }
        return null;
    }

    /** A growing byte array that big-endian numbers are written into directly. */
    private static final class Bin {
        byte[] a = new byte[256];
        int n;

        void ensure(int more) {
            if (n + more > a.length) {
                a = java.util.Arrays.copyOf(a, Math.max(a.length * 2, n + more));
            }
        }

        void i32(int v) {
            ensure(4);
            a[n++] = (byte) (v >> 24);
            a[n++] = (byte) (v >> 16);
            a[n++] = (byte) (v >> 8);
            a[n++] = (byte) v;
        }

        void i64(long v) {
            i32((int) (v >> 32));
            i32((int) v);
        }

        void raw(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, a, n, b.length);
            n += b.length;
        }
    }

    private static byte[] encodeBinaryArray(Object value, int elementOid) {
        int[] shape = shapeOf(value);
        if (shape == null || shape.length == 0) {
            return null;
        }
        boolean empty = shape[0] == 0;
        for (int i = 1; i < shape.length; i++) {
            if (shape[i] == 0) {
                return null;                   // an empty sublist: PostgreSQL has no such array, let the text form say so
            }
        }
        Bin out = new Bin();
        out.i32(empty ? 0 : shape.length);
        out.i32(0);                            // the flags (has nulls) are filled in below
        out.i32(elementOid);
        if (empty) {
            return java.util.Arrays.copyOf(out.a, out.n);
        }
        for (int d : shape) {
            out.i32(d);
            out.i32(1);
        }
        boolean[] hasNull = {false};
        if (!appendBinaryElements(out, value, elementOid, hasNull)) {
            return null;
        }
        byte[] all = java.util.Arrays.copyOf(out.a, out.n);
        if (hasNull[0]) {
            all[7] = 1;
        }
        return all;
    }

    private static boolean appendBinaryElements(Bin out, Object value, int elementOid, boolean[] hasNull) {
        for (Object item : itemsOf(value)) {
            if (item == null) {
                hasNull[0] = true;
                out.i32(-1);
            } else if (item instanceof Integer i && elementOid == INT4) {
                out.i32(4);
                out.i32(i);
            } else if (item instanceof Long l && elementOid == INT8) {
                out.i32(8);
                out.i64(l);
            } else if (item instanceof String str) {
                if (!isTextElement(elementOid)) {
                    return false;
                }
                byte[] b = str.getBytes(StandardCharsets.UTF_8);
                out.i32(b.length);
                out.raw(b);
            } else if (itemsOf(item) != null) {
                if (!appendBinaryElements(out, item, elementOid, hasNull)) {
                    return false;
                }
            } else {
                byte[] b = encodeBinary(item, elementOid);
                if (b == null) {
                    return false;
                }
                out.i32(b.length);
                out.raw(b);
            }
        }
        return true;
    }

    private static boolean isTextElement(int oid) {
        return oid == 25 || oid == 1043 || oid == 1042 || oid == 19;
    }

    /** A {@code numeric} in the binary form: base-10000 digits, the weight of the first, the sign and the number of digits after the point. */
    static byte[] encodeNumeric(BigDecimal d) {
        int dscale = Math.max(d.scale(), 0);
        BigDecimal abs = d.abs().setScale(dscale);
        String plain = abs.toPlainString();
        int dot = plain.indexOf('.');
        String intPart = dot < 0 ? plain : plain.substring(0, dot);
        String fracPart = dot < 0 ? "" : plain.substring(dot + 1);
        if (intPart.equals("0")) {
            intPart = "";
        }
        // integer digits grouped from the right, fraction digits from the left (padded)
        List<Integer> groups = new ArrayList<>();
        int intGroups = (intPart.length() + 3) / 4;
        for (int g = 0; g < intGroups; g++) {
            int end = intPart.length() - 4 * (intGroups - 1 - g);
            int start = Math.max(0, end - 4);
            groups.add(Integer.parseInt(intPart.substring(start, end)));
        }
        StringBuilder frac = new StringBuilder(fracPart);
        while (frac.length() % 4 != 0) {
            frac.append('0');
        }
        for (int i = 0; i < frac.length(); i += 4) {
            groups.add(Integer.parseInt(frac.substring(i, i + 4)));
        }
        int weight = intGroups - 1;
        int lead = 0;
        while (lead < groups.size() && groups.get(lead) == 0) {          // leading zero groups are dropped and lower the weight
            lead++;
            weight--;
        }
        int endIdx = groups.size();
        while (endIdx > lead && groups.get(endIdx - 1) == 0) {           // trailing zero groups are dropped
            endIdx--;
        }
        List<Integer> digits = groups.subList(lead, endIdx);
        if (digits.isEmpty()) {
            weight = 0;
        }
        ByteBuffer b = ByteBuffer.allocate(8 + 2 * digits.size());
        b.putShort((short) digits.size()).putShort((short) weight).putShort((short) (d.signum() < 0 ? 0x4000 : 0)).putShort((short) dscale);
        for (int digit : digits) {
            b.putShort((short) digit);
        }
        return b.array();
    }

    /** Decodes a binary {@code numeric}: a {@link BigDecimal}, or the text {@code "NaN"} / {@code "Infinity"} / {@code "-Infinity"} like the text form. */
    static Object decodeNumeric(byte[] v) {
        ByteBuffer b = ByteBuffer.wrap(v);
        int nDigits = b.getShort() & 0xffff;
        int weight = b.getShort();
        int sign = b.getShort() & 0xffff;
        int dscale = b.getShort() & 0xffff;
        if (sign == 0xC000) {
            return "NaN";
        }
        if (sign == 0xD000) {
            return "Infinity";
        }
        if (sign == 0xF000) {
            return "-Infinity";
        }
        BigInteger unscaled = BigInteger.ZERO;
        BigInteger base = BigInteger.valueOf(10_000);
        for (int i = 0; i < nDigits; i++) {
            unscaled = unscaled.multiply(base).add(BigInteger.valueOf(b.getShort()));
        }
        BigDecimal value = new BigDecimal(unscaled).scaleByPowerOfTen(4 * (weight - nDigits + 1));
        if (nDigits == 0) {
            value = BigDecimal.ZERO;
        }
        value = value.setScale(Math.max(dscale, 0), java.math.RoundingMode.UNNECESSARY);
        return sign == 0x4000 ? value.negate() : value;
    }

    /** What {@link #decode} gives for the same value in text form, from the binary form of a type {@link #binaryResultOk} accepts. */
    static Object decodeBinary(int oid, byte[] v) {
        switch (oid) {
            case BOOL:
                return v[0] != 0;
            case INT2:
                return ByteBuffer.wrap(v).getShort();
            case INT4:
                return ByteBuffer.wrap(v).getInt();
            case INT8:
                return ByteBuffer.wrap(v).getLong();
            case BYTEA:
                return v.clone();
            case UUID_OID: {
                ByteBuffer b = ByteBuffer.wrap(v);
                return new UUID(b.getLong(), b.getLong());
            }
            case NUMERIC:
                return decodeNumeric(v);
            default: {
                Integer element = ARRAY_OF.get(oid);
                if (element == null) {
                    throw new IllegalArgumentException("no binary decoding for type " + oid);
                }
                return decodeBinaryArray(element, v);
            }
        }
    }

    private static List<Object> decodeBinaryArray(int elementOid, byte[] v) {
        ByteBuffer b = ByteBuffer.wrap(v);
        int ndim = b.getInt();
        b.getInt();                                   // flags
        b.getInt();                                   // the element type, as sent
        if (ndim == 0) {
            return new ArrayList<>();
        }
        int[] sizes = new int[ndim];
        for (int i = 0; i < ndim; i++) {
            sizes[i] = b.getInt();
            b.getInt();                               // lower bound
        }
        return readBinaryDimension(b, elementOid, sizes, 0);
    }

    private static List<Object> readBinaryDimension(ByteBuffer b, int elementOid, int[] sizes, int dim) {
        List<Object> out = new ArrayList<>(sizes[dim]);
        for (int i = 0; i < sizes[dim]; i++) {
            if (dim + 1 < sizes.length) {
                out.add(readBinaryDimension(b, elementOid, sizes, dim + 1));
            } else {
                int len = b.getInt();
                if (len < 0) {
                    out.add(null);
                } else {
                    byte[] e = new byte[len];
                    b.get(e);
                    out.add(isTextElement(elementOid) ? new String(e, StandardCharsets.UTF_8) : decodeBinary(elementOid, e));
                }
            }
        }
        return out;
    }

    /** The text PostgreSQL would have sent for a value that arrived in binary (see {@link #binaryResultOk}), so {@code getString} does not depend on the format. */
    static String textOfBinary(int oid, byte[] v) {
        switch (oid) {
            case BOOL:
                return v[0] != 0 ? "t" : "f";
            case INT2, INT4, INT8:
                return decodeBinary(oid, v).toString();
            case BYTEA: {
                StringBuilder sb = new StringBuilder(2 + 2 * v.length).append("\\x");
                for (byte x : v) {
                    sb.append(HEX[(x >> 4) & 0xf]).append(HEX[x & 0xf]);
                }
                return sb.toString();
            }
            case UUID_OID:
                return decodeBinary(oid, v).toString();
            case NUMERIC: {
                Object n = decodeNumeric(v);
                return n instanceof BigDecimal d ? d.toPlainString() : (String) n;
            }
            default: {
                Integer element = ARRAY_OF.get(oid);
                ByteBuffer b = ByteBuffer.wrap(v);
                int ndim = b.getInt();
                StringBuilder sb = new StringBuilder();
                if (ndim == 0) {
                    return "{}";
                }
                b.getInt();
                b.getInt();
                int[] sizes = new int[ndim];
                int[] lower = new int[ndim];
                boolean decorated = false;
                for (int i = 0; i < ndim; i++) {
                    sizes[i] = b.getInt();
                    lower[i] = b.getInt();
                    decorated |= lower[i] != 1;
                }
                if (decorated) {                      // an array that does not start at 1 says so: [0:2]={...}
                    for (int i = 0; i < ndim; i++) {
                        sb.append('[').append(lower[i]).append(':').append(lower[i] + sizes[i] - 1).append(']');
                    }
                    sb.append('=');
                }
                appendBinaryText(sb, b, element, sizes, 0);
                return sb.toString();
            }
        }
    }

    private static void appendBinaryText(StringBuilder sb, ByteBuffer b, int elementOid, int[] sizes, int dim) {
        sb.append('{');
        for (int i = 0; i < sizes[dim]; i++) {
            if (i > 0) {
                sb.append(',');
            }
            if (dim + 1 < sizes.length) {
                appendBinaryText(sb, b, elementOid, sizes, dim + 1);
            } else {
                int len = b.getInt();
                if (len < 0) {
                    sb.append("NULL");
                } else {
                    byte[] e = new byte[len];
                    b.get(e);
                    appendElement(sb, isTextElement(elementOid) ? new String(e, StandardCharsets.UTF_8) : textOfBinary(elementOid, e));
                }
            }
        }
        sb.append('}');
    }

    static Object decode(int oid, byte[] v) {
        switch (oid) {
            case BOOL:
                return v[0] == 't';
            case INT2:
                return (short) parseLong(v);
            case INT4:
                return (int) parseLong(v);
            case INT8:
                return parseLong(v);
            case FLOAT4:
                return Float.parseFloat(ascii(v));
            case FLOAT8:
                return Double.parseDouble(ascii(v));
            case NUMERIC: {
                String s = ascii(v);
                return s.equals("NaN") || s.equals("Infinity") || s.equals("-Infinity") ? (Object) s : new BigDecimal(s);      // PostgreSQL 14 and later have numeric infinity
            }
            case BYTEA:
                return decodeBytea(v);
            case UUID_OID:
                return UUID.fromString(ascii(v));
            case DATE: {
                String s = ascii(v);
                return special(s) ? s : LocalDate.parse(s);
            }
            case TIME:
                return LocalTime.parse(ascii(v));
            case TIMESTAMP: {
                String s = ascii(v);
                return special(s) ? s : LocalDateTime.parse(s.replace(' ', 'T'));
            }
            case TIMESTAMPTZ: {
                String s = ascii(v);
                return special(s) ? s : parseTimestamptz(s);
            }
            default: {
                Integer element = ARRAY_OF.get(oid);
                if (element != null) {
                    return decodeArray(element, new String(v, StandardCharsets.UTF_8));
                }
                return new String(v, StandardCharsets.UTF_8);
            }
        }
    }

    static boolean isArray(int oid) {
        return ARRAY_OF.containsKey(oid);
    }

    /**
     * The text form of an array, {@code {1,2,3}} or {@code {{"a b",NULL},{"c\"d","e"}}}, as nested lists of decoded elements (a NULL is null). A leading dimension
     * decoration ({@code [0:2]={...}}, for arrays that do not start at 1) is skipped.
     */
    static List<Object> decodeArray(int elementOid, String text) {
        int start = text.indexOf('{');
        if (start < 0) {
            throw new IllegalArgumentException("not an array: " + text);
        }
        int[] pos = {start};
        List<Object> list = parseArray(elementOid, text, pos);
        return list;
    }

    private static List<Object> parseArray(int elementOid, String t, int[] pos) {
        List<Object> out = new ArrayList<>();
        pos[0]++;                                     // the {
        boolean any = false;
        for (;;) {
            if (pos[0] >= t.length()) {
                throw new IllegalArgumentException("unterminated array: " + t);
            }
            char c = t.charAt(pos[0]);
            if (c == '}') {
                pos[0]++;
                return out;
            }
            if (c == ',') {
                pos[0]++;
                continue;
            }
            if (c == '{') {
                out.add(parseArray(elementOid, t, pos));
            } else if (c == '"') {
                StringBuilder sb = new StringBuilder();
                pos[0]++;
                for (; pos[0] < t.length(); pos[0]++) {
                    char d = t.charAt(pos[0]);
                    if (d == '\\') {
                        sb.append(t.charAt(++pos[0]));
                    } else if (d == '"') {
                        break;
                    } else {
                        sb.append(d);
                    }
                }
                pos[0]++;                             // the closing quote
                out.add(decode(elementOid, sb.toString().getBytes(StandardCharsets.UTF_8)));
            } else {
                int end = pos[0];
                while (end < t.length() && t.charAt(end) != ',' && t.charAt(end) != '}') {
                    end++;
                }
                String token = t.substring(pos[0], end).trim();
                pos[0] = end;
                out.add(token.equalsIgnoreCase("NULL") ? null : decode(elementOid, token.getBytes(StandardCharsets.UTF_8)));
            }
            any = true;
        }
    }

    private static boolean special(String s) {
        return s.contains("infinity") || s.endsWith("BC");
    }

    /** {@code 2020-01-02 03:04:05.123456+00}, {@code ...+05:30}, {@code ...-03:30:15}: a zone offset of hours, hours:minutes or hours:minutes:seconds. */
    static OffsetDateTime parseTimestamptz(String s) {
        int t = s.indexOf(' ');
        int z = Math.max(s.lastIndexOf('+'), s.lastIndexOf('-'));
        if (t < 0 || z < t) {
            throw new IllegalArgumentException("bad timestamptz: " + s);
        }
        String offset = s.substring(z);
        if (offset.length() == 3) {
            offset += ":00";
        }
        return OffsetDateTime.of(LocalDateTime.parse(s.substring(0, z).replace(' ', 'T')), ZoneOffset.of(offset));
    }

    static byte[] decodeBytea(byte[] v) {
        if (v.length >= 2 && v[0] == '\\' && v[1] == 'x') {
            byte[] out = new byte[(v.length - 2) / 2];
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) ((Character.digit(v[2 + 2 * i], 16) << 4) | Character.digit(v[3 + 2 * i], 16));
            }
            return out;
        }
        throw new IllegalArgumentException("bytea in the old escape format: set bytea_output = 'hex'");
    }

    static long parseLong(byte[] v) {
        int i = 0;
        boolean neg = false;
        if (v.length > 0 && (v[0] == '-' || v[0] == '+')) {
            neg = v[0] == '-';
            i = 1;
        }
        if (i == v.length) {
            throw new NumberFormatException("empty number");
        }
        long r = 0;
        for (; i < v.length; i++) {
            int d = v[i] - '0';
            if (d < 0 || d > 9) {
                throw new NumberFormatException("not an integer: " + new String(v, StandardCharsets.UTF_8));
            }
            r = Math.addExact(Math.multiplyExact(r, 10), neg ? -d : d);
        }
        return r;
    }

    private static String ascii(byte[] v) {
        return new String(v, StandardCharsets.US_ASCII);
    }
}
