package io.hydrangea.mysql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One row. The bytes the server sent stay in one array and a column is read from it when asked for, so a column nobody reads costs nothing.
 * The same getters work for rows of the text protocol (a call without parameters) and of the binary protocol (a prepared statement).
 * Indexes start at 0. A NULL column gives {@code null} from the getters that return objects and 0 or false from the ones that return
 * primitives (as JDBC does): ask {@link #isNull} first when the column may be NULL.
 */
public final class MyRow implements io.hydrangea.sql.SqlRow {

    /** What the rows of one result share: the columns and the name lookup. */
    static final class Meta {
        final MyColumn[] columns;
        final Map<String, Integer> byName = new HashMap<>();

        Meta(List<MyColumn> list) {
            columns = list.toArray(new MyColumn[0]);
            for (int i = 0; i < columns.length; i++) {
                byName.putIfAbsent(columns[i].name().toLowerCase(java.util.Locale.ROOT), i);
            }
        }
    }

    // MySQL enum_field_types
    static final int T_DECIMAL = 0, T_TINY = 1, T_SHORT = 2, T_LONG = 3, T_FLOAT = 4, T_DOUBLE = 5, T_NULL = 6, T_TIMESTAMP = 7, T_LONGLONG = 8,
            T_INT24 = 9, T_DATE = 10, T_TIME = 11, T_DATETIME = 12, T_YEAR = 13, T_NEWDATE = 14, T_VARCHAR = 15, T_BIT = 16, T_JSON = 245,
            T_NEWDECIMAL = 246, T_ENUM = 247, T_SET = 248, T_TINY_BLOB = 249, T_MEDIUM_BLOB = 250, T_LONG_BLOB = 251, T_BLOB = 252, T_VAR_STRING = 253,
            T_STRING = 254, T_GEOMETRY = 255;

    private final Meta meta;
    private final byte[] data;
    private final int[] pos;          // offset and length of each column; length -1 is NULL
    private final boolean binary;

    private MyRow(Meta meta, byte[] data, int[] pos, boolean binary) {
        this.meta = meta;
        this.data = data;
        this.pos = pos;
        this.binary = binary;
    }

    // ---- building (from the packet of a row) ----

    static MyRow text(Meta meta, byte[] d) {
        int n = meta.columns.length;
        int[] pos = new int[2 * n];
        int p = 0;
        for (int i = 0; i < n; i++) {
            int b = d[p] & 0xFF;
            if (b == 0xFB) {
                pos[2 * i] = p;
                pos[2 * i + 1] = -1;
                p++;
                continue;
            }
            long len;
            if (b < 251) {
                len = b;
                p++;
            } else if (b == 0xFC) {
                len = (d[p + 1] & 0xFF) | (d[p + 2] & 0xFF) << 8;
                p += 3;
            } else if (b == 0xFD) {
                len = (d[p + 1] & 0xFF) | (d[p + 2] & 0xFF) << 8 | (d[p + 3] & 0xFF) << 16;
                p += 4;
            } else if (b == 0xFE) {
                len = leLong(d, p + 1);
                p += 9;
            } else {
                throw new MyException("protocol error: bad length-encoded value in a row", false);
            }
            if (len < 0 || p + len > d.length) {
                throw new MyException("protocol error: a column runs past the end of its row", false);
            }
            pos[2 * i] = p;
            pos[2 * i + 1] = (int) len;
            p += (int) len;
        }
        return new MyRow(meta, d, pos, false);
    }

    static MyRow binary(Meta meta, byte[] d) {
        int n = meta.columns.length;
        int[] pos = new int[2 * n];
        int bitmap = (n + 7 + 2) / 8;
        int p = 1 + bitmap;
        for (int i = 0; i < n; i++) {
            int bit = i + 2;
            if ((d[1 + (bit >> 3)] & (1 << (bit & 7))) != 0) {
                pos[2 * i] = p;
                pos[2 * i + 1] = -1;
                continue;
            }
            int len;
            switch (meta.columns[i].type()) {
                case T_TINY -> len = 1;
                case T_SHORT, T_YEAR -> len = 2;
                case T_LONG, T_INT24, T_FLOAT -> len = 4;
                case T_LONGLONG, T_DOUBLE -> len = 8;
                case T_DATE, T_DATETIME, T_TIMESTAMP, T_TIME, T_NEWDATE -> len = 1 + (d[p] & 0xFF);       // the length byte stays part of the value
                default -> {
                    int b = d[p] & 0xFF;
                    long l;
                    if (b < 251) {
                        l = b;
                        p += 1;
                    } else if (b == 0xFC) {
                        l = (d[p + 1] & 0xFF) | (d[p + 2] & 0xFF) << 8;
                        p += 3;
                    } else if (b == 0xFD) {
                        l = (d[p + 1] & 0xFF) | (d[p + 2] & 0xFF) << 8 | (d[p + 3] & 0xFF) << 16;
                        p += 4;
                    } else if (b == 0xFE) {
                        l = leLong(d, p + 1);
                        p += 9;
                    } else {
                        throw new MyException("protocol error: bad length-encoded value in a row", false);
                    }
                    if (l < 0 || l > d.length) {
                        throw new MyException("protocol error: a column runs past the end of its row", false);
                    }
                    len = (int) l;
                }
            }
            if (p + len > d.length) {
                throw new MyException("protocol error: a column runs past the end of its row", false);
            }
            pos[2 * i] = p;
            pos[2 * i + 1] = len;
            p += len;
        }
        return new MyRow(meta, d, pos, true);
    }

    private static long leLong(byte[] d, int p) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = v << 8 | (d[p + i] & 0xFF);
        }
        return v;
    }

    private static long le(byte[] d, int p, int len) {
        long v = 0;
        for (int i = len - 1; i >= 0; i--) {
            v = v << 8 | (d[p + i] & 0xFF);
        }
        return v;
    }

    // ---- access ----

    public int size() {
        return meta.columns.length;
    }

    public List<MyColumn> columns() {
        return List.of(meta.columns);
    }

    /** The index of a column by name (case-insensitive; the first of several with that name), or -1. */
    public int indexOf(String name) {
        Integer i = meta.byName.get(name.toLowerCase(java.util.Locale.ROOT));
        return i == null ? -1 : i;
    }

    private int idx(String name) {
        int i = indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("no column named " + name);
        }
        return i;
    }

    public boolean wasBinary() {
        return binary;
    }

    public boolean isNull(int i) {
        return pos[2 * i + 1] < 0;
    }

    public boolean isNull(String name) {
        return isNull(idx(name));
    }

    private int off(int i) {
        return pos[2 * i];
    }

    /** The value of a numeric column, or 0 for NULL (check {@link #isNull}). */
    public long getLong(int i) {
        if (isNull(i)) {
            return 0;
        }
        int o = off(i), len = pos[2 * i + 1];
        MyColumn c = meta.columns[i];
        if (!binary) {
            return parseLong(o, len, c);
        }
        return switch (c.type()) {
            case T_TINY -> c.unsigned() ? data[o] & 0xFF : data[o];
            case T_SHORT, T_YEAR -> c.unsigned() || c.type() == T_YEAR ? le(data, o, 2) : (short) le(data, o, 2);
            case T_LONG, T_INT24 -> c.unsigned() ? le(data, o, 4) : (int) le(data, o, 4);
            case T_LONGLONG -> le(data, o, 8);
            case T_FLOAT -> (long) Float.intBitsToFloat((int) le(data, o, 4));
            case T_DOUBLE -> (long) Double.longBitsToDouble(le(data, o, 8));
            case T_BIT -> le(data, o, Math.min(len, 8));      // the bytes of a BIT are big-endian: see getBytes
            default -> parseLong(o, len, c);
        };
    }

    private long parseLong(int o, int len, MyColumn c) {
        if (len == 0) {
            throw new NumberFormatException("empty value in column " + c.name());
        }
        int p = o, end = o + len;
        boolean neg = false;
        if (data[p] == '-') {
            neg = true;
            p++;
        } else if (data[p] == '+') {
            p++;
        }
        long v = 0;
        for (; p < end; p++) {
            int d = data[p] - '0';
            if (d < 0 || d > 9) {
                if (data[p] == '.') {          // 12.0 or a DECIMAL: the integer part, like a cast
                    break;
                }
                throw new NumberFormatException("not a number in column " + c.name() + ": " + new String(data, o, len, StandardCharsets.ISO_8859_1));
            }
            v = v * 10 + d;      // an unsigned BIGINT above Long.MAX_VALUE wraps around, as in two's complement
        }
        return neg ? -v : v;
    }

    public long getLong(String name) {
        return getLong(idx(name));
    }

    public int getInt(int i) {
        return Math.toIntExact(getLong(i));
    }

    public int getInt(String name) {
        return getInt(idx(name));
    }

    public boolean getBoolean(int i) {
        if (isNull(i)) {
            return false;
        }
        if (!binary && meta.columns[i].type() != T_TINY && meta.columns[i].type() != T_BIT) {
            String s = getString(i);
            return !(s.equals("0") || s.equalsIgnoreCase("false") || s.isEmpty());
        }
        return getLong(i) != 0;
    }

    public boolean getBoolean(String name) {
        return getBoolean(idx(name));
    }

    public double getDouble(int i) {
        if (isNull(i)) {
            return 0;
        }
        int o = off(i), len = pos[2 * i + 1];
        if (binary) {
            switch (meta.columns[i].type()) {
                case T_DOUBLE:
                    return Double.longBitsToDouble(le(data, o, 8));
                case T_FLOAT:
                    return Float.intBitsToFloat((int) le(data, o, 4));
                case T_TINY, T_SHORT, T_LONG, T_INT24, T_LONGLONG, T_YEAR:
                    return meta.columns[i].unsigned() && meta.columns[i].type() == T_LONGLONG ? new BigInteger(Long.toUnsignedString(getLong(i))).doubleValue() : getLong(i);
                default:
                    break;
            }
        }
        return Double.parseDouble(new String(data, o, len, StandardCharsets.ISO_8859_1));
    }

    public double getDouble(String name) {
        return getDouble(idx(name));
    }

    public float getFloat(int i) {
        return (float) getDouble(i);
    }

    public String getString(int i) {
        if (isNull(i)) {
            return null;
        }
        int o = pos[2 * i], len = pos[2 * i + 1];
        if (!binary) {
            return new String(data, o, len, StandardCharsets.UTF_8);
        }
        switch (meta.columns[i].type()) {
            case T_TINY, T_SHORT, T_LONG, T_INT24, T_YEAR:
                return Long.toString(getLong(i));
            case T_LONGLONG:
                return meta.columns[i].unsigned() ? Long.toUnsignedString(getLong(i)) : Long.toString(getLong(i));
            case T_FLOAT:
                return Float.toString(Float.intBitsToFloat((int) le(data, o, 4)));
            case T_DOUBLE:
                return Double.toString(Double.longBitsToDouble(le(data, o, 8)));
            case T_DATE, T_NEWDATE, T_DATETIME, T_TIMESTAMP, T_TIME:
                Object t = getObject(i);
                return t == null ? "0000-00-00" : t.toString();
            default:
                return new String(data, o, len, StandardCharsets.UTF_8);
        }
    }

    public String getString(String name) {
        return getString(idx(name));
    }

    /** The bytes of the column as the server sent them (a copy). For a BIT column in the text protocol they are the raw bytes too. */
    public byte[] getBytes(int i) {
        if (isNull(i)) {
            return null;
        }
        int o = pos[2 * i], len = pos[2 * i + 1];
        return java.util.Arrays.copyOfRange(data, o, o + len);
    }

    public byte[] getBytes(String name) {
        return getBytes(idx(name));
    }

    public BigDecimal getBigDecimal(int i) {
        if (isNull(i)) {
            return null;
        }
        if (binary) {
            switch (meta.columns[i].type()) {
                case T_TINY, T_SHORT, T_LONG, T_INT24, T_YEAR:
                    return BigDecimal.valueOf(getLong(i));
                case T_LONGLONG:
                    return meta.columns[i].unsigned() ? new BigDecimal(Long.toUnsignedString(getLong(i))) : BigDecimal.valueOf(getLong(i));
                case T_FLOAT, T_DOUBLE:
                    return BigDecimal.valueOf(getDouble(i));
                default:
                    break;
            }
        }
        return new BigDecimal(new String(data, pos[2 * i], pos[2 * i + 1], StandardCharsets.ISO_8859_1));
    }

    public BigDecimal getBigDecimal(String name) {
        return getBigDecimal(idx(name));
    }

    public LocalDate getLocalDate(int i) {
        Object o = getObject(i);
        if (o == null) {
            return null;
        }
        if (o instanceof LocalDate d) {
            return d;
        }
        if (o instanceof LocalDateTime t) {
            return t.toLocalDate();
        }
        throw new ClassCastException("column " + meta.columns[i].name() + " is not a date");
    }

    public LocalDateTime getLocalDateTime(int i) {
        Object o = getObject(i);
        if (o == null) {
            return null;
        }
        if (o instanceof LocalDateTime t) {
            return t;
        }
        if (o instanceof LocalDate d) {
            return d.atStartOfDay();
        }
        throw new ClassCastException("column " + meta.columns[i].name() + " is not a date and time");
    }

    public LocalTime getLocalTime(int i) {
        Object o = getObject(i);
        if (o == null) {
            return null;
        }
        if (o instanceof Duration d) {
            return LocalTime.ofNanoOfDay(d.toNanos());
        }
        if (o instanceof LocalDateTime t) {
            return t.toLocalTime();
        }
        throw new ClassCastException("column " + meta.columns[i].name() + " is not a time");
    }

    /**
     * The column as a Java object by its type: Integer (TINY, SHORT, INT24, and LONG when signed), Long (LONG unsigned, LONGLONG), BigInteger (unsigned BIGINT above
     * Long.MAX_VALUE), Float, Double, BigDecimal (DECIMAL), LocalDate (DATE), LocalDateTime (DATETIME, TIMESTAMP), Duration (TIME, which can be negative or above 24 h),
     * Integer (YEAR), byte[] (BLOB types with the binary character set, BIT, GEOMETRY), String (everything else, including JSON, ENUM and SET). A zero date gives null.
     */
    public Object getObject(int i) {
        if (isNull(i)) {
            return null;
        }
        MyColumn c = meta.columns[i];
        int o = pos[2 * i], len = pos[2 * i + 1];
        switch (c.type()) {
            case T_TINY, T_SHORT, T_INT24, T_YEAR:
                return Math.toIntExact(getLong(i));
            case T_LONG:
                return c.unsigned() ? (Object) getLong(i) : (Object) Math.toIntExact(getLong(i));
            case T_LONGLONG: {
                long v = getLong(i);
                return c.unsigned() && v < 0 ? new BigInteger(Long.toUnsignedString(v)) : (Object) v;
            }
            case T_FLOAT:
                return binary ? (Object) Float.intBitsToFloat((int) le(data, o, 4)) : (Object) Float.parseFloat(new String(data, o, len, StandardCharsets.ISO_8859_1));
            case T_DOUBLE:
                return getDouble(i);
            case T_DECIMAL, T_NEWDECIMAL:
                return getBigDecimal(i);
            case T_DATE, T_NEWDATE:
                return binary ? binaryDateTime(o, true) : textDate(new String(data, o, len, StandardCharsets.ISO_8859_1));
            case T_DATETIME, T_TIMESTAMP:
                return binary ? binaryDateTime(o, false) : textDateTime(new String(data, o, len, StandardCharsets.ISO_8859_1));
            case T_TIME:
                return binary ? binaryTime(o) : textTime(new String(data, o, len, StandardCharsets.ISO_8859_1));
            case T_BIT, T_GEOMETRY:
                return getBytes(i);
            case T_TINY_BLOB, T_MEDIUM_BLOB, T_LONG_BLOB, T_BLOB, T_VAR_STRING, T_STRING, T_VARCHAR:
                return c.binary() ? (Object) getBytes(i) : (Object) getString(i);
            default:
                return getString(i);
        }
    }

    public Object getObject(String name) {
        return getObject(idx(name));
    }

    private Object binaryDateTime(int o, boolean dateOnly) {
        int len = data[o] & 0xFF;
        if (len == 0) {
            return null;
        }
        int year = (int) le(data, o + 1, 2), month = data[o + 3], day = data[o + 4];
        if (year == 0 || month == 0 || day == 0) {
            return null;
        }
        if (dateOnly) {
            return LocalDate.of(year, month, day);
        }
        int h = 0, mi = 0, s = 0, nanos = 0;
        if (len >= 7) {
            h = data[o + 5];
            mi = data[o + 6];
            s = data[o + 7];
        }
        if (len >= 11) {
            nanos = (int) le(data, o + 8, 4) * 1000;
        }
        return LocalDateTime.of(year, month, day, h, mi, s, nanos);
    }

    private Object binaryTime(int o) {
        int len = data[o] & 0xFF;
        if (len == 0) {
            return Duration.ZERO;
        }
        boolean neg = data[o + 1] != 0;
        long days = le(data, o + 2, 4);
        Duration d = Duration.ofDays(days).plusHours(data[o + 6]).plusMinutes(data[o + 7]).plusSeconds(data[o + 8]);
        if (len >= 12) {
            d = d.plusNanos(le(data, o + 9, 4) * 1000);
        }
        return neg ? d.negated() : d;
    }

    private static LocalDate textDate(String s) {
        return s.startsWith("0000") || s.contains("-00") ? null : LocalDate.parse(s);
    }

    private static LocalDateTime textDateTime(String s) {
        return s.startsWith("0000") || s.contains("-00") ? null : LocalDateTime.parse(s.replace(' ', 'T'));
    }

    private static Duration textTime(String s) {
        boolean neg = s.startsWith("-");
        if (neg) {
            s = s.substring(1);
        }
        String[] hms = s.split(":");
        long h = Long.parseLong(hms[0]), m = Long.parseLong(hms[1]);
        String sec = hms[2];
        int dot = sec.indexOf('.');
        long whole = Long.parseLong(dot < 0 ? sec : sec.substring(0, dot));
        long nanos = 0;
        if (dot >= 0) {
            String frac = (sec.substring(dot + 1) + "000000000").substring(0, 9);
            nanos = Long.parseLong(frac);
        }
        Duration d = Duration.ofHours(h).plusMinutes(m).plusSeconds(whole).plusNanos(nanos);
        return neg ? d.negated() : d;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("MyRow{");
        for (int i = 0; i < size(); i++) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(meta.columns[i].name()).append('=').append(isNull(i) ? "NULL" : getString(i));
        }
        return b.append('}').toString();
    }
}
