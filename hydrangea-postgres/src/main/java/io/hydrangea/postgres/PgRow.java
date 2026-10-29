package io.hydrangea.postgres;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * One row. Values arrive as text and are converted when asked for: {@link #get(int)} picks the Java type from the column's type
 * ({@code int4} to {@link Integer}, {@code int8} to {@link Long}, {@code numeric} to {@link BigDecimal}, {@code bool} to {@link Boolean},
 * {@code bytea} to {@code byte[]}, {@code uuid}, the date and time types to {@code java.time}, everything else including json to {@link String}).
 * Columns are numbered from 0; a SQL NULL is {@code null} (or {@link #isNull}).
 */
public final class PgRow implements io.hydrangea.sql.SqlRow {

    /** As {@link #get(int)}: the column as a Java object by its type. */
    @Override
    public Object getObject(int i) {
        return get(i);
    }


    private final PgColumn[] columns;
    private final byte[][] values;
    private final boolean[] binary;        // which columns arrived in binary (null = none): what the query's Bind asked for

    PgRow(PgColumn[] columns, byte[][] values) {
        this(columns, values, null);
    }

    PgRow(PgColumn[] columns, byte[][] values, boolean[] binary) {
        this.columns = columns;
        this.values = values;
        this.binary = binary;
    }

    /** True if column {@code i} of this row arrived in binary (see {@link PgConfig#binaryTransfer()}); for tests and curiosity: nothing a caller reads depends on it. */
    boolean wasBinary(int i) {
        return bin(i);
    }

    private boolean bin(int i) {
        return binary != null && i < binary.length && binary[i];
    }

    public int size() {
        return values.length;
    }

    /** The index of the column called {@code name} (the first if several share it), or -1. */
    public int indexOf(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private int index(String name) {
        int i = indexOf(name);
        if (i < 0) {
            throw new IllegalArgumentException("no column " + name);
        }
        return i;
    }

    public boolean isNull(int i) {
        return values[i] == null;
    }

    public boolean isNull(String name) {
        return isNull(index(name));
    }

    public Object get(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        return bin(i) ? PgTypes.decodeBinary(columns[i].typeOid(), v) : PgTypes.decode(columns[i].typeOid(), v);
    }

    public Object get(String name) {
        return get(index(name));
    }

    /** The text of the value as the server wrote it, for any type. */
    public String getString(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        return bin(i) ? PgTypes.textOfBinary(columns[i].typeOid(), v) : new String(v, StandardCharsets.UTF_8);
    }

    public String getString(String name) {
        return getString(index(name));
    }

    /** The value of an integer column, or 0 for NULL (check {@link #isNull}). */
    public int getInt(int i) {
        byte[] v = values[i];
        return v == null ? 0 : Math.toIntExact(integer(i, v));
    }

    /** An integer column's value; binary integers are read as they are, other binary types through their text. */
    private long integer(int i, byte[] v) {
        if (bin(i)) {
            switch (columns[i].typeOid()) {
                case PgTypes.INT2:
                    return java.nio.ByteBuffer.wrap(v).getShort();
                case PgTypes.INT4:
                    return java.nio.ByteBuffer.wrap(v).getInt();
                case PgTypes.INT8:
                    return java.nio.ByteBuffer.wrap(v).getLong();
                default:
                    return PgTypes.parseLong(PgTypes.textOfBinary(columns[i].typeOid(), v).getBytes(StandardCharsets.US_ASCII));
            }
        }
        return PgTypes.parseLong(v);
    }

    public int getInt(String name) {
        return getInt(index(name));
    }

    public long getLong(int i) {
        byte[] v = values[i];
        return v == null ? 0 : integer(i, v);
    }

    public long getLong(String name) {
        return getLong(index(name));
    }

    public double getDouble(int i) {
        byte[] v = values[i];
        return v == null ? 0 : Double.parseDouble(bin(i) ? PgTypes.textOfBinary(columns[i].typeOid(), v) : new String(v, StandardCharsets.US_ASCII));
    }

    public double getDouble(String name) {
        return getDouble(index(name));
    }

    public boolean getBoolean(int i) {
        byte[] v = values[i];
        if (v == null) {
            return false;
        }
        if (bin(i)) {
            char c = PgTypes.textOfBinary(columns[i].typeOid(), v).charAt(0);
            return c == 't' || c == 'T' || c == '1';
        }
        return v[0] == 't' || v[0] == 'T' || v[0] == '1';
    }

    public boolean getBoolean(String name) {
        return getBoolean(index(name));
    }

    public BigDecimal getBigDecimal(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        return new BigDecimal(bin(i) ? PgTypes.textOfBinary(columns[i].typeOid(), v) : new String(v, StandardCharsets.US_ASCII));
    }

    public BigDecimal getBigDecimal(String name) {
        return getBigDecimal(index(name));
    }

    /** The bytes of a {@code bytea} column. */
    public byte[] getBytes(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        return bin(i) && columns[i].typeOid() == PgTypes.BYTEA ? v.clone() : bin(i) ? PgTypes.decodeBytea(PgTypes.textOfBinary(columns[i].typeOid(), v).getBytes(StandardCharsets.US_ASCII)) : PgTypes.decodeBytea(v);
    }

    public byte[] getBytes(String name) {
        return getBytes(index(name));
    }

    /**
     * An array column as a list of decoded elements ({@link #get(int)} gives the same); a multi-dimensional array is a list of lists, and a NULL element is null.
     * Fails if the column is not an array type.
     */
    @SuppressWarnings("unchecked")
    public List<Object> getList(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        if (!PgTypes.isArray(columns[i].typeOid())) {
            throw new IllegalStateException("column " + columns[i].name() + " (type " + columns[i].typeOid() + ") is not an array");
        }
        return (List<Object>) get(i);
    }

    public List<Object> getList(String name) {
        return getList(index(name));
    }

    /** A one-dimensional array column with its elements as {@code type} ({@code getList(i, Integer.class)} for an {@code int[]} column); a NULL element is null. */
    public <T> List<T> getList(int i, Class<T> type) {
        List<Object> raw = getList(i);
        if (raw == null) {
            return null;
        }
        List<T> out = new java.util.ArrayList<>(raw.size());
        for (Object o : raw) {
            out.add(type.cast(o));
        }
        return out;
    }

    public <T> List<T> getList(String name, Class<T> type) {
        return getList(index(name), type);
    }

    public UUID getUuid(int i) {
        byte[] v = values[i];
        if (v == null) {
            return null;
        }
        return bin(i) ? UUID.fromString(PgTypes.textOfBinary(columns[i].typeOid(), v)) : UUID.fromString(new String(v, StandardCharsets.US_ASCII));
    }

    public UUID getUuid(String name) {
        return getUuid(index(name));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(columns[i].name()).append('=').append(values[i] == null ? "NULL" : getString(i));
        }
        return sb.append(')').toString();
    }
}
