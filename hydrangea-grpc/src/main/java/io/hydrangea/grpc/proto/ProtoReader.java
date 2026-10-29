package io.hydrangea.grpc.proto;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads protobuf wire format field by field. Unknown fields are skipped with {@link #skip()}, so a message can grow without breaking readers.
 * <pre>{@code
 * ProtoReader r = new ProtoReader(bytes);
 * while (r.next()) {
 *     switch (r.field()) {
 *         case 1 -> name = r.string();
 *         case 2 -> count = (int) r.varint();
 *         default -> r.skip();
 *     }
 * }
 * }</pre>
 * Malformed input (truncation, a length past the end, a varint over 10 bytes, wire type 3/4/6/7) throws {@link IllegalArgumentException}.
 */
public final class ProtoReader {

    private final byte[] data;
    private int pos;
    private int field;
    private int wireType;
    private boolean valueConsumed = true;

    public ProtoReader(byte[] data) {
        this.data = data;
    }

    /** Advances to the next field; false at the end of the message. A value not read before is skipped. */
    public boolean next() {
        if (!valueConsumed) {
            skip();
        }
        if (pos >= data.length) {
            return false;
        }
        long tag = readVarint();
        wireType = (int) (tag & 7);
        long f = tag >>> 3;
        if (f < 1 || f > 536_870_911L) {
            throw new IllegalArgumentException("invalid field number " + f);
        }
        field = (int) f;
        if (wireType == 3 || wireType == 4 || wireType > 5) {
            throw new IllegalArgumentException("unsupported wire type " + wireType);
        }
        valueConsumed = false;
        return true;
    }

    public int field() {
        return field;
    }

    public int wireType() {
        return wireType;
    }

    public long varint() {
        expect(ProtoWriter.VARINT);
        valueConsumed = true;
        return readVarint();
    }

    public boolean bool() {
        return varint() != 0;
    }

    public long sint() {
        long v = varint();
        return (v >>> 1) ^ -(v & 1);
    }

    public int fixed32() {
        expect(ProtoWriter.FIXED32);
        valueConsumed = true;
        need(4);
        int v = (data[pos] & 0xff) | (data[pos + 1] & 0xff) << 8 | (data[pos + 2] & 0xff) << 16 | (data[pos + 3] & 0xff) << 24;
        pos += 4;
        return v;
    }

    public long fixed64() {
        expect(ProtoWriter.FIXED64);
        valueConsumed = true;
        need(8);
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (data[pos + i] & 0xff);
        }
        pos += 8;
        return v;
    }

    public float floatValue() {
        return Float.intBitsToFloat(fixed32());
    }

    public double doubleValue() {
        return Double.longBitsToDouble(fixed64());
    }

    public byte[] bytes() {
        expect(ProtoWriter.LENGTH_DELIMITED);
        valueConsumed = true;
        long len = readVarint();
        if (len < 0 || len > data.length - pos) {
            throw new IllegalArgumentException("length-delimited field runs past the end of the message");
        }
        byte[] out = java.util.Arrays.copyOfRange(data, pos, pos + (int) len);
        pos += (int) len;
        return out;
    }

    /** UTF-8; invalid sequences are replaced rather than rejected. */
    public String string() {
        return new String(bytes(), StandardCharsets.UTF_8);
    }

    /** A reader positioned inside an embedded message. */
    public ProtoReader message() {
        return new ProtoReader(bytes());
    }

    /** A repeated varint field in either encoding: call once for a packed field, or once per occurrence for an unpacked one and concatenate. */
    public List<Long> varints() {
        List<Long> out = new ArrayList<>();
        if (wireType == ProtoWriter.LENGTH_DELIMITED) {
            ProtoReader inner = new ProtoReader(bytes());
            while (inner.pos < inner.data.length) {
                out.add(inner.readVarint());
            }
        } else {
            out.add(varint());
        }
        return out;
    }

    public void skip() {
        valueConsumed = true;
        switch (wireType) {
            case ProtoWriter.VARINT -> readVarint();
            case ProtoWriter.FIXED64 -> {
                need(8);
                pos += 8;
            }
            case ProtoWriter.FIXED32 -> {
                need(4);
                pos += 4;
            }
            case ProtoWriter.LENGTH_DELIMITED -> {
                long len = readVarint();
                if (len < 0 || len > data.length - pos) {
                    throw new IllegalArgumentException("length-delimited field runs past the end of the message");
                }
                pos += (int) len;
            }
            default -> throw new IllegalArgumentException("unsupported wire type " + wireType);
        }
    }

    private void expect(int type) {
        if (wireType != type || valueConsumed) {
            throw new IllegalArgumentException("field " + field + " has wire type " + wireType + ", not " + type);
        }
    }

    private void need(int n) {
        if (n > data.length - pos) {
            throw new IllegalArgumentException("truncated message");
        }
    }

    private long readVarint() {
        long result = 0;
        for (int shift = 0; shift < 70; shift += 7) {
            if (pos >= data.length) {
                throw new IllegalArgumentException("truncated varint");
            }
            int b = data[pos++] & 0xff;
            result |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IllegalArgumentException("varint longer than 10 bytes");
    }
}
