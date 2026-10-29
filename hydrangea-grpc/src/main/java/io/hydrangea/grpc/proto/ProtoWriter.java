package io.hydrangea.grpc.proto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Writes protobuf wire format (proto3 rules: write a field only if you want it on the wire; default values are normally skipped). Enough to
 * hand-code messages without a protobuf library. Chainable.
 */
public final class ProtoWriter {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream(64);

    public static final int VARINT = 0, FIXED64 = 1, LENGTH_DELIMITED = 2, FIXED32 = 5;

    private void tag(int field, int wireType) {
        if (field < 1) {
            throw new IllegalArgumentException("field numbers start at 1");
        }
        varint(((long) field << 3) | wireType);
    }

    private void varint(long v) {
        while ((v & ~0x7fL) != 0) {
            out.write((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    /** int32, int64, uint32, uint64, enum: a negative int32 takes 10 bytes, as the specification says. */
    public ProtoWriter varint(int field, long value) {
        tag(field, VARINT);
        varint(value);
        return this;
    }

    public ProtoWriter bool(int field, boolean value) {
        return varint(field, value ? 1 : 0);
    }

    /** sint32 / sint64 (zigzag). */
    public ProtoWriter sint(int field, long value) {
        return varint(field, (value << 1) ^ (value >> 63));
    }

    public ProtoWriter fixed32(int field, int value) {
        tag(field, FIXED32);
        for (int i = 0; i < 4; i++) {
            out.write((value >>> (8 * i)) & 0xff);
        }
        return this;
    }

    public ProtoWriter fixed64(int field, long value) {
        tag(field, FIXED64);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((value >>> (8 * i)) & 0xff));
        }
        return this;
    }

    public ProtoWriter floatValue(int field, float value) {
        return fixed32(field, Float.floatToRawIntBits(value));
    }

    public ProtoWriter doubleValue(int field, double value) {
        return fixed64(field, Double.doubleToRawLongBits(value));
    }

    public ProtoWriter string(int field, String value) {
        return bytes(field, value.getBytes(StandardCharsets.UTF_8));
    }

    public ProtoWriter bytes(int field, byte[] value) {
        tag(field, LENGTH_DELIMITED);
        varint(value.length);
        out.write(value, 0, value.length);
        return this;
    }

    /** An embedded message, already encoded. */
    public ProtoWriter message(int field, byte[] encoded) {
        return bytes(field, encoded);
    }

    /** A packed repeated varint field. */
    public ProtoWriter packedVarints(int field, long... values) {
        ByteArrayOutputStream inner = new ByteArrayOutputStream();
        for (long v : values) {
            long x = v;
            while ((x & ~0x7fL) != 0) {
                inner.write((int) ((x & 0x7f) | 0x80));
                x >>>= 7;
            }
            inner.write((int) x);
        }
        return bytes(field, inner.toByteArray());
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }
}
