package io.hydrangea.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Big-endian primitives of the Kafka protocol (the non-flexible encodings used by the versions this client speaks) and the varints of the record format. */
final class Wire {

    private Wire() {
    }

    static final class Writer {
        byte[] buf;
        int len;

        Writer(int capacity) {
            buf = new byte[Math.max(capacity, 16)];
        }

        private void room(int n) {
            if (len + n > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + n));
            }
        }

        Writer i8(int v) {
            room(1);
            buf[len++] = (byte) v;
            return this;
        }

        Writer i16(int v) {
            room(2);
            buf[len++] = (byte) (v >>> 8);
            buf[len++] = (byte) v;
            return this;
        }

        Writer i32(int v) {
            room(4);
            buf[len++] = (byte) (v >>> 24);
            buf[len++] = (byte) (v >>> 16);
            buf[len++] = (byte) (v >>> 8);
            buf[len++] = (byte) v;
            return this;
        }

        Writer i64(long v) {
            room(8);
            for (int s = 56; s >= 0; s -= 8) {
                buf[len++] = (byte) (v >>> s);
            }
            return this;
        }

        /** A string: int16 length then UTF-8; null is length -1. */
        Writer str(String s) {
            if (s == null) {
                return i16(-1);
            }
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            if (b.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("string too long");
            }
            i16(b.length);
            return raw(b, 0, b.length);
        }

        /** Bytes: int32 length then the bytes; null is length -1. */
        Writer bytes(byte[] b) {
            if (b == null) {
                return i32(-1);
            }
            i32(b.length);
            return raw(b, 0, b.length);
        }

        Writer raw(byte[] b, int off, int n) {
            room(n);
            System.arraycopy(b, off, buf, len, n);
            len += n;
            return this;
        }

        Writer varint(int v) {
            return varlong(v);
        }

        /** Zig-zag varint, as in a record. */
        Writer varlong(long v) {
            long z = (v << 1) ^ (v >> 63);
            room((63 - Long.numberOfLeadingZeros(z | 1)) / 7 + 1);   // exactly what it takes: reserving the maximum would grow a buffer that was sized to fit
            while ((z & ~0x7FL) != 0) {
                buf[len++] = (byte) ((z & 0x7F) | 0x80);
                z >>>= 7;
            }
            buf[len++] = (byte) z;
            return this;
        }

        /** Overwrites a 4-byte int at a position already written. */
        void patchI32(int at, int v) {
            buf[at] = (byte) (v >>> 24);
            buf[at + 1] = (byte) (v >>> 16);
            buf[at + 2] = (byte) (v >>> 8);
            buf[at + 3] = (byte) v;
        }

        byte[] toArray() {
            return Arrays.copyOf(buf, len);
        }
    }

    static int varlongSize(long v) {
        long z = (v << 1) ^ (v >> 63);
        return (63 - Long.numberOfLeadingZeros(z | 1)) / 7 + 1;
    }

    /** A bounds-checked reader: anything that would read past the end is a protocol error, not an exception from the JDK. */
    static final class Reader {
        private final byte[] b;
        int pos;
        final int end;

        Reader(byte[] b, int off, int len) {
            this.b = b;
            this.pos = off;
            this.end = off + len;
        }

        Reader(byte[] b) {
            this(b, 0, b.length);
        }

        int remaining() {
            return end - pos;
        }

        private void need(int n) {
            if (n < 0 || n > end - pos) {
                throw new KafkaException("protocol error: truncated message (" + n + " bytes wanted, " + (end - pos) + " left)", false);
            }
        }

        int i8() {
            need(1);
            return b[pos++];
        }

        int i16() {
            need(2);
            int v = (short) (((b[pos] & 0xFF) << 8) | (b[pos + 1] & 0xFF));
            pos += 2;
            return v;
        }

        int i32() {
            need(4);
            int v = ((b[pos] & 0xFF) << 24) | ((b[pos + 1] & 0xFF) << 16) | ((b[pos + 2] & 0xFF) << 8) | (b[pos + 3] & 0xFF);
            pos += 4;
            return v;
        }

        long i64() {
            need(8);
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (b[pos + i] & 0xFF);
            }
            pos += 8;
            return v;
        }

        String str() {
            int n = i16();
            if (n < 0) {
                return null;
            }
            need(n);
            String s = new String(b, pos, n, StandardCharsets.UTF_8);
            pos += n;
            return s;
        }

        byte[] bytes() {
            int n = i32();
            if (n < 0) {
                return null;
            }
            need(n);
            byte[] r = Arrays.copyOfRange(b, pos, pos + n);
            pos += n;
            return r;
        }

        /** The length of an array, checked against what could possibly follow (each element takes at least one byte). */
        int arrayLength() {
            int n = i32();
            if (n < -1 || n > end - pos) {
                throw new KafkaException("protocol error: array of " + n + " elements in " + (end - pos) + " bytes", false);
            }
            return Math.max(n, 0);
        }

        long varlong() {
            if (pos < end) {
                int first = b[pos];
                if (first >= 0) {   // one byte: most lengths and deltas
                    pos++;
                    return (first >>> 1) ^ -(first & 1);
                }
            }
            long z = 0;
            int shift = 0;
            for (;;) {
                need(1);
                int x = b[pos++];
                z |= (long) (x & 0x7F) << shift;
                if ((x & 0x80) == 0) {
                    break;
                }
                shift += 7;
                if (shift > 63) {
                    throw new KafkaException("protocol error: varint too long", false);
                }
            }
            return (z >>> 1) ^ -(z & 1);
        }

        int varint() {
            if (pos < end) {
                int first = b[pos];
                if (first >= 0) {
                    pos++;
                    return (first >>> 1) ^ -(first & 1);
                }
            }
            long v = varlong();
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new KafkaException("protocol error: varint out of range", false);
            }
            return (int) v;
        }

        /** A sub-reader over the next n bytes, which are consumed. */
        Reader slice(int n) {
            need(n);
            Reader r = new Reader(b, pos, n);
            pos += n;
            return r;
        }

        byte[] rawBytes(int n) {
            need(n);
            byte[] r = Arrays.copyOfRange(b, pos, pos + n);
            pos += n;
            return r;
        }

        byte[] array() {
            return b;
        }

        ByteBuffer asBuffer() {
            return ByteBuffer.wrap(b, pos, end - pos);
        }
    }
}
