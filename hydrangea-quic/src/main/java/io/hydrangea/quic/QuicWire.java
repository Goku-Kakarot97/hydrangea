package io.hydrangea.quic;

import java.util.Arrays;

/** Variable-length integers (RFC 9000 section 16), and a small growable byte writer. */
final class QuicWire {
    private QuicWire() {
    }

    static int varintLength(long v) {
        if (v < 0x40) return 1;
        if (v < 0x4000) return 2;
        if (v < 0x40000000L) return 4;
        return 8;
    }

    /** Growable big-endian writer. */
    static final class Writer {
        byte[] b;
        int n;

        Writer(int capacity) {
            b = new byte[capacity];
        }

        private void ensure(int more) {
            if (n + more > b.length) {
                b = Arrays.copyOf(b, Math.max(b.length * 2, n + more));
            }
        }

        Writer u8(int v) {
            ensure(1);
            b[n++] = (byte) v;
            return this;
        }

        Writer u16(int v) {
            ensure(2);
            b[n++] = (byte) (v >> 8);
            b[n++] = (byte) v;
            return this;
        }

        Writer u32(long v) {
            ensure(4);
            for (int i = 3; i >= 0; i--) b[n++] = (byte) (v >> (8 * i));
            return this;
        }

        Writer varint(long v) {
            if (v < 0 || v >= (1L << 62)) throw new IllegalArgumentException("varint out of range: " + v);
            ensure(8);
            if (v < 0x40) {
                b[n++] = (byte) v;
            } else if (v < 0x4000) {
                b[n++] = (byte) (0x40 | (v >> 8));
                b[n++] = (byte) v;
            } else if (v < 0x40000000L) {
                b[n++] = (byte) (0x80 | (v >> 24));
                b[n++] = (byte) (v >> 16);
                b[n++] = (byte) (v >> 8);
                b[n++] = (byte) v;
            } else {
                b[n++] = (byte) (0xc0 | (v >> 56));
                for (int i = 6; i >= 0; i--) b[n++] = (byte) (v >> (8 * i));
            }
            return this;
        }

        Writer bytes(byte[] src) {
            return bytes(src, 0, src.length);
        }

        Writer bytes(byte[] src, int off, int len) {
            ensure(len);
            System.arraycopy(src, off, b, n, len);
            n += len;
            return this;
        }

        byte[] toByteArray() {
            return Arrays.copyOf(b, n);
        }
    }

    /** Bounds-checked reader over a byte range; every failure is a {@link Malformed}. */
    static final class Reader {
        final byte[] b;
        int pos;
        final int end;

        Reader(byte[] b, int from, int to) {
            this.b = b;
            this.pos = from;
            this.end = to;
        }

        boolean more() {
            return pos < end;
        }

        int remaining() {
            return end - pos;
        }

        int u8() {
            if (pos >= end) throw new Malformed();
            return b[pos++] & 0xff;
        }

        int u16() {
            if (pos + 2 > end) throw new Malformed();
            int v = ((b[pos] & 0xff) << 8) | (b[pos + 1] & 0xff);
            pos += 2;
            return v;
        }

        long u32() {
            if (pos + 4 > end) throw new Malformed();
            long v = 0;
            for (int i = 0; i < 4; i++) v = (v << 8) | (b[pos++] & 0xff);
            return v;
        }

        long varint() {
            int first = u8();
            int len = 1 << (first >> 6);
            long v = first & 0x3f;
            if (pos + len - 1 > end) throw new Malformed();
            for (int i = 1; i < len; i++) v = (v << 8) | (b[pos++] & 0xff);
            return v;
        }

        byte[] bytes(long n) {
            if (n < 0 || n > remaining()) throw new Malformed();
            byte[] r = Arrays.copyOfRange(b, pos, pos + (int) n);
            pos += (int) n;
            return r;
        }

        void skip(long n) {
            if (n < 0 || n > remaining()) throw new Malformed();
            pos += (int) n;
        }
    }

    /** Truncated or inconsistent bytes: a FRAME_ENCODING_ERROR or a dropped packet, depending on where it was found. */
    static final class Malformed extends RuntimeException {
        Malformed() {
            super("malformed", null, false, false);
        }
    }
}
