package io.hydrangea.postgres;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A growing byte array with the encodings of the PostgreSQL protocol: big-endian numbers, NUL-terminated strings, length-prefixed messages. */
final class Buf {

    byte[] a;
    int n;

    Buf(int capacity) {
        a = new byte[Math.max(capacity, 16)];
    }

    private void ensure(int more) {
        if (n + more > a.length) {
            a = Arrays.copyOf(a, Math.max(a.length * 2, n + more));
        }
    }

    Buf b(int v) {
        ensure(1);
        a[n++] = (byte) v;
        return this;
    }

    Buf i16(int v) {
        ensure(2);
        a[n++] = (byte) (v >> 8);
        a[n++] = (byte) v;
        return this;
    }

    Buf i32(int v) {
        ensure(4);
        a[n++] = (byte) (v >> 24);
        a[n++] = (byte) (v >> 16);
        a[n++] = (byte) (v >> 8);
        a[n++] = (byte) v;
        return this;
    }

    Buf raw(byte[] src) {
        ensure(src.length);
        System.arraycopy(src, 0, a, n, src.length);
        n += src.length;
        return this;
    }

    /** A string and its terminating NUL. */
    Buf cstr(String s) {
        int len = s.length();
        ensure(len + 1);
        int i = 0;
        for (; i < len; i++) { // ASCII fast path
            char c = s.charAt(i);
            if (c >= 0x80) {
                break;
            }
            if (c == 0) {
                throw new IllegalArgumentException("a NUL character cannot be sent");
            }
            a[n + i] = (byte) c;
        }
        if (i == len) {
            n += len;
            a[n++] = 0;
            return this;
        }
        byte[] u = s.getBytes(StandardCharsets.UTF_8);
        for (byte x : u) {
            if (x == 0) {
                throw new IllegalArgumentException("a NUL character cannot be sent");
            }
        }
        raw(u);
        return b(0);
    }

    /** Starts a message of {@code type}; returns what {@link #end} needs. */
    int begin(char type) {
        b(type);
        int at = n;
        i32(0);
        return at;
    }

    /** Fills in the length of the message started at {@code at}. */
    void end(int at) {
        int len = n - at;
        a[at] = (byte) (len >> 24);
        a[at + 1] = (byte) (len >> 16);
        a[at + 2] = (byte) (len >> 8);
        a[at + 3] = (byte) len;
    }

    byte[] toByteArray() {
        return Arrays.copyOf(a, n);
    }
}
