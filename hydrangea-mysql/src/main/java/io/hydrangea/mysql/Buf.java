package io.hydrangea.mysql;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A growing byte array with the encodings of the MySQL protocol: little-endian numbers, length-encoded integers and strings, 4-byte packet headers. */
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
        a[n++] = (byte) v;
        a[n++] = (byte) (v >> 8);
        return this;
    }

    Buf i24(int v) {
        ensure(3);
        a[n++] = (byte) v;
        a[n++] = (byte) (v >> 8);
        a[n++] = (byte) (v >> 16);
        return this;
    }

    Buf i32(int v) {
        ensure(4);
        a[n++] = (byte) v;
        a[n++] = (byte) (v >> 8);
        a[n++] = (byte) (v >> 16);
        a[n++] = (byte) (v >> 24);
        return this;
    }

    Buf i64(long v) {
        ensure(8);
        for (int i = 0; i < 8; i++) {
            a[n++] = (byte) (v >> (8 * i));
        }
        return this;
    }

    Buf raw(byte[] src) {
        return raw(src, 0, src.length);
    }

    Buf raw(byte[] src, int off, int len) {
        ensure(len);
        System.arraycopy(src, off, a, n, len);
        n += len;
        return this;
    }

    Buf zeros(int count) {
        ensure(count);
        n += count;      // a new array is zero filled, but a reused one is not: callers only use this on fresh space
        Arrays.fill(a, n - count, n, (byte) 0);
        return this;
    }

    /** A string and its terminating NUL. */
    Buf cstr(String s) {
        raw(s.getBytes(StandardCharsets.UTF_8));
        return b(0);
    }

    Buf lenenc(long v) {
        if (v < 251) {
            return b((int) v);
        }
        if (v < 65536) {
            return b(0xFC).i16((int) v);
        }
        if (v < 16777216) {
            return b(0xFD).i24((int) v);
        }
        return b(0xFE).i64(v);
    }

    Buf lenencBytes(byte[] v) {
        lenenc(v.length);
        return raw(v);
    }

    Buf lenencString(String s) {
        return lenencBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Starts a packet; returns what {@link #end} needs. The 4 header bytes are filled in by {@link #end}. */
    int begin() {
        int at = n;
        ensure(4);
        n += 4;
        return at;
    }

    /** Fills in the header of the packet started at {@code at}: its length and sequence number. A payload of 16 MB or more is split into packets of 0xFFFFFF. */
    void end(int at, int seq) {
        int len = n - at - 4;
        if (len < 0xFFFFFF) {
            a[at] = (byte) len;
            a[at + 1] = (byte) (len >> 8);
            a[at + 2] = (byte) (len >> 16);
            a[at + 3] = (byte) seq;
            return;
        }
        // split: the payload moves up to make room for the extra headers
        int parts = len / 0xFFFFFF + 1;
        byte[] payload = Arrays.copyOfRange(a, at + 4, n);
        n = at;
        int off = 0;
        for (int i = 0; i < parts; i++) {
            int chunk = Math.min(0xFFFFFF, len - off);
            i24(chunk).b(seq++);
            raw(payload, off, chunk);
            off += chunk;
        }
    }
}
