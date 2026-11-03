package io.hydrangea.log;

import java.util.Arrays;

/** Encodes text into a reusable byte array, so writing a line makes no String, no byte[] and no charset lookup. */
final class Utf8 {

    private Utf8() {
    }

    /** Returns the array to use (the given one, or a bigger one) with the encoded bytes at the start; {@code length[0]} gets the count. */
    static byte[] encode(CharSequence s, byte[] buf, int[] length) {
        int len = s.length();
        if (buf.length < len * 3) {
            buf = new byte[Math.max(len * 3, buf.length * 2)];
        }
        int n = 0;
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                buf[n++] = (byte) c;
            } else if (c < 0x800) {
                buf[n++] = (byte) (0xc0 | (c >> 6));
                buf[n++] = (byte) (0x80 | (c & 0x3f));
            } else if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, s.charAt(++i));
                buf[n++] = (byte) (0xf0 | (cp >> 18));
                buf[n++] = (byte) (0x80 | ((cp >> 12) & 0x3f));
                buf[n++] = (byte) (0x80 | ((cp >> 6) & 0x3f));
                buf[n++] = (byte) (0x80 | (cp & 0x3f));
            } else if (Character.isSurrogate(c)) {
                buf[n++] = (byte) 0xef;      // lone surrogate: U+FFFD
                buf[n++] = (byte) 0xbf;
                buf[n++] = (byte) 0xbd;
            } else {
                buf[n++] = (byte) (0xe0 | (c >> 12));
                buf[n++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                buf[n++] = (byte) (0x80 | (c & 0x3f));
            }
        }
        length[0] = n;
        return buf;
    }

    /** A fresh small array when the one in use grew past a megabyte, so one huge line does not pin memory for good. */
    static byte[] shrink(byte[] buf) {
        return buf.length > 1 << 20 ? Arrays.copyOf(buf, 0) : buf;
    }
}
