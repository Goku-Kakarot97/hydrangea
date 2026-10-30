package io.hydrangea.mongodb;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** A 12-byte MongoDB ObjectId: 4 bytes of seconds since the epoch, 5 random bytes fixed per process, a 3-byte counter. */
public final class ObjectId implements Comparable<ObjectId> {

    private static final byte[] PROCESS = new byte[5];
    private static final AtomicInteger COUNTER;

    static {
        SecureRandom r = new SecureRandom();
        r.nextBytes(PROCESS);
        COUNTER = new AtomicInteger(r.nextInt());
    }

    private final byte[] bytes;

    private ObjectId(byte[] bytes) {
        this.bytes = bytes;
    }

    /** A new id for now. */
    public static ObjectId generate() {
        byte[] b = new byte[12];
        int t = (int) (System.currentTimeMillis() / 1000);
        b[0] = (byte) (t >> 24);
        b[1] = (byte) (t >> 16);
        b[2] = (byte) (t >> 8);
        b[3] = (byte) t;
        System.arraycopy(PROCESS, 0, b, 4, 5);
        int c = COUNTER.getAndIncrement();
        b[9] = (byte) (c >> 16);
        b[10] = (byte) (c >> 8);
        b[11] = (byte) c;
        return new ObjectId(b);
    }

    /** From 24 hex characters. @throws IllegalArgumentException otherwise */
    public static ObjectId of(String hex) {
        if (hex == null || hex.length() != 24) {
            throw new IllegalArgumentException("an ObjectId is 24 hex characters: " + hex);
        }
        byte[] b = new byte[12];
        for (int i = 0; i < 12; i++) {
            int hi = Character.digit(hex.charAt(2 * i), 16), lo = Character.digit(hex.charAt(2 * i + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("an ObjectId is 24 hex characters: " + hex);
            }
            b[i] = (byte) (hi << 4 | lo);
        }
        return new ObjectId(b);
    }

    /** From 12 bytes (copied). */
    public static ObjectId of(byte[] bytes) {
        if (bytes == null || bytes.length != 12) {
            throw new IllegalArgumentException("an ObjectId is 12 bytes");
        }
        return new ObjectId(bytes.clone());
    }

    /** True if {@code s} is 24 hex characters. */
    public static boolean isValid(String s) {
        if (s == null || s.length() != 24) {
            return false;
        }
        for (int i = 0; i < 24; i++) {
            if (Character.digit(s.charAt(i), 16) < 0) {
                return false;
            }
        }
        return true;
    }

    public byte[] toBytes() {
        return bytes.clone();
    }

    byte[] bytes() {
        return bytes;
    }

    /** The second the id was made, as an instant. */
    public Instant timestamp() {
        long t = (bytes[0] & 0xFFL) << 24 | (bytes[1] & 0xFFL) << 16 | (bytes[2] & 0xFFL) << 8 | (bytes[3] & 0xFFL);
        return Instant.ofEpochSecond(t);
    }

    public String toHexString() {
        char[] out = new char[24];
        for (int i = 0; i < 12; i++) {
            out[2 * i] = Character.forDigit((bytes[i] >> 4) & 0xF, 16);
            out[2 * i + 1] = Character.forDigit(bytes[i] & 0xF, 16);
        }
        return new String(out);
    }

    @Override
    public String toString() {
        return toHexString();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ObjectId other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(ObjectId o) {
        return Arrays.compareUnsigned(bytes, o.bytes);
    }
}
