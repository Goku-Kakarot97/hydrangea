package io.hydrangea.grpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The length-prefixed message framing of gRPC: one flag byte (1 = the payload is compressed with the call's {@code grpc-encoding}), a 4 byte big
 * endian length, then the payload. {@link Decoder} is incremental, so it takes whatever the transport delivers (half a prefix, three messages and
 * a bit).
 */
final class GrpcFraming {

    private GrpcFraming() {
    }

    /** One framed message, compressed with gzip if asked. */
    /** The message inside a frame made by {@link #frame}, uncompressed (for logging). */
    static byte[] payloadOf(byte[] framed) throws IOException {
        byte[] body = java.util.Arrays.copyOfRange(framed, 5, framed.length);
        if (framed[0] == 0) {
            return body;
        }
        try (java.util.zip.GZIPInputStream in = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(body))) {
            return in.readAllBytes();
        }
    }

    /** The 5 bytes in front of an uncompressed message of {@code n} bytes. */
    static byte[] prefix(int n) {
        return new byte[] {0, (byte) (n >>> 24), (byte) (n >>> 16), (byte) (n >>> 8), (byte) n};
    }

    static byte[] frame(byte[] payload, boolean gzip) throws IOException {
        byte[] body = payload;
        int flag = 0;
        if (gzip) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length / 2 + 32);
            try (GZIPOutputStream g = new GZIPOutputStream(out)) {
                g.write(payload);
            }
            body = out.toByteArray();
            flag = 1;
        }
        int n = body.length;
        byte[] framed = new byte[5 + n];
        framed[0] = (byte) flag;
        framed[1] = (byte) (n >>> 24);
        framed[2] = (byte) (n >>> 16);
        framed[3] = (byte) (n >>> 8);
        framed[4] = (byte) n;
        System.arraycopy(body, 0, framed, 5, n);
        return framed;
    }

    interface Sink {
        void message(byte[] payload) throws StatusException;
    }

    static final class Decoder {
        private final int maxMessageBytes;
        private final boolean gzipAccepted;
        private final byte[] prefix = new byte[5];
        private int prefixLen;
        private boolean compressed;
        private int length;
        private byte[] payload;      // a message that arrives in pieces; grows as the bytes really arrive: a claimed length is not memory
        private int have;
        private boolean inPayload;

        Decoder(int maxMessageBytes, boolean gzipAccepted) {
            this.maxMessageBytes = maxMessageBytes;
            this.gzipAccepted = gzipAccepted;
        }

        /** Consumes {@code data}; calls {@code sink} for each message completed by it. */
        void feed(ByteBuffer data, Sink sink) throws StatusException {
            while (data.hasRemaining()) {
                if (!inPayload) {
                    while (prefixLen < 5 && data.hasRemaining()) {
                        prefix[prefixLen++] = data.get();
                    }
                    if (prefixLen < 5) {
                        return;
                    }
                    int flag = prefix[0] & 0xff;
                    if (flag > 1) {
                        throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("invalid gRPC compression flag " + flag));
                    }
                    if (flag == 1 && !gzipAccepted) {
                        throw new StatusException(Status.of(Status.Code.INTERNAL)
                                .withDescription("a compressed message arrived but no compression was negotiated"));
                    }
                    long len = ((prefix[1] & 0xffL) << 24) | ((prefix[2] & 0xff) << 16) | ((prefix[3] & 0xff) << 8) | (prefix[4] & 0xff);
                    if (len > maxMessageBytes) {
                        throw new StatusException(Status.of(Status.Code.RESOURCE_EXHAUSTED)
                                .withDescription("received message larger than the maximum (" + len + " vs. " + maxMessageBytes + ")"));
                    }
                    compressed = flag == 1;
                    length = (int) len;
                    inPayload = true;
                    if (data.remaining() >= length) {   // the whole message is here: one exact-size copy, no staging buffer
                        byte[] bytes = new byte[length];
                        data.get(bytes);
                        inPayload = false;
                        prefixLen = 0;
                        sink.message(compressed ? gunzip(bytes) : bytes);
                        continue;
                    }
                    payload = new byte[Math.min(length, 128 * 1024)];   // a message of up to 128 KiB in pieces never regrows; a bigger claimed length costs only what arrives (doubling)
                    have = 0;
                }
                int take = Math.min(length - have, data.remaining());
                if (have + take > payload.length) {
                    payload = java.util.Arrays.copyOf(payload, (int) Math.min(length, Math.max((long) payload.length * 2, have + take)));
                }
                data.get(payload, have, take);
                have += take;
                if (have == length) {
                    byte[] bytes = payload;
                    inPayload = false;
                    prefixLen = 0;
                    payload = null;
                    sink.message(compressed ? gunzip(bytes) : bytes);
                }
            }
        }

        /** The stream ended: a message cut in half is an error. */
        void finish() throws StatusException {
            if (inPayload || prefixLen > 0) {
                throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("the stream ended inside a message"));
            }
        }

        private byte[] gunzip(byte[] bytes) throws StatusException {
            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(maxMessageBytes, bytes.length * 4 + 64));
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) {
                    if (out.size() + n > maxMessageBytes) {
                        throw new StatusException(Status.of(Status.Code.RESOURCE_EXHAUSTED)
                                .withDescription("decompressed message larger than the maximum (" + maxMessageBytes + ")"));
                    }
                    out.write(buf, 0, n);
                }
                return out.toByteArray();
            } catch (IOException e) {
                throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("corrupt compressed message").withCause(e));
            }
        }
    }

    // ---- grpc-timeout ----

    /** A timeout header value: at most 8 digits and a unit, in the finest unit that fits. */
    static String encodeTimeout(java.time.Duration d) {
        long nanos = Math.max(1, d.toNanos());
        long[] divisors = {1L, 1_000L, 1_000_000L, 1_000_000_000L, 60_000_000_000L, 3_600_000_000_000L};
        char[] units = {'n', 'u', 'm', 'S', 'M', 'H'};
        for (int i = 0; i < units.length; i++) {
            long v = (nanos + divisors[i] - 1) / divisors[i];   // rounded up: never shorter than asked
            if (v <= 99_999_999L) {
                return v + String.valueOf(units[i]);
            }
        }
        return "99999999H";
    }

    /** @return the timeout, or null if the value is not a valid one */
    static java.time.Duration decodeTimeout(String v) {
        if (v == null || v.length() < 2 || v.length() > 9) {
            return null;
        }
        long n = 0;
        for (int i = 0; i < v.length() - 1; i++) {
            char c = v.charAt(i);
            if (c < '0' || c > '9') {
                return null;
            }
            n = n * 10 + (c - '0');
        }
        return switch (v.charAt(v.length() - 1)) {
            case 'n' -> java.time.Duration.ofNanos(n);
            case 'u' -> java.time.Duration.ofNanos(n * 1_000L);
            case 'm' -> java.time.Duration.ofMillis(n);
            case 'S' -> java.time.Duration.ofSeconds(n);
            case 'M' -> java.time.Duration.ofMinutes(n);
            case 'H' -> java.time.Duration.ofHours(n);
            default -> null;
        };
    }

    // ---- grpc-message ----

    /** Percent-encodes the description: UTF-8, with everything outside printable ASCII (and '%') as %XX. */
    static String encodeMessage(String s) {
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(b.length);
        for (byte x : b) {
            int c = x & 0xff;
            if (c >= 0x20 && c <= 0x7e && c != '%') {
                sb.append((char) c);
            } else {
                sb.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16))).append(Character.toUpperCase(Character.forDigit(c & 15, 16)));
            }
        }
        return sb.toString();
    }

    /** The inverse; a malformed escape is kept as it is. */
    static String decodeMessage(String s) {
        if (s.indexOf('%') < 0) {
            return s;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() && Character.digit(s.charAt(i + 1), 16) >= 0 && Character.digit(s.charAt(i + 2), 16) >= 0) {
                out.write(Character.digit(s.charAt(i + 1), 16) << 4 | Character.digit(s.charAt(i + 2), 16));
                i += 2;
            } else {
                byte[] enc = String.valueOf(c).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.write(enc, 0, enc.length);
            }
        }
        return out.toString(java.nio.charset.StandardCharsets.UTF_8);
    }
}
