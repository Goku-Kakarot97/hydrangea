package io.hydrangea.trace;

import java.util.concurrent.ThreadLocalRandom;

/**
 * What identifies a span on the wire: the trace it belongs to, its own id, and whether the trace is sampled (recorded). This is
 * exactly what a W3C {@code traceparent} header carries, so it is what crosses process boundaries.
 *
 * @param traceId 32 lower-case hex digits, not all zero
 * @param spanId  16 lower-case hex digits, not all zero
 * @param sampled the {@code 01} flag: whether spans of this trace are recorded
 */
public record SpanContext(String traceId, String spanId, boolean sampled) {

    private static final String ZERO_TRACE = "00000000000000000000000000000000";
    private static final String ZERO_SPAN = "0000000000000000";

    public SpanContext {
        if (traceId == null || traceId.length() != 32 || !hex(traceId, 0, 32) || traceId.equals(ZERO_TRACE)) {
            throw new IllegalArgumentException("trace id must be 32 hex digits, not all zero: " + traceId);
        }
        if (spanId == null || spanId.length() != 16 || !hex(spanId, 0, 16) || spanId.equals(ZERO_SPAN)) {
            throw new IllegalArgumentException("span id must be 16 hex digits, not all zero: " + spanId);
        }
    }

    /**
     * Reads a {@code traceparent} header ({@code 00-<trace id>-<parent id>-<flags>}); {@code null} when it is absent or not
     * usable (wrong shape, non-hex, all-zero ids, version {@code ff}). Upper-case hex is accepted and lowered. Nothing is thrown:
     * a client's header is not the server's fault.
     */
    public static SpanContext parse(String traceparent) {
        if (traceparent == null || traceparent.length() < 55) {
            return null;
        }
        String s = traceparent.trim();
        if (s.length() < 55 || s.charAt(2) != '-' || s.charAt(35) != '-' || s.charAt(52) != '-') {
            return null;
        }
        if (!hex(s, 0, 2) || !hex(s, 3, 35) || !hex(s, 36, 52) || !hex(s, 53, 55)) {
            return null;
        }
        boolean version0 = s.charAt(0) == '0' && s.charAt(1) == '0';
        if ((version0 && s.length() != 55) || (!version0 && s.length() > 55 && s.charAt(55) != '-') || s.startsWith("ff")) {
            return null;
        }
        String traceId = s.substring(3, 35).toLowerCase();
        String spanId = s.substring(36, 52).toLowerCase();
        if (traceId.equals(ZERO_TRACE) || spanId.equals(ZERO_SPAN)) {
            return null;
        }
        int flags = Integer.parseInt(s.substring(53, 55), 16);
        return new SpanContext(traceId, spanId, (flags & 1) == 1);
    }

    /** The {@code traceparent} header value for this context. */
    public String traceparent() {
        return "00-" + traceId + "-" + spanId + "-" + (sampled ? "01" : "00");
    }

    /** A copy with another span id (a child span in the same trace). */
    public SpanContext child(String spanId) {
        return new SpanContext(traceId, spanId, sampled);
    }

    public static String newTraceId() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long hi = r.nextLong();
        long lo = r.nextLong();
        if (hi == 0 && lo == 0) {
            lo = 1;
        }
        return hex16(hi) + hex16(lo);
    }

    public static String newSpanId() {
        long v = ThreadLocalRandom.current().nextLong();
        return hex16(v == 0 ? 1 : v);
    }

    private static String hex16(long v) {
        char[] c = new char[16];
        for (int i = 15; i >= 0; i--) {
            int d = (int) (v & 0xf);
            c[i] = (char) (d < 10 ? '0' + d : 'a' + d - 10);
            v >>>= 4;
        }
        return new String(c);
    }

    static boolean hex(String s, int from, int to) {
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }
}
