package io.hydrangea.log;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Formats epoch milliseconds as {@code 2026-11-03 10:15:42.123} (or ISO {@code 2026-11-03T10:15:42.123+07:00} / {@code ...Z}).
 * The part down to the second is computed once per second and reused, so formatting a timestamp is normally a few array copies.
 * Thread safe; the race on the cache is benign (each entry is immutable).
 */
final class Timestamps {

    private final ZoneId zone;
    private final boolean iso;
    private volatile Cached cached;

    private record Cached(long second, char[] prefix, String offset) {
    }

    Timestamps(ZoneId zone, boolean iso) {
        this.zone = zone;
        this.iso = iso;
    }

    void append(StringBuilder sb, long epochMillis) {
        long second = Math.floorDiv(epochMillis, 1000L);
        int millis = (int) Math.floorMod(epochMillis, 1000L);
        Cached c = cached;
        if (c == null || c.second != second) {
            c = compute(second);
            cached = c;
        }
        sb.append(c.prefix).append('.').append((char) ('0' + millis / 100)).append((char) ('0' + millis / 10 % 10)).append((char) ('0' + millis % 10));
        if (iso) {
            sb.append(c.offset);
        }
    }

    private Cached compute(long second) {
        ZoneOffset offset = zone.getRules().getOffset(Instant.ofEpochSecond(second));
        LocalDateTime t = LocalDateTime.ofEpochSecond(second, 0, offset);
        char[] p = new char[19];
        int year = t.getYear();
        p[0] = (char) ('0' + year / 1000 % 10);
        p[1] = (char) ('0' + year / 100 % 10);
        p[2] = (char) ('0' + year / 10 % 10);
        p[3] = (char) ('0' + year % 10);
        p[4] = '-';
        two(p, 5, t.getMonthValue());
        p[7] = '-';
        two(p, 8, t.getDayOfMonth());
        p[10] = iso ? 'T' : ' ';
        two(p, 11, t.getHour());
        p[13] = ':';
        two(p, 14, t.getMinute());
        p[16] = ':';
        two(p, 17, t.getSecond());
        return new Cached(second, p, offset.getId());
    }

    private static void two(char[] p, int at, int v) {
        p[at] = (char) ('0' + v / 10);
        p[at + 1] = (char) ('0' + v % 10);
    }
}
