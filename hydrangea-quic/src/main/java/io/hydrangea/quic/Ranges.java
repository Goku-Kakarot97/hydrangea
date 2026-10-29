package io.hydrangea.quic;

import java.util.Map;
import java.util.TreeMap;

/** A set of disjoint half-open ranges [start, end) of longs (packet numbers, stream offsets), merged as they are added. */
final class Ranges {
    private final TreeMap<Long, Long> r = new TreeMap<>();   // start -> end

    boolean isEmpty() {
        return r.isEmpty();
    }

    void add(long start, long end) {
        if (end <= start) {
            return;
        }
        Map.Entry<Long, Long> e = r.floorEntry(start);
        if (e != null && e.getValue() >= start) {
            start = e.getKey();
            end = Math.max(end, e.getValue());
            r.remove(e.getKey());
        }
        for (e = r.ceilingEntry(start); e != null && e.getKey() <= end; e = r.ceilingEntry(start)) {
            end = Math.max(end, e.getValue());
            r.remove(e.getKey());
        }
        r.put(start, end);
    }

    boolean contains(long v) {
        Map.Entry<Long, Long> e = r.floorEntry(v);
        return e != null && v < e.getValue();
    }

    /** True if [start, end) is covered completely. */
    boolean covers(long start, long end) {
        if (end <= start) {
            return true;
        }
        Map.Entry<Long, Long> e = r.floorEntry(start);
        return e != null && e.getValue() >= end;
    }

    /** End of the range containing {@code start}, or {@code start} itself if it is not covered. */
    long contiguousEnd(long start) {
        Map.Entry<Long, Long> e = r.floorEntry(start);
        return e != null && e.getValue() > start ? e.getValue() : start;
    }

    /** The first sub-range of [start, end) that is not covered, as {from, to}, or null if all of it is. */
    long[] firstGap(long start, long end) {
        long pos = start;
        while (pos < end) {
            long covered = contiguousEnd(pos);
            if (covered > pos) {
                pos = covered;
                continue;
            }
            Long next = r.higherKey(pos);
            long to = next == null ? end : Math.min(end, next);
            return new long[]{pos, to};
        }
        return null;
    }

    /** Highest value covered plus one, or 0. */
    long max() {
        return r.isEmpty() ? 0 : r.lastEntry().getValue();
    }

    /** Ranges from the highest to the lowest, as {start, end} pairs (for building ACK frames). */
    java.util.List<long[]> descending(int limit) {
        java.util.List<long[]> out = new java.util.ArrayList<>();
        for (Map.Entry<Long, Long> e : r.descendingMap().entrySet()) {
            if (out.size() >= limit) {
                break;
            }
            out.add(new long[]{e.getKey(), e.getValue()});
        }
        return out;
    }

    /** Drops everything below {@code below} (old packet numbers nobody can still send). */
    void trimBelow(long below) {
        for (Map.Entry<Long, Long> e = r.firstEntry(); e != null && e.getValue() <= below; e = r.firstEntry()) {
            r.remove(e.getKey());
        }
    }

    int size() {
        return r.size();
    }
}
