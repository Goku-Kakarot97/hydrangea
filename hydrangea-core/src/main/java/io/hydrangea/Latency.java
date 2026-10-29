package io.hydrangea;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

/**
 * A latency distribution that costs almost nothing where the work happens and does its arithmetic elsewhere.
 * <p>
 * <b>Hot path</b> ({@link #begin}, {@link #end}, {@link #record}): read the clock, and one atomic add into a pre-allocated bucket (plus one
 * {@link LongAdder} for the sum). No allocation, no lock, no queue. Writers are spread over a few stripes by thread so that event loops do not
 * fight over a cache line. While no {@link MetricsSink} is bound, recording is one volatile read.
 * <p>
 * <b>Background</b>: a single daemon thread of the owning {@link Metrics} wakes every few seconds, merges the stripes and turns the difference
 * since its last visit into quantiles (p50, p90, p99, p99.9, max) of <i>that window</i>, not of the whole life of the process. The sink reads
 * those, plus the running count and sum, when the monitoring system scrapes.
 * <p>
 * Buckets are logarithmic with eight steps per doubling (a value is off by at most 12.5%); quantiles report the upper edge of the bucket, so they
 * never understate. Range 1 ns to about 18 minutes; longer values land in the last bucket.
 */
public final class Latency {

    private static final int SUB_BITS = 3;
    private static final int SUB = 1 << SUB_BITS;
    private static final int MAX_MSB = 40;
    static final int BUCKETS = (MAX_MSB - SUB_BITS + 1) * SUB + SUB;
    private static final int STRIPES = 8;

    static final double[] QUANTILES = {0.5, 0.9, 0.99, 0.999, 1.0};

    private final Metrics owner;
    private final AtomicLongArray[] stripes = new AtomicLongArray[STRIPES];
    private final LongAdder sumNanos = new LongAdder();

    private long[] previous = new long[BUCKETS];          // background thread only
    private final double[] window = new double[QUANTILES.length];
    private volatile double[] published = nans();

    Latency(Metrics owner) {
        this.owner = owner;
        for (int i = 0; i < STRIPES; i++) {
            stripes[i] = new AtomicLongArray(BUCKETS);
        }
    }

    private static double[] nans() {
        double[] a = new double[QUANTILES.length];
        java.util.Arrays.fill(a, Double.NaN);
        return a;
    }

    /** The start of something to time, or 0 when nobody is listening (then {@link #end} does nothing). Costs one clock read. */
    public long begin() {
        if (!owner.enabled) {
            return 0;
        }
        long t = System.nanoTime();
        return t == 0 ? 1 : t;
    }

    /** Records the time since {@code start} (what {@link #begin} returned); does nothing for 0. */
    public void end(long start) {
        if (start != 0 && owner.enabled) {
            add(System.nanoTime() - start);
        }
    }

    /**
     * The clock, or 0 when nobody is listening. For code that handles many things in one go (an event loop draining a queue): read it once for
     * the batch, hand it to every item as its start, and give it to {@link #endAt} as the end. Reading the clock is the dearest part of timing:
     * measured on a Mac, {@code System.nanoTime()} cost 12 ns for one thread but 345 ns per call with 8 threads reading at once (a counter
     * increment is 7-13 ns either way).
     */
    public long now() {
        return begin();
    }

    /** Records {@code endNanos - startNanos}; does nothing if either is 0 (not timed). */
    public void endAt(long startNanos, long endNanos) {
        if (startNanos != 0 && endNanos != 0 && owner.enabled) {
            add(endNanos - startNanos);
        }
    }

    /** Records a duration you measured yourself. */
    public void record(long nanos) {
        if (owner.enabled) {
            add(nanos);
        }
    }

    private void add(long nanos) {
        if (nanos < 0) {
            nanos = 0;
        }
        stripes[(int) Thread.currentThread().threadId() & (STRIPES - 1)].incrementAndGet(bucketOf(nanos));
        sumNanos.add(nanos);
    }

    static int bucketOf(long v) {
        if (v < SUB) {
            return (int) v;
        }
        int msb = 63 - Long.numberOfLeadingZeros(v);
        if (msb > MAX_MSB) {
            return BUCKETS - 1;
        }
        return (msb - SUB_BITS + 1) * SUB + (int) ((v >>> (msb - SUB_BITS)) & (SUB - 1));
    }

    /** The smallest value that is NOT in bucket {@code i}: its upper edge. */
    static long upperEdge(int i) {
        if (i < SUB) {
            return i + 1;
        }
        int msb = i / SUB + SUB_BITS - 1;
        long sub = i % SUB;
        return (SUB + sub + 1) << (msb - SUB_BITS);
    }

    private long[] merged() {
        long[] m = new long[BUCKETS];
        for (AtomicLongArray s : stripes) {
            for (int i = 0; i < BUCKETS; i++) {
                m[i] += s.get(i);
            }
        }
        return m;
    }

    /** Observations so far. Read by the sink at scrape time. */
    public long count() {
        long n = 0;
        for (long c : merged()) {
            n += c;
        }
        return n;
    }

    /** Total of all observations, in seconds. */
    public double sumSeconds() {
        return sumNanos.sum() / 1e9;
    }

    /** Quantile {@code i} of {@link #QUANTILES} for the last window, in seconds; NaN when the window saw nothing. */
    double quantile(int i) {
        return published[i];
    }

    /** Called by the owner's background thread: quantiles of what happened since the previous call. */
    void tick() {
        long[] now = merged();
        long[] delta = new long[BUCKETS];
        long total = 0;
        for (int i = 0; i < BUCKETS; i++) {
            delta[i] = Math.max(0, now[i] - previous[i]);
            total += delta[i];
        }
        previous = now;
        if (total == 0) {
            published = nans();
            return;
        }
        int q = 0;
        long seen = 0;
        double[] out = new double[QUANTILES.length];
        for (int i = 0; i < BUCKETS && q < QUANTILES.length; i++) {
            seen += delta[i];
            while (q < QUANTILES.length && seen >= Math.ceil(QUANTILES[q] * total) && delta[i] > 0) {
                out[q++] = upperEdge(i) / 1e9;
            }
        }
        while (q < QUANTILES.length) {
            out[q] = out[q - 1];
            q++;
        }
        published = out;
    }
}
