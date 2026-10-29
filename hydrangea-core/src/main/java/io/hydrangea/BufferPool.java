package io.hydrangea;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe pool of power-of-two sized buffers (256 B .. 1 MiB) to cut allocation and GC pressure.
 * Larger requests are allocated normally and never pooled.
 * <p>
 * Contract: {@link #release} must be called at most once per {@link #acquire}, and the buffer (and any
 * slice of it) must not be used afterwards. Pair it with {@link Connection#write(ByteBuffer, Runnable)}.
 */
public final class BufferPool {

    private static final int MIN_SHIFT = 8;   // 256 B
    private static final int MAX_SHIFT = 20;  // 1 MiB

    private final boolean direct;
    private final int maxPerClass;
    private final ConcurrentLinkedQueue<ByteBuffer>[] free;
    private final AtomicInteger[] sizes;
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public BufferPool(boolean direct, int maxPerClass) {
        this.direct = direct;
        this.maxPerClass = maxPerClass;
        @SuppressWarnings("unchecked")
        ConcurrentLinkedQueue<ByteBuffer>[] q = new ConcurrentLinkedQueue[MAX_SHIFT - MIN_SHIFT + 1];
        this.free = q;
        this.sizes = new AtomicInteger[q.length];
        for (int i = 0; i < q.length; i++) {
            free[i] = new ConcurrentLinkedQueue<>();
            sizes[i] = new AtomicInteger();
        }
    }

    public static BufferPool heap() {
        return new BufferPool(false, 256);
    }

    /** @return a cleared buffer with capacity >= {@code minCapacity} */
    public ByteBuffer acquire(int minCapacity) {
        if (minCapacity < 0) {
            throw new IllegalArgumentException("minCapacity < 0");
        }
        int shift = shiftFor(minCapacity);
        if (shift > MAX_SHIFT) {
            misses.increment();
            return allocate(minCapacity);
        }
        int idx = shift - MIN_SHIFT;
        ByteBuffer b = free[idx].poll();
        if (b != null) {
            sizes[idx].decrementAndGet();
            hits.increment();
            return b.clear();
        }
        misses.increment();
        return allocate(1 << shift);
    }

    /** Returns a buffer obtained from {@link #acquire}. Buffers of foreign sizes are simply dropped. */
    public void release(ByteBuffer buffer) {
        int cap = buffer.capacity();
        if (buffer.isDirect() != direct || cap < (1 << MIN_SHIFT) || cap > (1 << MAX_SHIFT)
                || Integer.bitCount(cap) != 1 || buffer.isReadOnly()) {
            return;
        }
        int idx = Integer.numberOfTrailingZeros(cap) - MIN_SHIFT;
        if (sizes[idx].incrementAndGet() > maxPerClass) {
            sizes[idx].decrementAndGet();
            return;
        }
        free[idx].add(buffer.clear());
    }

    public long hits() {
        return hits.sum();
    }

    public long misses() {
        return misses.sum();
    }

    private ByteBuffer allocate(int n) {
        return direct ? ByteBuffer.allocateDirect(n) : ByteBuffer.allocate(n);
    }

    private static int shiftFor(int n) {
        return n <= (1 << MIN_SHIFT) ? MIN_SHIFT : 32 - Integer.numberOfLeadingZeros(n - 1);
    }
}
