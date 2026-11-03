package io.hydrangea.log;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A bounded lock-free queue: many producers, one consumer (Vyukov's bounded MPMC queue, used here with a single consumer).
 * Every slot carries a sequence number that says whose turn it is, so a producer claims a slot with one CAS on the tail and
 * publishes it with one volatile write, and the consumer never touches the tail. Head and tail are padded so they do not share a
 * cache line. The capacity is rounded up to a power of two.
 */
final class RingBuffer {

    private final int mask;
    private final Object[] items;
    private final AtomicLongArray seq;
    private final PaddedLong tail = new PaddedLong();
    private final PaddedLong head = new PaddedLong();

    RingBuffer(int capacity) {
        int cap = 2;
        while (cap < capacity) {
            cap <<= 1;
        }
        mask = cap - 1;
        items = new Object[cap];
        seq = new AtomicLongArray(cap);
        for (int i = 0; i < cap; i++) {
            seq.set(i, i);
        }
    }

    int capacity() {
        return mask + 1;
    }

    /** Any thread. {@code false} when full. */
    boolean offer(Object e) {
        long pos = tail.get();
        for (;;) {
            int idx = (int) (pos & mask);
            long diff = seq.get(idx) - pos;
            if (diff == 0) {
                if (tail.compareAndSet(pos, pos + 1)) {
                    items[idx] = e;
                    seq.set(idx, pos + 1);
                    return true;
                }
            } else if (diff < 0) {
                return false;
            }
            pos = tail.get();
        }
    }

    /** Consumer only. {@code null} when empty (or when the next slot is claimed but not yet published: try again). */
    Object poll() {
        long pos = head.getPlain();
        int idx = (int) (pos & mask);
        if (seq.get(idx) - (pos + 1) < 0) {
            return null;
        }
        Object e = items[idx];
        items[idx] = null;
        seq.set(idx, pos + mask + 1);
        head.setOpaque(pos + 1);
        return e;
    }

    /** Consumer only. */
    boolean isEmpty() {
        long pos = head.getPlain();
        return seq.get((int) (pos & mask)) - (pos + 1) < 0;
    }

    /** Slots claimed by producers so far (published or about to be). */
    long claimed() {
        return tail.get();
    }

    /** Slots consumed so far. */
    long consumed() {
        return head.getOpaque();
    }

    int size() {
        return (int) Math.max(0, Math.min(mask + 1, claimed() - consumed()));
    }

    @SuppressWarnings("unused")
    abstract static class LeftPad {
        long p00, p01, p02, p03, p04, p05, p06, p07;
    }

    abstract static class Value extends LeftPad {
        volatile long value;
    }

    @SuppressWarnings("unused")
    static final class PaddedLong extends Value {
        private static final VarHandle VALUE;

        static {
            try {
                VALUE = MethodHandles.lookup().findVarHandle(Value.class, "value", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        long p10, p11, p12, p13, p14, p15, p16, p17;

        long get() {
            return value;
        }

        long getPlain() {
            return (long) VALUE.get(this);
        }

        long getOpaque() {
            return (long) VALUE.getOpaque(this);
        }

        void setOpaque(long v) {
            VALUE.setOpaque(this, v);
        }

        boolean compareAndSet(long expected, long v) {
            return VALUE.compareAndSet(this, expected, v);
        }
    }
}
