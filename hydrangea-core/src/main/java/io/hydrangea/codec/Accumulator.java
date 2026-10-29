package io.hydrangea.codec;

import io.hydrangea.BufferPool;

import java.nio.ByteBuffer;

/**
 * Collects bytes across reads. {@link #buffer()} is always in read mode: consume from it by advancing its
 * position; the consumed prefix is reclaimed on the next {@link #append}.
 */
public final class Accumulator {

    private final BufferPool pool; // nullable
    private ByteBuffer buf;

    public Accumulator() {
        this(null);
    }

    public Accumulator(BufferPool pool) {
        this.pool = pool;
        this.buf = alloc(256).limit(0);
    }

    /** The unread bytes (position..limit). */
    public ByteBuffer buffer() {
        return buf;
    }

    public int remaining() {
        return buf.remaining();
    }

    public void append(ByteBuffer data) {
        int incoming = data.remaining();
        if (!buf.hasRemaining()) {
            buf.clear().limit(0); // fully consumed: restart from the front
        }
        int start = 0; // where unread data begins once back in read mode
        if (buf.capacity() - buf.limit() >= incoming) {
            start = buf.position();
            buf.position(buf.limit()).limit(buf.capacity()); // enough tail room: switch to write mode in place
        } else {
            buf.compact(); // write mode; reclaims the consumed prefix
            if (buf.remaining() < incoming) {
                ByteBuffer bigger = alloc(Math.max(buf.position() + incoming, buf.capacity() * 2));
                bigger.put(buf.flip());
                free(buf);
                buf = bigger;
            }
        }
        buf.put(data);
        buf.limit(buf.position()).position(start);
    }

    /** Returns the internal buffer to the pool (if any). The accumulator must not be used afterwards. */
    public void release() {
        free(buf);
        buf = ByteBuffer.allocate(0);
    }

    private ByteBuffer alloc(int n) {
        return pool != null ? pool.acquire(n) : ByteBuffer.allocate(n);
    }

    private void free(ByteBuffer b) {
        if (pool != null) {
            pool.release(b);
        }
    }
}
