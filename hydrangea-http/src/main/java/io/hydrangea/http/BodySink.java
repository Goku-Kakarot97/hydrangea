package io.hydrangea.http;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Where a {@link RequestBody} puts the bytes of an upload. All methods are thread-safe, and calls from one thread keep
 * their order.
 * <p>
 * Backpressure: the connection queues what you write, so a producer that ignores flow control would buffer the whole
 * upload in memory. Before each write wait for {@link #whenWritable()} (blocking on a virtual thread is fine and is
 * what {@link RequestBody#file} does); it completes once the socket has drained below its high-water mark.
 */
public interface BodySink {

    /**
     * Queues a copy of the remaining bytes of {@code data} (the buffer may be reused immediately). The future completes
     * when the bytes are queued, or fails when the request is no longer being sent: it failed or was cancelled, the
     * server answered early, or the body would exceed its declared length.
     */
    CompletableFuture<Void> write(ByteBuffer data);

    /**
     * Like {@link #write} but hands {@code data} over: the caller must not touch the buffer or its array afterwards, in exchange for the
     * connection not copying it. Falls back to a copy where a sink cannot take it.
     */
    default CompletableFuture<Void> writeOwned(ByteBuffer data) {
        return write(data);
    }

    boolean isWritable();

    /** Completes when {@link #isWritable()}; fails if the request ended meanwhile. */
    CompletableFuture<Void> whenWritable();

    /** Ends the body. With a declared length, fewer bytes than declared aborts the request. */
    void finish();

    /** Aborts the request with {@code cause}. */
    void fail(Throwable cause);
}
