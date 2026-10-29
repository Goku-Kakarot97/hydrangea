package io.hydrangea;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * One bidirectional byte pipe seen by a {@link Handler} or a pipeline stage: a TCP or Unix socket, or a single HTTP/2 stream
 * (the SSE and WebSocket stages run unchanged on either). Everything that is not documented as thread-safe must be called on
 * {@link #eventLoop()}.
 */
public interface Connection {

    /** The loop all of this connection's events run on. */
    EventLoop eventLoop();

    SocketAddress remoteAddress();

    SocketAddress localAddress();

    boolean isOpen();

    /** False while more than the high watermark is queued; true again once it drains below the low watermark ({@link Handler#onWritabilityChanged}). */
    boolean isWritable();

    /**
     * Completes at once if {@link #isWritable()}, otherwise when the queue has drained below the low watermark; fails if the
     * connection closes first, so a producer waiting on it cannot hang. Continuations run on {@link #eventLoop()}. The usual loop:
     * <pre>
     * void produce() {
     *     while (c.isWritable()) { if (!more()) return; c.write(next()); }
     *     c.whenWritable().thenRun(this::produce);
     * }
     * </pre>
     * Thread-safe. The default polls every millisecond (correct for any wrapper, such as an HTTP/2 stream); a socket connection
     * overrides it with an exact wake-up.
     */
    default CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        eventLoop().execute(() -> pollWritable(this, f));
        return f;
    }

    private static void pollWritable(Connection c, CompletableFuture<Void> f) {
        if (!c.isOpen()) {
            f.completeExceptionally(new java.io.IOException("the connection is closed"));
        } else if (c.isWritable()) {
            f.complete(null);
        } else {
            c.eventLoop().schedule(() -> pollWritable(c, f), 1, TimeUnit.MILLISECONDS);
        }
    }

    /** Bytes queued for sending and not yet accepted by the peer or the transport. Thread-safe. */
    long pendingBytes();

    /** Completes when the connection has closed. */
    CompletableFuture<Void> closeFuture();

    /**
     * Queues {@code data} for sending. Ownership of the buffer passes to the connection: do not touch it afterwards.
     * Silently dropped if the connection is closed. Thread-safe.
     */
    void write(ByteBuffer data);

    /**
     * Like {@link #write(ByteBuffer)}, and runs {@code done} on the loop thread once the buffer is no longer referenced by the
     * connection: fully sent, or discarded because the connection closed. Use it to return the buffer to a {@link BufferPool}.
     */
    void write(ByteBuffer data, Runnable done);

    /** Stops reading until re-enabled; the way to apply backpressure to a fast sender. Thread-safe. */
    void setAutoRead(boolean enabled);

    /** Closes now, discarding unsent data. Idempotent. Thread-safe. */
    void close();

    /** Sends everything already queued, then closes. Thread-safe. */
    void closeAfterFlush();

    /**
     * Sends everything already queued, then closes <em>lingeringly</em>: the sending side ends cleanly and incoming bytes are
     * read and thrown away until the peer closes or {@code linger} elapses (so a final message is not destroyed by a reset).
     * A null {@code linger} closes right after flushing. Thread-safe.
     */
    void closeAfterFlush(Duration linger);
}
