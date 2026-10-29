package io.hydrangea.websocket;

import io.hydrangea.EventLoop;

import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * One end of an established WebSocket. All methods are thread-safe; sends from one thread keep their order.
 * <p>
 * The futures returned by the {@code send*} methods complete when the frame has been <b>queued</b> on the connection
 * (not when the peer received it), or fail if the socket is closed. For flow control use {@link #isWritable()} and
 * {@link WebSocketListener#onWritabilityChanged}: when it turns false, stop producing until it is true again.
 */
public interface WebSocket {

    CompletableFuture<Void> sendText(String text);

    /** The remaining bytes of {@code data} are copied; the buffer may be reused immediately. */
    CompletableFuture<Void> sendBinary(ByteBuffer data);

    /**
     * Sends one piece of a text message, which goes out as a WebSocket fragment: the first call starts the message
     * (a text frame without FIN), later calls continue it, and {@code last = true} ends it. Pieces may be split anywhere,
     * even inside a surrogate pair. Pings, pongs and close frames may be sent between pieces; another data message may not
     * (that future fails with {@link IllegalStateException}). There is no size limit on pieces
     * or on the message as a whole: the pieces are yours to size, and a peer that buffers messages enforces its own limit.
     */
    CompletableFuture<Void> sendTextPart(String part, boolean last);

    /** Binary counterpart of {@link #sendTextPart}. */
    CompletableFuture<Void> sendBinaryPart(ByteBuffer part, boolean last);

    /** Completes when {@link #isWritable()} is true (immediately if it is), or fails if the socket closes meanwhile. */
    CompletableFuture<Void> whenWritable();

    /**
     * Sends everything {@code source} yields as one binary message in 64 KiB fragments, reading on a virtual thread and
     * waiting for {@link #whenWritable()} between fragments, so memory use stays constant whatever the size. The
     * stream is closed at the end. The future completes when the last fragment has been queued.
     */
    default CompletableFuture<Void> sendBinaryStream(InputStream source) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try (source) {
                byte[] current = new byte[64 * 1024];
                byte[] next = new byte[64 * 1024];
                int n = source.readNBytes(current, 0, current.length);
                while (true) {
                    // read ahead one block so the block being sent is known to be the last one or not
                    int ahead = n < current.length ? 0 : source.readNBytes(next, 0, next.length);
                    boolean last = ahead == 0;
                    whenWritable().get();
                    sendBinaryPart(ByteBuffer.wrap(current, 0, n), last).get();
                    if (last) {
                        break;
                    }
                    byte[] t = current;
                    current = next;
                    next = t;
                    n = ahead;
                }
                done.complete(null);
            } catch (Throwable t) {
                done.completeExceptionally(t instanceof java.util.concurrent.ExecutionException && t.getCause() != null ? t.getCause() : t);
            }
        });
        return done;
    }

    /** @param data at most 125 bytes */
    CompletableFuture<Void> sendPing(ByteBuffer data);

    /**
     * Starts the closing handshake. After this call no more messages can be sent. The future completes when the
     * connection is actually closed: after the peer answered, or after {@link WebSocketConfig#closeTimeout()}.
     *
     * @param code 1000-1003, 1007-1014 or 3000-4999
     * @param reason at most 123 UTF-8 bytes
     */
    CompletableFuture<Void> close(int code, String reason);

    default CompletableFuture<Void> close() {
        return close(1000, "");
    }

    /** True from the moment the handshake completed until a close is started or received. */
    boolean isOpen();

    /** False while too much outgoing data is queued; see {@link io.hydrangea.Options#highWaterMark()}. */
    boolean isWritable();

    /**
     * Pause/resume reading incoming frames (backpressure towards a fast peer). While paused nothing is delivered to the
     * listener, and pings (hence pongs) and a peer's Close frame wait too, so do not stay paused longer than the
     * peer's patience. Unlike a plain socket pause, bytes already read are held back as well.
     */
    void setAutoRead(boolean enabled);

    /** The subprotocol agreed in the handshake, or null. */
    String subprotocol();

    SocketAddress remoteAddress();

    EventLoop eventLoop();
}
