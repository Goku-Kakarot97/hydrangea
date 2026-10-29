package io.hydrangea.pipeline;

/**
 * One step of a {@link Pipeline}. Stage 0 is closest to the socket.
 * <ul>
 *   <li><b>Inbound</b> events (activation, data, writability, close) travel 0 → n-1; a stage that wants the next
 *       stage to see an event must forward it with the matching {@code ctx.fire*} call. The defaults forward.</li>
 *   <li><b>Outbound</b> data travels n-1 → 0 via {@code ctx.write(msg)}; stage 0 hands {@link java.nio.ByteBuffer}s
 *       to the connection. The default forwards.</li>
 * </ul>
 * Stages are per-connection and called on the connection's loop thread; they may keep plain-field state.
 * An exception thrown by a stage closes the connection.
 */
public interface Stage {

    default void onActive(Context ctx) throws Exception {
        ctx.fireActive();
    }

    default void onRead(Context ctx, Object msg) throws Exception {
        ctx.fireRead(msg);
    }

    default void onWrite(Context ctx, Object msg) throws Exception {
        ctx.write(msg);
    }

    default void onWritabilityChanged(Context ctx) throws Exception {
        ctx.fireWritabilityChanged();
    }

    default void onClose(Context ctx, Throwable cause) {
        ctx.fireClose(cause);
    }

    /** Inbound: the server is shutting down gracefully; wind down and close (see {@code Handler#onDrain}). The default forwards. */
    default void onDrain(Context ctx) throws Exception {
        ctx.fireDrain();
    }

    /**
     * Outbound: a later stage asked to close the connection once everything written so far has been sent
     * ({@link Context#closeAfterFlush()}). A stage that buffers or transforms data (TLS) overrides this to flush
     * and finish its own protocol (close_notify) first; the default forwards towards the socket.
     */
    default void onCloseRequest(Context ctx) throws Exception {
        onCloseRequest(ctx, null);
    }

    /** As {@link #onCloseRequest(Context)}; a non-null {@code linger} asks for a lingering close (see {@code Connection#closeAfterFlush(Duration)}). */
    default void onCloseRequest(Context ctx, java.time.Duration linger) throws Exception {
        ctx.closeAfterFlush(linger);
    }
}
