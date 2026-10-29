package io.hydrangea.grpc;

import java.util.concurrent.CompletableFuture;

/**
 * One call as the client drives it: send request messages, say when you are done, cancel if you lose interest; the {@link Listener} hears
 * about everything that comes back. For a unary call use {@link GrpcChannel#unary}; this is the general form behind it and what
 * client-streaming and bidirectional calls are written with.
 * <pre>{@code
 * ClientCall<Msg, Msg> call = channel.bidiStreaming(BIDI, CallOptions.DEFAULT, listener);
 * call.send(new Msg("one"));
 * call.send(new Msg("two"));
 * call.halfClose();               // no more requests; replies keep coming until the server's status
 * }</pre>
 * Methods are thread-safe.
 */
public interface ClientCall<Req, Resp> {

    interface Listener<Resp> {
        /** The response headers arrived. */
        default void onHeaders(Metadata headers) {
        }

        /** A response message. Callbacks of one call never overlap and arrive in order; do not block in them. */
        default void onMessage(Resp message) {
        }

        /** The call ended: the final status (OK or not) and the trailing metadata. Exactly once, last. */
        default void onClose(Status status, Metadata trailers) {
        }
    }

    /**
     * Queues a request message. The future completes when it has been handed to the transport; it fails if the call is already over. A fast
     * producer should wait for {@link #whenReady()} between messages, or it buffers the whole stream in memory.
     */
    CompletableFuture<Void> send(Req message);

    /** No more request messages; the server may still answer. */
    void halfClose();

    /** Gives up on the call: the server is told (RST_STREAM), and the listener gets CANCELLED. */
    void cancel(String message);

    /** False while the output buffer is over its high-water mark. */
    boolean isReady();

    /** Completes when the call can take another message without buffering much (immediately if it can); fails if the call is over. */
    CompletableFuture<Void> whenReady();

    /** Stops delivering response messages (the server is slowed by HTTP/2 flow control) until {@link #resume()}. */
    void pause();

    void resume();
}
