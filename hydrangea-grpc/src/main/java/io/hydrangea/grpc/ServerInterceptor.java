package io.hydrangea.grpc;

/**
 * Sees every call a {@link GrpcServer} serves: to authenticate (reject by throwing a {@link StatusException}), log, count, trace, or add
 * trailing metadata through {@link CallContext#trailers()}. It observes messages but does not change them.
 * <pre>{@code
 * GrpcServer.builder().intercept((method, ctx) -> {
 *     if (ctx.requestMetadata().get("authorization") == null) throw new StatusException(Status.of(Status.Code.UNAUTHENTICATED));
 *     long t0 = System.nanoTime();
 *     return new ServerInterceptor.Listener() {
 *         public void onClose(Status s) { log(method.fullName(), s, System.nanoTime() - t0); }
 *     };
 * })
 * }</pre>
 * Interceptors run in the order they were added, before the handler; a rejected call never reaches the handler (or the interceptors after the
 * rejecting one). The HTTP/2 response head is already on the wire by then, so a rejection arrives as a status in the trailers.
 */
@FunctionalInterface
public interface ServerInterceptor {

    /** Called when a call starts. Return a listener for its events, or null. */
    Listener start(MethodDescriptor<?, ?> method, CallContext ctx) throws StatusException;

    /**
     * Events of one call. Calls are never concurrent with each other (they are serialised), but may come from different threads:
     * request messages from the I/O thread, responses from whichever thread the handler answers on.
     */
    interface Listener {
        /** A request message was received (already parsed). */
        default void onRequest(Object message) {
        }

        /** A response message is being sent. */
        default void onResponse(Object message) {
        }

        /** The call ended, with this status: sent to the client, or CANCELLED when the client or connection went away. Exactly once. */
        default void onClose(Status status) {
        }
    }
}
