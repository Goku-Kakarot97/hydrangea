package io.hydrangea.http;

import io.hydrangea.Connection;

/**
 * A taken-over HTTP/2 stream with what HTTP/2 has beyond a byte pipe. The {@link Connection} a takeover stage gets from its context is
 * one of these when the request arrived over HTTP/2: {@code ctx.connection() instanceof Http2Stream}.
 */
public interface Http2Stream extends Connection {

    /** The HTTP/2 stream number. */
    int streamId();

    /** The request headers (names in lower case, pseudo-headers excluded; {@code :path} etc. are in the {@link HttpRequest}). */
    Headers requestHeaders();

    /**
     * Ends the response with a trailing header block after everything written so far, instead of an empty DATA frame. Names must be lower
     * case and must not start with a colon. Thread-safe; later writes are dropped.
     */
    void sendTrailers(Headers trailers);

    /**
     * Adds headers to the response head if it has not been sent yet, which it has not when the response was marked
     * {@link HttpResponse#deferHead() deferred} and nothing has been written. Names must be lower case and not start with a colon.
     * Does not wait: it is ordered before whatever you write or end afterwards, and ignored if the head has already gone out.
     */
    default void addResponseHeaders(Headers headers) {
    }
}
