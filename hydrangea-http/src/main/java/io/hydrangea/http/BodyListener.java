package io.hydrangea.http;

import java.nio.ByteBuffer;

/**
 * Receives a response as it arrives instead of after it ended. All callbacks run on the connection's event loop
 * thread, one at a time and in order; they must not block. Exactly one of {@link #onComplete} and {@link #onError}
 * ends the stream (neither is called if the request failed before it could start only when the client was closed
 * without a result; normally every stream ends with one of them).
 * <p>
 * A throwing callback aborts the stream: the connection is closed and {@link #onError} receives the exception.
 */
public interface BodyListener {

    /** The response head arrived (after any redirects). Data follows. */
    default void onHead(ResponseHead head, BodyControl control) throws Exception {
    }

    /**
     * A piece of the body. The buffer is a read-only view that is only valid during this call: copy what you keep.
     * Piece boundaries have no meaning (they follow TCP reads and HTTP chunks).
     */
    default void onData(ByteBuffer data, BodyControl control) throws Exception {
    }

    /**
     * HTTP/2 only: the trailing header block that ended the response (gRPC puts its status there). Called after the last {@link #onData} and
     * before {@link #onComplete}; not called when there are no trailers. Streamed responses only: a buffered {@link ClientResponse} drops them.
     */
    default void onTrailers(Headers trailers) throws Exception {
    }

    /** The body ended normally. */
    default void onComplete() {
    }

    /** The stream failed or was cancelled; also reports a request that failed before any response arrived. */
    default void onError(Throwable error) {
    }
}
