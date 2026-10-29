package io.hydrangea.http;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Application entry point. Called on the connection's event loop, so it must not block: do slow work elsewhere
 * and complete the returned stage when done (any thread). A failed stage or thrown exception yields a 500.
 * Requests on one connection are handled strictly one at a time and answered in order.
 */
@FunctionalInterface
public interface HttpHandler {

    CompletionStage<HttpResponse> handle(HttpRequest request) throws Exception;

    /**
     * HTTP/2 only. Return true to have {@link #handle} called <em>as soon as the request head arrives</em>, before any body, and to take
     * the whole exchange into your own hands: the response you return must be a {@link HttpResponse#stream takeover} whose stage then
     * receives the request body as it comes ({@code onRead}), may write the response as it likes, can end it with trailers through
     * {@link Http2Stream#sendTrailers}, and, if it implements {@link HalfCloseListener}, hears when the client has finished sending.
     * This is what gRPC needs (streaming in both directions, a status in the trailers). {@code head.body()} is empty. Ignored for HTTP/1.1,
     * where {@code handle} runs after the whole request as always.
     */
    default boolean wantsStream(HttpRequest head) {
        return false;
    }

    /**
     * Asked once the request head is parsed and a body follows: return true to receive that body as a stream via
     * {@link #handleStreaming} instead of having it buffered (and size-limited) for {@link #handle}.
     */
    default boolean wantsStreamingBody(HttpRequest head) {
        return false;
    }

    /**
     * Called as soon as the head of a request with a body has been parsed, before any body byte is read.
     * {@code head.body()} is empty. The returned stage may complete before the body ends (an early reject); the server
     * then answers, reads and discards a bounded amount of the remaining body, and closes the connection.
     */
    default CompletionStage<HttpResponse> handleStreaming(HttpRequest head, RequestBodyStream body) throws Exception {
        throw new UnsupportedOperationException("not a streaming handler");
    }

    /** Adapter for handlers that answer immediately. */
    static HttpHandler sync(Sync handler) {
        return request -> CompletableFuture.completedFuture(handler.handle(request));
    }

    @FunctionalInterface
    interface Sync {
        HttpResponse handle(HttpRequest request) throws Exception;
    }
}
