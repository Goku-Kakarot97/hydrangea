package io.hydrangea.http;

import java.util.concurrent.CompletableFuture;

/** Handle to a running streamed request. */
public final class StreamHandle {

    private final HttpClient.Exchange exchange;

    StreamHandle(HttpClient.Exchange exchange) {
        this.exchange = exchange;
    }

    /** Aborts the request and closes its connection. Safe from any thread; no effect once the stream has ended. */
    public void cancel() {
        exchange.fail(new java.util.concurrent.CancellationException("stream cancelled"));
    }

    /** Completes when the stream ended normally; fails with the same error {@link BodyListener#onError} received. */
    public CompletableFuture<Void> completion() {
        return exchange.result.thenApply(r -> null);
    }
}
