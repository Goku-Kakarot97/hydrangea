package io.hydrangea.postgres;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The rows of a query read in pieces, from {@link PgClient#stream}: the server is asked for {@code fetchSize} rows at a time (a cursor), and the next piece only
 * when the caller has taken the last one, so a result of any size is read with memory for about two pieces. Thread-safe.
 * <p>
 * <b>The connection belongs to the stream until it ends</b> (the last row was read, an error happened, or {@link #close} was called): queries sent to the same
 * {@link PgClient} meanwhile wait and run afterwards, in order. Always read to the end or close it.
 */
public final class PgRowStream {

    private final PgClient client;
    private final Object op;
    private final List<PgColumn> columns;

    PgRowStream(PgClient client, Object op, PgColumn[] columns) {
        this.client = client;
        this.op = op;
        this.columns = List.of(columns);
    }

    public List<PgColumn> columns() {
        return columns;
    }

    /**
     * The next piece, at most {@code fetchSize} rows; an <b>empty list means the end</b>. Fails with the server's error if the query failed after it began, or with the
     * connection's if it was lost. Wait for one piece before asking for the next.
     */
    public CompletableFuture<List<PgRow>> next() {
        CompletableFuture<List<PgRow>> f = new CompletableFuture<>();
        client.streamNext(op, f);
        return f;
    }

    /** Gives up on the rest; completes when the connection is free again. Harmless after the end or a second time. */
    public CompletableFuture<Void> close() {
        return client.streamClose(op);
    }

    /** Hands every remaining row to {@code consumer} (on the thread that completes each piece: do not block in it), then completes with the number of rows. Closes the stream if the consumer throws. */
    public CompletableFuture<Long> forEach(Consumer<PgRow> consumer) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        pump(consumer, 0L, result);
        return result;
    }

    private void pump(Consumer<PgRow> consumer, long total, CompletableFuture<Long> result) {
        next().whenComplete((batch, err) -> {
            if (err != null) {
                result.completeExceptionally(err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err);
                return;
            }
            if (batch.isEmpty()) {
                result.complete(total);
                return;
            }
            try {
                for (PgRow row : batch) {
                    consumer.accept(row);
                }
            } catch (Throwable t) {
                close().whenComplete((v, e) -> result.completeExceptionally(t));
                return;
            }
            pump(consumer, total + batch.size(), result);
        });
    }
}
