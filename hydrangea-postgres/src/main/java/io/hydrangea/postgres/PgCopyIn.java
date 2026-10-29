package io.hydrangea.postgres;

import java.util.concurrent.CompletableFuture;

/**
 * An open {@code COPY ... FROM STDIN}, from {@link PgClient#copyIn(String)}: the server is waiting for data. Send it with {@link #write}, then
 * {@link #finish} (or {@link #abort}). Thread-safe, so the data can come from a thread that reads a file or a socket.
 * <p>
 * <b>The connection belongs to the copy until it ends</b>: queries sent to the same {@link PgClient} meanwhile wait and run afterwards, in order. Always end
 * the copy; one that is left open blocks its connection.
 */
public final class PgCopyIn {

    private final PgClient client;
    private final Object op;
    private final CompletableFuture<Long> result;

    PgCopyIn(PgClient client, Object op, CompletableFuture<Long> result) {
        this.client = client;
        this.op = op;
        this.result = result;
    }

    /**
     * Sends a piece of the data (any size; pieces need not end at a row boundary). Completes when the connection can take more: wait for it before the next
     * {@code write} to keep memory bounded. Fails if the copy has ended, for example because the server rejected the data (see {@link #finish} for the error).
     */
    public CompletableFuture<Void> write(byte[] chunk) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        client.copyWrite(op, chunk, f);
        return f;
    }

    /** Ends the data: completes with the number of rows copied, or fails with the server's error (a bad row, a constraint). */
    public CompletableFuture<Long> finish() {
        client.copyEnd(op, null);
        return result;
    }

    /** Cancels the copy; the server undoes it and the returned future fails with its error (SQLSTATE 57014), which mentions {@code reason}. */
    public CompletableFuture<Long> abort(String reason) {
        client.copyEnd(op, reason == null ? "aborted" : reason);
        return result;
    }
}
