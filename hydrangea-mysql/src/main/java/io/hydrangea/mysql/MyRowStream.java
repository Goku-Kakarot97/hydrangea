package io.hydrangea.mysql;

import io.hydrangea.EventLoop;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A query read in pieces through a server-side cursor; from {@link MyClient#stream}. {@link #next()} asks the server for the next {@code fetchSize} rows (one
 * {@code COM_STMT_FETCH}), so memory holds about one piece whatever the size of the result. The connection belongs to the stream until the last piece has been
 * taken or {@link #close()} is called: a stream that is dropped half way keeps the connection waiting, so always read to the end or close it.
 * <pre>{@code
 * MyRowStream s = db.stream(1000, "SELECT id, payload FROM events WHERE day = ?", day).join();
 * for (List<MyRow> piece = s.next().join(); !piece.isEmpty(); piece = s.next().join()) {
 *     for (MyRow r : piece) { ... }
 * }
 * }</pre>
 */
public final class MyRowStream {

    private final MyClient client;
    private final EventLoop loop;
    final int fetchSize;
    MyClient.Stmt stmt;                 // set when the cursor is open
    private List<MyColumn> columns = List.of();
    MyRow.Meta meta;
    boolean open;                       // the EXECUTE answered: the statement exists on the server and the stream counts as a call in flight
    // loop thread
    private List<MyRow> whole;          // the server could not open a cursor: the whole result, given as the one piece
    private boolean last;               // the server said the last row has been sent
    private boolean done;               // nothing more to give: the statement has been closed
    private MyException failure;
    private CompletableFuture<List<MyRow>> inFlight;

    MyRowStream(MyClient client, EventLoop loop, int fetchSize) {
        this.client = client;
        this.loop = loop;
        this.fetchSize = fetchSize;
    }

    void opened(MyClient.Stmt stmt, List<MyColumn> columns, MyRow.Meta meta) {
        this.stmt = stmt;
        this.columns = columns;
        this.meta = meta;
        this.open = true;
    }

    public List<MyColumn> columns() {
        return columns;
    }

    /** The next piece: up to {@code fetchSize} rows, or an empty list once every row has been given (the stream then closes itself). One call at a time. */
    public CompletableFuture<List<MyRow>> next() {
        CompletableFuture<List<MyRow>> f = new CompletableFuture<>();
        onLoop(() -> {
            if (failure != null) {
                f.completeExceptionally(failure);
                return;
            }
            if (done) {
                f.complete(List.of());
                return;
            }
            if (inFlight != null && !inFlight.isDone()) {
                f.completeExceptionally(new IllegalStateException("a piece is already being fetched"));
                return;
            }
            if (whole != null) {
                List<MyRow> rows = whole;
                whole = null;
                end();
                f.complete(rows);
                return;
            }
            if (last) {
                end();
                f.complete(List.of());
                return;
            }
            inFlight = f;
            client.fetch(this, f);
            f.whenComplete((rows, e) -> {
                if (e == null && last && rows.isEmpty()) {
                    end();
                }
            });
        });
        return f;
    }

    /** Gives the connection back without reading the rest. */
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            end();
            f.complete(null);
        });
        return f;
    }

    /** True once every row has been given or the stream was closed. */
    public boolean isDone() {
        return done;
    }

    // loop thread
    void last() {
        last = true;
    }

    void whole(List<MyRow> rows) {
        whole = rows;
    }

    void failed(MyException e) {
        if (failure == null) {
            failure = e;
        }
        done = true;
    }

    void done() {
        done = true;
    }

    private void end() {
        if (done) {
            return;
        }
        done = true;
        client.endStream(this);
    }

    private void onLoop(Runnable r) {
        if (loop.inEventLoop()) {
            r.run();
        } else {
            try {
                loop.execute(r);
            } catch (RuntimeException shutDown) {
                // the loop is gone; nothing more can happen
            }
        }
    }
}
