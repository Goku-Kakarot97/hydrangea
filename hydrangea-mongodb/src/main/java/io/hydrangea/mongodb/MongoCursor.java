package io.hydrangea.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The documents of a {@code find}, {@code aggregate} or {@code listIndexes}, batch by batch: {@link #next} gives the next batch and an empty list at the end, after
 * which nothing is left open on the server. Batches must be asked for one at a time. A cursor left half-read holds a server-side cursor for 10 minutes of
 * inactivity; {@link #close} kills it at once. The cursor sticks to the connection that opened it (a server cursor belongs to its connection).
 */
public final class MongoCursor {

    private final MongoConnection conn;
    private final String db;
    private final String collection;
    private final int batchSize;
    private final MongoSession session;                            // the session the cursor was opened in (getMore must carry it), or null
    private Runnable onDone;                                       // releases an implicit session, once
    private long cursorId;
    private List<Map<String, Object>> first;
    private boolean busy;
    private boolean done;

    MongoCursor(MongoConnection conn, String db, Map<String, Object> reply, int batchSize, MongoSession session, Runnable onDone) {
        this.conn = conn;
        this.db = db;
        this.batchSize = batchSize;
        this.session = session;
        this.onDone = onDone;
        Map<String, Object> cursor = Bson.doc();
        if (reply.get("cursor") instanceof Map<?, ?> m) {
            @SuppressWarnings("unchecked") Map<String, Object> c = (Map<String, Object>) m;
            cursor = c;
        }
        this.cursorId = cursor.get("id") instanceof Number n ? n.longValue() : 0;
        String ns = cursor.get("ns") instanceof String s ? s : db + ".";
        this.collection = ns.substring(ns.indexOf('.') + 1);
        this.first = docs(cursor.get("firstBatch"));
        this.done = cursorId == 0 && first.isEmpty();
        if (cursorId == 0) {
            finished();
        }
    }

    /** The server-side cursor is gone (exhausted, killed, or lost): the implicit session may be reused. */
    private void finished() {
        Runnable r;
        synchronized (this) {
            r = onDone;
            onDone = null;
        }
        if (r != null) {
            r.run();
        }
    }

    private Map<String, Object> inSession(Map<String, Object> command) {
        return session == null ? command : session.decorate(command, false);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> docs(Object batch) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (batch instanceof List<?> l) {
            for (Object o : l) {
                if (o instanceof Map<?, ?> m) {
                    out.add((Map<String, Object>) m);
                }
            }
        }
        return out;
    }

    /** The next batch; empty when there is no more. */
    public CompletableFuture<List<Map<String, Object>>> next() {
        synchronized (this) {
            if (done) {
                return CompletableFuture.completedFuture(List.of());
            }
            if (busy) {
                return CompletableFuture.failedFuture(new IllegalStateException("the previous batch has not arrived yet"));
            }
            if (first != null) {
                List<Map<String, Object>> f = first;
                first = null;
                if (cursorId == 0) {
                    done = true;
                }
                return CompletableFuture.completedFuture(f);
            }
            busy = true;
        }
        Map<String, Object> getMore = Bson.doc("getMore", cursorId, "collection", collection);
        if (batchSize > 0) {
            getMore.put("batchSize", batchSize);
        }
        return conn.command(db, inSession(getMore)).handle((reply, err) -> {
            boolean over;
            List<Map<String, Object>> batch;
            synchronized (this) {
                busy = false;
                if (err != null) {
                    done = true;
                    cursorId = 0;
                    over = true;
                    batch = null;
                } else {
                    Map<String, Object> cursor = reply.get("cursor") instanceof Map<?, ?> m ? castMap(m) : Map.of();
                    cursorId = cursor.get("id") instanceof Number n ? n.longValue() : 0;
                    batch = docs(cursor.get("nextBatch"));
                    if (cursorId == 0) {
                        done = true;
                    }
                    over = done;
                    if (batch.isEmpty() && !done) {
                        batch = null;                              // an empty batch with the cursor still open (a tailable cursor, say): ask again
                    }
                }
            }
            if (over) {
                finished();
            }
            if (err != null) {
                throw err instanceof RuntimeException re ? re : new java.util.concurrent.CompletionException(err);
            }
            return batch;
        }).thenCompose(batch -> batch == null ? next() : CompletableFuture.completedFuture(batch));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    /** Every remaining document, in order, to {@code consumer} (on the event loop: it must not block); the count at the end. */
    public CompletableFuture<Long> forEach(Consumer<Map<String, Object>> consumer) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        step(consumer, 0, result);
        return result;
    }

    /** Every remaining document as a record ({@link Bson#toRecord}). */
    public <T> CompletableFuture<Long> forEach(Class<T> type, Consumer<T> consumer) {
        return forEach(d -> consumer.accept(Bson.toRecord(type, d)));
    }

    /** Every remaining document in one list (use with a limit, or a cursor is the point). */
    public CompletableFuture<List<Map<String, Object>>> toList() {
        List<Map<String, Object>> all = new ArrayList<>();
        return forEach(all::add).thenApply(n -> all);
    }

    public <T> CompletableFuture<List<T>> toList(Class<T> type) {
        List<T> all = new ArrayList<>();
        return forEach(type, all::add).thenApply(n -> all);
    }

    private void step(Consumer<Map<String, Object>> consumer, long count, CompletableFuture<Long> result) {
        next().whenComplete((batch, err) -> {
            if (err != null) {
                result.completeExceptionally(err);
                return;
            }
            if (batch.isEmpty()) {
                result.complete(count);
                return;
            }
            try {
                for (Map<String, Object> d : batch) {
                    consumer.accept(d);
                }
            } catch (Throwable t) {
                close();
                result.completeExceptionally(t);
                return;
            }
            step(consumer, count + batch.size(), result);
        });
    }

    /** The server-side cursor id, 0 once exhausted or closed. */
    public long id() {
        synchronized (this) {
            return cursorId;
        }
    }

    /** Kills the server-side cursor if one is open; further {@link #next} calls give an empty batch. */
    public CompletableFuture<Void> close() {
        long id;
        synchronized (this) {
            done = true;
            first = null;
            id = cursorId;
            cursorId = 0;
        }
        if (id == 0) {
            finished();
            return CompletableFuture.completedFuture(null);
        }
        return conn.command(db, inSession(Bson.doc("killCursors", collection, "cursors", List.of(id)))).handle((r, e) -> {
            finished();
            return null;
        });
    }
}
