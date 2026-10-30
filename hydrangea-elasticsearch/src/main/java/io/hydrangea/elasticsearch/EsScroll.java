package io.hydrangea.elasticsearch;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Every hit of a search, page by page, through the scroll API: {@link #next} gives the next page (the search's {@code size} hits, the last one shorter) and an
 * empty list at the end, after which the scroll has been cleared on the cluster. Pages must be asked for one at a time. A scroll left half-read holds resources on
 * the cluster until {@code keepAlive} passes since the last page: {@link #close} clears it at once.
 * <pre>{@code
 * EsScroll s = es.scroll("logs", Json.obj("size", 1000, "query", Json.obj("match_all", Json.obj())), Duration.ofMinutes(1));
 * s.forEach(hit -> ...).thenAccept(count -> ...);
 * }</pre>
 * The scroll keeps the view of the index it had when it was opened; writes after that are not seen. For a live, resumable walk use {@code search_after} with a
 * {@code sort} and {@link EsSearchResponse#lastSort}.
 */
public final class EsScroll {

    private final EsClient client;
    private final String index;
    private final Object body;
    private final String keepAlive;
    private String scrollId;
    private long total = -1;
    private boolean busy;
    private boolean done;

    EsScroll(EsClient client, String index, Object body, Duration keepAlive) {
        this.client = client;
        this.index = index;
        this.body = body;
        this.keepAlive = keepAlive.toMillis() + "ms";
    }

    /** The next page; empty when there is no more (the scroll is then cleared). */
    public CompletableFuture<List<EsHit>> next() {
        CompletableFuture<EsSearchResponse> page;
        synchronized (this) {
            if (done) {
                return CompletableFuture.completedFuture(List.of());
            }
            if (busy) {
                return CompletableFuture.failedFuture(new IllegalStateException("the previous page has not arrived yet"));
            }
            busy = true;
            page = scrollId == null
                    ? client.search(index, body, Map.of("scroll", keepAlive))
                    : client.request("POST", "/_search/scroll", null, Json.obj("scroll", keepAlive, "scroll_id", scrollId)).thenApply(r -> EsSearchResponse.of(r.json()));
        }
        return page.handle((r, err) -> {
            synchronized (this) {
                busy = false;
                if (err != null) {
                    done = true;
                    throw err instanceof RuntimeException re ? re : new java.util.concurrent.CompletionException(err);
                }
                scrollId = r.scrollId() != null ? r.scrollId() : scrollId;
                if (total < 0) {
                    total = r.total();
                }
                if (r.hits().isEmpty()) {
                    done = true;
                }
            }
            if (r.hits().isEmpty()) {
                return clear().thenApply(v -> r.hits());
            }
            return CompletableFuture.completedFuture(r.hits());
        }).thenCompose(f -> f);
    }

    /** Every remaining hit, in order, to {@code consumer} (on the event loop: it must not block); the count when the end is reached. */
    public CompletableFuture<Long> forEach(Consumer<EsHit> consumer) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        step(consumer, 0, result);
        return result;
    }

    /** Every remaining hit's source as a record ({@link Json#to}) to {@code consumer}; the count when the end is reached. */
    public <T> CompletableFuture<Long> forEach(Class<T> type, Consumer<T> consumer) {
        return forEach(h -> consumer.accept(h.source(type)));
    }

    private void step(Consumer<EsHit> consumer, long count, CompletableFuture<Long> result) {
        next().whenComplete((hits, err) -> {
            if (err != null) {
                result.completeExceptionally(err);
                return;
            }
            if (hits.isEmpty()) {
                result.complete(count);
                return;
            }
            try {
                for (EsHit h : hits) {
                    consumer.accept(h);
                }
            } catch (Throwable t) {
                close();
                result.completeExceptionally(t);
                return;
            }
            step(consumer, count + hits.size(), result);
        });
    }

    /** The matching document count, known after the first page; -1 before. */
    public long total() {
        synchronized (this) {
            return total;
        }
    }

    /** Clears the scroll on the cluster; further {@link #next} calls give an empty page. */
    public CompletableFuture<Void> close() {
        synchronized (this) {
            done = true;
        }
        return clear();
    }

    private CompletableFuture<Void> clear() {
        String id;
        synchronized (this) {
            id = scrollId;
            scrollId = null;
        }
        if (id == null) {
            return CompletableFuture.completedFuture(null);
        }
        return client.request("DELETE", "/_search/scroll", null, Json.obj("scroll_id", List.of(id))).handle((r, err) -> null);   // a scroll that expired already is fine
    }
}
