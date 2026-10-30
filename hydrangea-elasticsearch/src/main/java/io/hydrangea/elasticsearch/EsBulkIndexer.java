package io.hydrangea.elasticsearch;

import io.hydrangea.Backoff;
import io.hydrangea.Scheduled;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Gathers actions from any thread into bulk requests: a request goes when {@link Options#maxActions} or {@link Options#maxBytes} is reached, or
 * {@link Options#flushInterval} after the first action of a batch, with at most {@link Options#maxInFlight} requests under way. Each {@link #add} gives a future
 * for that one action: the item when the cluster took it, an {@link EsException} when it refused it. Items the cluster rejected under load (429) and requests no
 * node answered are sent again after a {@link Options#backoff} pause, up to {@link Options#maxRetries} times.
 * <pre>{@code
 * EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withMaxActions(1000));
 * for (Map<String, Object> doc : docs) ix.add(BulkOp.index("products", null, doc));
 * ix.close().join();        // sends what is left and waits for every answer
 * }</pre>
 * Back-pressure: with more than {@link Options#maxPending} actions buffered or under way, {@link #add} fails at once (status 0, type {@code indexer_full});
 * a loader that gets that waits for some of its futures before adding more.
 */
public final class EsBulkIndexer {

    /**
     * @param maxActions    actions per request
     * @param maxBytes      bytes of NDJSON per request (a request is sent when the next action would exceed it)
     * @param flushInterval how long a partial batch waits; null for never (only size and {@link #flush} send)
     * @param maxInFlight   requests under way at once
     * @param maxRetries    sends of one action after its first
     * @param backoff       the pause before a retry (attempt 1, 2, ...)
     * @param maxPending    actions buffered or under way before {@link #add} refuses; 0 for no limit
     * @param params        query parameters of every bulk request ({@code refresh}, {@code pipeline}, ...)
     */
    public record Options(int maxActions, long maxBytes, Duration flushInterval, int maxInFlight, int maxRetries, Backoff backoff, int maxPending, Map<String, String> params) {

        public Options {
            if (maxActions < 1 || maxBytes < 1 || maxInFlight < 1 || maxRetries < 0 || maxPending < 0) {
                throw new IllegalArgumentException("invalid limits");
            }
            params = params == null ? Map.of() : Map.copyOf(params);
        }

        /** 500 actions or 5 MB per request, a batch waits 1 s at most, 1 request under way, 3 retries from 1 s apart, up to 10 000 actions pending. */
        public static Options defaults() {
            return new Options(500, 5L << 20, Duration.ofSeconds(1), 1, 3, Backoff.defaults().withDelays(Duration.ofSeconds(1), Duration.ofSeconds(30)), 10_000, null);
        }

        public Options withMaxActions(int n) {
            return new Options(n, maxBytes, flushInterval, maxInFlight, maxRetries, backoff, maxPending, params);
        }

        public Options withMaxBytes(long n) {
            return new Options(maxActions, n, flushInterval, maxInFlight, maxRetries, backoff, maxPending, params);
        }

        public Options withFlushInterval(Duration d) {
            return new Options(maxActions, maxBytes, d, maxInFlight, maxRetries, backoff, maxPending, params);
        }

        public Options withMaxInFlight(int n) {
            return new Options(maxActions, maxBytes, flushInterval, n, maxRetries, backoff, maxPending, params);
        }

        public Options withMaxRetries(int n) {
            return new Options(maxActions, maxBytes, flushInterval, maxInFlight, n, backoff, maxPending, params);
        }

        public Options withBackoff(Backoff b) {
            return new Options(maxActions, maxBytes, flushInterval, maxInFlight, maxRetries, b, maxPending, params);
        }

        public Options withMaxPending(int n) {
            return new Options(maxActions, maxBytes, flushInterval, maxInFlight, maxRetries, backoff, n, params);
        }

        public Options withParams(Map<String, String> p) {
            return new Options(maxActions, maxBytes, flushInterval, maxInFlight, maxRetries, backoff, maxPending, p);
        }
    }

    private static final class Entry {
        final BulkOp op;
        final int bytes;
        final CompletableFuture<EsBulkResponse.Item> future = new CompletableFuture<>();
        int attempts;

        Entry(BulkOp op) {
            this.op = op;
            this.bytes = op.approximateBytes();
        }
    }

    private final EsClient client;
    private final Options options;
    private final Object lock = new Object();
    private List<Entry> buffer = new ArrayList<>();
    private long bufferBytes;
    private int inFlight;                                          // requests under way
    private int pending;                                           // actions buffered, under way, or waiting for a retry
    private Scheduled timer;
    private boolean closed;
    private boolean flushAsked;                                    // send the buffer as soon as a request slot frees, however small it is
    private final List<CompletableFuture<Void>> flushWaiters = new ArrayList<>();

    EsBulkIndexer(EsClient client, Options options) {
        this.client = client;
        this.options = options;
    }

    /** Queues the action. The future gives the cluster's item for it, or fails with the item's error (or the transport failure) once retries are used up. */
    public CompletableFuture<EsBulkResponse.Item> add(BulkOp op) {
        Entry e = new Entry(op);
        List<Entry> toSend = null;
        synchronized (lock) {
            if (closed) {
                e.future.completeExceptionally(new EsException(0, "indexer_closed", "the indexer is closed", null, null));
                return e.future;
            }
            if (options.maxPending() > 0 && pending >= options.maxPending()) {
                e.future.completeExceptionally(new EsException(0, "indexer_full", options.maxPending() + " actions are pending: wait for some of them", null, null));
                return e.future;
            }
            pending++;
            if (!buffer.isEmpty() && bufferBytes + e.bytes > options.maxBytes()) {
                toSend = takeBuffer();                             // this one would push the batch past the byte limit: it opens the next
            }
            buffer.add(e);
            bufferBytes += e.bytes;
            if (buffer.size() == 1) {
                armTimer();
            }
            if (toSend == null && buffer.size() >= options.maxActions()) {
                toSend = takeBuffer();
            }
        }
        if (toSend != null) {
            send(toSend);
        }
        return e.future;
    }

    /** Sends what is buffered now; completes when every request sent so far (retries included) has been answered. */
    public CompletableFuture<Void> flush() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        List<Entry> toSend;
        synchronized (lock) {
            if (pending == 0) {
                f.complete(null);
                return f;
            }
            flushWaiters.add(f);
            flushAsked = true;
            toSend = takeBuffer();
        }
        if (toSend != null) {
            send(toSend);
        }
        return f;
    }

    /** No more actions are taken; what is buffered is sent and the future completes when everything has been answered. */
    public CompletableFuture<Void> close() {
        synchronized (lock) {
            closed = true;
        }
        return flush();
    }

    /** Actions buffered, under way, or waiting for a retry. */
    public int pending() {
        synchronized (lock) {
            return pending;
        }
    }

    // ---- internals (lock held where it says so) ----

    /** Lock held. The next request's worth of the buffer (up to maxActions / maxBytes) if a slot is free, else null (it stays and goes when a slot frees). */
    private List<Entry> takeBuffer() {
        if (buffer.isEmpty() || inFlight >= options.maxInFlight()) {
            return null;
        }
        int n = 0;
        long bytes = 0;
        while (n < buffer.size() && n < options.maxActions() && (n == 0 || bytes + buffer.get(n).bytes <= options.maxBytes())) {
            bytes += buffer.get(n).bytes;
            n++;
        }
        List<Entry> b;
        if (n == buffer.size()) {
            b = buffer;
            buffer = new ArrayList<>();
            bufferBytes = 0;
        } else {
            b = new ArrayList<>(buffer.subList(0, n));
            buffer = new ArrayList<>(buffer.subList(n, buffer.size()));
            bufferBytes -= bytes;
        }
        inFlight++;
        if (buffer.isEmpty()) {
            disarmTimer();
        } else {
            armTimer();
        }
        return b;
    }

    private void armTimer() {
        if (options.flushInterval() == null || timer != null) {
            return;
        }
        try {
            timer = client.eventLoop().schedule(this::onTimer, options.flushInterval().toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException shutDown) {
            timer = null;
        }
    }

    private void disarmTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    private void onTimer() {
        List<Entry> toSend;
        synchronized (lock) {
            timer = null;
            flushAsked = true;                                     // whatever is there goes, now or when a slot frees
            toSend = takeBuffer();
        }
        if (toSend != null) {
            send(toSend);
        }
    }

    private void send(List<Entry> batch) {
        List<BulkOp> ops = new ArrayList<>(batch.size());
        for (Entry e : batch) {
            e.attempts++;
            ops.add(e.op);
        }
        client.bulk(ops, options.params()).whenComplete((resp, err) -> {
            if (err != null) {
                Throwable t = err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err;
                boolean retry = t instanceof EsException e && e.isRetriable();
                List<Entry> again = new ArrayList<>();
                for (Entry e : batch) {
                    if (retry && e.attempts <= options.maxRetries()) {
                        again.add(e);
                    } else {
                        settle(e, null, t);
                    }
                }
                retryLater(again);
            } else {
                List<EsBulkResponse.Item> items = resp.items();
                List<Entry> again = new ArrayList<>();
                for (int i = 0; i < batch.size(); i++) {
                    Entry e = batch.get(i);
                    EsBulkResponse.Item item = i < items.size() ? items.get(i) : null;
                    if (item == null) {
                        settle(e, null, new EsException(0, "no_item", "the bulk answer had no item for this action", null, null));
                    } else if (item.rejected() && e.attempts <= options.maxRetries()) {
                        again.add(e);
                    } else if (item.failed()) {
                        settle(e, null, item.toException());
                    } else {
                        settle(e, item, null);
                    }
                }
                retryLater(again);
            }
            requestEnded();
        });
    }

    private void settle(Entry e, EsBulkResponse.Item item, Throwable err) {
        synchronized (lock) {
            pending--;
        }
        if (err != null) {
            e.future.completeExceptionally(err);
        } else {
            e.future.complete(item);
        }
    }

    /** Puts the entries back into the buffer after the backoff pause (they count as pending all along). */
    private void retryLater(List<Entry> again) {
        if (again.isEmpty()) {
            return;
        }
        int attempt = again.get(0).attempts;
        Duration delay = options.backoff().delayFor(attempt);
        Runnable requeue = () -> {
            List<Entry> toSend;
            synchronized (lock) {
                buffer.addAll(0, again);
                for (Entry e : again) {
                    bufferBytes += e.bytes;
                }
                flushAsked = true;
                toSend = takeBuffer();
            }
            if (toSend != null) {
                send(toSend);
            }
        };
        try {
            client.eventLoop().schedule(requeue, delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException shutDown) {
            for (Entry e : again) {
                settle(e, null, new EsException(0, "indexer_closed", "the event loop is shut down", null, null));
            }
        }
    }

    /** A request slot freed: the next batch goes if one is ready or a flush asked for it; flush waiters complete when nothing is left. */
    private void requestEnded() {
        List<Entry> toSend = null;
        List<CompletableFuture<Void>> done = null;
        synchronized (lock) {
            inFlight--;
            if (!buffer.isEmpty() && (flushAsked || buffer.size() >= options.maxActions() || bufferBytes >= options.maxBytes())) {
                toSend = takeBuffer();
            }
            if (buffer.isEmpty() && inFlight == 0) {
                flushAsked = false;
            }
            if (pending == 0 && !flushWaiters.isEmpty()) {
                done = new ArrayList<>(flushWaiters);
                flushWaiters.clear();
            }
        }
        if (toSend != null) {
            send(toSend);
        }
        if (done != null) {
            done.forEach(f -> f.complete(null));
        }
    }
}
