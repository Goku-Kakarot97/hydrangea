package io.hydrangea;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/** A fixed set of {@link EventLoop}s; new connections are spread over them round-robin. */
public final class EventLoopGroup implements AutoCloseable {

    private static final AtomicInteger GROUP_IDS = new AtomicInteger();

    private final List<EventLoop> loops;
    private final AtomicInteger next = new AtomicInteger();
    private final Metrics metrics = new Metrics();

    public EventLoopGroup() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public EventLoopGroup(int threads) {
        this(threads, false);
    }

    /**
     * With {@code daemon}, the loop threads do not keep the JVM alive: for a group that only serves a background client (the OTLP
     * exporter), so that a program ends when its own work does. A server's group must not be a daemon one.
     */
    public EventLoopGroup(int threads, boolean daemon) {
        if (threads < 1) {
            throw new IllegalArgumentException("threads must be >= 1");
        }
        int id = GROUP_IDS.incrementAndGet();
        loops = IntStream.range(0, threads).mapToObj(i -> new EventLoop("hydrangea-" + id + "-loop-" + i, metrics, daemon)).toList();
        loops.forEach(EventLoop::start);
    }

    public EventLoop next() {
        return loops.get(Math.floorMod(next.getAndIncrement(), loops.size()));
    }

    public Metrics metrics() {
        return metrics;
    }

    public int size() {
        return loops.size();
    }

    /** Closes every channel and stops all loops. */
    public CompletableFuture<Void> shutdown() {
        metrics.close();
        return CompletableFuture.allOf(loops.stream().map(EventLoop::shutdown).toArray(CompletableFuture[]::new));
    }

    @Override
    public void close() {
        shutdown().join();
    }
}
