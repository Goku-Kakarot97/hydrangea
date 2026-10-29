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
        if (threads < 1) {
            throw new IllegalArgumentException("threads must be >= 1");
        }
        int id = GROUP_IDS.incrementAndGet();
        loops = IntStream.range(0, threads).mapToObj(i -> new EventLoop("hydrangea-" + id + "-loop-" + i, metrics)).toList();
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
