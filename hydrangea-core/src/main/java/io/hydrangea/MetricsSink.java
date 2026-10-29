package io.hydrangea;

import java.util.function.DoubleSupplier;

/**
 * Where metrics go: the one seam between the framework and a monitoring library. The core only knows this interface; a separate
 * module (for example {@code hydrangea-micrometer}) implements it, so the core keeps no dependency and you add the module only if you
 * want it.
 * <p>
 * Meters are registered once and then used through a handle, so the hot path is one call on an object, with no name lookup and no
 * allocation. Names are dotted ({@code connections.opened}); {@code tags} are alternating keys and values
 * ({@code "class", "2xx"}); {@code unit} is {@code "bytes"}, {@code "seconds"} or {@code null}. A sink adds its own prefix.
 * <p>
 * Pull meters ({@link #counterFunction}, {@link #gauge}) are read by the monitoring system when it scrapes: nothing runs between scrapes.
 * Push meters ({@link #counter}, {@link #timer}) are for things that have no counter of their own, such as the latency of a client call.
 * Implementations must be thread safe; suppliers and handles are called from any thread.
 */
public interface MetricsSink {

    /** A total that only goes up, read when scraped. */
    void counterFunction(String name, String unit, DoubleSupplier total, String... tags);

    /** A value that goes up and down, read when scraped. */
    void gauge(String name, String unit, DoubleSupplier value, String... tags);

    /** A counter the caller increments. */
    Counter counter(String name, String unit, String... tags);

    /** Durations (latency); the sink decides how to summarize them (histogram, percentiles ...). */
    Timer timer(String name, String... tags);

    interface Counter {
        void add(long amount);

        default void increment() {
            add(1);
        }
    }

    interface Timer {
        void record(long nanos);
    }

    /** Drops everything. */
    MetricsSink NOOP = new MetricsSink() {
        private final Counter counter = amount -> { };
        private final Timer timer = nanos -> { };

        @Override
        public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
        }

        @Override
        public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
        }

        @Override
        public Counter counter(String name, String unit, String... tags) {
            return counter;
        }

        @Override
        public Timer timer(String name, String... tags) {
            return timer;
        }
    };
}
