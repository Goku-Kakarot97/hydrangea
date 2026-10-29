package io.hydrangea;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters shared by all loops of an {@link EventLoopGroup}. Updates are contention-free ({@link LongAdder});
 * {@link #snapshot()} is a cheap, slightly racy read intended for dashboards and logs.
 */
public final class Metrics {

    private final List<MetricsSink> sinks = new CopyOnWriteArrayList<>();
    /** True once a sink is bound: until then {@link Latency} recording is a no-op. */
    volatile boolean enabled;
    private final Object registry = new Object();
    private final Map<String, Count> counts = new LinkedHashMap<>();
    private final Map<String, Latency> latencies = new LinkedHashMap<>();
    private final Map<String, Object[]> registered = new LinkedHashMap<>();   // key -> {kind, name, unit, tags}
    private ScheduledExecutorService ticker;
    /** How long a window of latency quantiles is. */
    static final int WINDOW_SECONDS = 5;
    /** Time from a request's head being parsed to its response being produced (HTTP/1.1 and HTTP/2 servers). */
    final Latency httpDuration = new Latency(this);
    /** HTTP requests whose head has been read and whose response has not been produced yet (HTTP/1.1, HTTP/2, HTTP/3 servers). */
    private final Count httpActive = new Count();
    final LongAdder connectionsOpened = new LongAdder();
    final LongAdder connectionsClosed = new LongAdder();
    final LongAdder bytesRead = new LongAdder();
    final LongAdder bytesWritten = new LongAdder();
    final LongAdder tasksExecuted = new LongAdder();
    final LongAdder selectorRebuilds = new LongAdder();
    final LongAdder datagramsReceived = new LongAdder();
    final LongAdder datagramsSent = new LongAdder();
    final LongAdder datagramsDropped = new LongAdder();
    final LongAdder connectionsRejected = new LongAdder();
    final LongAdder httpRequests = new LongAdder();
    final LongAdder http2xx = new LongAdder();
    final LongAdder http3xx = new LongAdder();
    final LongAdder http4xx = new LongAdder();
    final LongAdder http5xx = new LongAdder();
    final LongAdder httpTimeouts = new LongAdder();
    /** Longest time any single task or I/O callback kept a loop busy: the best "loop lag" indicator. */
    final LongAccumulator maxBlockNanos = new LongAccumulator(Math::max, 0);

    /** A connection was refused because a configured limit was reached. */
    public void recordRejected() {
        connectionsRejected.increment();
    }

    /** The latency distribution of HTTP responses, for servers that time requests themselves ({@link Latency#begin} / {@code recordHttpResponse(status, start)}). */
    public Latency httpDuration() {
        return httpDuration;
    }

    /** A request's head has been read (the server stage calls this once per request and pairs it with {@link #recordHttpEnd}). */
    public void recordHttpStart() {
        httpActive.increment();
    }

    /** That request has been answered, or its connection or stream went away before it could be. */
    public void recordHttpEnd() {
        httpActive.decrement();
    }

    /** An HTTP response was produced after {@code startNanos} ({@link System#nanoTime()}, 0 = unknown). */
    public void recordHttpResponse(int status, long startNanos) {
        recordHttpResponse(status);
        if (startNanos != 0) {
            httpDuration.end(startNanos);
        }
    }

    /** An HTTP response was sent (called by the server stage). */
    public void recordHttpResponse(int status) {
        httpRequests.increment();
        switch (status / 100) {
            case 2 -> http2xx.increment();
            case 3 -> http3xx.increment();
            case 4 -> http4xx.increment();
            case 5 -> http5xx.increment();
            default -> { }
        }
    }

    /** A request or idle connection was cut off by one of the HTTP server's timeouts. */
    public void recordHttpTimeout() {
        httpTimeouts.increment();
    }

    public Snapshot snapshot() {
        long opened = connectionsOpened.sum();
        long closed = connectionsClosed.sum();
        return new Snapshot(opened, closed, Math.max(0, opened - closed), bytesRead.sum(), bytesWritten.sum(),
                tasksExecuted.sum(), selectorRebuilds.sum(), datagramsReceived.sum(), datagramsSent.sum(),
                datagramsDropped.sum(), maxBlockNanos.get(), connectionsRejected.sum(), httpRequests.sum(), http2xx.sum(),
                http3xx.sum(), http4xx.sum(), http5xx.sum(), httpTimeouts.sum());
    }

    /**
     * Registers every number of this group with {@code sink}; the sink reads them when it scrapes, so the loops pay nothing extra.
     * Names: {@code connections.opened|closed|rejected|active}, {@code network.read|written} (bytes), {@code loop.tasks},
     * {@code loop.selector.rebuilds}, {@code loop.max.block} (seconds: the longest a task or I/O callback has held a loop),
     * {@code udp.datagrams} (tag {@code direction}=received|sent|dropped), {@code http.requests},
     * {@code http.responses} (tag {@code class}=2xx..5xx), {@code http.timeouts}, {@code http.server.active} (requests being handled),
     * {@code http.server.duration}. Add tags (a service name, say) in the sink.
     */
    public void bindTo(MetricsSink sink) {
        sink.counterFunction("connections.opened", null, connectionsOpened::sum);
        sink.counterFunction("connections.closed", null, connectionsClosed::sum);
        sink.counterFunction("connections.rejected", null, connectionsRejected::sum);
        sink.gauge("connections.active", null, () -> Math.max(0, connectionsOpened.sum() - connectionsClosed.sum()));
        sink.counterFunction("network.read", "bytes", bytesRead::sum);
        sink.counterFunction("network.written", "bytes", bytesWritten::sum);
        sink.counterFunction("loop.tasks", null, tasksExecuted::sum);
        sink.counterFunction("loop.selector.rebuilds", null, selectorRebuilds::sum);
        sink.gauge("loop.max.block", "seconds", () -> maxBlockNanos.get() / 1e9);
        sink.counterFunction("udp.datagrams", null, datagramsReceived::sum, "direction", "received");
        sink.counterFunction("udp.datagrams", null, datagramsSent::sum, "direction", "sent");
        sink.counterFunction("udp.datagrams", null, datagramsDropped::sum, "direction", "dropped");
        sink.counterFunction("http.requests", null, httpRequests::sum);
        sink.counterFunction("http.responses", null, http2xx::sum, "class", "2xx");
        sink.counterFunction("http.responses", null, http3xx::sum, "class", "3xx");
        sink.counterFunction("http.responses", null, http4xx::sum, "class", "4xx");
        sink.counterFunction("http.responses", null, http5xx::sum, "class", "5xx");
        sink.counterFunction("http.timeouts", null, httpTimeouts::sum);
        sink.gauge("http.server.active", null, httpActive::value);
        bindLatency(sink, "http.server.duration", httpDuration);
        synchronized (registry) {
            sinks.add(sink);   // kept: some sinks (Micrometer) must stay alive as long as what feeds them, and later meters go to it too
            counts.forEach((key, c) -> bindCount(sink, key, c));
            latencies.forEach((key, l) -> bindLatency(sink, ((Object[]) registered.get(key))[1].toString(), l, (String[]) registered.get(key)[3]));
            enabled = true;
            if (ticker == null) {
                ticker = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "hydrangea-metrics");
                    t.setDaemon(true);
                    return t;
                });
                ticker.scheduleAtFixedRate(this::tick, WINDOW_SECONDS, WINDOW_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A counter that the code you give it increments ({@code redis.commands}, tags {@code "outcome","ok"}). The same name and tags give the same
     * counter, so many clients of one group add up. Reaches every sink bound now or later.
     */
    public Count counter(String name, String unit, String... tags) {
        return count("counter", name, unit, tags);
    }

    /** Like {@link #counter} but a value that goes up and down (requests in flight): use {@link Count#add} with a negative number to lower it. */
    public Count gauge(String name, String unit, String... tags) {
        return count("gauge", name, unit, tags);
    }

    private Count count(String kind, String name, String unit, String[] tags) {
        String key = kind + ' ' + name + ' ' + String.join(",", tags);
        synchronized (registry) {
            Count c = counts.get(key);
            if (c == null) {
                c = new Count();
                counts.put(key, c);
                registered.put(key, new Object[] {kind, name, unit, tags});
                for (MetricsSink sink : sinks) {
                    bindCount(sink, key, c);
                }
            }
            return c;
        }
    }

    /** A latency distribution (see {@link Latency}); same name and tags give the same one. Reaches every sink bound now or later. */
    public Latency latency(String name, String... tags) {
        String key = "latency " + name + ' ' + String.join(",", tags);
        synchronized (registry) {
            Latency l = latencies.get(key);
            if (l == null) {
                l = new Latency(this);
                latencies.put(key, l);
                registered.put(key, new Object[] {"latency", name, null, tags});
                for (MetricsSink sink : sinks) {
                    bindLatency(sink, name, l, tags);
                }
            }
            return l;
        }
    }

    private void bindCount(MetricsSink sink, String key, Count c) {
        Object[] r = registered.get(key);
        String name = (String) r[1];
        String unit = (String) r[2];
        String[] tags = (String[]) r[3];
        if (r[0].equals("gauge")) {
            sink.gauge(name, unit, c::value, tags);
        } else {
            sink.counterFunction(name, unit, c::value, tags);
        }
    }

    private static void bindLatency(MetricsSink sink, String name, Latency l, String... tags) {
        sink.counterFunction(name + ".count", null, l::count, tags);
        sink.counterFunction(name + ".sum", "seconds", l::sumSeconds, tags);
        for (int i = 0; i < Latency.QUANTILES.length; i++) {
            int q = i;
            String[] t = java.util.Arrays.copyOf(tags, tags.length + 2);
            t[tags.length] = "quantile";
            t[tags.length + 1] = Latency.QUANTILES[i] == 1.0 ? "1" : Double.toString(Latency.QUANTILES[i]);
            sink.gauge(name, "seconds", () -> l.quantile(q), t);
        }
    }

    private void tick() {
        List<Latency> all;
        synchronized (registry) {
            all = new ArrayList<>(latencies.values());
        }
        all.add(httpDuration);
        for (Latency l : all) {
            try {
                l.tick();
            } catch (RuntimeException ignored) {
                // one histogram must not stop the others
            }
        }
    }

    /** Stops the background thread (the group does this when it shuts down). Latencies stop recording. */
    public void close() {
        synchronized (registry) {
            enabled = false;
            if (ticker != null) {
                ticker.shutdownNow();
                ticker = null;
            }
        }
    }

    /** A number the owner adds to; {@link LongAdder}-backed, so adding from many threads is cheap. */
    public static final class Count {
        private final LongAdder v = new LongAdder();

        public void add(long n) {
            v.add(n);
        }

        public void increment() {
            v.increment();
        }

        public void decrement() {
            v.decrement();
        }

        public long value() {
            return v.sum();
        }
    }

    /** Resets {@code maxBlockNanos} and returns its previous value (to track lag per reporting interval). */
    public long resetMaxBlockNanos() {
        return maxBlockNanos.getThenReset();
    }

    public record Snapshot(
            long connectionsOpened,
            long connectionsClosed,
            long activeConnections,
            long bytesRead,
            long bytesWritten,
            long tasksExecuted,
            long selectorRebuilds,
            long datagramsReceived,
            long datagramsSent,
            long datagramsDropped,
            long maxBlockNanos,
            long connectionsRejected,
            long httpRequests,
            long http2xx,
            long http3xx,
            long http4xx,
            long http5xx,
            long httpTimeouts) {

        /**
         * The numbers in the Prometheus / OpenMetrics text exposition format, ready to serve from a {@code /metrics}
         * route. Counters end in {@code _total}; {@code connections_active} and {@code loop_max_block_seconds} are gauges.
         *
         * @param prefix metric name prefix, e.g. {@code "hydrangea"}
         */
        public String toPrometheus(String prefix) {
            StringBuilder sb = new StringBuilder(2048);
            counter(sb, prefix, "connections_opened_total", "Connections that became active", connectionsOpened);
            counter(sb, prefix, "connections_closed_total", "Connections closed", connectionsClosed);
            counter(sb, prefix, "connections_rejected_total", "Connections refused by a configured limit", connectionsRejected);
            gauge(sb, prefix, "connections_active", "Connections currently open", activeConnections);
            counter(sb, prefix, "bytes_read_total", "Bytes read from sockets", bytesRead);
            counter(sb, prefix, "bytes_written_total", "Bytes written to sockets", bytesWritten);
            counter(sb, prefix, "loop_tasks_total", "Tasks executed by event loops", tasksExecuted);
            counter(sb, prefix, "selector_rebuilds_total", "Selector rebuilds after spurious wakeups", selectorRebuilds);
            counter(sb, prefix, "datagrams_received_total", "UDP datagrams received", datagramsReceived);
            counter(sb, prefix, "datagrams_sent_total", "UDP datagrams sent", datagramsSent);
            counter(sb, prefix, "datagrams_dropped_total", "UDP datagrams dropped under overload", datagramsDropped);
            counter(sb, prefix, "http_requests_total", "HTTP responses sent", httpRequests);
            sb.append("# TYPE ").append(prefix).append("_http_responses_total counter\n");
            for (Object[] row : new Object[][]{{"2xx", http2xx}, {"3xx", http3xx}, {"4xx", http4xx}, {"5xx", http5xx}}) {
                sb.append(prefix).append("_http_responses_total{class=\"").append(row[0]).append("\"} ").append(row[1]).append('\n');
            }
            counter(sb, prefix, "http_timeouts_total", "HTTP requests or idle connections cut off by a timeout", httpTimeouts);
            sb.append("# HELP ").append(prefix).append("_loop_max_block_seconds Longest time an event loop was kept busy by one task or callback\n");
            sb.append("# TYPE ").append(prefix).append("_loop_max_block_seconds gauge\n");
            sb.append(prefix).append("_loop_max_block_seconds ").append(maxBlockNanos / 1e9).append('\n');
            return sb.toString();
        }

        private static void counter(StringBuilder sb, String prefix, String name, String help, long value) {
            sb.append("# HELP ").append(prefix).append('_').append(name).append(' ').append(help).append('\n');
            sb.append("# TYPE ").append(prefix).append('_').append(name).append(" counter\n");
            sb.append(prefix).append('_').append(name).append(' ').append(value).append('\n');
        }

        private static void gauge(StringBuilder sb, String prefix, String name, String help, long value) {
            sb.append("# HELP ").append(prefix).append('_').append(name).append(' ').append(help).append('\n');
            sb.append("# TYPE ").append(prefix).append('_').append(name).append(" gauge\n");
            sb.append(prefix).append('_').append(name).append(' ').append(value).append('\n');
        }
    }
}
