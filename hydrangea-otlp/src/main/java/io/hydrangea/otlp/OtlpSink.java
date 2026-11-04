package io.hydrangea.otlp;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.log.Logger;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link TraceSink} that sends the framework's spans to an OpenTelemetry collector (or anything else that speaks OTLP: Jaeger,
 * Tempo ...) without an OpenTelemetry library: the spans are encoded by hand and sent with the framework's own gRPC or HTTP client.
 *
 * <pre>{@code
 * Tracing.bindTo(OtlpSink.fromEnvironment().build());        // OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_TRACES_SAMPLER ...
 * Tracing.bindTo(OtlpSink.builder().serviceName("orders").endpoint("http://otel-collector:4317").build());
 * }</pre>
 *
 * Ending a span puts it on a bounded queue and costs nothing else; one daemon thread takes spans off it and sends a batch every
 * {@link Builder#scheduleDelay} or whenever {@link Builder#maxBatch} are waiting, like the OpenTelemetry SDK's batch processor.
 * When the queue is full the span is dropped and counted ({@link #dropped()}); a batch the collector refuses is dropped too, with a
 * warning the first time and when it recovers. {@link #forceFlush()} sends what is queued now; {@link #close()} does that and stops,
 * and a JVM shutdown hook calls it, so a pod that receives SIGTERM still delivers its last spans.
 *
 * <p>The environment variables {@link #fromEnvironment()} reads are the OpenTelemetry ones, so a Deployment is configured the same way
 * whichever sink the service uses: {@code OTEL_SERVICE_NAME}, {@code OTEL_RESOURCE_ATTRIBUTES} ({@code k=v,k=v}),
 * {@code OTEL_TRACES_EXPORTER} ({@code otlp}, {@code logging}, {@code none}), {@code OTEL_EXPORTER_OTLP_PROTOCOL} ({@code grpc},
 * {@code http/protobuf}), {@code OTEL_EXPORTER_OTLP_ENDPOINT} (and the {@code _TRACES_} variants of endpoint, protocol, headers and
 * timeout), {@code OTEL_EXPORTER_OTLP_HEADERS}, {@code OTEL_EXPORTER_OTLP_TIMEOUT}, {@code OTEL_BSP_SCHEDULE_DELAY},
 * {@code OTEL_BSP_MAX_QUEUE_SIZE}, {@code OTEL_BSP_MAX_EXPORT_BATCH_SIZE}, {@code OTEL_BSP_EXPORT_TIMEOUT}, {@code OTEL_TRACES_SAMPLER}
 * with {@code OTEL_TRACES_SAMPLER_ARG} (applied to {@link Tracing#sampleRatio}; every sampler here is parent-based: a trace started
 * elsewhere keeps its decision), {@code OTEL_SDK_DISABLED}.
 */
public final class OtlpSink implements TraceSink, AutoCloseable {

    private static final Logger log = Logger.get(OtlpSink.class);

    /** The version this sink reports as {@code telemetry.sdk.version}. */
    static final String VERSION = "1.0.2";

    private final Exporter exporter;
    private final EventLoopGroup ownedGroup;
    private final byte[] resource;
    private final Map<String, String> resourceAttributes;
    private final ArrayBlockingQueue<Object> queue;
    private final int maxBatch;
    private final long delayNanos;
    private final Duration exportTimeout;
    private final Thread worker;
    private final Thread hook;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong exported = new AtomicLong();
    private final AtomicLong failedBatches = new AtomicLong();
    private volatile boolean exporterDown;

    /** A marker on the queue: export everything before it, then complete; with {@code stop}, the worker exits afterwards. */
    private static final class Flush {
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final boolean stop;

        Flush(boolean stop) {
            this.stop = stop;
        }
    }

    private OtlpSink(Builder b) {
        // the transport's own loop is a daemon one: the sink must never keep a JVM alive that has nothing else to do
        this.ownedGroup = b.exporter == null && b.group == null ? new EventLoopGroup(1, true) : null;
        if (ownedGroup != null) {
            b.group = ownedGroup;
        }
        this.exporter = b.exporter != null ? b.exporter : b.newExporter();
        this.resourceAttributes = Map.copyOf(b.resourceAttributes());
        this.resource = OtlpEncoder.resource(resourceAttributes);
        this.queue = new ArrayBlockingQueue<>(b.maxQueue);
        this.maxBatch = Math.min(b.maxBatch, b.maxQueue);
        this.delayNanos = b.scheduleDelay.toNanos();
        this.exportTimeout = b.exportTimeout;
        if (b.sampleRatio != null) {
            Tracing.sampleRatio(b.sampleRatio);
        }
        this.worker = new Thread(this::run, "hydrangea-otlp");
        worker.setDaemon(true);
        worker.start();
        this.hook = b.shutdownHook ? new Thread(this::close, "hydrangea-otlp-shutdown") : null;
        if (hook != null) {
            Runtime.getRuntime().addShutdownHook(hook);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A builder configured from the {@code OTEL_*} environment variables (see the class comment); call {@link Builder#build}. */
    public static Builder fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /** Like {@link #fromEnvironment()} from the given variables. */
    public static Builder fromEnvironment(Map<String, String> env) {
        return new Builder().environment(env);
    }

    @Override
    public Span start(String name, Span.Kind kind, SpanContext parent) {
        SpanContext context = parent != null ? parent.child(SpanContext.newSpanId())
                : new SpanContext(SpanContext.newTraceId(), SpanContext.newSpanId(), true);
        return new OtlpSpan(this, name, kind, context, parent == null ? null : parent.spanId());
    }

    void finished(OtlpSpan span) {
        if (closed.get() || !queue.offer(span)) {
            dropped.incrementAndGet();
        }
    }

    /** The resource attributes every span is sent with ({@code service.name} ...). */
    public Map<String, String> resourceAttributes() {
        return resourceAttributes;
    }

    /** Spans dropped because the queue was full (or the sink closed). */
    public long dropped() {
        return dropped.get();
    }

    /** Spans the collector accepted. */
    public long exported() {
        return exported.get();
    }

    /** Batches the collector refused or that timed out; their spans are gone. */
    public long failedBatches() {
        return failedBatches.get();
    }

    /**
     * Sends every span ended so far; the future completes when the exporter answered for all of them (or failed). With the queue
     * full, the call waits (up to the export timeout) for the worker to make room for the request.
     */
    public CompletableFuture<Void> forceFlush() {
        if (closed.get()) {
            return CompletableFuture.completedFuture(null);
        }
        Flush flush = new Flush(false);
        try {
            if (!queue.offer(flush, exportTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                flush.done.completeExceptionally(new IllegalStateException("the span queue stayed full for " + exportTimeout));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flush.done.completeExceptionally(e);
        }
        return flush.done;
    }

    /** Flushes, stops the worker and closes the exporter. Idempotent; spans ended afterwards are dropped. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (hook != null && Thread.currentThread() != hook) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // the JVM is already shutting down
            }
        }
        Flush stop = new Flush(true);
        try {
            if (!queue.offer(stop, 1, TimeUnit.SECONDS)) {
                worker.interrupt();
            }
            worker.join(exportTimeout.toMillis() + 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        exporter.close();
        if (ownedGroup != null) {
            ownedGroup.close();
        }
    }

    private void run() {
        List<OtlpSpan> batch = new ArrayList<>(maxBatch);
        boolean stop = false;
        while (!stop) {
            Flush flush = null;
            long deadline = System.nanoTime() + delayNanos;
            try {
                while (batch.size() < maxBatch) {
                    long wait = deadline - System.nanoTime();
                    Object next = wait > 0 ? queue.poll(wait, TimeUnit.NANOSECONDS) : queue.poll();
                    if (next == null) {
                        break;
                    }
                    if (next instanceof Flush f) {
                        flush = f;
                        break;
                    }
                    batch.add((OtlpSpan) next);
                }
            } catch (InterruptedException e) {
                stop = true;
            }
            if (!batch.isEmpty()) {
                export(batch);
                batch.clear();
            }
            if (flush != null) {
                flush.done.complete(null);
                stop |= flush.stop;
            }
        }
        // closing: what ended while the stop marker travelled through the queue goes out too
        Object left;
        while ((left = queue.poll()) != null) {
            if (left instanceof OtlpSpan s) {
                batch.add(s);
            } else if (left instanceof Flush f) {
                f.done.complete(null);
            }
        }
        if (!batch.isEmpty()) {
            export(batch);
        }
    }

    private void export(List<OtlpSpan> batch) {
        byte[] request = OtlpEncoder.request(resource, batch);
        try {
            exporter.export(request).get(exportTimeout.toMillis(), TimeUnit.MILLISECONDS);
            exported.addAndGet(batch.size());
            if (exporterDown) {
                exporterDown = false;
                log.info("otlp export works again");
            }
        } catch (Exception e) {
            failedBatches.incrementAndGet();
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (!exporterDown) {
                exporterDown = true;
                log.warn("otlp export of {} spans failed, dropping this and later batches until it works again: {}", batch.size(), cause.toString());
            } else {
                log.debug("otlp export of {} spans failed: {}", batch.size(), cause.toString());
            }
        }
    }

    public static final class Builder {
        private String endpoint;
        private String protocol = "grpc";
        private String exporterName = "otlp";
        private String serviceName;
        private final Map<String, String> resource = new LinkedHashMap<>();
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Duration timeout = Duration.ofSeconds(10);
        private Duration exportTimeout = Duration.ofSeconds(30);
        private Duration scheduleDelay = Duration.ofSeconds(5);
        private int maxQueue = 2048;
        private int maxBatch = 512;
        private Double sampleRatio;
        private boolean shutdownHook = true;
        private Exporter exporter;
        private EventLoopGroup group;

        private Builder() {
        }

        /** The collector: {@code http://host:4317} for gRPC, the {@code http://host:4318/v1/traces} URL for HTTP; default localhost. */
        public Builder endpoint(String endpoint) {
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
            return this;
        }

        /** {@code grpc} (default) or {@code http/protobuf}. */
        public Builder protocol(String protocol) {
            String p = protocol.toLowerCase(Locale.ROOT);
            if (!p.equals("grpc") && !p.equals("http/protobuf")) {
                throw new IllegalArgumentException("protocol is grpc or http/protobuf: " + protocol);
            }
            this.protocol = p;
            return this;
        }

        /** The {@code service.name} the collector and Jaeger show. */
        public Builder serviceName(String name) {
            this.serviceName = Objects.requireNonNull(name, "name");
            return this;
        }

        /** A resource attribute ({@code deployment.environment}, {@code service.version}, {@code k8s.pod.name} ...). */
        public Builder resource(String key, String value) {
            resource.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
            return this;
        }

        /** A header (gRPC metadata) sent with every export: an API key for a hosted backend. */
        public Builder header(String name, String value) {
            headers.put(Objects.requireNonNull(name, "name"), Objects.requireNonNull(value, "value"));
            return this;
        }

        /** How long one export request may take on the wire; default 10 s. */
        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** How long the worker waits for a batch to be accepted before dropping it; default 30 s. */
        public Builder exportTimeout(Duration timeout) {
            this.exportTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** How long spans wait for company before a batch is sent; default 5 s. */
        public Builder scheduleDelay(Duration delay) {
            this.scheduleDelay = Objects.requireNonNull(delay, "delay");
            return this;
        }

        /** Spans waiting at most; beyond it they are dropped; default 2048. */
        public Builder maxQueue(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("maxQueue must be positive");
            }
            this.maxQueue = size;
            return this;
        }

        /** Spans per export request at most; default 512. */
        public Builder maxBatch(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("maxBatch must be positive");
            }
            this.maxBatch = size;
            return this;
        }

        /** Applies {@link Tracing#sampleRatio} when built. */
        public Builder sampleRatio(double ratio) {
            this.sampleRatio = ratio;
            return this;
        }

        /** Whether a JVM shutdown hook closes the sink; default yes. */
        public Builder shutdownHook(boolean on) {
            this.shutdownHook = on;
            return this;
        }

        /** Somewhere else than the OTLP endpoint, {@link Exporter#logging()} for instance; replaces endpoint and protocol. */
        public Builder exporter(Exporter exporter) {
            this.exporter = Objects.requireNonNull(exporter, "exporter");
            return this;
        }

        /** The event loops the gRPC or HTTP client runs on; default: one daemon loop of its own, closed with the sink. */
        public Builder eventLoopGroup(EventLoopGroup group) {
            this.group = group;
            return this;
        }

        /** Reads the {@code OTEL_*} variables from {@code env}; what is absent keeps its current value. */
        public Builder environment(Map<String, String> env) {
            String v;
            if ((v = env.get("OTEL_SERVICE_NAME")) != null) {
                serviceName = v;
            }
            if ((v = env.get("OTEL_RESOURCE_ATTRIBUTES")) != null) {
                resource.putAll(pairs(v));
            }
            if ((v = first(env, "OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_HEADERS")) != null) {
                headers.putAll(pairs(v));
            }
            if ((v = first(env, "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_PROTOCOL")) != null) {
                protocol(v);
            }
            String traces = env.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
            String base = env.get("OTEL_EXPORTER_OTLP_ENDPOINT");
            if (traces != null) {
                endpoint = traces;
            } else if (base != null) {
                endpoint = protocol.equals("grpc") ? base : (base.endsWith("/") ? base : base + "/") + "v1/traces";
            }
            if ((v = first(env, "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "OTEL_EXPORTER_OTLP_TIMEOUT")) != null) {
                timeout = Duration.ofMillis(Long.parseLong(v.trim()));
            }
            if ((v = env.get("OTEL_BSP_EXPORT_TIMEOUT")) != null) {
                exportTimeout = Duration.ofMillis(Long.parseLong(v.trim()));
            }
            if ((v = env.get("OTEL_BSP_SCHEDULE_DELAY")) != null) {
                scheduleDelay = Duration.ofMillis(Long.parseLong(v.trim()));
            }
            if ((v = env.get("OTEL_BSP_MAX_QUEUE_SIZE")) != null) {
                maxQueue(Integer.parseInt(v.trim()));
            }
            if ((v = env.get("OTEL_BSP_MAX_EXPORT_BATCH_SIZE")) != null) {
                maxBatch(Integer.parseInt(v.trim()));
            }
            if ((v = env.get("OTEL_TRACES_EXPORTER")) != null) {
                exporterName = v.trim().toLowerCase(Locale.ROOT);
            }
            if ("true".equalsIgnoreCase(env.get("OTEL_SDK_DISABLED"))) {
                exporterName = "none";
            }
            if ((v = env.get("OTEL_TRACES_SAMPLER")) != null) {
                String arg = env.get("OTEL_TRACES_SAMPLER_ARG");
                sampleRatio = switch (v.trim().toLowerCase(Locale.ROOT)) {
                    case "always_on", "parentbased_always_on" -> 1.0;
                    case "always_off", "parentbased_always_off" -> 0.0;
                    case "traceidratio", "parentbased_traceidratio" -> arg == null ? 1.0 : Double.parseDouble(arg.trim());
                    default -> throw new IllegalArgumentException("unknown OTEL_TRACES_SAMPLER: " + v);
                };
            }
            return this;
        }

        private static String first(Map<String, String> env, String... keys) {
            for (String k : keys) {
                String v = env.get(k);
                if (v != null && !v.isBlank()) {
                    return v;
                }
            }
            return null;
        }

        /** {@code k=v,k=v} as OTEL_RESOURCE_ATTRIBUTES and OTEL_EXPORTER_OTLP_HEADERS write it. */
        static Map<String, String> pairs(String list) {
            Map<String, String> out = new LinkedHashMap<>();
            for (String pair : list.split(",")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
                }
            }
            return out;
        }

        Map<String, String> resourceAttributes() {
            Map<String, String> all = new LinkedHashMap<>();
            all.put("service.name", "unknown_service");
            all.put("telemetry.sdk.name", "hydrangea-otlp");
            all.put("telemetry.sdk.language", "java");
            all.put("telemetry.sdk.version", VERSION);
            all.putAll(resource);
            if (serviceName != null) {
                all.put("service.name", serviceName);
            }
            return all;
        }

        Exporter newExporter() {
            return switch (exporterName) {
                case "none" -> Exporter.none();
                case "logging", "console", "logging-otlp" -> Exporter.logging();
                case "otlp" -> protocol.equals("grpc")
                        ? Exporter.grpc(endpoint != null ? endpoint : "http://localhost:4317", headers, timeout, group)
                        : Exporter.http(endpoint != null ? endpoint : "http://localhost:4318/v1/traces", headers, timeout, group);
                default -> throw new IllegalArgumentException("OTEL_TRACES_EXPORTER is otlp, logging or none: " + exporterName);
            };
        }

        public OtlpSink build() {
            return new OtlpSink(this);
        }
    }
}
