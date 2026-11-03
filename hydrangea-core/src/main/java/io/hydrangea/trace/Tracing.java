package io.hydrangea.trace;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The tracing system: the bound {@link TraceSink}, sampling, and the calling thread's current span.
 *
 * <pre>{@code
 * Tracing.bindTo(OtlpSink.fromEnvironment().build());            // once, at startup; nothing recorded without it (hydrangea-otlp: OTLP to a collector)
 * Tracing.sampleRatio(0.1);                                      // record one new trace in ten (a parent's decision is always followed)
 *
 * try (Span span = Tracing.start("charge card", Span.Kind.INTERNAL); var scope = span.makeCurrent()) {
 *     span.attribute("order", id);
 *     gateway.charge(...);                                       // a client call made here becomes a child span
 * }
 * }</pre>
 *
 * The current span is per thread and, as with {@link io.hydrangea.log.LogContext}, never crosses a thread by itself: on an event loop
 * that serves many requests the framework opens and closes it around each handler, and work that continues on another thread
 * reopens it with {@link Span#makeCurrent}.
 */
public final class Tracing {

    /** The log-context key the framework puts the current trace id under while it serves a request. */
    public static final String TRACE_ID = "trace_id";
    /** The log-context key for the current span id. */
    public static final String SPAN_ID = "span_id";

    private static volatile TraceSink sink = TraceSink.NOOP;
    private static volatile double sampleRatio = 1.0;
    private static final ThreadLocal<Span> CURRENT = new ThreadLocal<>();

    private Tracing() {
    }

    /** Makes {@code sink} the destination of recorded spans. */
    public static void bindTo(TraceSink sink) {
        Tracing.sink = Objects.requireNonNull(sink, "sink");
    }

    public static TraceSink sink() {
        return sink;
    }

    /** Whether a sink is bound. */
    public static boolean enabled() {
        return sink != TraceSink.NOOP;
    }

    /** The share of new traces (those without a sampled parent) that are recorded, 0 to 1; 1 by default. */
    public static void sampleRatio(double ratio) {
        if (ratio < 0 || ratio > 1 || Double.isNaN(ratio)) {
            throw new IllegalArgumentException("ratio must be between 0 and 1");
        }
        sampleRatio = ratio;
    }

    public static double sampleRatio() {
        return sampleRatio;
    }

    /**
     * Starts a span. With a parent, the span joins its trace and follows its sampling decision; without one it starts a new trace,
     * sampled according to {@link #sampleRatio}. An unsampled span, or any span while no sink is bound, is a propagating one.
     */
    public static Span start(String name, Span.Kind kind, SpanContext parent) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        TraceSink s = sink;
        boolean sampled = parent != null ? parent.sampled() : sample();
        if (s == TraceSink.NOOP || !sampled) {
            return propagating(parent);
        }
        return s.start(name, kind, parent);
    }

    /** Starts a span whose parent is the calling thread's current span (or a new trace when there is none). */
    public static Span start(String name, Span.Kind kind) {
        Span current = CURRENT.get();
        return start(name, kind, current == null ? null : current.context());
    }

    /** The calling thread's current span, or {@code null}. */
    public static Span current() {
        return CURRENT.get();
    }

    /** The calling thread's current span context, or {@code null}. */
    public static SpanContext currentContext() {
        Span s = CURRENT.get();
        return s == null ? null : s.context();
    }

    static Span.Scope makeCurrent(Span span) {
        Span previous = CURRENT.get();
        CURRENT.set(span);
        return () -> {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        };
    }

    /** A span that records nothing and only carries ids: a child of {@code parent}, or the root of a new unsampled trace. */
    public static Span propagating(SpanContext parent) {
        SpanContext context = parent != null ? parent.child(SpanContext.newSpanId())
                : new SpanContext(SpanContext.newTraceId(), SpanContext.newSpanId(), false);
        return new Propagating(context);
    }

    private static boolean sample() {
        double r = sampleRatio;
        return r >= 1.0 || (r > 0 && ThreadLocalRandom.current().nextDouble() < r);
    }

    private static final class Propagating implements Span {
        private final SpanContext context;

        Propagating(SpanContext context) {
            this.context = context;
        }

        @Override
        public SpanContext context() {
            return context;
        }

        @Override
        public boolean isRecording() {
            return false;
        }

        @Override
        public Span name(String name) {
            return this;
        }

        @Override
        public Span attribute(String key, String value) {
            return this;
        }

        @Override
        public Span attribute(String key, long value) {
            return this;
        }

        @Override
        public Span attribute(String key, boolean value) {
            return this;
        }

        @Override
        public Span error(Throwable cause) {
            return this;
        }

        @Override
        public Span error(String description) {
            return this;
        }

        @Override
        public void end() {
        }

        @Override
        public String toString() {
            return "Span[" + context.traceparent() + " not recorded]";
        }
    }
}
