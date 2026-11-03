package io.hydrangea.trace;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Keeps every span in memory, for tests and for looking at what the framework produces. */
public final class MemoryTraceSink implements TraceSink {

    private final List<Recorded> spans = new ArrayList<>();

    @Override
    public Span start(String name, Span.Kind kind, SpanContext parent) {
        SpanContext context = parent != null ? parent.child(SpanContext.newSpanId())
                : new SpanContext(SpanContext.newTraceId(), SpanContext.newSpanId(), true);
        Recorded r = new Recorded(name, kind, context, parent == null ? null : parent.spanId());
        synchronized (spans) {
            spans.add(r);
        }
        return r;
    }

    /** Every span started so far, in start order, ended or not. */
    public List<Recorded> spans() {
        synchronized (spans) {
            return new ArrayList<>(spans);
        }
    }

    /** The spans with that name. */
    public List<Recorded> named(String name) {
        return spans().stream().filter(s -> s.name().equals(name)).toList();
    }

    public void clear() {
        synchronized (spans) {
            spans.clear();
        }
    }

    public static final class Recorded implements Span {
        private volatile String name;
        private final Kind kind;
        private final SpanContext context;
        private final String parentSpanId;
        private final long startNanos = System.nanoTime();
        private final Map<String, Object> attributes = new LinkedHashMap<>();
        private volatile long endNanos;
        private volatile boolean ended;
        private volatile boolean failed;
        private volatile String errorDescription;
        private volatile Throwable exception;

        Recorded(String name, Kind kind, SpanContext context, String parentSpanId) {
            this.name = name;
            this.kind = kind;
            this.context = context;
            this.parentSpanId = parentSpanId;
        }

        public String name() {
            return name;
        }

        public Kind kind() {
            return kind;
        }

        /** The parent's span id, or {@code null} for a root. */
        public String parentSpanId() {
            return parentSpanId;
        }

        public Map<String, Object> attributes() {
            synchronized (attributes) {
                return new LinkedHashMap<>(attributes);
            }
        }

        public boolean ended() {
            return ended;
        }

        public boolean failed() {
            return failed;
        }

        public String errorDescription() {
            return errorDescription;
        }

        public Throwable exception() {
            return exception;
        }

        public long durationNanos() {
            return (ended ? endNanos : System.nanoTime()) - startNanos;
        }

        @Override
        public SpanContext context() {
            return context;
        }

        @Override
        public boolean isRecording() {
            return !ended;
        }

        @Override
        public Span name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public Span attribute(String key, String value) {
            return put(key, value);
        }

        @Override
        public Span attribute(String key, long value) {
            return put(key, value);
        }

        @Override
        public Span attribute(String key, boolean value) {
            return put(key, value);
        }

        private Span put(String key, Object value) {
            if (!ended) {
                synchronized (attributes) {
                    attributes.put(key, value);
                }
            }
            return this;
        }

        @Override
        public Span error(Throwable cause) {
            failed = true;
            exception = cause;
            errorDescription = cause == null ? null : cause.toString();
            return this;
        }

        @Override
        public Span error(String description) {
            failed = true;
            errorDescription = description;
            return this;
        }

        @Override
        public void end() {
            if (!ended) {
                endNanos = System.nanoTime();
                ended = true;
            }
        }

        @Override
        public String toString() {
            return "Span[" + name + " " + kind + " " + context.traceparent() + (parentSpanId == null ? "" : " parent=" + parentSpanId) + (failed ? " FAILED" : "") + "]";
        }
    }
}
