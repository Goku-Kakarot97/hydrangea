package io.hydrangea.otlp;

import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * A recorded span: what {@link OtlpEncoder} turns into OTLP. Attributes may arrive from any thread (a callback that ends the span
 * elsewhere), so they are guarded; everything is frozen by {@link #end}, after which the span is handed to the sink's queue.
 */
final class OtlpSpan implements Span {

    /** One attribute: the value is a String, a Long or a Boolean. */
    record Attribute(String key, Object value) {
    }

    private final OtlpSink sink;
    private final Kind kind;
    private final SpanContext context;
    private final String parentSpanId;
    private final long startEpochNanos;
    private final long startNanoTime = System.nanoTime();
    private volatile String name;
    private final List<Attribute> attributes = new ArrayList<>(8);
    private long endEpochNanos;
    private boolean ended;
    private boolean failed;
    private String errorDescription;
    private Throwable exception;

    OtlpSpan(OtlpSink sink, String name, Kind kind, SpanContext context, String parentSpanId) {
        this.sink = sink;
        this.name = name;
        this.kind = kind;
        this.context = context;
        this.parentSpanId = parentSpanId;
        this.startEpochNanos = System.currentTimeMillis() * 1_000_000L;
    }

    String name() {
        return name;
    }

    Kind kind() {
        return kind;
    }

    String parentSpanId() {
        return parentSpanId;
    }

    long startEpochNanos() {
        return startEpochNanos;
    }

    long endEpochNanos() {
        return endEpochNanos;
    }

    boolean failed() {
        return failed;
    }

    String errorDescription() {
        return errorDescription;
    }

    Throwable exception() {
        return exception;
    }

    /** The attributes as they were at {@link #end}: only the sink's thread reads them, after the span is ended. */
    List<Attribute> attributes() {
        synchronized (attributes) {
            return attributes;
        }
    }

    @Override
    public SpanContext context() {
        return context;
    }

    @Override
    public boolean isRecording() {
        synchronized (attributes) {
            return !ended;
        }
    }

    @Override
    public Span name(String name) {
        if (name != null) {
            this.name = name;
        }
        return this;
    }

    @Override
    public Span attribute(String key, String value) {
        return value == null ? this : put(key, value);
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
        if (key == null) {
            return this;
        }
        synchronized (attributes) {
            if (ended) {
                return this;
            }
            for (int i = 0; i < attributes.size(); i++) {
                if (attributes.get(i).key().equals(key)) {
                    attributes.set(i, new Attribute(key, value));
                    return this;
                }
            }
            attributes.add(new Attribute(key, value));
        }
        return this;
    }

    @Override
    public Span error(Throwable cause) {
        synchronized (attributes) {
            if (!ended) {
                failed = true;
                exception = cause;
                errorDescription = cause == null ? null : cause.toString();
            }
        }
        return this;
    }

    @Override
    public Span error(String description) {
        synchronized (attributes) {
            if (!ended) {
                failed = true;
                errorDescription = description;
            }
        }
        return this;
    }

    @Override
    public void end() {
        synchronized (attributes) {
            if (ended) {
                return;
            }
            ended = true;
            endEpochNanos = startEpochNanos + (System.nanoTime() - startNanoTime);
        }
        sink.finished(this);
    }

    /** The stack trace of the recorded exception, for the {@code exception} event. */
    static String stackTrace(Throwable t) {
        StringWriter sw = new StringWriter(512);
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    @Override
    public String toString() {
        return "OtlpSpan[" + name + " " + kind + " " + context.traceparent() + (parentSpanId == null ? "" : " parent=" + parentSpanId)
                + (failed ? " FAILED" : "") + "]";
    }
}
