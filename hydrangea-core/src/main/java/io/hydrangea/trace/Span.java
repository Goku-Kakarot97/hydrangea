package io.hydrangea.trace;

/**
 * One unit of work in a trace: a request being served, a call to another service, a query. Started through {@link Tracing#start},
 * ended once with {@link #end} (or try-with-resources). Attributes and errors are recorded only while {@link #isRecording()}; on an
 * unsampled trace, or with no {@link TraceSink} bound, a span still has a {@link #context()} so the trace id keeps flowing
 * downstream, but costs nothing else.
 */
public interface Span extends AutoCloseable {

    enum Kind { SERVER, CLIENT, PRODUCER, CONSUMER, INTERNAL }

    SpanContext context();

    /** Whether anything is being recorded for this span. */
    boolean isRecording();

    /** Changes the name, for when the right one is only known later (a route's pattern after matching). */
    Span name(String name);

    Span attribute(String key, String value);

    Span attribute(String key, long value);

    Span attribute(String key, boolean value);

    /** Marks the span as failed and records the exception. */
    Span error(Throwable cause);

    /** Marks the span as failed. */
    Span error(String description);

    /** Ends the span; later calls do nothing. */
    void end();

    @Override
    default void close() {
        end();
    }

    /** Makes this the calling thread's current span until the scope closes, when the previous one comes back. */
    default Scope makeCurrent() {
        return Tracing.makeCurrent(this);
    }

    interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
