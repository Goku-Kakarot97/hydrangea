package io.hydrangea.trace;

/**
 * Where spans go: the one seam between the framework and a tracing library, as {@link io.hydrangea.MetricsSink} is for metrics.
 * The framework only knows this interface; a separate module ({@code hydrangea-otlp}) implements it. Bind one
 * with {@link Tracing#bindTo}; without one, spans are not recorded but trace ids still propagate.
 * <p>
 * The sink creates the span's ids (keeping the parent's trace id) and owns its timing. {@code parent} is {@code null} for the root
 * of a new trace. Implementations must be thread safe: spans are started and ended from any thread.
 */
@FunctionalInterface
public interface TraceSink {

    Span start(String name, Span.Kind kind, SpanContext parent);

    /** Records nothing: every span is a {@link Tracing#propagating propagating} one. */
    TraceSink NOOP = (name, kind, parent) -> Tracing.propagating(parent);
}
