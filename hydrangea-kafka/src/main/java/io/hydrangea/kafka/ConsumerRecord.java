package io.hydrangea.kafka;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** A record read from a partition. */
public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {

    /** The producer's span, from the record's {@code traceparent} header, or {@code null} when it has none. */
    public io.hydrangea.trace.SpanContext spanContext() {
        for (Header h : headers) {
            if ("traceparent".equals(h.key()) && h.value() != null) {
                return io.hydrangea.trace.SpanContext.parse(new String(h.value(), java.nio.charset.StandardCharsets.US_ASCII));
            }
        }
        return null;
    }

    /**
     * Starts a CONSUMER span ({@code <topic> process}) for handling this record, child of the producer's span when the record carries
     * one, else the root of a new trace. End it when the handling is done:
     * <pre>{@code
     * try (Span span = record.startProcessing(); var scope = span.makeCurrent()) { handle(record); }
     * }</pre>
     */
    public io.hydrangea.trace.Span startProcessing() {
        io.hydrangea.trace.Span span = io.hydrangea.trace.Tracing.start(topic + " process", io.hydrangea.trace.Span.Kind.CONSUMER, spanContext());
        if (span.isRecording()) {
            span.attribute("messaging.system", "kafka").attribute("messaging.destination.name", topic).attribute("messaging.operation.type", "process")
                    .attribute("messaging.destination.partition.id", String.valueOf(partition)).attribute("messaging.kafka.offset", offset);
        }
        return span;
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
