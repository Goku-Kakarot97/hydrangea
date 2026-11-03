package io.hydrangea.amqp;

/**
 * A message handed to a consumer or returned by {@code basicGet}. {@code deliveryTag} is what {@code basicAck}/{@code basicNack}
 * need; it is only valid on the channel that delivered it.
 */
public record Delivery(String consumerTag, long deliveryTag, boolean redelivered, String exchange, String routingKey,
                       BasicProperties properties, byte[] body) {

    /** The publisher's span, from the {@code traceparent} header of the message, or {@code null} when it has none. */
    public io.hydrangea.trace.SpanContext spanContext() {
        Object v = properties == null || properties.headers() == null ? null : properties.headers().get("traceparent");
        return v == null ? null : io.hydrangea.trace.SpanContext.parse(v.toString());
    }

    /**
     * Starts a CONSUMER span for handling this delivery, child of the publisher's span when the message carries one, else the root
     * of a new trace: {@code try (Span span = d.startProcessing(); var scope = span.makeCurrent()) { handle(d); }}.
     */
    public io.hydrangea.trace.Span startProcessing() {
        String name = exchange == null || exchange.isEmpty() ? routingKey : exchange;
        io.hydrangea.trace.Span span = io.hydrangea.trace.Tracing.start(name + " process", io.hydrangea.trace.Span.Kind.CONSUMER, spanContext());
        if (span.isRecording()) {
            span.attribute("messaging.system", "rabbitmq").attribute("messaging.destination.name", String.valueOf(exchange))
                    .attribute("messaging.rabbitmq.destination.routing_key", String.valueOf(routingKey)).attribute("messaging.operation.type", "process");
        }
        return span;
    }
}
