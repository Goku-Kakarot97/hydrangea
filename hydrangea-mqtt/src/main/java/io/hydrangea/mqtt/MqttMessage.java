package io.hydrangea.mqtt;

import java.nio.charset.StandardCharsets;

/**
 * An application message received from the broker. {@code qos} is the level it was delivered at (the lower of what was published and what
 * the subscription asked for); {@code retained} is set on a message the broker stored and replayed on subscribe; {@code duplicate} on a
 * redelivery of a QoS 1 or 2 message. {@code properties} are those of MQTT 5 ({@link MqttProperties#NONE} on MQTT 3.1.1). The payload array is
 * shared by every handler the message goes to: do not change it.
 */
public record MqttMessage(String topic, byte[] payload, QoS qos, boolean retained, boolean duplicate, MqttProperties properties) {

    /** The publisher's span, from the {@code traceparent} user property (MQTT 5), or {@code null} when there is none. */
    public io.hydrangea.trace.SpanContext spanContext() {
        if (properties != null) {
            for (UserProperty u : properties.userProperties()) {
                if ("traceparent".equals(u.name())) {
                    return io.hydrangea.trace.SpanContext.parse(u.value());
                }
            }
        }
        return null;
    }

    /**
     * Starts a CONSUMER span for handling this message, child of the publisher's span when the message carries one, else the root
     * of a new trace: {@code try (Span span = m.startProcessing(); var scope = span.makeCurrent()) { handle(m); }}.
     */
    public io.hydrangea.trace.Span startProcessing() {
        io.hydrangea.trace.Span span = io.hydrangea.trace.Tracing.start(topic + " process", io.hydrangea.trace.Span.Kind.CONSUMER, spanContext());
        if (span.isRecording()) {
            span.attribute("messaging.system", "mqtt").attribute("messaging.destination.name", topic).attribute("messaging.operation.type", "process")
                    .attribute("messaging.mqtt.qos", qos == null ? -1 : qos.level());
        }
        return span;
    }

    public MqttMessage(String topic, byte[] payload, QoS qos, boolean retained, boolean duplicate) {
        this(topic, payload, qos, retained, duplicate, MqttProperties.NONE);
    }

    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "MqttMessage[" + topic + ", " + payload.length + " bytes, " + qos + (retained ? ", retained" : "") + "]";
    }
}
