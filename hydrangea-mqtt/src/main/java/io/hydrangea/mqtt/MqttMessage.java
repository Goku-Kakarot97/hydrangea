package io.hydrangea.mqtt;

import java.nio.charset.StandardCharsets;

/**
 * An application message received from the broker. {@code qos} is the level it was delivered at (the lower of what was published and what
 * the subscription asked for); {@code retained} is set on a message the broker stored and replayed on subscribe; {@code duplicate} on a
 * redelivery of a QoS 1 or 2 message. {@code properties} are those of MQTT 5 ({@link MqttProperties#NONE} on MQTT 3.1.1). The payload array is
 * shared by every handler the message goes to: do not change it.
 */
public record MqttMessage(String topic, byte[] payload, QoS qos, boolean retained, boolean duplicate, MqttProperties properties) {

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
