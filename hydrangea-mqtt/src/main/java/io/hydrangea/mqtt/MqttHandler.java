package io.hydrangea.mqtt;

/** Receives the messages of a subscription. Runs on the client's event-loop thread, so it must not block. */
@FunctionalInterface
public interface MqttHandler {
    void onMessage(MqttMessage message);
}
