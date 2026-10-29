package io.hydrangea.mqtt;

import java.time.Duration;

/**
 * Lifecycle events of a {@link MqttClient}. All methods run on the client's event-loop thread and must not block; the defaults do nothing.
 */
public interface MqttListener {

    /** The broker accepted the connection (the first time, and after every reconnect). */
    default void connected(boolean sessionPresent) {
    }

    /** An established connection was lost; {@code cause} says why. With a reconnect policy the client now waits and tries again. */
    default void connectionLost(Throwable cause) {
    }

    /** A reconnect attempt is scheduled in {@code delay}. */
    default void reconnecting(int attempt, Duration delay) {
    }

    /** The client has closed for good after a failure: it will not reconnect, and everything pending has failed. */
    default void gaveUp(Throwable cause) {
    }

    /** After a reconnect to a broker that had lost the session, the broker refused to take {@code filter} again. */
    default void resubscribeFailed(String filter) {
    }

    /** A message that no subscription's handler claimed (for instance one the broker sent from a subscription of an earlier session). */
    default void unhandledMessage(MqttMessage message) {
    }
}
