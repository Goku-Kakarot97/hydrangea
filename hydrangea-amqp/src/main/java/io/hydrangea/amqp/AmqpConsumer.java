package io.hydrangea.amqp;

/** Receives the messages of one {@code basicConsume}. Both callbacks run on the connection's event-loop thread: do not block in them. */
public interface AmqpConsumer {

    void onDelivery(Delivery delivery);

    /**
     * The consumer ended without the application asking for it: the broker cancelled it (queue deleted, node failure) or the
     * channel/connection closed. {@code cause} says why (null for a broker-initiated cancel).
     */
    default void onCancel(String consumerTag, Throwable cause) {
    }
}
