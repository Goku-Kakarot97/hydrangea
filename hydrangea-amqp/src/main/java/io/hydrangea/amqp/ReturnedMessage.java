package io.hydrangea.amqp;

/** A message published with {@code mandatory = true} that the broker could not route to any queue. */
public record ReturnedMessage(int replyCode, String replyText, String exchange, String routingKey, BasicProperties properties, byte[] body) {
}
