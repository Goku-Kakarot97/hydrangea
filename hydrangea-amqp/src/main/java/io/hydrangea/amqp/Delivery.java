package io.hydrangea.amqp;

/**
 * A message handed to a consumer or returned by {@code basicGet}. {@code deliveryTag} is what {@code basicAck}/{@code basicNack}
 * need; it is only valid on the channel that delivered it.
 */
public record Delivery(String consumerTag, long deliveryTag, boolean redelivered, String exchange, String routingKey,
                       BasicProperties properties, byte[] body) {
}
