package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.amqp.AmqpChannel;
import io.hydrangea.amqp.AmqpConfig;
import io.hydrangea.amqp.AmqpConnection;
import io.hydrangea.amqp.BasicProperties;
import io.hydrangea.amqp.Delivery;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ client: declare a queue, consume from it with acknowledgements, publish with broker confirmations.
 * <pre>
 * docker run -d --name rabbit -p 5672:5672 rabbitmq:3
 * java -cp target/classes io.hydrangea.examples.AmqpExample [host] [port]
 * </pre>
 * Consumer callbacks run on the event-loop thread, so they must not block; hand slow work to an executor and acknowledge afterwards.
 */
public final class AmqpExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5672;

        try (EventLoopGroup group = new EventLoopGroup(1)) {
            AmqpConnection connection = AmqpConnection.connect(group, AmqpConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            AmqpChannel channel = connection.openChannel().get(5, TimeUnit.SECONDS);

            String queue = channel.queueDeclare().get(5, TimeUnit.SECONDS).name(); // broker-named, deleted when we disconnect
            channel.confirmSelect().get(5, TimeUnit.SECONDS);
            channel.basicQos(10).get(5, TimeUnit.SECONDS);                          // at most 10 unacknowledged messages at a time

            CountDownLatch received = new CountDownLatch(3);
            channel.basicConsume(queue, false, (Delivery d) -> {
                System.out.println("received: " + new String(d.body(), StandardCharsets.UTF_8) + " (tag " + d.deliveryTag() + ")");
                channel.basicAck(d.deliveryTag(), false);                           // without this the broker keeps the message
                received.countDown();
            }).get(5, TimeUnit.SECONDS);

            for (int i = 1; i <= 3; i++) {
                byte[] body = ("message " + i).getBytes(StandardCharsets.UTF_8);
                channel.basicPublish("", queue, false, BasicProperties.PERSISTENT.withContentType("text/plain"), body)
                        .get(5, TimeUnit.SECONDS);                                  // completes when the broker confirms it
            }
            received.await(10, TimeUnit.SECONDS);
            connection.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }
}
