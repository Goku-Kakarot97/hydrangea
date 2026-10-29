package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.amqp.AmqpChannel;
import io.hydrangea.amqp.AmqpConfig;
import io.hydrangea.amqp.AmqpConnection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fast producer that does not outrun the connection. {@code basicPublish} never blocks, so a loop that just calls it queues everything in memory;
 * {@code whenWritable()} says when the socket can take more. The loop below runs on the connection's event loop (each continuation of
 * {@code whenWritable()} does), publishes while {@code isWritable()} and otherwise asks to be called again.
 * <pre>
 * docker run -d --name rabbit -p 5672:5672 rabbitmq:3
 * java -cp target/classes io.hydrangea.examples.AmqpProducerExample [host] [port] [messages] [kib]
 * </pre>
 * A producer on a thread of its own can instead call {@code connection.whenWritable().join()} when {@code isWritable()} is false.
 */
public final class AmqpProducerExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5672;
        int total = args.length > 2 ? Integer.parseInt(args[2]) : 200;
        int kib = args.length > 3 ? Integer.parseInt(args[3]) : 256;

        try (EventLoopGroup group = new EventLoopGroup(1)) {
            AmqpConnection connection = AmqpConnection.connect(group, AmqpConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            AmqpChannel channel = connection.openChannel().get(5, TimeUnit.SECONDS);
            String queue = channel.queueDeclare().get(5, TimeUnit.SECONDS).name();

            AtomicInteger changes = new AtomicInteger();
            connection.onWritabilityChanged(writable -> changes.incrementAndGet()); // optional: every transition, on the loop thread

            byte[] body = new byte[kib * 1024];
            CompletableFuture<Void> done = new CompletableFuture<>();
            int[] sent = {0};
            int[] waits = {0};
            Runnable[] produce = new Runnable[1];
            produce[0] = () -> {
                while (connection.isWritable()) {
                    if (sent[0] == total) {
                        done.complete(null);
                        return;
                    }
                    channel.basicPublish("", queue, false, null, body);
                    sent[0]++;
                }
                waits[0]++;
                connection.whenWritable().whenComplete((v, e) -> { // fails if the connection dies, so this cannot wait forever
                    if (e != null) {
                        done.completeExceptionally(e);
                    } else {
                        produce[0].run();
                    }
                });
            };
            long t0 = System.nanoTime();
            connection.whenWritable().thenRun(produce[0]).join(); // first iteration; the later ones are continuations on the event loop
            done.get(60, TimeUnit.SECONDS);
            double secs = (System.nanoTime() - t0) / 1e9;

            Thread.sleep(300);
            long inQueue = channel.queueDeclarePassive(queue).get(5, TimeUnit.SECONDS).messageCount();
            System.out.printf("published %d x %d KiB in %.2f s; waited for writable %d times (%d writability changes); %d messages in the queue%n",
                    total, kib, secs, waits[0], changes.get(), inQueue);
            connection.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }
}
