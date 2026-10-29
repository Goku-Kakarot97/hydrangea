package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Against a real RabbitMQ. Start one with:
 * <pre>docker run -d --name hydrangea-amqp-test -p 127.0.0.1:25672:5672 rabbitmq:3-management</pre>
 * (port and host from {@code -Damqp.port} / {@code -Damqp.host}). Skipped when nothing listens there.
 */
class AmqpTest {

    private static final String HOST = System.getProperty("amqp.host", "127.0.0.1");
    private static final int PORT = Integer.getInteger("amqp.port", 25672);
    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no RabbitMQ on " + HOST + ":" + PORT);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static AmqpConnection connect() throws Exception {
        return connect(AmqpConfig.of(HOST, PORT));
    }

    private static AmqpConnection connect(AmqpConfig c) throws Exception {
        return AmqpConnection.connect(group, c).get(10, TimeUnit.SECONDS);
    }

    private static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    private static String uniqueQueue() {
        return "hydrangea-test-" + UUID.randomUUID();
    }

    @Test
    void connectsAndReportsTheBroker() throws Exception {
        AmqpConnection c = connect();
        try {
            assertTrue(c.isOpen());
            assertEquals("RabbitMQ", c.serverProperties().get("product"));
            assertTrue(c.heartbeat().toSeconds() > 0);
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
        assertFalse(c.isOpen());
    }

    @Test
    void backPressureIsSignalledAndLifted() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, true, true, null));
            List<Boolean> changes = new CopyOnWriteArrayList<>();
            c.onWritabilityChanged(changes::add);
            byte[] mb = new byte[1 << 20];
            for (int i = 0; i < 40; i++) { // 40 MiB queued without ever waiting: far above the 64 KiB high-water mark
                ch.basicPublish("", q, false, null, mb);
            }
            await(ch.whenWritable());
            assertTrue(c.isWritable());
            assertTrue(ch.isWritable());
            Thread.sleep(200);
            assertFalse(changes.isEmpty(), "the listener was told");
            assertFalse(changes.get(0), "first it filled up");
            assertTrue(changes.get(changes.size() - 1), "and it ended writable");
            assertEquals(40, await(ch.queueDeclarePassive(q)).messageCount() + 0, "nothing was lost on the way");
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aProducerDrivenByWhenWritableNeverQueuesMoreThanAFewMessages() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, true, true, null));
            byte[] mb = new byte[1 << 20];
            CompletableFuture<Void> done = new CompletableFuture<>();
            // the producer loop from the javadoc of whenWritable(): publish while writable, otherwise carry on when told to
            Runnable[] step = new Runnable[1];
            int[] sent = {0};
            int[] maxBurst = {0};
            step[0] = () -> {
                int burst = 0;
                while (c.isWritable()) {
                    if (sent[0] == 100) {
                        done.complete(null);
                        return;
                    }
                    ch.basicPublish("", q, false, null, mb);
                    sent[0]++;
                    maxBurst[0] = Math.max(maxBurst[0], ++burst);
                }
                c.whenWritable().thenRun(step[0]);
            };
            c.onLoop(step[0]); // package-private: start the loop on the connection's own thread, as a continuation would
            done.get(30, TimeUnit.SECONDS);
            Thread.sleep(300);
            assertEquals(100, await(ch.queueDeclarePassive(q)).messageCount());
            // The high-water mark counts bytes queued in the library; the operating system's socket buffer takes a few MiB more before that
            // queue fills, so a burst is a handful of messages, not one. Without whenWritable() the whole 100 would go out in a single burst.
            assertTrue(maxBurst[0] <= 16, "published " + maxBurst[0] + " messages in one go");
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void wrongPasswordIsRefused() {
        CompletionException e = assertThrows(CompletionException.class,
                () -> AmqpConnection.connect(group, AmqpConfig.of(HOST, PORT).withCredentials("guest", "wrong")).join());
        AmqpException a = (AmqpException) e.getCause();
        assertEquals(403, a.replyCode());
        assertTrue(a.isConnectionError());
    }

    @Test
    void publishAndGet() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            AmqpChannel.QueueInfo info = await(ch.queueDeclare(q, false, true, true, null));
            assertEquals(q, info.name());
            assertNull(await(ch.basicGet(q, true)), "empty queue");
            BasicProperties props = BasicProperties.PERSISTENT.withContentType("text/plain").withCorrelationId("c-1")
                    .withHeaders(Map.of("n", 7, "s", "x", "flag", true));
            await(ch.basicPublish("", q, false, props, "hello".getBytes(StandardCharsets.UTF_8)));
            Delivery d = null;
            for (int i = 0; i < 50 && d == null; i++) {
                d = await(ch.basicGet(q, true));
                if (d == null) {
                    Thread.sleep(20);
                }
            }
            assertNotNull(d);
            assertEquals("hello", new String(d.body(), StandardCharsets.UTF_8));
            assertEquals("text/plain", d.properties().contentType());
            assertEquals("c-1", d.properties().correlationId());
            assertEquals(2, d.properties().deliveryMode());
            assertEquals(7, d.properties().headers().get("n"));
            assertEquals("x", d.properties().headers().get("s"));
            assertEquals(true, d.properties().headers().get("flag"));
            assertEquals(q, d.routingKey());
            await(ch.close());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void bigMessagesSpanSeveralFrames() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, true, true, null));
            for (int size : new int[] {0, 1, 131064, 131065, 131072, 1_000_000, 5_000_000}) {
                byte[] body = new byte[size];
                new java.util.Random(size).nextBytes(body);
                await(ch.basicPublish("", q, false, null, body));
                Delivery d = null;
                for (int i = 0; i < 200 && d == null; i++) {
                    d = await(ch.basicGet(q, true));
                    if (d == null) {
                        Thread.sleep(10);
                    }
                }
                assertNotNull(d, "size " + size);
                assertArrayEquals(body, d.body(), "size " + size);
            }
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void consumeWithAcksAndPrefetchKeepsOrder() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel pub = await(c.openChannel());
            AmqpChannel sub = await(c.openChannel());
            String q = uniqueQueue();
            await(pub.queueDeclare(q, false, false, true, null));
            int n = 2000;
            await(sub.basicQos(25));
            List<Integer> seen = new CopyOnWriteArrayList<>();
            AtomicInteger maxUnacked = new AtomicInteger();
            AtomicInteger unacked = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(n);
            await(sub.basicConsume(q, false, d -> {
                int u = unacked.incrementAndGet();
                maxUnacked.accumulateAndGet(u, Math::max);
                seen.add(Integer.parseInt(new String(d.body(), StandardCharsets.UTF_8)));
                unacked.decrementAndGet();
                sub.basicAck(d.deliveryTag(), false);
                done.countDown();
            }));
            List<CompletableFuture<Void>> sends = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                sends.add(pub.basicPublish("", q, false, null, Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
            }
            CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
            assertTrue(done.await(20, TimeUnit.SECONDS), "got " + seen.size());
            for (int i = 0; i < n; i++) {
                assertEquals(i, seen.get(i));
            }
            AmqpChannel.QueueInfo after = await(pub.queueDeclarePassive(q));
            assertEquals(0, after.messageCount(), "everything acknowledged");
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void unacknowledgedMessagesComeBackAfterNack() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, false, true, null));
            await(ch.basicPublish("", q, false, null, "again".getBytes(StandardCharsets.UTF_8)));
            List<Delivery> got = new CopyOnWriteArrayList<>();
            CountDownLatch twice = new CountDownLatch(2);
            await(ch.basicConsume(q, false, d -> {
                got.add(d);
                if (got.size() == 1) {
                    ch.basicNack(d.deliveryTag(), false, true);
                } else {
                    ch.basicAck(d.deliveryTag(), false);
                }
                twice.countDown();
            }));
            assertTrue(twice.await(10, TimeUnit.SECONDS));
            assertFalse(got.get(0).redelivered());
            assertTrue(got.get(1).redelivered());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void publisherConfirms() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, true, true, null));
            await(ch.confirmSelect());
            List<CompletableFuture<Void>> fs = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                fs.add(ch.basicPublish("", q, false, null, new byte[100]));
            }
            CompletableFuture.allOf(fs.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
            assertEquals(1000, await(ch.queueDeclarePassive(q)).messageCount());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void mandatoryMessageThatCannotBeRoutedIsReturned() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            await(ch.confirmSelect());
            CompletableFuture<ReturnedMessage> returned = new CompletableFuture<>();
            ch.onReturn(returned::complete);
            await(ch.basicPublish("", "no-such-queue-" + UUID.randomUUID(), true, null, "lost".getBytes(StandardCharsets.UTF_8)));
            ReturnedMessage m = returned.get(10, TimeUnit.SECONDS);
            assertEquals(312, m.replyCode());
            assertEquals("lost", new String(m.body(), StandardCharsets.UTF_8));
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aBrokerErrorClosesOnlyThatChannel() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            CompletionException e = assertThrows(CompletionException.class, () -> ch.queueDeclarePassive("no-such-" + UUID.randomUUID()).join());
            AmqpException a = (AmqpException) e.getCause();
            assertEquals(404, a.replyCode());
            assertFalse(a.isConnectionError());
            assertFalse(ch.isOpen());
            assertThrows(CompletionException.class, () -> ch.queueDeclare().join());
            assertTrue(c.isOpen(), "the connection survives");
            AmqpChannel next = await(c.openChannel());
            assertNotNull(await(next.queueDeclare()).name());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void redeclaringWithOtherSettingsIsAPreconditionFailure() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, false, true, null));
            AmqpChannel ch2 = await(c.openChannel());
            CompletionException e = assertThrows(CompletionException.class, () -> ch2.queueDeclare(q, true, false, true, null).join());
            assertEquals(406, ((AmqpException) e.getCause()).replyCode());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void consumerIsToldWhenTheQueueIsDeleted() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            AmqpChannel other = await(c.openChannel());
            String q = uniqueQueue();
            await(ch.queueDeclare(q, false, false, false, null));
            CompletableFuture<String> cancelled = new CompletableFuture<>();
            String tag = await(ch.basicConsume(q, true, new AmqpConsumer() {
                @Override
                public void onDelivery(Delivery d) {
                }

                @Override
                public void onCancel(String consumerTag, Throwable cause) {
                    cancelled.complete(consumerTag);
                }
            }));
            await(other.queueDelete(q, false, false));
            assertEquals(tag, cancelled.get(10, TimeUnit.SECONDS));
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void exchangesRouteByBinding() throws Exception {
        AmqpConnection c = connect();
        try {
            AmqpChannel ch = await(c.openChannel());
            String ex = "hydrangea-ex-" + UUID.randomUUID();
            await(ch.exchangeDeclare(ex, "topic", false, true, null));
            AmqpChannel.QueueInfo a = await(ch.queueDeclare());
            AmqpChannel.QueueInfo b = await(ch.queueDeclare());
            await(ch.queueBind(a.name(), ex, "orders.*", null));
            await(ch.queueBind(b.name(), ex, "invoices.#", null));
            await(ch.basicPublish(ex, "orders.new", false, null, "o".getBytes(StandardCharsets.UTF_8)));
            Thread.sleep(200);
            assertNotNull(await(ch.basicGet(a.name(), true)));
            assertNull(await(ch.basicGet(b.name(), true)));
            assertEquals(0, await(ch.queuePurge(a.name())));
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void idleConnectionSurvivesThanksToHeartbeats() throws Exception {
        AmqpConnection c = connect(AmqpConfig.of(HOST, PORT).withHeartbeat(Duration.ofSeconds(2)));
        try {
            assertEquals(2, c.heartbeat().toSeconds());
            Thread.sleep(7000); // more than two heartbeat intervals with nothing else going on
            assertTrue(c.isOpen());
            AmqpChannel ch = await(c.openChannel());
            assertNotNull(await(ch.queueDeclare()).name());
        } finally {
            c.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void closingTheConnectionFailsPendingWorkAndNotifiesConsumers() throws Exception {
        AmqpConnection c = connect();
        AmqpChannel ch = await(c.openChannel());
        String q = uniqueQueue();
        await(ch.queueDeclare(q, false, true, true, null));
        CompletableFuture<Throwable> cancelled = new CompletableFuture<>();
        await(ch.basicConsume(q, true, new AmqpConsumer() {
            @Override
            public void onDelivery(Delivery d) {
            }

            @Override
            public void onCancel(String tag, Throwable cause) {
                cancelled.complete(cause);
            }
        }));
        await(c.closeAsync());
        assertNotNull(cancelled.get(5, TimeUnit.SECONDS));
        assertThrows(CompletionException.class, () -> ch.queueDeclare().join());
        assertThrows(CompletionException.class, () -> c.openChannel().join());
    }
}
