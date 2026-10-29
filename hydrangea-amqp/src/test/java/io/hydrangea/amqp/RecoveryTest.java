package io.hydrangea.amqp;

import io.hydrangea.testing.ChaosProxy;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Automatic recovery against a real RabbitMQ, with a proxy in between that breaks the connection on demand. */
class RecoveryTest {

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

    /** Records what the recovery does. */
    static final class Events implements RecoveryListener {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        final AtomicInteger attempts = new AtomicInteger();
        volatile Throwable gaveUp;

        @Override
        public void connectionLost(Throwable cause) {
            queue.add("lost");
        }

        @Override
        public void attemptStarting(int attempt) {
            attempts.incrementAndGet();
        }

        @Override
        public void recovered() {
            queue.add("recovered");
        }

        @Override
        public void declarationFailed(String what, Throwable cause) {
            queue.add("declarationFailed:" + ((AmqpException) cause).replyCode() + ":" + what);
        }

        @Override
        public void gaveUp(Throwable cause) {
            gaveUp = cause;
            queue.add("gaveUp");
        }

        void expect(String what) throws Exception {
            String got = queue.poll(20, TimeUnit.SECONDS);
            assertEquals(what, got);
        }
    }

    private static RecoveryPolicy fast() {
        return new RecoveryPolicy(Duration.ofMillis(100), Duration.ofMillis(400), 2.0, 0.0, 0);
    }

    private static RecoveringConnection connect(ChaosProxy proxy, RecoveryPolicy policy, Events events, int heartbeatSeconds) throws Exception {
        AmqpConfig cfg = AmqpConfig.of("127.0.0.1", proxy.port()).withHeartbeat(Duration.ofSeconds(heartbeatSeconds))
                .withConnectionName("hydrangea-recovery-test");
        // the listener must be registered before anything can break, so connect first and register straight away
        RecoveringConnection rc = RecoveringConnection.connect(group, cfg, policy).get(10, TimeUnit.SECONDS);
        rc.addRecoveryListener(events);
        return rc;
    }

    private static <T> T await(java.util.concurrent.CompletableFuture<T> f) throws Exception {
        return f.get(15, TimeUnit.SECONDS);
    }

    private static String id() {
        return UUID.randomUUID().toString();
    }

    @Test
    void declarationsAndConsumersComeBackAfterTheConnectionIsSevered() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            try {
                RecoveringChannel ch = await(rc.openChannel());
                String exchange = "ex-" + id();
                String queue = "q-" + id();
                await(ch.exchangeDeclare(exchange, "direct", false, false, null));
                await(ch.queueDeclare(queue, false, true, true, null)); // exclusive: the broker deletes it when the connection dies
                await(ch.queueBind(queue, exchange, "k", null));
                BlockingQueue<String> got = new LinkedBlockingQueue<>();
                await(ch.basicConsume(queue, true, d -> got.add(new String(d.body(), StandardCharsets.UTF_8))));

                await(ch.basicPublish(exchange, "k", false, null, "before".getBytes(StandardCharsets.UTF_8)));
                assertEquals("before", got.poll(5, TimeUnit.SECONDS));

                proxy.sever();
                ev.expect("lost");
                ev.expect("recovered");
                assertTrue(rc.isOpen());

                await(ch.basicPublish(exchange, "k", false, null, "after".getBytes(StandardCharsets.UTF_8)));
                assertEquals("after", got.poll(5, TimeUnit.SECONDS), "queue, binding and consumer were re-created");
                assertNotNull(await(ch.queueDeclarePassive(queue)));
                await(ch.exchangeDelete(exchange, false));
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void aQueueNamedByTheBrokerKeepsWorkingUnderItsOldName() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            try {
                RecoveringChannel ch = await(rc.openChannel());
                String name = await(ch.queueDeclare()).name();
                BlockingQueue<String> got = new LinkedBlockingQueue<>();
                await(ch.basicConsume(name, true, d -> got.add(new String(d.body(), StandardCharsets.UTF_8))));
                proxy.sever();
                ev.expect("lost");
                ev.expect("recovered");
                assertNotEquals(name, rc.queueName(name), "the broker gave the new queue another name");
                await(ch.basicPublish("", name, false, null, "hello".getBytes(StandardCharsets.UTF_8)));
                assertEquals("hello", got.poll(5, TimeUnit.SECONDS));
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void oldAcknowledgementsAreIgnoredAndUnacknowledgedMessagesAreRedelivered() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            String queue = "q-" + id();
            try {
                RecoveringChannel ch = await(rc.openChannel());
                await(ch.queueDeclare(queue, false, false, false, null)); // survives the connection
                await(ch.basicQos(10));
                List<Delivery> seen = new CopyOnWriteArrayList<>();
                await(ch.basicConsume(queue, false, seen::add));
                for (int i = 0; i < 3; i++) {
                    await(ch.basicPublish("", queue, false, null, ("m" + i).getBytes(StandardCharsets.UTF_8)));
                }
                waitFor(() -> seen.size() == 3);
                long highestBefore = seen.stream().mapToLong(Delivery::deliveryTag).max().orElseThrow();
                List<Long> oldTags = seen.stream().map(Delivery::deliveryTag).toList();
                assertTrue(seen.stream().noneMatch(Delivery::redelivered));

                proxy.sever(); // nothing was acknowledged
                ev.expect("lost");
                ev.expect("recovered");

                waitFor(() -> seen.size() == 6);
                List<Delivery> again = seen.subList(3, 6);
                assertTrue(again.stream().allMatch(Delivery::redelivered), "the broker marks the second delivery");
                assertTrue(again.stream().allMatch(d -> d.deliveryTag() > highestBefore), "tags keep increasing across the break");

                for (long old : oldTags) { // would close the channel with 406 if it reached the broker
                    ch.basicAck(old, false);
                }
                Thread.sleep(300);
                assertTrue(ch.isOpen(), "stale acknowledgements did no harm");
                for (Delivery d : again) {
                    ch.basicAck(d.deliveryTag(), false);
                }
                Thread.sleep(300); // let the acknowledgements reach the broker
                proxy.sever();     // a second break: what was acknowledged must not come back
                ev.expect("lost");
                ev.expect("recovered");
                Thread.sleep(800);
                assertEquals(6, seen.size(), "acknowledged messages were not delivered again");
                assertEquals(0, await(ch.queueDeclarePassive(queue)).messageCount());
            } finally {
                try {
                    await(await(rc.openChannel()).queueDelete(queue, false, false));
                } catch (Exception ignored) {
                    // cleanup only
                }
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    interface Check {
        boolean ok() throws Exception;
    }

    private static void waitFor(Check c) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < end) {
            if (c.ok()) {
                return;
            }
            Thread.sleep(25);
        }
        fail("condition not reached in time");
    }

    @Test
    void callsFailWhileTheBrokerIsUnreachableAndTheConnectionRetriesUntilItIsBack() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            try {
                RecoveringChannel ch = await(rc.openChannel());
                proxy.refuse(true);
                proxy.sever();
                ev.expect("lost");
                assertTrue(rc.isRecovering());
                CompletionException e = assertThrows(CompletionException.class, () -> ch.basicPublish("", "x", false, null, new byte[1]).join());
                AmqpException a = (AmqpException) e.getCause();
                assertTrue(a.isConnectionError());
                assertFalse(ch.isOpen());
                assertThrows(CompletionException.class, () -> rc.openChannel().join());

                waitFor(() -> ev.attempts.get() >= 3); // it keeps trying with the broker out of reach
                assertTrue(rc.isRecovering());
                proxy.refuse(false);
                ev.expect("recovered");
                assertTrue(ch.isOpen());
                await(ch.queueDeclare());
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void aNetworkThatDiesSilentlyIsNoticedByTheHeartbeat() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 1);
            try {
                RecoveringChannel ch = await(rc.openChannel());
                String q = await(ch.queueDeclare()).name();
                proxy.blackhole(true); // no reset, no close: just silence
                ev.expect("lost");     // two missed heartbeats
                proxy.blackhole(false);
                ev.expect("recovered");
                await(ch.basicPublish("", q, false, null, new byte[1]));
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void givesUpAfterTheAttemptLimitAndTellsEveryone() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast().withMaxAttempts(3), ev, 5);
            RecoveringChannel ch = await(rc.openChannel());
            String q = await(ch.queueDeclare()).name();
            java.util.concurrent.CompletableFuture<Throwable> cancelled = new java.util.concurrent.CompletableFuture<>();
            await(ch.basicConsume(q, true, new AmqpConsumer() {
                @Override
                public void onDelivery(Delivery d) {
                }

                @Override
                public void onCancel(String tag, Throwable cause) {
                    cancelled.complete(cause);
                }
            }));
            proxy.refuse(true);
            proxy.sever();
            ev.expect("lost");
            ev.expect("gaveUp");
            assertEquals(3, ev.attempts.get());
            assertNotNull(cancelled.get(5, TimeUnit.SECONDS), "the consumer is told why it ended");
            rc.closeFuture().get(5, TimeUnit.SECONDS);
            assertFalse(rc.isOpen());
            assertFalse(rc.isRecovering());
            assertFalse(ch.isOpen());
            assertThrows(CompletionException.class, () -> ch.queueDeclare().join());
        }
    }

    @Test
    void closingWhileRecoveringStopsTheAttempts() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            RecoveringChannel ch = await(rc.openChannel());
            proxy.refuse(true);
            proxy.sever();
            ev.expect("lost");
            waitFor(() -> ev.attempts.get() >= 1);
            rc.closeAsync().get(3, TimeUnit.SECONDS);
            int after = ev.attempts.get();
            Thread.sleep(1200);
            assertTrue(ev.attempts.get() <= after + 1, "no more attempts once closed (" + after + " -> " + ev.attempts.get() + ")");
            assertFalse(ch.isOpen());
        }
    }

    @Test
    void whenWritableFailsWhileDownAndWorksAgainAfterRecovery() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            try {
                RecoveringChannel ch = await(rc.openChannel());
                await(ch.whenWritable());
                assertTrue(ch.isWritable());
                proxy.refuse(true);
                proxy.sever();
                ev.expect("lost");
                assertFalse(ch.isWritable());
                CompletionException e = assertThrows(CompletionException.class, () -> ch.whenWritable().join());
                assertTrue(((AmqpException) e.getCause()).isConnectionError());
                proxy.refuse(false);
                ev.expect("recovered");
                await(ch.whenWritable());
                assertTrue(ch.isWritable());
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** A declaration the broker will keep refusing must not keep recovery in a loop for ever: it is dropped and the rest comes back. */
    @Test
    void aDeclarationThePeerNowRefusesIsDroppedAndRecoveryCompletes() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            AmqpConnection other = AmqpConnection.connect(group, AmqpConfig.of(HOST, PORT)).get(10, TimeUnit.SECONDS);
            String conflicting = "q-conflict-" + id();
            String fine = "q-fine-" + id();
            try {
                RecoveringChannel ch = await(rc.openChannel());
                await(ch.queueDeclare(conflicting, false, false, false, null));
                await(ch.queueDeclare(fine, false, false, false, null));
                AmqpChannel o = await(other.openChannel());

                proxy.refuse(true); // while the application is cut off, someone else re-creates the queue with other settings
                proxy.sever();
                ev.expect("lost");
                await(o.queueDelete(conflicting, false, false));
                await(o.queueDeclare(conflicting, true, false, false, null)); // durable now: declaring it non-durable is a 406
                proxy.refuse(false);

                assertEquals("declarationFailed:406:queue " + conflicting, ev.queue.poll(20, TimeUnit.SECONDS));
                ev.expect("recovered");
                assertTrue(ch.isOpen());
                assertEquals(fine, await(ch.queueDeclare(fine, false, false, false, null)).name(), "the declaration that still fits is intact");
                BlockingQueue<String> got = new LinkedBlockingQueue<>();
                await(ch.basicConsume(fine, true, d -> got.add(new String(d.body(), StandardCharsets.UTF_8))));
                await(ch.basicPublish("", fine, false, null, "ok".getBytes(StandardCharsets.UTF_8)));
                assertEquals("ok", got.poll(5, TimeUnit.SECONDS));
            } finally {
                try {
                    AmqpChannel o = await(other.openChannel());
                    await(o.queueDelete(conflicting, false, false));
                    await(o.queueDelete(fine, false, false));
                } catch (Exception ignored) {
                    // cleanup only
                }
                other.closeAsync().get(10, TimeUnit.SECONDS);
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void aChannelClosedByTheApplicationIsNotRecovered() throws Exception {
        try (ChaosProxy proxy = new ChaosProxy(HOST, PORT)) {
            Events ev = new Events();
            RecoveringConnection rc = connect(proxy, fast(), ev, 5);
            try {
                RecoveringChannel keep = await(rc.openChannel());
                RecoveringChannel gone = await(rc.openChannel());
                await(gone.close());
                proxy.sever();
                ev.expect("lost");
                ev.expect("recovered");
                assertTrue(keep.isOpen());
                assertFalse(gone.isOpen());
            } finally {
                rc.closeAsync().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
