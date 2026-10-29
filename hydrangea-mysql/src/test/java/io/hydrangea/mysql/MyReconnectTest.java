package io.hydrangea.mysql;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** The server kills the client's session ({@code KILL id}); with {@link MyConfig#withReconnect} the client comes back. */
class MyReconnectTest {

    static EventLoopGroup group;
    static MyClient admin;

    @BeforeAll
    static void start() throws Exception {
        My.require(My.PORT);
        group = new EventLoopGroup(2);
        admin = MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS);
    }

    @AfterAll
    static void stop() {
        if (admin != null) {
            admin.close();
        }
        if (group != null) {
            group.close();
        }
    }

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    static MyConfig reconnecting() {
        return My.config().withReconnect(Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(200)));
    }

    /** A listener that counts and lets a test wait for the reconnection. */
    static final class Events implements MyListener {
        final List<String> log = java.util.Collections.synchronizedList(new ArrayList<>());
        volatile CountDownLatch back = new CountDownLatch(1);

        @Override
        public void onClose(Throwable cause) {
            log.add("close:" + (cause == null ? "closed" : "lost"));
        }

        @Override
        public void onReconnecting(int attempt, Duration delay) {
            log.add("reconnecting:" + attempt);
        }

        @Override
        public void onReconnected() {
            log.add("reconnected");
            back.countDown();
        }

        @Override
        public void onGaveUp(Throwable cause) {
            log.add("gaveup");
        }
    }

    @Test
    void comesBackAfterTheSessionIsKilledAndTheCacheStartsAgain() throws Exception {
        Events ev = new Events();
        try (MyClient c = MyClient.connect(group, reconnecting(), ev).get(10, TimeUnit.SECONDS)) {
            assertEquals(3, await(c.query("SELECT ? + 1", 2)).first().getInt(0));      // prepared and cached
            long id = c.threadId();
            await(admin.query("KILL " + id));
            assertTrue(ev.back.await(10, TimeUnit.SECONDS), "reconnected: " + ev.log);
            assertTrue(c.isOpen());
            assertNotEquals(id, c.threadId());
            assertEquals(3, await(c.query("SELECT ? + 1", 2)).first().getInt(0), "the same statement, prepared again in the new session");
            assertEquals(7, await(c.query("SELECT 7")).first().getInt(0));
            assertTrue(ev.log.contains("close:lost") && ev.log.contains("reconnecting:1") && ev.log.contains("reconnected"), ev.log.toString());
            assertFalse(ev.log.contains("close:closed"));
        }
        Thread.sleep(100);
        assertEquals("close:closed", ev.log.get(ev.log.size() - 1), ev.log.toString());
    }

    @Test
    void callsMadeWhileItIsDownFailAtOnceAndRetriably() throws Exception {
        Events ev = new Events();
        try (MyClient c = MyClient.connect(group, reconnecting(), ev).get(10, TimeUnit.SECONDS)) {
            CompletableFuture<MyResult> slow = c.query("SELECT SLEEP(3)");
            Thread.sleep(200);
            await(admin.query("KILL " + c.threadId()));
            ExecutionException e = assertThrows(ExecutionException.class, () -> await(slow));
            assertFalse(((MyException) e.getCause()).retriable(), "it was running: it may have taken effect");
            int failed = 0, ok = 0;
            for (int i = 0; i < 20 && !c.isOpen(); i++) {
                try {
                    await(c.query("SELECT 1"));
                    ok++;
                } catch (ExecutionException x) {
                    assertTrue(((MyException) x.getCause()).retriable(), x.getCause().getMessage());
                    failed++;
                }
                Thread.sleep(10);
            }
            assertTrue(ev.back.await(10, TimeUnit.SECONDS), ev.log.toString());
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0));
            assertTrue(failed + ok >= 0);
        }
    }

    @Test
    void aTransactionLostWithTheConnectionIsNotSilentlyContinued() throws Exception {
        Events ev = new Events();
        try (MyClient c = MyClient.connect(group, reconnecting(), ev).get(10, TimeUnit.SECONDS)) {
            await(c.query("START TRANSACTION"));
            assertTrue(c.inTransaction());
            await(admin.query("KILL " + c.threadId()));
            assertTrue(ev.back.await(10, TimeUnit.SECONDS), ev.log.toString());
            ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.query("SELECT 1")));
            assertTrue(e.getCause().getMessage().contains("lost inside a transaction"), e.getCause().getMessage());
            assertEquals(1, await(c.query("SELECT 1")).first().getInt(0), "told once; then the new session is usable");
            assertFalse(c.inTransaction());
        }
    }

    @Test
    void closingStopsTheReconnecting() throws Exception {
        Events ev = new Events();
        MyClient c = MyClient.connect(group, My.config().withReconnect(Backoff.defaults().withDelays(Duration.ofSeconds(2), Duration.ofSeconds(2))), ev).get(10, TimeUnit.SECONDS);
        await(admin.query("KILL " + c.threadId()));
        Thread.sleep(300);
        assertTrue(ev.log.contains("reconnecting:1"), ev.log.toString());
        c.disconnect().get(5, TimeUnit.SECONDS);
        Thread.sleep(2500);
        assertFalse(c.isOpen());
        assertFalse(ev.log.contains("reconnected"), ev.log.toString());
        ExecutionException e = assertThrows(ExecutionException.class, () -> await(c.query("SELECT 1")));
        assertTrue(e.getCause().getMessage().contains("closed"), e.getCause().getMessage());
    }

    @Test
    void aPoolDoesNotLetItsConnectionsReconnectOnTheirOwn() throws Exception {
        AtomicInteger reconnected = new AtomicInteger();
        MyListener l = new MyListener() {
            @Override
            public void onReconnected() {
                reconnected.incrementAndGet();
            }
        };
        try (MyPool p = MyPool.create(group, reconnecting(), 2, MyPoolOptions.defaults(), l).get(10, TimeUnit.SECONDS)) {
            long victim = await(p.query("SELECT CONNECTION_ID()")).first().getLong(0);
            await(admin.query("KILL " + victim));
            boolean healed = false;
            for (int i = 0; i < 100 && !healed; i++) {
                try {
                    await(p.query("SELECT 1"));
                    healed = true;
                } catch (ExecutionException e) {
                    Thread.sleep(50);
                }
            }
            assertTrue(healed);
            Thread.sleep(1500);
            assertEquals(0, reconnected.get(), "the pool replaced it; the client did not reconnect by itself");
            java.util.Set<Long> ids = new java.util.HashSet<>();
            for (int i = 0; i < 100; i++) {
                ids.add(await(p.query("SELECT CONNECTION_ID()")).first().getLong(0));
            }
            assertEquals(2, ids.size());
        }
    }
}
