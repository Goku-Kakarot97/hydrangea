package io.hydrangea.postgres;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A client that gets its connection back by itself, against a real server whose backend is killed. */
class PgReconnectTest {

    private static final Backoff QUICK = new Backoff(Duration.ofMillis(100), Duration.ofMillis(400), 2.0, 0, 0);

    private EventLoopGroup group;
    private PgClient killer;

    @BeforeEach
    void setUp() throws Exception {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(2);
        killer = Pg.get(PgClient.connect(group, Pg.config()));
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private static final class Events implements PgListener {
        final List<String> log = new CopyOnWriteArrayList<>();
        final List<String> notes = new CopyOnWriteArrayList<>();
        final List<Throwable> causes = new CopyOnWriteArrayList<>();

        @Override
        public void onClose(Throwable cause) {
            log.add(cause == null ? "close" : "lost");
            if (cause != null) {
                causes.add(cause);
            }
        }

        @Override
        public void onReconnecting(int attempt, Duration delay) {
            log.add("reconnecting " + attempt);
        }

        @Override
        public void onReconnected() {
            log.add("back");
        }

        @Override
        public void onGaveUp(Throwable cause) {
            log.add("gave up");
        }

        @Override
        public void onNotification(int pid, String channel, String payload) {
            notes.add(channel + ":" + payload);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    private void kill(PgClient victim) throws Exception {
        Pg.get(killer.query("SELECT pg_terminate_backend($1)", victim.backendPid()));
    }

    @Test
    void theClientGetsItsConnectionBackAndTellsTheListener() throws Exception {
        Events ev = new Events();
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(QUICK), ev))) {
            int before = c.backendPid();
            CompletableFuture<PgResult> pending = c.query("SELECT pg_sleep(30)");
            Thread.sleep(150);
            kill(c);
            ExecutionException e = assertThrows(ExecutionException.class, () -> pending.get(10, TimeUnit.SECONDS));
            assertFalse(((PgException) e.getCause()).retriable(), "it may have run");
            waitFor(c::isOpen, "the connection to come back");
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertNotEquals(before, c.backendPid(), "a new backend: a new session");
            assertEquals(List.of("lost", "reconnecting 1", "back"), ev.log);
        }
    }

    @Test
    void theReasonForALossIsTheServersOwnError() throws Exception {
        Events ev = new Events();
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(QUICK), ev))) {
            kill(c);                                           // idle: no query is waiting when the server's FATAL arrives
            waitFor(() -> !ev.causes.isEmpty(), "the loss");
            PgException cause = (PgException) ev.causes.get(0);
            assertEquals("57P01", cause.sqlState(), "admin_shutdown, as the server said it: " + cause);
            assertFalse(cause.getMessage().contains("protocol error"), cause.getMessage());
            waitFor(c::isOpen, "the reconnect");
        }
    }

    @Test
    void queriesDuringTheOutageFailAtOnceAndRetriably() throws Exception {
        Backoff slow = new Backoff(Duration.ofSeconds(2), Duration.ofSeconds(2), 1.0, 0, 0);
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(slow)))) {
            kill(c);
            waitFor(() -> !c.isOpen(), "the loss to be noticed");
            long t0 = System.nanoTime();
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT 1").get(10, TimeUnit.SECONDS));
            assertTrue(((PgException) e.getCause()).retriable(), "it never left the client");
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 1000, "it did not wait for the reconnect");
            waitFor(c::isOpen, "the reconnect");
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void theStatementCacheAndTheBinaryDescriptionsStartOverInTheNewSession() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(QUICK)))) {
            for (int i = 0; i < 5; i++) {
                assertEquals(i, Pg.get(c.query("SELECT $1::int4, $2::bytea", i, new byte[]{1})).first().getInt(0));
            }
            Pg.get(c.query("SELECT $1::int4", 7));                      // the first run of a statement is text
            assertTrue(Pg.get(c.query("SELECT $1::int4", 7)).first().wasBinary(0), "binary before the loss");
            kill(c);
            waitFor(() -> !c.isOpen(), "the loss");
            waitFor(c::isOpen, "the reconnect");
            // the same statements again: had the old names been kept, the server would answer 'prepared statement does not exist'
            for (int i = 0; i < 5; i++) {
                assertEquals(i, Pg.get(c.query("SELECT $1::int4, $2::bytea", i, new byte[]{1})).first().getInt(0));
            }
            assertFalse(Pg.get(c.query("SELECT $1::int4", 7)).first().wasBinary(0), "the first run in the new session is text again");
            assertTrue(Pg.get(c.query("SELECT $1::int4", 7)).first().wasBinary(0));
        }
    }

    @Test
    void channelsGivenToListenAreListenedToAgain() throws Exception {
        Events ev = new Events();
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(QUICK), ev))) {
            Pg.get(c.listen("hydra reconnect \"chan\""));
            Pg.get(killer.query("SELECT pg_notify($1, 'one')", "hydra reconnect \"chan\""));
            waitFor(() -> ev.notes.size() == 1, "the first notification");
            kill(c);
            waitFor(() -> !c.isOpen(), "the loss");
            waitFor(c::isOpen, "the reconnect");
            Pg.get(killer.query("SELECT pg_notify($1, 'two')", "hydra reconnect \"chan\""));
            waitFor(() -> ev.notes.size() == 2, "the notification after the reconnect");
            assertEquals(List.of("hydra reconnect \"chan\":one", "hydra reconnect \"chan\":two"), ev.notes);
            Pg.get(c.unlisten("hydra reconnect \"chan\""));
            kill(c);
            waitFor(() -> !c.isOpen(), "the second loss");
            waitFor(c::isOpen, "the second reconnect");
            Pg.get(killer.query("SELECT pg_notify($1, 'three')", "hydra reconnect \"chan\""));
            Thread.sleep(300);
            assertEquals(2, ev.notes.size(), "no longer listened to after unlisten");
        }
    }

    @Test
    void aTransactionLostWithTheConnectionIsReportedInsteadOfBeingRunOnWithoutIt() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(QUICK)))) {
            Pg.get(c.execute("BEGIN"));
            Pg.get(c.query("SELECT 1"));
            kill(c);
            waitFor(() -> !c.isOpen(), "the loss");
            waitFor(c::isOpen, "the reconnect");
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT 2").get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().contains("inside a transaction"), e.getCause().getMessage());
            assertFalse(((PgException) e.getCause()).retriable());
            assertEquals(3, Pg.get(c.query("SELECT 3")).first().getInt(0), "told once; then the new session works");
            assertEquals('I', c.transactionStatus());
            // killed while idle: nothing was lost, the first query just runs
            kill(c);
            waitFor(() -> !c.isOpen(), "the second loss");
            waitFor(c::isOpen, "the second reconnect");
            assertEquals(4, Pg.get(c.query("SELECT 4")).first().getInt(0));
        }
    }

    @Test
    void closingStopsTheReconnecting() throws Exception {
        String app = "hydra-reconnect-" + UUID.randomUUID().toString().substring(0, 8);
        Backoff slow = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(1), 1.0, 0, 0);
        PgClient c = Pg.get(PgClient.connect(group, Pg.config().withReconnect(slow).withApplicationName(app)));
        kill(c);
        waitFor(() -> !c.isOpen(), "the loss");
        Pg.get(c.disconnect());
        Thread.sleep(2500);
        assertEquals(0, Pg.get(killer.query("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app)).first().getInt(0), "nothing came back after close");
        assertFalse(c.isOpen());
    }

    @Test
    void aPoolDoesNotReconnectTwice() throws Exception {
        String app = "hydra-pool-" + UUID.randomUUID().toString().substring(0, 8);
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config().withReconnect(QUICK).withApplicationName(app), 2))) {
            for (int i = 0; i < 40; i++) {
                Pg.get(pool.query("SELECT pg_backend_pid(), pg_sleep(0.002)"));
            }
            Pg.get(killer.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1 AND pid IN (SELECT pid FROM pg_stat_activity WHERE application_name = $1 LIMIT 1)", app));
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            int sessions = -1;
            while (System.nanoTime() < end) {
                try {
                    pool.query("SELECT 1").get(5, TimeUnit.SECONDS);
                } catch (ExecutionException ignored) {
                    // the one that was on the killed connection
                }
                sessions = Pg.get(killer.query("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app)).first().getInt(0);
                if (sessions == 2 && System.nanoTime() > end - TimeUnit.SECONDS.toNanos(6)) {
                    break;
                }
                Thread.sleep(100);
            }
            Thread.sleep(1500);
            assertEquals(2, Pg.get(killer.query("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app)).first().getInt(0), "two sessions, not three: the pool replaced the connection and the client did not also reconnect");
        }
    }
}
