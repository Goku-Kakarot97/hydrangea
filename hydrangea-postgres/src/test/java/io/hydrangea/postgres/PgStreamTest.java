package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reading in pieces (a cursor) and the limits on a result held whole. */
class PgStreamTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private PgClient connect() throws Exception {
        return Pg.get(PgClient.connect(group, Pg.config()));
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(Pg.REMOTE ? 120 : 20, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    // ---- streams ----

    @Test
    void aMillionRowsAreReadInPiecesOfTheFetchSize() throws Exception {
        try (PgClient c = connect()) {
            PgRowStream s = Pg.get(c.stream(5000, "SELECT g, 'row ' || g AS label FROM generate_series(1, 1000000) g"));
            assertEquals(List.of("g", "label"), s.columns().stream().map(PgColumn::name).toList());
            long rows = 0;
            int pieces = 0;
            int biggest = 0;
            long sum = 0;
            for (;;) {
                List<PgRow> piece = Pg.get(s.next());
                if (piece.isEmpty()) {
                    break;
                }
                pieces++;
                biggest = Math.max(biggest, piece.size());
                for (PgRow r : piece) {
                    sum += r.getInt("g");
                }
                rows += piece.size();
            }
            assertEquals(1_000_000, rows);
            assertEquals(200, pieces);
            assertEquals(5000, biggest);
            assertEquals(500_000_500_000L, sum);
            assertTrue(Pg.get(s.next()).isEmpty(), "the end stays the end");
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "the connection is free after the last row");
        }
    }

    @Test
    void forEachCountsEveryRow() throws Exception {
        try (PgClient c = connect()) {
            AtomicLong sum = new AtomicLong();
            PgRowStream s = Pg.get(c.stream(777, "SELECT g FROM generate_series(1, $1::int) g", 100_000));
            assertEquals(100_000L, Pg.get(s.forEach(r -> sum.addAndGet(r.getInt(0)))).longValue());
            assertEquals(5_000_050_000L, sum.get());
        }
    }

    @Test
    void queriesSentDuringAStreamWaitAndRunAfterIt() throws Exception {
        try (PgClient c = connect()) {
            PgRowStream s = Pg.get(c.stream(10, "SELECT g FROM generate_series(1, 100) g"));
            CompletableFuture<PgResult> during = c.query("SELECT 42");
            Pg.get(s.next());
            Thread.sleep(200);
            assertFalse(during.isDone(), "the connection belongs to the stream");
            assertEquals(100L, Pg.get(s.forEach(r -> { })).longValue() + 10, "the rest of the 100 rows");
            assertEquals(42, Pg.get(during).first().getInt(0));
        }
    }

    @Test
    void closingEarlyFreesTheConnectionAtOnce() throws Exception {
        try (PgClient c = connect()) {
            PgRowStream s = Pg.get(c.stream(1000, "SELECT g, repeat('x', 100) FROM generate_series(1, 20000000) g"));
            assertEquals(1000, Pg.get(s.next()).size());
            long t0 = System.nanoTime();
            Pg.get(s.close());
            Pg.get(s.close());                                     // twice is harmless
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 5000, "the server did not finish 20 million rows first");
            assertTrue(Pg.get(s.next()).isEmpty(), "a closed stream is at its end");
        }
    }

    @Test
    void anErrorInALaterPieceFailsNextAndTheConnectionCarriesOn() throws Exception {
        try (PgClient c = connect()) {
            PgRowStream s = Pg.get(c.stream(3, "SELECT 10 / (g - 5) FROM generate_series(1, 10) g"));
            assertEquals(3, Pg.get(s.next()).size());              // g = 1..3
            PgException e = failure(s.next());                      // g = 4..6 meets the division by zero
            assertEquals("22012", e.sqlState());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertEquals('I', c.transactionStatus());
        }
    }

    @Test
    void anErrorInTheFirstPieceFailsStream() throws Exception {
        try (PgClient c = connect()) {
            assertEquals("42P01", failure(c.stream(10, "SELECT * FROM no_such_table_" + Pg.table("x"))).sqlState());
            assertEquals("42601", failure(c.stream(10, "SELEC 1")).sqlState());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertEquals(0, c.inflight());
        }
    }

    @Test
    void noRowsAndAStatementWithoutRows() throws Exception {
        try (PgClient c = connect()) {
            assertTrue(Pg.get(Pg.get(c.stream(10, "SELECT 1 WHERE false")).next()).isEmpty());
            String t = Pg.table("sn");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgRowStream s = Pg.get(c.stream(10, "INSERT INTO " + t + " VALUES ($1)", 5));
            assertEquals(0, s.columns().size());
            assertTrue(Pg.get(s.next()).isEmpty());
            assertEquals(1, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0), "the insert ran");
        }
    }

    @Test
    void aConsumerThatThrowsClosesTheStream() throws Exception {
        try (PgClient c = connect()) {
            PgRowStream s = Pg.get(c.stream(100, "SELECT g FROM generate_series(1, 100000) g"));
            AtomicInteger seen = new AtomicInteger();
            ExecutionException e = assertThrows(ExecutionException.class, () -> s.forEach(r -> {
                if (seen.incrementAndGet() == 150) {
                    throw new IllegalStateException("enough");
                }
            }).get(20, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof IllegalStateException);
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "and the connection is free");
        }
    }

    @Test
    void streamsWorkWithoutAStatementCache() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withStatementCacheSize(0)))) {
            assertEquals(20_000L, Pg.get(Pg.get(c.stream(1000, "SELECT g FROM generate_series(1, 20000) g")).forEach(r -> { })).longValue());
            assertEquals(3L, Pg.get(Pg.get(c.stream(2, "SELECT g FROM generate_series(1, $1::int) g", 3)).forEach(r -> { })).longValue());
        }
    }

    @Test
    void aStreamInATransactionOfThePool() throws Exception {
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 2))) {
            Long n = Pg.get(pool.<Long>transaction(db -> db.stream(100, "SELECT g FROM generate_series(1, 1000) g").thenCompose(s -> s.forEach(r -> { }))));
            assertEquals(1000L, n.longValue());
            assertEquals(1, Pg.get(pool.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void theConnectionDyingDuringAStreamFailsTheReader() throws Exception {
        try (PgClient victim = connect(); PgClient killer = connect()) {
            PgRowStream s = Pg.get(victim.stream(10, "SELECT g FROM generate_series(1, 1000000) g"));
            Pg.get(s.next());
            Pg.get(killer.query("SELECT pg_terminate_backend($1)", victim.backendPid()));
            ExecutionException e = assertThrows(ExecutionException.class, () -> {
                for (int i = 0; i < 100_000; i++) {
                    if (s.next().get(10, TimeUnit.SECONDS).isEmpty()) {
                        return;
                    }
                }
            });
            assertTrue(e.getCause() instanceof PgException);
        }
    }

    // ---- limits on a result held whole ----

    @Test
    void aResultBeyondTheRowLimitFailsAndTheConnectionStaysInStep() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withResultLimits(1000, 1 << 24)))) {
            assertEquals(1000, Pg.get(c.query("SELECT g FROM generate_series(1, 1000) g")).size(), "exactly at the limit is fine");
            PgException e = failure(c.query("SELECT g FROM generate_series(1, 5000) g"));
            assertTrue(e.getMessage().contains("larger than the limit") && e.getMessage().contains("stream()"), e.getMessage());
            assertFalse(e.retriable());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "the rest was read and dropped");
            assertEquals(5000L, Pg.get(Pg.get(c.stream(500, "SELECT g FROM generate_series(1, 5000) g")).forEach(r -> { })).longValue(), "a stream is not subject to it");
            // the simple protocol too, and a pipeline around the oversized query
            assertTrue(failure(c.execute("SELECT g FROM generate_series(1, 2000) g")).getMessage().contains("limit"));
            var before = c.query("SELECT 'before'");
            var big = c.query("SELECT g FROM generate_series(1, 3000) g");
            var after = c.query("SELECT 'after'");
            assertEquals("before", Pg.get(before).first().getString(0));
            failure(big);
            assertEquals("after", Pg.get(after).first().getString(0));
        }
    }

    @Test
    void aResultBeyondTheByteLimitFails() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withResultLimits(1_000_000, 100_000)))) {
            assertEquals(50, Pg.get(c.query("SELECT repeat('x', 1000) FROM generate_series(1, 50)")).size());
            assertTrue(failure(c.query("SELECT repeat('x', 1000) FROM generate_series(1, 2000)")).getMessage().contains("larger than the limit"));
            assertEquals(2000L, Pg.get(Pg.get(c.stream(100, "SELECT repeat('x', 1000) FROM generate_series(1, 2000)")).forEach(r -> { })).longValue());
        }
    }

    @Test
    void thereIsADefaultLimitThatCoversOrdinaryUse() throws Exception {
        assertEquals(1_000_000, Pg.config().maxResultRows());
        assertEquals(256 * 1024 * 1024, Pg.config().maxResultBytes());
        assertThrows(IllegalArgumentException.class, () -> Pg.config().withResultLimits(0, 1 << 20));
    }
}
