package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Against a real PostgreSQL (see {@code start-postgres.sh}); skipped when none is running. */
class PgClientTest {

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
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    // ---- login ----

    @Test
    void logsInWithScramMd5AndCleartext() throws Exception {
        Pg.notOnRemote();
        for (int port : new int[]{Pg.PORT, Pg.MD5_PORT, Pg.PASSWORD_PORT}) {
            Pg.require(port);
            try (PgClient c = Pg.get(PgClient.connect(group, Pg.config(port)))) {
                assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "port " + port);
                assertTrue(c.backendPid() > 0);
                assertTrue(c.serverParameter("server_version").startsWith("16"), c.serverParameter("server_version"));
                assertEquals('I', c.transactionStatus());
            }
        }
    }

    @Test
    void aWrongPasswordFailsTheLoginWithTheServersError() throws Exception {
        Pg.notOnRemote();
        for (int port : new int[]{Pg.PORT, Pg.MD5_PORT, Pg.PASSWORD_PORT}) {
            Pg.require(port);
            PgException e = failure(PgClient.connect(group, Pg.config(port).withPassword("nope")));
            assertEquals("28P01", e.sqlState(), "port " + port + ": " + e);
        }
    }

    @Test
    void noPasswordWhenOneIsAskedForFailsClearly() throws Exception {
        Pg.notOnRemote();
        PgException e = failure(PgClient.connect(group, PgConfig.of(Pg.HOST, Pg.PORT, "hydra", "hydra")));
        assertTrue(e.getMessage().contains("password"), e.getMessage());
    }

    @Test
    void anUnknownDatabaseAndAClosedPortFail() throws Exception {
        Pg.notOnRemote();
        assertEquals("3D000", failure(PgClient.connect(group, PgConfig.of(Pg.HOST, Pg.PORT, "hydra", "nosuchdb").withPassword("hydra"))).sqlState());
        PgException e = failure(PgClient.connect(group, PgConfig.of("127.0.0.1", 1, "hydra", "hydra")));
        assertTrue(e.retriable(), "nothing was sent: a retry is safe");
    }

    // ---- types ----

    @Test
    void decodesTheCommonTypes() throws Exception {
        try (PgClient c = connect()) {
            PgRow r = Pg.get(c.query("SELECT 1::int2 a, 2::int4 b, 3::int8 c, 1.5::float4 d, 2.5::float8 e, 12.340::numeric f, true g, 'héllo wörld'::text h,"
                    + " '\\xdeadbeef'::bytea i, '123e4567-e89b-12d3-a456-426614174000'::uuid j, '2020-02-29'::date k, '03:04:05.25'::time l,"
                    + " '2020-01-02 03:04:05.5'::timestamp m, '2020-01-02 03:04:05.5+05:30'::timestamptz n, '{\"a\": 1}'::jsonb o, NULL::int p")).first();
            assertEquals((short) 1, r.get("a"));
            assertEquals(2, r.get("b"));
            assertEquals(3L, r.get("c"));
            assertEquals(1.5f, r.get("d"));
            assertEquals(2.5, r.get("e"));
            assertEquals(new BigDecimal("12.340"), r.get("f"));
            assertEquals(true, r.get("g"));
            assertEquals("héllo wörld", r.get("h"));
            assertArrayEquals(new byte[]{(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}, r.getBytes("i"));
            assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), r.get("j"));
            assertEquals(LocalDate.of(2020, 2, 29), r.get("k"));
            assertEquals(LocalTime.of(3, 4, 5, 250_000_000), r.get("l"));
            assertEquals(LocalDateTime.of(2020, 1, 2, 3, 4, 5, 500_000_000), r.get("m"));
            // the server shows timestamptz in its session time zone: compare the instant
            assertEquals(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)).toInstant(),
                    ((OffsetDateTime) r.get("n")).toInstant());
            assertEquals("{\"a\": 1}", r.get("o"));
            assertNull(r.get("p"));
            assertTrue(r.isNull(15));
            assertTrue(r.isNull("p"));
            assertFalse(r.isNull("a"));
            assertEquals(0, r.getInt("p"));
            assertEquals(16, r.size());
        }
    }

    @Test
    void sendsParametersOfEveryJavaType() throws Exception {
        try (PgClient c = connect()) {
            UUID uuid = UUID.randomUUID();
            Instant now = Instant.parse("2024-05-06T07:08:09.123456Z");
            PgRow r = Pg.get(c.query("SELECT $1::int2, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15",
                    (short) 7, 8, 9L, 1.25f, 2.5, new BigDecimal("3.50"), true, "text ü", new byte[]{1, 2, 3}, uuid,
                    LocalDate.of(2020, 1, 2), LocalTime.of(1, 2, 3), LocalDateTime.of(2020, 1, 2, 3, 4, 5), now, null)).first();
            assertEquals((short) 7, r.get(0));
            assertEquals(8, r.get(1));
            assertEquals(9L, r.get(2));
            assertEquals(1.25f, r.get(3));
            assertEquals(2.5, r.get(4));
            assertEquals(new BigDecimal("3.50"), r.get(5));
            assertEquals(true, r.get(6));
            assertEquals("text ü", r.get(7));
            assertArrayEquals(new byte[]{1, 2, 3}, r.getBytes(8));
            assertEquals(uuid, r.get(9));
            assertEquals(LocalDate.of(2020, 1, 2), r.get(10));
            assertEquals(LocalTime.of(1, 2, 3), r.get(11));
            assertEquals(LocalDateTime.of(2020, 1, 2, 3, 4, 5), r.get(12));
            assertEquals(now, ((OffsetDateTime) r.get(13)).toInstant());
            assertNull(r.get(14));
        }
    }

    @Test
    void aStringParameterTakesTheTypeTheSqlAsksFor() throws Exception {
        try (PgClient c = connect()) {
            assertEquals(43, Pg.get(c.query("SELECT $1::int + 1", "42")).first().getInt(0));
            assertEquals(1, Pg.get(c.query("SELECT 1 WHERE 42 = $1", "42")).size());
            assertEquals("x", Pg.get(c.query("SELECT $1", "x")).first().getString(0), "even with no context the server picks text");
        }
    }

    @Test
    void rejectsAParameterItCannotEncodeWithoutTouchingTheConnection() throws Exception {
        try (PgClient c = connect()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT $1", new Object()).get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            ExecutionException nul = assertThrows(ExecutionException.class, () -> c.query("SELECT 'a\0b'").get(5, TimeUnit.SECONDS));
            assertTrue(nul.getCause() instanceof IllegalArgumentException);
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    // ---- results ----

    @Test
    void reportsColumnsTagsAndUpdateCounts() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("t");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int PRIMARY KEY, name text)"));
            PgResult ins = Pg.get(c.query("INSERT INTO " + t + " VALUES ($1, $2), ($3, $4)", 1, "a", 2, "b"));
            assertEquals("INSERT 0 2", ins.tag());
            assertEquals(2, ins.updateCount());
            assertEquals(0, ins.size());
            PgResult sel = Pg.get(c.query("SELECT id, name FROM " + t + " ORDER BY id"));
            assertEquals(List.of("id", "name"), sel.columns().stream().map(PgColumn::name).toList());
            assertEquals(2, sel.size());
            assertEquals("b", sel.rows().get(1).getString("name"));
            assertEquals(2, sel.updateCount());
            PgResult upd = Pg.get(c.query("UPDATE " + t + " SET name = $1 WHERE id = $2", "z", 1));
            assertEquals(1, upd.updateCount());
            assertEquals(1, Pg.get(c.query("DELETE FROM " + t + " WHERE id = $1", 2)).updateCount());
            PgResult ret = Pg.get(c.query("INSERT INTO " + t + " VALUES (5, 'five') RETURNING id, name"));
            assertEquals("five", ret.first().getString("name"));
            assertEquals(-1, Pg.get(c.query("DROP TABLE " + t)).updateCount(), "DROP has no count");
        }
    }

    @Test
    void anEmptyQueryAndANoRowQuery() throws Exception {
        try (PgClient c = connect()) {
            PgResult empty = Pg.get(c.query(""));
            assertEquals("", empty.tag());
            assertEquals(0, empty.size());
            PgResult none = Pg.get(c.query("SELECT 1 WHERE false"));
            assertEquals(0, none.size());
            assertNull(none.first());
            assertEquals(1, none.columns().size());
        }
    }

    @Test
    void executeRunsSeveralStatementsAndReturnsEachResult() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("m");
            List<PgResult> r = Pg.get(c.execute("CREATE TEMP TABLE " + t + " (x int); INSERT INTO " + t + " VALUES (1),(2),(3); SELECT sum(x) s FROM " + t
                    + "; SELECT x FROM " + t + " ORDER BY x; ;"));
            assertEquals(4, r.size(), "the server drops the empty statement after the last semicolon");
            assertEquals("CREATE TABLE", r.get(0).tag());
            assertEquals(3, r.get(1).updateCount());
            assertEquals(6, r.get(2).first().getInt("s"));
            assertEquals(3, r.get(3).size());
            List<PgResult> blank = Pg.get(c.execute(""));
            assertEquals(1, blank.size());
            assertEquals("", blank.get(0).tag(), "an entirely empty query is answered with EmptyQueryResponse");
        }
    }

    @Test
    void aSimpleQueryThatFailsHalfWayFailsAsAWhole() throws Exception {
        try (PgClient c = connect()) {
            PgException e = failure(c.execute("SELECT 1; SELECT 1/0; SELECT 2"));
            assertEquals("22012", e.sqlState());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "the connection recovered");
        }
    }

    @Test
    void largeResultsAndLargeValues() throws Exception {
        try (PgClient c = connect()) {
            PgResult big = Pg.get(c.query("SELECT g, 'row ' || g FROM generate_series(1, 100000) g"));
            assertEquals(100000, big.size());
            assertEquals("row 100000", big.rows().get(99999).getString(1));
            String text = "ü".repeat(2_000_000);
            byte[] bytes = new byte[3_000_000];
            new java.util.Random(1).nextBytes(bytes);
            PgRow r = Pg.get(c.query("SELECT $1::text, $2::bytea", text, bytes)).first();
            assertEquals(text, r.getString(0));
            assertArrayEquals(bytes, r.getBytes(1));
        }
    }

    // ---- errors ----

    @Test
    void aFailingQueryReportsTheServersFieldsAndLeavesTheConnectionUsable() throws Exception {
        try (PgClient c = connect()) {
            PgException syntax = failure(c.query("SELEC 1"));
            assertEquals("42601", syntax.sqlState());
            assertEquals("ERROR", syntax.severity());
            assertTrue(syntax.fields().containsKey('P'), "the position of the error");
            assertFalse(syntax.retriable());
            String t = Pg.table("u");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int PRIMARY KEY)"));
            Pg.get(c.query("INSERT INTO " + t + " VALUES (1)"));
            PgException dup = failure(c.query("INSERT INTO " + t + " VALUES (1)"));
            assertEquals("23505", dup.sqlState());
            assertNotNull(dup.detail());
            assertEquals(1, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0));
        }
    }

    @Test
    void anErrorInTheMiddleOfAPipelineSpoilsOnlyThatQuery() throws Exception {
        try (PgClient c = connect()) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                fs.add(i % 3 == 1 ? c.query("SELECT 1/($1::int - $1::int)", i) : c.query("SELECT $1::int", i));
            }
            for (int i = 0; i < 300; i++) {
                if (i % 3 == 1) {
                    assertEquals("22012", failure(fs.get(i)).sqlState(), "query " + i);
                } else {
                    assertEquals(i, Pg.get(fs.get(i)).first().getInt(0), "query " + i);
                }
            }
        }
    }

    @Test
    void aStatementThatFailedToParseIsPreparedAgainOnTheNextCall() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("late");
            assertEquals("42P01", failure(c.query("SELECT * FROM " + t)).sqlState());
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (x int)"));
            assertEquals(0, Pg.get(c.query("SELECT * FROM " + t)).size(), "the failure was not cached");
        }
    }

    // ---- pipelining, threads ----

    @Test
    void thousandsOfQueriesInOneBurstAnswerInOrder() throws Exception {
        try (PgClient c = connect()) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 20_000; i++) {
                fs.add(c.query("SELECT $1::int", i));
            }
            for (int i = 0; i < fs.size(); i++) {
                assertEquals(i, Pg.get(fs.get(i)).first().getInt(0));
            }
            assertEquals(0, c.inflight());
        }
    }

    @Test
    void manyThreadsShareOneConnection() throws Exception {
        try (PgClient c = connect()) {
            List<Thread> threads = new ArrayList<>();
            AtomicReference<Throwable> error = new AtomicReference<>();
            for (int t = 0; t < 32; t++) {
                int id = t;
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                        for (int i = 0; i < 200; i++) {
                            PgRow r = c.query("SELECT $1::int, $2::text", id * 1000 + i, "t" + id).get(10, TimeUnit.SECONDS).first();
                            assertEquals(id * 1000 + i, r.getInt(0));
                            assertEquals("t" + id, r.getString(1));
                        }
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
            assertNull(error.get(), String.valueOf(error.get()));
        }
    }

    @Test
    void maxPendingRejectsTheOverflowAtOnceAndRetriably() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withLimits(5, 1 << 20)))) {
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                fs.add(c.query("SELECT pg_sleep(0.02)"));
            }
            int rejected = 0;
            for (var f : fs) {
                try {
                    f.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    assertTrue(((PgException) e.getCause()).retriable());
                    rejected++;
                }
            }
            assertTrue(rejected > 0, "some were over the limit");
        }
    }

    // ---- statement cache ----

    private static List<String> prepared(PgClient c) throws Exception {
        List<String> names = new ArrayList<>();
        for (PgRow r : Pg.get(c.query("SELECT statement FROM pg_prepared_statements ORDER BY prepare_time, statement")).rows()) {
            names.add(r.getString(0));
        }
        return names;
    }

    @Test
    void aStatementIsPreparedOnceAndReused() throws Exception {
        try (PgClient c = connect()) {
            for (int i = 0; i < 50; i++) {
                assertEquals(i, Pg.get(c.query("SELECT $1::int AS reused", i)).first().getInt("reused"));
            }
            assertEquals(1, prepared(c).stream().filter(s -> s.contains("AS reused")).count());
        }
    }

    @Test
    void theCacheEvictsTheLeastRecentlyUsedAndClosesItOnTheServer() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withStatementCacheSize(3)))) {
            for (int i = 0; i < 10; i++) {
                Pg.get(c.query("SELECT " + i + " AS distinct_" + i));
            }
            List<String> now = prepared(c);       // includes the query that asks
            assertEquals(3, now.size(), now.toString());
            assertTrue(now.stream().noneMatch(s -> s.contains("distinct_0")), "the oldest is gone");
            assertEquals(5, Pg.get(c.query("SELECT 5 AS distinct_5")).first().getInt(0), "an evicted statement is simply prepared again");
        }
    }

    @Test
    void withoutACacheEveryQueryUsesTheUnnamedStatement() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withStatementCacheSize(0)))) {
            for (int i = 0; i < 20; i++) {
                assertEquals(i, Pg.get(c.query("SELECT $1::int", i)).first().getInt(0));
            }
            assertEquals(0, Pg.get(c.query("SELECT count(*) FROM pg_prepared_statements")).first().getInt(0));
            assertEquals(List.of("id"), Pg.get(c.query("SELECT 1 AS id")).columns().stream().map(PgColumn::name).toList());
        }
    }

    @Test
    void sameSqlWithDifferentParameterTypesGetsItsOwnStatement() throws Exception {
        try (PgClient c = connect()) {
            assertEquals("5", Pg.get(c.query("SELECT $1 AS poly", 5)).first().getString(0));
            assertEquals("5", Pg.get(c.query("SELECT $1 AS poly", 5L)).first().getString(0));
            assertEquals("t", Pg.get(c.query("SELECT $1 AS poly", true)).first().getString(0));
        }
    }

    // ---- session state, notices, notifications ----

    @Test
    void transactionStatusFollowsTheServer() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("BEGIN"));
            assertEquals('T', c.transactionStatus());
            failure(c.query("SELECT 1/0"));
            assertEquals('E', c.transactionStatus());
            Pg.get(c.execute("ROLLBACK"));
            assertEquals('I', c.transactionStatus());
        }
    }

    @Test
    void noticesGoToTheListener() throws Exception {
        List<Map<Character, String>> notices = new CopyOnWriteArrayList<>();
        PgListener l = new PgListener() {
            @Override
            public void onNotice(Map<Character, String> fields) {
                notices.add(fields);
            }
        };
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config(), l))) {
            Pg.get(c.execute("DO $$ BEGIN RAISE NOTICE 'hello %', 42; END $$"));
            assertEquals(1, notices.size());
            assertEquals("hello 42", notices.get(0).get('M'));
            assertEquals("NOTICE", notices.get(0).get('S'));
        }
    }

    @Test
    void listenNotifyDeliversNotifications() throws Exception {
        List<String> got = new CopyOnWriteArrayList<>();
        PgListener l = new PgListener() {
            @Override
            public void onNotification(int pid, String channel, String payload) {
                got.add(channel + ":" + payload + ":" + (pid > 0));
            }
        };
        try (PgClient listener = Pg.get(PgClient.connect(group, Pg.config(), l)); PgClient sender = connect()) {
            Pg.get(listener.execute("LISTEN hydra_chan"));
            Pg.get(sender.query("SELECT pg_notify('hydra_chan', $1)", "ping"));
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (got.isEmpty() && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            assertEquals(List.of("hydra_chan:ping:true"), got);
        }
    }

    // ---- COPY, lifecycle ----

    @Test
    void copyIsRefusedAndTheConnectionStaysUsable() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cp");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (x int)"));
            assertTrue(failure(c.execute("COPY " + t + " FROM STDIN")).getMessage().contains("COPY"));
            assertTrue(failure(c.execute("COPY (SELECT 1) TO STDOUT")).getMessage().contains("COPY"));
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void whenTheServerKillsTheConnectionPendingQueriesFailAndTheListenerHearsOfIt() throws Exception {
        AtomicReference<Throwable> closedWith = new AtomicReference<>();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        PgListener l = new PgListener() {
            @Override
            public void onClose(Throwable cause) {
                closedWith.set(cause);
                closed.complete(null);
            }
        };
        try (PgClient victim = Pg.get(PgClient.connect(group, Pg.config(), l)); PgClient killer = connect()) {
            int pid = victim.backendPid();
            CompletableFuture<PgResult> slow = victim.query("SELECT pg_sleep(30)");
            Thread.sleep(200);
            Pg.get(killer.query("SELECT pg_terminate_backend($1)", pid));
            PgException e = failure(slow);
            assertTrue(e.getMessage().contains("57P01") || e.getMessage().contains("terminat") || e.getMessage().contains("lost"), e.getMessage());
            closed.get(5, TimeUnit.SECONDS);
            assertNotNull(closedWith.get());
            assertFalse(victim.isOpen());
            assertFalse(failure(victim.query("SELECT 1")).getMessage().isEmpty());
        }
    }

    @Test
    void closingFailsWhatIsWaitingAndInformsTheListenerWithoutACause() throws Exception {
        AtomicReference<Throwable> closedWith = new AtomicReference<>(new RuntimeException("not called"));
        CompletableFuture<Void> closed = new CompletableFuture<>();
        PgListener l = new PgListener() {
            @Override
            public void onClose(Throwable cause) {
                closedWith.set(cause);
                closed.complete(null);
            }
        };
        PgClient c = Pg.get(PgClient.connect(group, Pg.config(), l));
        CompletableFuture<PgResult> slow = c.query("SELECT pg_sleep(30)");
        Thread.sleep(100);
        Pg.get(c.disconnect());
        assertTrue(failure(slow).getMessage().contains("closed"));
        closed.get(5, TimeUnit.SECONDS);
        assertNull(closedWith.get());
        assertFalse(c.isOpen());
        assertTrue(failure(c.query("SELECT 1")).getMessage().contains("closed"));
    }

    @Test
    void duplicateIsAnotherSession() throws Exception {
        try (PgClient a = connect(); PgClient b = Pg.get(a.duplicate())) {
            assertTrue(a.backendPid() != b.backendPid());
        }
    }
}
