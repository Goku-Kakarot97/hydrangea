package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PgCopyTest {

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

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static long count(PgClient c, String table) throws Exception {
        return Pg.get(c.query("SELECT count(*) FROM " + table)).first().getLong(0);
    }

    // ---- COPY FROM STDIN ----

    @Test
    void copiesInTextCsvAndUnicode() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("ci");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, name text)"));
            assertEquals(2, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", b("1\ta\n2\tb\n"))));
            assertEquals(2, Pg.get(c.copyIn("COPY " + t + " FROM STDIN WITH (FORMAT csv)", b("3,\"x,y\"\n4,\"héllo \"\"wörld\"\"\"\n"))));
            PgResult r = Pg.get(c.query("SELECT id, name FROM " + t + " ORDER BY id"));
            assertEquals(4, r.size());
            assertEquals("x,y", r.rows().get(2).getString("name"));
            assertEquals("héllo \"wörld\"", r.rows().get(3).getString("name"));
        }
    }

    @Test
    void chunksNeedNotEndOnRowBoundaries() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cb");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, name text)"));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 20_000; i++) {
                sb.append(i).append("\tname ").append(i).append('\n');
            }
            byte[] all = b(sb.toString());
            Random rnd = new Random(7);
            List<byte[]> chunks = new ArrayList<>();
            for (int p = 0; p < all.length; ) {
                int n = Math.min(all.length - p, 1 + rnd.nextInt(3000));
                chunks.add(java.util.Arrays.copyOfRange(all, p, p + n));
                p += n;
            }
            assertEquals(20_000, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", chunks)));
            assertEquals(20_000, count(c, t));
            assertEquals("name 19999", Pg.get(c.query("SELECT name FROM " + t + " WHERE id = 19999")).first().getString(0));
        }
    }

    @Test
    void aLazySourceOfHalfAMillionRowsIsPulledAsTheConnectionTakesIt() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cl");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, name text)"));
            AtomicLong produced = new AtomicLong();
            Iterable<byte[]> source = () -> new Iterator<>() {
                int next = 0;

                public boolean hasNext() {
                    return next < 500_000;
                }

                public byte[] next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 1000 && next < 500_000; i++, next++) {
                        sb.append(next).append("\trow ").append(next).append('\n');
                    }
                    produced.addAndGet(1000);
                    return b(sb.toString());
                }
            };
            assertEquals(500_000, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", source)));
            assertEquals(500_000, produced.get());
            assertEquals(500_000, count(c, t));
        }
    }

    @Test
    void thePushApiTakesDataFromAnotherThread() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cp");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgCopyIn copy = Pg.get(c.copyIn("COPY " + t + " FROM STDIN"));
            Thread producer = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        copy.write(b(i + "\n")).get(10, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            producer.join();
            assertEquals(1000, Pg.get(copy.finish()));
            assertEquals(1000, count(c, t));
        }
    }

    @Test
    void badDataFailsWithTheServersErrorAndTheConnectionCarriesOn() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cx");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgException e = failure(c.copyIn("COPY " + t + " FROM STDIN", b("1\n2\nnot a number\n4\n")));
            assertEquals("22P02", e.sqlState());
            assertEquals(0, count(c, t), "a failed COPY copies nothing");
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            // a failure far into a big stream: the writes after it are refused or ignored, the end still comes
            List<byte[]> many = new ArrayList<>();
            many.add(b("zzz\n"));
            for (int i = 0; i < 2000; i++) {
                many.add(b("5\n".repeat(1000)));
            }
            assertEquals("22P02", failure(c.copyIn("COPY " + t + " FROM STDIN", many)).sqlState());
            assertEquals(2, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", b("8\n9\n"))), "and the next copy works");
        }
    }

    @Test
    void queriesSentDuringACopyWaitAndRunAfterItInOrder() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cw");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgCopyIn copy = Pg.get(c.copyIn("COPY " + t + " FROM STDIN"));
            CompletableFuture<PgResult> during = c.query("SELECT count(*) FROM " + t);
            CompletableFuture<PgResult> second = c.query("SELECT $1::int", 42);
            Thread.sleep(300);
            assertFalse(during.isDone(), "the connection belongs to the copy");
            Pg.get(copy.write(b("1\n2\n3\n")));
            assertFalse(during.isDone());
            assertEquals(3, Pg.get(copy.finish()));
            assertEquals(3, Pg.get(during).first().getInt(0), "it ran after the copy and saw its rows");
            assertEquals(42, Pg.get(second).first().getInt(0));
        }
    }

    @Test
    void anAbortedCopyCopiesNothing() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("ca");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgCopyIn copy = Pg.get(c.copyIn("COPY " + t + " FROM STDIN"));
            Pg.get(copy.write(b("1\n2\n")));
            PgException e = failure(copy.abort("changed my mind"));
            assertEquals("57014", e.sqlState());
            assertTrue(e.getMessage().contains("changed my mind"), e.getMessage());
            assertEquals(0, count(c, t));
        }
    }

    @Test
    void aSourceThatThrowsAbortsTheCopy() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("cs");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            Iterable<byte[]> source = () -> new Iterator<>() {
                int n = 0;

                public boolean hasNext() {
                    return true;
                }

                public byte[] next() {
                    if (n++ == 3) {
                        throw new IllegalStateException("disk gone");
                    }
                    return b("1\n");
                }
            };
            assertTrue(assertThrows(ExecutionException.class, () -> c.copyIn("COPY " + t + " FROM STDIN", source).get(20, TimeUnit.SECONDS)).getCause()
                    instanceof IllegalStateException);
            assertEquals(0, count(c, t));
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void aStatementThatIsNotACopyFromStdinFailsAndTheConnectionIsFree() throws Exception {
        try (PgClient c = connect()) {
            assertTrue(failure(c.copyIn("SELECT 1")).getMessage().contains("did not start a COPY"));
            assertEquals("42P01", failure(c.copyIn("COPY nosuchtable_" + Pg.table("x") + " FROM STDIN")).sqlState());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
            assertEquals(0, c.inflight());
        }
    }

    // ---- COPY TO STDOUT ----

    @Test
    void copiesOutIntoMemoryAndIntoASink() throws Exception {
        try (PgClient c = connect()) {
            byte[] all = Pg.get(c.copyOut("COPY (SELECT g, 'x' || g FROM generate_series(1, 5) g) TO STDOUT"));
            assertEquals("1\tx1\n2\tx2\n3\tx3\n4\tx4\n5\tx5\n", new String(all, StandardCharsets.UTF_8));
            List<String> rows = new ArrayList<>();
            long n = Pg.get(c.copyOut("COPY (SELECT g FROM generate_series(1, 3) g) TO STDOUT", chunk -> rows.add(new String(chunk, StandardCharsets.UTF_8))));
            assertEquals(3, n);
            assertEquals(List.of("1\n", "2\n", "3\n"), rows);
            assertEquals(0, Pg.get(c.copyOut("COPY (SELECT 1 WHERE false) TO STDOUT")).length);
        }
    }

    @Test
    void aMillionRowsOut() throws Exception {
        try (PgClient c = connect()) {
            AtomicLong bytes = new AtomicLong();
            long n = Pg.get(c.copyOut("COPY (SELECT g, 'row ' || g FROM generate_series(1, 1000000) g) TO STDOUT", chunk -> bytes.addAndGet(chunk.length)));
            assertEquals(1_000_000, n);
            assertTrue(bytes.get() > 10_000_000, "bytes: " + bytes.get());
        }
    }

    @Test
    void binaryFormatPassesThroughUntouched() throws Exception {
        try (PgClient c = connect()) {
            byte[] data = Pg.get(c.copyOut("COPY (SELECT 1 a, 'x' b) TO STDOUT WITH (FORMAT binary)"));
            assertArrayEquals(b("PGCOPY\n"), java.util.Arrays.copyOf(data, 7));
            String t = Pg.table("bin");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (a int, b text)"));
            assertEquals(1, Pg.get(c.copyIn("COPY " + t + " FROM STDIN WITH (FORMAT binary)", data)));
            assertEquals("x", Pg.get(c.query("SELECT b FROM " + t)).first().getString(0));
        }
    }

    @Test
    void aSinkThatThrowsFailsTheCopyAndLeavesTheConnectionInStep() throws Exception {
        try (PgClient c = connect()) {
            PgException e = failure(c.copyOut("COPY (SELECT g FROM generate_series(1, 1000) g) TO STDOUT", chunk -> {
                throw new IllegalStateException("sink broke");
            }));
            assertTrue(e.getMessage().contains("sink broke"), e.getMessage());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "the rest of the data was read and dropped");
        }
    }

    @Test
    void whatGoesInComesOut() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("rt");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, s text, b bytea)"));
            Random rnd = new Random(3);
            StringBuilder in = new StringBuilder();
            for (int i = 0; i < 2000; i++) {
                in.append(i).append('\t').append("s\\t").append(i).append(" ü").append('\t').append("\\\\x").append(Integer.toHexString(rnd.nextInt(1 << 20) + (1 << 20))).append('\n');
            }
            assertEquals(2000, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", b(in.toString()))));
            String out = new String(Pg.get(c.copyOut("COPY " + t + " TO STDOUT")), StandardCharsets.UTF_8);
            assertEquals(in.toString(), out);
        }
    }

    // ---- COPY and the rest ----

    @Test
    void aCopyInsideATransactionRollsBackWithIt() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 2))) {
            String t = Pg.table("ct");
            Pg.get(pool.execute("CREATE TABLE " + t + " (n int)"));
            try {
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.copyIn("COPY " + t + " FROM STDIN", b("1\n2\n3\n"))
                        .thenCompose(n -> db.query("SELECT 1/0"))).get(20, TimeUnit.SECONDS));
                assertEquals("22012", ((PgException) e.getCause()).sqlState());
                assertEquals(0, count(pool, t), "the copy was part of the transaction that rolled back");
                Long committed = Pg.get(pool.<Long>transaction(db -> db.copyIn("COPY " + t + " FROM STDIN", b("1\n2\n3\n"))));
                assertEquals(3L, committed.longValue());
                assertEquals(3, count(pool, t));
                assertEquals(5, Pg.get(pool.copyIn("COPY " + t + " FROM STDIN", b("7\n8\n9\n1\n2\n"))));
                assertEquals(8, Pg.get(pool.copyOut("COPY " + t + " TO STDOUT", chunk -> { })), "the pool's copyOut, and 3 + 5 rows");
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }

    private static long count(PgPool pool, String table) throws Exception {
        return Pg.get(pool.query("SELECT count(*) FROM " + table)).first().getLong(0);
    }

    @Test
    void theConnectionDyingDuringACopyFailsEverythingThatWaits() throws Exception {
        try (PgClient victim = connect(); PgClient killer = connect()) {
            String t = Pg.table("ck");
            Pg.get(victim.execute("CREATE TEMP TABLE " + t + " (n int)"));
            PgCopyIn copy = Pg.get(victim.copyIn("COPY " + t + " FROM STDIN"));
            CompletableFuture<PgResult> waiting = victim.query("SELECT 1");
            Pg.get(copy.write(b("1\n")));
            Pg.get(killer.query("SELECT pg_terminate_backend($1)", victim.backendPid()));
            assertTrue(failure(waiting).getMessage().length() > 0);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (victim.isOpen() && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            assertFalse(victim.isOpen());
            assertThrows(ExecutionException.class, () -> copy.write(b("2\n")).get(5, TimeUnit.SECONDS));
            assertTrue(failure(copy.finish()).getMessage().length() > 0);
        }
    }

    @Test
    void execThroughQueryIsStillRefused() throws Exception {
        try (PgClient c = connect()) {
            assertTrue(failure(c.execute("COPY (SELECT 1) TO STDOUT")).getMessage().contains("COPY"));
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }
}
