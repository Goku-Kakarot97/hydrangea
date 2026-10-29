package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Calling functions and procedures. They are created in {@code pg_temp}, which exists for the session only, so nothing is left behind. */
class PgRoutineTest {

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
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(Pg.REMOTE ? 120 : 15, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    @Test
    void callsAFunctionWithParameters() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE FUNCTION pg_temp.add(a int, b int) RETURNS int LANGUAGE sql AS 'SELECT a + b'"));
            assertEquals(5, Pg.get(c.query("SELECT pg_temp.add($1, $2)", 2, 3)).first().getInt(0));
            assertEquals(7, Pg.get(c.query("SELECT pg_temp.add($1, $2) AS total", 3, 4)).first().getInt("total"));
            assertEquals(9, Pg.get(c.query("SELECT pg_temp.add($1::int, $2::int)", "4", "5")).first().getInt(0), "text parameters with casts");
        }
    }

    @Test
    void aFunctionThatReturnsATableOrASet() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE FUNCTION pg_temp.pairs(n int) RETURNS TABLE (i int, label text) LANGUAGE sql AS "
                    + "'SELECT g, ''item '' || g FROM generate_series(1, n) g'"));
            PgResult r = Pg.get(c.query("SELECT * FROM pg_temp.pairs($1)", 3));
            assertEquals(List.of("i", "label"), r.columns().stream().map(PgColumn::name).toList());
            assertEquals(3, r.size());
            assertEquals("item 3", r.rows().get(2).getString("label"));
            Pg.get(c.execute("CREATE FUNCTION pg_temp.nums(n int) RETURNS SETOF int LANGUAGE sql AS 'SELECT generate_series(1, n)'"));
            assertEquals(4, Pg.get(c.query("SELECT pg_temp.nums($1)", 4)).size());
        }
    }

    @Test
    void aFunctionWithOutParametersAndAVoidFunction() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE FUNCTION pg_temp.divmod(a int, b int, OUT q int, OUT r int) LANGUAGE sql AS 'SELECT a / b, a % b'"));
            PgRow row = Pg.get(c.query("SELECT * FROM pg_temp.divmod($1, $2)", 17, 5)).first();
            assertEquals(3, row.getInt("q"));
            assertEquals(2, row.getInt("r"));
            Pg.get(c.execute("CREATE FUNCTION pg_temp.noop() RETURNS void LANGUAGE sql AS ''"));
            assertEquals(1, Pg.get(c.query("SELECT pg_temp.noop()")).size(), "a void function answers one row with one empty column");
            assertEquals("SELECT 1", Pg.get(c.query("SELECT pg_temp.noop()")).tag());
        }
    }

    @Test
    void callsAProcedureAndReadsItsOutAndInoutValues() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE PROCEDURE pg_temp.double_it(INOUT x int) LANGUAGE plpgsql AS $$ BEGIN x := x * 2; END $$"));
            PgResult r = Pg.get(c.query("CALL pg_temp.double_it($1::int)", 21));
            assertEquals("CALL", r.tag());
            assertEquals(42, r.first().getInt("x"), "the INOUT value comes back as the one row of the CALL");

            Pg.get(c.execute("CREATE PROCEDURE pg_temp.stats(IN a int, IN b int, OUT total int, OUT product int) LANGUAGE plpgsql AS "
                    + "$$ BEGIN total := a + b; product := a * b; END $$"));
            PgRow s = Pg.get(c.query("CALL pg_temp.stats($1::int, $2::int, NULL, NULL)", 6, 7)).first();
            assertEquals(13, s.getInt("total"));
            assertEquals(42, s.getInt("product"));
        }
    }

    @Test
    void aProcedureWithoutOutValuesAnswersNoRows() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("rt");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            Pg.get(c.execute("CREATE PROCEDURE pg_temp.add_rows(k int) LANGUAGE sql AS $$ INSERT INTO pg_temp." + t + " SELECT generate_series(1, k) $$"));
            PgResult r = Pg.get(c.query("CALL pg_temp.add_rows($1)", 5));
            assertEquals("CALL", r.tag());
            assertEquals(0, r.size());
            assertEquals(-1, r.updateCount(), "a CALL has no row count");
            assertEquals(5, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0));
        }
    }

    @Test
    void aProcedureThatCommitsInsideItself() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("rc");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (n int)"));
            Pg.get(c.execute("CREATE PROCEDURE pg_temp.batches() LANGUAGE plpgsql AS $$ BEGIN "
                    + "FOR i IN 1..3 LOOP INSERT INTO pg_temp." + t + " VALUES (i); COMMIT; END LOOP; END $$"));
            assertEquals("CALL", Pg.get(c.query("CALL pg_temp.batches()")).tag());
            assertEquals(3, Pg.get(c.query("SELECT count(*) FROM " + t)).first().getInt(0));
            assertEquals('I', c.transactionStatus());
        }
    }

    @Test
    void anExceptionRaisedInsideCarriesItsCodeMessageDetailAndHint() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE PROCEDURE pg_temp.refuse(who text) LANGUAGE plpgsql AS $$ BEGIN "
                    + "RAISE EXCEPTION 'no entry for %', who USING ERRCODE = 'U0001', DETAIL = 'the list is closed', HINT = 'try tomorrow'; END $$"));
            PgException e = failure(c.query("CALL pg_temp.refuse($1)", "Ada"));
            assertEquals("U0001", e.sqlState());
            assertEquals("no entry for Ada", e.fields().get('M'));
            assertEquals("the list is closed", e.detail());
            assertEquals("try tomorrow", e.hint());
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0), "the connection is fine");
        }
    }

    @Test
    void noticesRaisedInsideAFunctionReachTheListener() throws Exception {
        List<String> notices = new java.util.concurrent.CopyOnWriteArrayList<>();
        PgListener l = new PgListener() {
            @Override
            public void onNotice(java.util.Map<Character, String> fields) {
                notices.add(fields.get('M'));
            }
        };
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config(), l))) {
            Pg.get(c.execute("CREATE FUNCTION pg_temp.chatty(n int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'got %', n; RETURN n; END $$"));
            Pg.get(c.query("SELECT pg_temp.chatty($1)", 7));
            assertEquals(List.of("got 7"), notices);
        }
    }

    @Test
    void aFunctionWhoseResultColumnsChangeIsPlannedAgain() throws Exception {
        try (PgClient c = connect()) {
            Pg.get(c.execute("CREATE FUNCTION pg_temp.shape() RETURNS TABLE (a int) LANGUAGE sql AS 'SELECT 1'"));
            assertEquals(1, Pg.get(c.query("SELECT * FROM pg_temp.shape()")).columns().size());
            Pg.get(c.execute("DROP FUNCTION pg_temp.shape(); CREATE FUNCTION pg_temp.shape() RETURNS TABLE (a int, b int) LANGUAGE sql AS 'SELECT 1, 2'"));
            assertEquals(2, Pg.get(c.query("SELECT * FROM pg_temp.shape()")).columns().size(), "the cached statement is replaced, not an error");
        }
    }

    @Test
    void functionsAndProceduresInATransactionOfThePool() throws Exception {
        Pg.notOnRemote();
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 2))) {
            String t = Pg.table("rp");
            Pg.get(pool.execute("CREATE TABLE " + t + " (n int); CREATE FUNCTION " + t + "_ins(k int) RETURNS int LANGUAGE sql AS 'INSERT INTO " + t + " VALUES (k) RETURNING n'"));
            try {
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("SELECT " + t + "_ins($1)", 1)
                        .thenCompose(x -> db.query("SELECT 1/0"))).get(15, TimeUnit.SECONDS));
                assertEquals("22012", ((PgException) e.getCause()).sqlState());
                assertEquals(0, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0), "what the function did rolled back with the transaction");
                Pg.get(pool.<PgResult>transaction(db -> db.query("SELECT " + t + "_ins($1)", 2)));
                assertEquals(1, Pg.get(pool.query("SELECT count(*) FROM " + t)).first().getInt(0));
            } finally {
                Pg.get(pool.execute("DROP FUNCTION " + t + "_ins(int); DROP TABLE " + t));
            }
        }
    }
}
