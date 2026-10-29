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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A table changes under a cached prepared statement: "cached plan must not change result type". */
class PgStalePlanTest {

    private EventLoopGroup group;
    private final java.util.Map<String, java.util.function.DoubleSupplier> meters = new java.util.HashMap<>();

    @BeforeEach
    void setUp() {
        Pg.notOnRemote();
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(2);
        group.metrics().bindTo(new io.hydrangea.MetricsSink() {
            public void counterFunction(String name, String unit, java.util.function.DoubleSupplier total, String... tags) {
                meters.put(name, total);
            }

            public void gauge(String name, String unit, java.util.function.DoubleSupplier value, String... tags) {
                meters.put(name, value);
            }

            public Counter counter(String name, String unit, String... tags) {
                return NOOP.counter(name, unit, tags);
            }

            public Timer timer(String name, String... tags) {
                return NOOP.timer(name, tags);
            }
        });
    }

    private double replanned() {
        return meters.get("pg.statements.replanned").getAsDouble();
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    @Test
    void aCachedSelectStarSurvivesAnAlterTable() throws Exception {
        String t = Pg.table("sp");
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config())); PgClient other = Pg.get(PgClient.connect(group, Pg.config()))) {
            Pg.get(c.execute("CREATE TABLE " + t + " (id int, v int); INSERT INTO " + t + " VALUES (1, 10)"));
            try {
                assertEquals(2, Pg.get(c.query("SELECT * FROM " + t)).columns().size());
                assertEquals(2, Pg.get(c.query("SELECT * FROM " + t)).columns().size(), "now cached");

                Pg.get(other.execute("ALTER TABLE " + t + " ADD COLUMN w text DEFAULT 'x'"));
                PgResult after = Pg.get(c.query("SELECT * FROM " + t));       // the server refuses the old plan; the client prepares again and runs it
                assertEquals(3, after.columns().size());
                assertEquals("x", after.first().getString("w"));
                assertEquals(3, Pg.get(c.query("SELECT * FROM " + t)).columns().size(), "and the new statement is the cached one");

                Pg.get(other.execute("ALTER TABLE " + t + " ALTER COLUMN v TYPE bigint"));
                assertEquals(10L, Pg.get(c.query("SELECT * FROM " + t)).first().getLong("v"));
                assertEquals(2.0, replanned(), "the server really refused the old plan twice, and the client dealt with it");
            } finally {
                Pg.get(c.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void aBurstOfStaleQueriesAllRunAgain() throws Exception {
        String t = Pg.table("sb");
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config())); PgClient other = Pg.get(PgClient.connect(group, Pg.config()))) {
            Pg.get(c.execute("CREATE TABLE " + t + " (id int, v int); INSERT INTO " + t + " VALUES (1, 10), (2, 20)"));
            try {
                Pg.get(c.query("SELECT * FROM " + t + " WHERE id = $1", 1));
                Pg.get(other.execute("ALTER TABLE " + t + " DROP COLUMN v"));
                List<CompletableFuture<PgResult>> fs = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    fs.add(c.query("SELECT * FROM " + t + " WHERE id = $1", 1 + i % 2));
                }
                for (int i = 0; i < fs.size(); i++) {
                    PgResult r = Pg.get(fs.get(i));
                    assertEquals(1, r.columns().size(), "query " + i);
                    assertEquals(1 + i % 2, r.first().getInt("id"));
                }
                assertEquals(1.0, replanned(), "one stale statement, one replan: the other 99 found the new statement in the cache");
            } finally {
                Pg.get(c.execute("DROP TABLE " + t));
            }
        }
    }

    @Test
    void insideATransactionTheErrorIsReportedAndTheNextAttemptWorks() throws Exception {
        String t = Pg.table("st");
        try (PgPool pool = Pg.get(PgPool.create(group, Pg.config(), 1)); PgClient other = Pg.get(PgClient.connect(group, Pg.config()))) {
            Pg.get(pool.execute("CREATE TABLE " + t + " (id int)"));
            try {
                Pg.get(pool.query("SELECT * FROM " + t));          // cached on the pool's one connection
                Pg.get(other.execute("ALTER TABLE " + t + " ADD COLUMN w int"));
                ExecutionException e = assertThrows(ExecutionException.class, () -> pool.transaction(db -> db.query("SELECT * FROM " + t)).get(15, TimeUnit.SECONDS));
                assertEquals("0A000", ((PgException) e.getCause()).sqlState(), "a transaction cannot be retried by the client: it is aborted");
                assertEquals(2, Pg.get(pool.transaction(db -> db.query("SELECT * FROM " + t))).columns().size(), "the stale statement was dropped, so this one works");
            } finally {
                Pg.get(pool.execute("DROP TABLE " + t));
            }
        }
    }
}
