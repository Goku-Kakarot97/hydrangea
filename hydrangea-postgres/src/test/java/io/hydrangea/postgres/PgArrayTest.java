package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Arrays in both directions, against a real server. */
class PgArrayTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(1);
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

    @Test
    void readsArrayColumnsOfTheCommonTypes() throws Exception {
        try (PgClient c = connect()) {
            PgRow r = Pg.get(c.query("SELECT ARRAY[1,2,3]::int[] a, ARRAY[10000000000, NULL]::bigint[] b, ARRAY['x y', 'q\"r', 'NULL', '', 'c,d']::text[] c, ARRAY[true, false] d,"
                    + " ARRAY[1.5, 2]::numeric[] e, ARRAY[ARRAY[1,2],ARRAY[3,4]] f, '{}'::int[] g, NULL::int[] h, ARRAY['2020-01-02']::date[] i,"
                    + " ARRAY['2020-01-02 03:04:05+00']::timestamptz[] j, ARRAY['\\xcafe'::bytea] k, ARRAY['{\"a\": 1}']::jsonb[] l,"
                    + " ARRAY['123e4567-e89b-12d3-a456-426614174000']::uuid[] m, ARRAY[1.25, 2.5]::float8[] n, ARRAY['a','b']::varchar[] o")).first();
            assertEquals(List.of(1, 2, 3), r.get("a"));
            assertEquals(Arrays.asList(10_000_000_000L, null), r.getList("b"));
            assertEquals(List.of("x y", "q\"r", "NULL", "", "c,d"), r.getList("c", String.class));
            assertEquals(List.of(true, false), r.get("d"));
            assertEquals(List.of(new BigDecimal("1.5"), new BigDecimal("2")), r.get("e"));
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), r.get("f"));
            assertEquals(List.of(), r.get("g"));
            assertNull(r.get("h"));
            assertNull(r.getList("h"));
            assertEquals(List.of(LocalDate.of(2020, 1, 2)), r.get("i"));
            assertEquals(Instant.parse("2020-01-02T03:04:05Z"), ((java.time.OffsetDateTime) r.getList("j").get(0)).toInstant());
            assertArrayEquals(new byte[]{(byte) 0xca, (byte) 0xfe}, (byte[]) r.getList("k").get(0));
            assertEquals(List.of("{\"a\": 1}"), r.get("l"));
            assertEquals(List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), r.get("m"));
            assertEquals(List.of(1.25, 2.5), r.get("n"));
            assertEquals(List.of("a", "b"), r.get("o"));
            assertEquals("{1,2,3}", r.getString("a"), "the text as the server wrote it is still there");
            assertThrows(IllegalStateException.class, () -> Pg.get(c.query("SELECT 1 AS x")).first().getList("x"));
        }
    }

    @Test
    void sendsListsAndArraysAsParameters() throws Exception {
        try (PgClient c = connect()) {
            PgRow r = Pg.get(c.query("SELECT $1::int[] AS ints, $2::bigint[] AS longs, $3::text[] AS strs, $4::boolean[] AS flags, $5::uuid[] AS ids, $6::date[] AS days,"
                            + " $7::numeric[] AS nums, $8::int[] AS nested, $9::bytea[] AS blobs, $10::timestamptz[] AS ats, $11::int[] AS empty, $12::text[] AS nulls",
                    List.of(1, 2, 3), new long[]{4, 5}, new String[]{"a", "b c", "d\"e", "f\\g", "", "NULL", "h,i", "{j}", "ünï"}, new boolean[]{true, false},
                    List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), List.of(LocalDate.of(2020, 1, 2)), List.of(new BigDecimal("1.50")),
                    List.of(List.of(1, 2), List.of(3, 4)), List.of(new byte[]{(byte) 0xca, (byte) 0xfe}), List.of(Instant.parse("2020-01-02T03:04:05Z")),
                    List.of(), Arrays.asList("x", null))).first();
            assertEquals(List.of(1, 2, 3), r.get("ints"));
            assertEquals(List.of(4L, 5L), r.get("longs"));
            assertEquals(List.of("a", "b c", "d\"e", "f\\g", "", "NULL", "h,i", "{j}", "ünï"), r.get("strs"));
            assertEquals(List.of(true, false), r.get("flags"));
            assertEquals(List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), r.get("ids"));
            assertEquals(List.of(LocalDate.of(2020, 1, 2)), r.get("days"));
            assertEquals(List.of(new BigDecimal("1.50")), r.get("nums"));
            assertEquals(List.of(List.of(1, 2), List.of(3, 4)), r.get("nested"));
            assertArrayEquals(new byte[]{(byte) 0xca, (byte) 0xfe}, (byte[]) r.getList("blobs").get(0));
            assertEquals(Instant.parse("2020-01-02T03:04:05Z"), ((java.time.OffsetDateTime) r.getList("ats").get(0)).toInstant());
            assertEquals(List.of(), r.get("empty"));
            assertEquals(Arrays.asList("x", null), r.get("nulls"));
        }
    }

    @Test
    void anArrayParameterWorksWithAnyAndInListQueries() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("ar");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, name text, tags text[])"));
            Pg.get(c.query("INSERT INTO " + t + " SELECT g, 'n' || g, ARRAY['t' || g, 'all'] FROM generate_series(1, 10) g"));
            // the type of $1 comes from the column: a List of Integer, a List of String and a String[] all work
            assertEquals(3, Pg.get(c.query("SELECT * FROM " + t + " WHERE id = ANY($1)", List.of(2, 4, 6))).size());
            assertEquals(2, Pg.get(c.query("SELECT * FROM " + t + " WHERE name = ANY($1)", (Object) new String[]{"n1", "n9"})).size());
            assertEquals(2, Pg.get(c.query("SELECT * FROM " + t + " WHERE id = ANY($1)", List.of("3", "5"))).size(), "strings inferred as int[] from the column");
            assertEquals(0, Pg.get(c.query("SELECT * FROM " + t + " WHERE id = ANY($1)", List.of())).size(), "an empty list matches nothing");
            assertEquals(10, Pg.get(c.query("SELECT * FROM " + t + " WHERE $1::text = ANY(tags)", "all")).size());
            // an array in a column, written and read back
            Pg.get(c.query("INSERT INTO " + t + " VALUES ($1, $2, $3)", 11, "n11", List.of("a b", "c,d", "e\"f")));
            assertEquals(List.of("a b", "c,d", "e\"f"), Pg.get(c.query("SELECT tags FROM " + t + " WHERE id = $1", 11)).first().getList("tags", String.class));
            // in a batch
            long[] counts = Pg.get(c.executeBatch("INSERT INTO " + t + " VALUES ($1, $2, $3)", List.of(new Object[]{20, "x", List.of("p")}, new Object[]{21, "y", List.of("q", "r")})));
            assertArrayEquals(new long[]{1, 1}, counts);
            assertEquals(List.of("q", "r"), Pg.get(c.query("SELECT tags FROM " + t + " WHERE id = 21")).first().getList(0, String.class));
        }
    }

    @Test
    void aMultiDimensionalArrayThatIsNotRectangularIsRefusedByTheServer() throws Exception {
        try (PgClient c = connect()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT $1::int[]", List.of(List.of(1, 2), List.of(3))).get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof PgException);
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void arraysOfEveryElementTypeSurviveARoundTripThroughATemporaryTable() throws Exception {
        try (PgClient c = connect()) {
            String t = Pg.table("rt");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (i int[], l bigint[], s text[], b boolean[], d date[], ts timestamp[], u uuid[], n numeric[], f float8[], y bytea[])"));
            Pg.get(c.query("INSERT INTO " + t + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", List.of(1, 2), List.of(3L), List.of("s"), List.of(true), List.of(LocalDate.of(2024, 2, 29)),
                    List.of(LocalDateTime.of(2024, 2, 29, 12, 30, 45)), List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), List.of(new BigDecimal("0.1")), List.of(0.5), List.of(new byte[]{9})));
            PgRow r = Pg.get(c.query("SELECT * FROM " + t)).first();
            assertEquals(List.of(1, 2), r.get("i"));
            assertEquals(List.of(3L), r.get("l"));
            assertEquals(List.of("s"), r.get("s"));
            assertEquals(List.of(true), r.get("b"));
            assertEquals(List.of(LocalDate.of(2024, 2, 29)), r.get("d"));
            assertEquals(List.of(LocalDateTime.of(2024, 2, 29, 12, 30, 45)), r.get("ts"));
            assertEquals(List.of(new BigDecimal("0.1")), r.get("n"));
            assertEquals(List.of(0.5), r.get("f"));
            assertArrayEquals(new byte[]{9}, (byte[]) r.getList("y").get(0));
        }
    }
}
